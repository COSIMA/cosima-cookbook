from datetime import datetime
import logging
import os
from pathlib import Path
import re
import subprocess

import cftime
from dask.distributed import as_completed
import netCDF4
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Index
from sqlalchemy import MetaData, Table, select, sql, exists

from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base

from . import netcdf_utils
from .database_utils import *

__DB_VERSION__ = 2
__DEFAULT_DB__ = '/g/data/hh5/tmp/cosima/database/access-om2.db'

Base = declarative_base()

class NCExperiment(Base):
    __tablename__ = 'experiments'
    # composite index since an experiment name may not be unique
    __table_args__ = (Index('ix_experiments_experiment_rootdir', 'experiment', 'root_dir', unique=True),)

    id = Column(Integer, primary_key=True)

    #: Experiment name
    experiment = Column(String, nullable=False)
    #: Root directory containing 'output???' directories
    root_dir = Column(String, nullable=False)
    #: Human-readable experiment description
    description = Column(String)

    #: Files in this experiment
    ncfiles = relationship('NCFile', back_populates='experiment')

class NCFile(Base):
    __tablename__ = 'ncfiles'
    __table_args__ = (Index('ix_ncfiles_experiment_ncfile', 'experiment_id', 'ncfile', unique=True),)

    id = Column(Integer, primary_key=True)

    #: When this file was indexed
    index_time = Column(DateTime)
    #: The file name
    ncfile = Column(String, index=True)
    #: Is the file actually present on the filesystem?
    present = Column(Boolean)
    #: The experiment to which the file belongs
    experiment_id = Column(Integer, ForeignKey('experiments.id'), nullable=False, index=True)
    experiment = relationship('NCExperiment', back_populates='ncfiles')
    #: CF timeunits attribute
    timeunits = Column(String)
    #: CF calendar attribute
    calendar = Column(String)
    #: Start time of data in the file
    time_start = Column(String)
    #: End time of data in the file
    time_end = Column(String)
    #: Temporal frequency of the file
    frequency = Column(String)

    #: variables in this file
    ncvars = relationship('NCVar', back_populates='ncfile')

    @property
    def ncfile_path(self):
        return Path(self.experiment.root_dir) / Path(self.ncfile)

class CFVariable(UniqueMixin, Base):
    __tablename__ = 'variables'
    __table_args__ = (Index('ix_variables_name_long_name', 'name', 'long_name', unique=True),)

    id = Column(Integer, primary_key=True)

    #: Attributes associated with the variable that should
    #: be stored in the database
    attributes = ['long_name', 'standard_name', 'units']

    #: The variable name
    name = Column(String, nullable=False, index=True)
    #: The variable long name (CF Conventions §3.2)
    long_name = Column(String)
    #: The variable long name (CF Conventions §3.3)
    standard_name = Column(String)
    #: The variable long name (CF Conventions §3.1)
    units = Column(String)

    #: Back-populate a list of ncvars that use this variable
    ncvars = relationship('NCVar', back_populates='variable')

    def __init__(self, name, long_name=None, standard_name=None, units=None):
        self.name = name
        self.long_name = long_name
        self.standard_name = standard_name
        self.units = units

    @classmethod
    def unique_hash(cls, name, long_name, *arg):
        return '{}_{}'.format(name, long_name)

    @classmethod
    def unique_filter(cls, query, name, long_name, *arg):
        return (query
                .filter(CFVariable.name == name)
                .filter(CFVariable.long_name == long_name))

class NCVar(Base):
    __tablename__ = 'ncvars'

    id = Column(Integer, primary_key=True)

    #: The ncfile to which this variable belongs
    ncfile_id = Column(Integer, ForeignKey('ncfiles.id'), nullable=False, index=True)
    ncfile = relationship('NCFile', back_populates='ncvars')
    #: The generic form of this variable (name and attributes)
    variable_id = Column(Integer, ForeignKey('variables.id'), nullable=False)
    variable = relationship('CFVariable', back_populates='ncvars', uselist=False)
    #: Proxy for the variable name
    varname = association_proxy('variable', 'name')
    #: Serialised tuple of variable dimensions
    dimensions = Column(String)
    #: Serialised tuple of chunking along each dimension
    chunking = Column(String)

def create_session(db=None, debug=False):
    """Create a session for the specified database file.

    If debug=True, the session will output raw SQL whenever it is executed on the database.
    """

    if db is None:
        db = os.getenv('COSIMA_COOKBOOK_DB', __DEFAULT_DB__)

    engine = create_engine('sqlite:///' + db, echo=debug)
    Base.metadata.create_all(engine)

    # if database version is 0, we've created it anew
    conn = engine.connect()
    ver = conn.execute('PRAGMA user_version').fetchone()[0]
    if ver == 0:
        # seems we can't use usual SQL parameter strings, so we'll just format the version in...
        conn.execute('PRAGMA user_version={}'.format(__DB_VERSION__))
    elif ver < __DB_VERSION__:
        raise Exception('Incompatible database versions, expected {}, got {}'.format(ver, __DB_VERSION__))
    conn.close()

    Session = sessionmaker(bind=engine)
    return Session()

def update_timeinfo(f, ncfile):
    """Extract time information from a single netCDF file: units, calendar,
    start time, end time, and frequency."""

    with netCDF4.Dataset(f, 'r') as ds:
        # we assume the record dimension corresponds to time
        time_dim = netcdf_utils.find_record_dimension(ds)
        if time_dim is None:
            return None

        time_var = ds.variables[time_dim]
        has_bounds = hasattr(time_var, 'bounds')

        if len(time_var) == 0:
            return None

        if hasattr(time_var, 'units'):
            ncfile.timeunits = time_var.units
        if hasattr(time_var, 'calendar'):
            ncfile.calendar = time_var.calendar

        if ncfile.timeunits is None or ncfile.calendar is None:
            # non CF-compliant file -- don't process further
            return

        # Helper function to get a date
        def todate(t):
            return cftime.num2date(t, time_var.units, calendar=time_var.calendar)

        if has_bounds:
            bounds_var = ds.variables[time_var.bounds]
            ncfile.time_start = todate(bounds_var[0,0])
            ncfile.time_end = todate(bounds_var[-1,1])
        else:
            ncfile.time_start = todate(time_var[0])
            ncfile.time_end = todate(time_var[-1])

        if len(time_var) > 1 or has_bounds:
            # calculate frequency -- I don't see any easy way to do this, so
            # it's somewhat heuristic
            #
            # using bounds_var gets us the averaging period instead of the
            # difference between the centre of averaging periods, which is easier
            # to work with
            if has_bounds:
                next_time = todate(bounds_var[0,1])
            else:
                next_time = todate(time_var[1])

            dt = next_time - ncfile.time_start
            if dt.days >= 365:
                years = round(dt.days / 365)
                ncfile.frequency = '{} yearly'.format(years)
            elif dt.days >= 28:
                months = round(dt.days / 30)
                ncfile.frequency = '{} monthly'.format(months)
            elif dt.days >= 1:
                ncfile.frequency = '{} daily'.format(dt.days)
            else:
                ncfile.frequency = '{} hourly'.format(dt.seconds // 3600)
        else:
            # single time value in this file and no averaging
            ncfile.frequency = 'static'

        # convert start/end times to timestamps
        # strftime doesn't like years with fewer than 4 digits (pads with spaces
        # instead of zeroes)...
        def zeropad(s):
            ss = s.lstrip()
            return (len(s)-len(ss))*'0' + ss

        ncfile.time_start = zeropad(ncfile.time_start.strftime('%Y-%m-%d %H:%M:%S'))
        ncfile.time_end = zeropad(ncfile.time_end.strftime('%Y-%m-%d %H:%M:%S'))

def index_file(ncfile_name, experiment):
    """Index a single netCDF file within an experiment by retrieving all variables, their dimensions
    and chunking.
    """

    # construct absolute path to file
    f = str(Path(experiment.root_dir) / ncfile_name)

    # try to index this file, and mark it 'present' if indexing succeeds
    ncfile = NCFile(index_time=datetime.now(),
                    ncfile=ncfile_name,
                    present=False,
                    experiment=experiment)
    try:
        with netCDF4.Dataset(f, 'r') as ds:
            for v in ds.variables.values():
                # create the generic cf variable structure
                cfvar = CFVariable(name=v.name)

                # check for other attributes
                for att in CFVariable.attributes:
                    if att in v.ncattrs():
                        setattr(cfvar, att, v.getncattr(att))

                # fill in the specifics for this file: dimensions and chunking
                ncvar = NCVar(variable=cfvar,
                              dimensions=str(v.dimensions),
                              chunking=str(v.chunking()))

                ncfile.ncvars.append(ncvar)

        ncfile.present = True
        update_timeinfo(f, ncfile)
    except FileNotFoundError:
        logging.info('Unable to find file: %s', f)
    except Exception as e:
        logging.error('Error indexing %s: %s', f, e)

    return ncfile

class IndexingError(Exception): pass

def index_experiment(experiment_dir, session=None, client=None, update=False):
    """Index all output files for a single experiment."""

    # find all netCDF files in the hierarchy below this directory
    files = []
    try:
        results = subprocess.check_output(['find', experiment_dir, '-name', '*.nc'])
        results = [s for s in results.decode('utf-8').split()]
        files.extend(results)
    except Exception as e:
        logging.error('Error occurred while finding output files: %s', e)

    expt_path = Path(experiment_dir)
    expt = NCExperiment(experiment=str(expt_path.name),
                        root_dir=str(expt_path.absolute()))

    # make all files relative to the experiment path
    files = [str(Path(f).relative_to(expt_path)) for f in files]

    if update:
        # prune file list to only new files
        # first construct a query for the current experiment
        q = (session
             .query(NCFile.ncfile)
             .filter(NCExperiment.experiment == expt.experiment)
             .filter(NCExperiment.root_dir == expt.root_dir))

        # we can't really leverage the database to do this query, so we'll
        # just have to iterate over files and see if they're in the database
        # already. ncfile is an indexed column, so it's not too bad
        files = [f for f in files if not q.filter(NCFile.ncfile == f).count()]

    results = []

    # index in parallel or serial, depending on whether we have a client
    if client is not None:
        futures = client.map(index_file, files, experiment=expt)
        results = client.gather(futures)
    else:
        results = [index_file(f, experiment=expt) for f in files]

    # update all variables to be unique
    for ncfile in results:
        for ncvar in ncfile.ncvars:
            v = ncvar.variable
            ncvar.variable = CFVariable.as_unique(session,
                                                  v.name, v.long_name,
                                                  v.standard_name, v.units)

    session.add_all(results)
    return len(results)

def build_index(directories, session, client=None, update=False):
    """Index all netcdf files contained within experiment directories.

    Requires a session for the database that's been created with the create_session() function.
    If client is not None, use a distributed client for processing files in parallel.
    May scan for only new entries to add to database with the update flag.

    Returns the number of files that were indexed.
    """

    if not isinstance(directories, list):
        directories = [directories]

    indexed = 0
    for directory in directories:
        indexed += index_experiment(directory, session, client, update)

    # if everything went smoothly, commit these changes to the database
    session.commit()
    return indexed
