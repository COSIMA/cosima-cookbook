from datetime import datetime
import logging
import os
from pathlib import Path
import re
import subprocess
from tqdm import tqdm
import warnings

import cftime
from dask.distributed import as_completed
import netCDF4
import yaml
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, Index
from sqlalchemy import MetaData, Table, select, sql, exists

from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property

from . import netcdf_utils
from .database_utils import *
from .date_utils import format_datetime

logging.captureWarnings(True)

__DB_VERSION__ = 3
__DEFAULT_DB__ = '/g/data/ik11/databases/cosima_master.db'

Base = declarative_base()

keyword_assoc_table = Table(
    'keyword_assoc', Base.metadata,
    Column('expt_id', Integer, ForeignKey('experiments.id')),
    Column('keyword_id', Integer, ForeignKey('keywords.id'))
)

class NCExperiment(Base):
    __tablename__ = 'experiments'
    # composite index since an experiment name may not be unique
    __table_args__ = (Index('ix_experiments_experiment_rootdir', 'experiment', 'root_dir', unique=True),)

    id = Column(Integer, primary_key=True)

    #: Experiment name
    experiment = Column(String, nullable=False)
    #: Root directory containing 'output???' directories
    root_dir = Column(String, nullable=False)

    # Other experiment metadata (populated from metadata.yaml)
    metadata_keys = ['contact', 'email', 'created', 'description', 'notes', 'keywords']
    contact = Column(String)
    email = Column(String)
    created = Column(DateTime)
    #: Human-readable experiment description
    description = Column(Text)
    #: Any other notes
    notes = Column(Text)
    #: Short, categorical keywords
    kw = relationship(
        'Keyword',
        secondary=keyword_assoc_table,
        back_populates='experiments',
        cascade='merge', # allow unique constraints on uncommitted session
        collection_class=set
    )
    # add an association proxy to the keyword column of the keywords table
    # this lets us add keywords as strings rather than Keyword objects
    keywords = association_proxy('kw', 'keyword')

    #: Files in this experiment
    ncfiles = relationship('NCFile', back_populates='experiment')

class Keyword(UniqueMixin, Base):
    __tablename__ = 'keywords'

    id = Column(Integer, primary_key=True)
    # enable sqlite case-insensitive string collation
    _keyword = Column(String(collation='NOCASE'), nullable=False, unique=True, index=True)

    # hybrid property lets us define different behaviour at the instance
    # and expression levels: for an instance, we return the lowercased keyword
    @hybrid_property
    def keyword(self):
        return self._keyword.lower()

    @keyword.setter
    def keyword(self, keyword):
        self._keyword = keyword

    # in an expression, because the column is 'collate nocase', we can just
    # use the raw keyword
    @keyword.expression
    def keyword(cls):
        return cls._keyword

    experiments = relationship('NCExperiment', secondary=keyword_assoc_table, back_populates='kw')

    def __init__(self, keyword):
        self.keyword = keyword

    @classmethod
    def unique_hash(cls, keyword):
        return keyword

    @classmethod
    def unique_filter(cls, query, keyword):
        return query.filter(Keyword.keyword == keyword)

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
    #: Start time of data in the file
    time_start = Column(String)
    #: End time of data in the file
    time_end = Column(String)
    #: Temporal frequency of the file
    frequency = Column(String)

    #: variables in this file
    ncvars = relationship('NCVar', back_populates='ncfile', cascade='all, delete-orphan')

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
    variable = relationship('CFVariable', back_populates='ncvars', uselist=False, cascade="merge")
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

    # if database version is 0, we've created it anew
    conn = engine.connect()
    ver = conn.execute('PRAGMA user_version').fetchone()[0]
    if ver == 0:
        # seems we can't use usual SQL parameter strings, so we'll just format the version in...
        conn.execute('PRAGMA user_version={}'.format(__DB_VERSION__))
    elif ver < __DB_VERSION__:
        raise Exception('Incompatible database versions, expected {}, got {}'.format(ver, __DB_VERSION__))

    Base.metadata.create_all(conn)
    conn.close()

    Session = sessionmaker(bind=engine)
    return Session()

class EmptyFileError(Exception): pass

def update_timeinfo(f, ncfile):
    """Extract time information from a single netCDF file: start time, end time, and frequency."""

    with netCDF4.Dataset(f, 'r') as ds:
        # we assume the record dimension corresponds to time
        time_dim = netcdf_utils.find_time_dimension(ds)
        if time_dim is None:
            return None

        time_var = ds.variables[time_dim]
        has_bounds = hasattr(time_var, 'bounds')

        if len(time_var) == 0:
            raise EmptyFileError('{} has a valid unlimited dimension, but no data'.format(f))

        if not hasattr(time_var, 'units') or not hasattr(time_var, 'calendar'):
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
        ncfile.time_start = format_datetime(ncfile.time_start)
        ncfile.time_end = format_datetime(ncfile.time_end)

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

        update_timeinfo(f, ncfile)
        ncfile.present = True
    except FileNotFoundError:
        logging.info('Unable to find file: %s', f)
    except Exception as e:
        logging.error('Error indexing %s: %s', f, e)

    return ncfile

def update_metadata(experiment, session):
    """Look for a metadata.yaml for a given experiment, and populate
    the row with any data found."""

    metadata_file = Path(experiment.root_dir) / 'metadata.yaml'
    if not metadata_file.exists():
        return

    try:
        metadata = yaml.safe_load(metadata_file.open())
        for k in NCExperiment.metadata_keys:
            if k in metadata:
                v = metadata[k]

                # special case for keywords: ensure we get a list
                if k == "keywords" and isinstance(v, str):
                    v = [v]

                setattr(experiment, k, v)
    except yaml.YAMLError as e:
        logging.warning('Error reading metadata file %s: %s', metadata_file, e)

    # update keywords to be unique
    experiment.kw = { Keyword.as_unique(session, kw.keyword) for kw in experiment.kw }

class IndexingError(Exception): pass

def index_experiment(experiment_dir, session=None, client=None, update=False, prune=True, delete=True, followsymlinks=False):
    """Index all output files for a single experiment."""

    # find all netCDF files in the hierarchy below this directory
    files = []

    options = []
    if followsymlinks:
        options.append('-L')

    cmd = ['find', *options, experiment_dir, '-name', '*.nc']
    proc = subprocess.run(cmd, encoding='utf-8', stdout=subprocess.PIPE, 
                          stderr=subprocess.PIPE)
    if proc.returncode != 0:
        warnings.warn('Some files or directories could not be read while finding output files: %s', UserWarning)

    results = [s for s in proc.stdout.split()]
    files.extend(results)

    expt_path = Path(experiment_dir)
    expt = NCExperiment(experiment=str(expt_path.name),
                        root_dir=str(expt_path.absolute()))

    # look for this experiment in the database
    q = (session
         .query(NCExperiment)
         .filter(NCExperiment.experiment == expt.experiment)
         .filter(NCExperiment.root_dir == expt.root_dir))
    r = q.one_or_none()
    if r is not None:
        if update:
            expt = r
        else:
            print('Not re-indexing experiment: {}\nPass `update=True` to build_index()'
                  .format(expt_path.name))
            return 0

    print('Indexing experiment: {}'.format(expt_path.name))

    update_metadata(expt, session)

    # make all files relative to the experiment path
    files = { str(Path(f).relative_to(expt_path)) for f in files }

    for fobj in expt.ncfiles:
        f = fobj.ncfile
        if f in files:
            # remove existing files from list, only index new files
            files.remove(f)
        else:
            if prune:
                # prune missing files from database
                if delete:
                    session.delete(fobj)
                else:
                    fobj.present = False

    results = []

    # index in parallel or serial, depending on whether we have a client
    if client is not None:
        futures = client.map(index_file, files, experiment=expt)
        results = client.gather(futures)
    else:
        results = [index_file(f, experiment=expt) for f in tqdm(files)]

    # update all variables to be unique
    for ncfile in results:
        for ncvar in ncfile.ncvars:
            v = ncvar.variable
            ncvar.variable = CFVariable.as_unique(session,
                                                  v.name, v.long_name,
                                                  v.standard_name, v.units)

    session.add_all(results)
    return len(results)

def build_index(directories, session, client=None, update=False, prune=True, 
                delete=True, followsymlinks=False):
    """Index all netcdf files contained within experiment directories.

    Requires a session for the database that's been created with the create_session() function.
    If client is not None, use a distributed client for processing files in parallel.
    May scan for only new entries to add to database with the update flag.
    If prune is True files that are already in the database but are missing from the filesystem
    will be either removed if delete is also True, or flagged as missing if delete is False.
    Symbolically linked files and/or directories will be indexed if followsymlinks is True.

    Returns the number of new files that were indexed.
    """

    if not isinstance(directories, list):
        directories = [directories]

    indexed = 0
    for directory in directories:
        indexed += index_experiment(directory, session, client, update, prune, 
                                    delete, followsymlinks)

    # if everything went smoothly, commit these changes to the database
    session.commit()
    return indexed

def prune_experiment(experiment, session, delete=True):
    """Delete or mark as not present the database entries for files
    within the given experiment that no longer exist or were broken at
    index time.
    """

    expt = (session
            .query(NCExperiment)
            .filter(NCExperiment.experiment == experiment)
            .one_or_none())

    if not expt:
        print("No such experiment: {}".format(experiment))
        return

    for f in expt.ncfiles:
        # check whether file exists
        if not f.ncfile_path.exists() or not f.present:

            if delete:
                session.delete(f)
            else:
                f.present = False

    session.commit()

