from datetime import datetime
import logging
import os
from pathlib import Path
import re
import subprocess
from tqdm import tqdm
import warnings

import cftime
import netCDF4
import yaml

from sqlalchemy import create_engine
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy import MetaData, Table, select, sql, exists, event
from sqlalchemy import func, case, literal_column

from sqlalchemy.orm import (
    object_session,
    relationship,
    Session,
    sessionmaker,
    validates,
)
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.pool import StaticPool
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property

from . import netcdf_utils
from .database_utils import *
from .date_utils import format_datetime

logging.captureWarnings(True)

__DB_VERSION__ = 3
__DEFAULT_DB__ = "/g/data/ik11/databases/cosima_master.db"

Base = declarative_base()

keyword_assoc_table = Table(
    "keyword_assoc",
    Base.metadata,
    Column("expt_id", Integer, ForeignKey("experiments.id")),
    Column("keyword_id", Integer, ForeignKey("keywords.id")),
)


class NCExperiment(Base):
    __tablename__ = "experiments"
    # composite index since an experiment name may not be unique
    __table_args__ = (
        Index(
            "ix_experiments_experiment_rootdir", "experiment", "root_dir", unique=True
        ),
    )

    id = Column(Integer, primary_key=True)

    #: Experiment name
    experiment = Column(String, nullable=False)
    #: Root directory containing 'output???' directories
    root_dir = Column(String, nullable=False)

    # Other experiment metadata (populated from metadata.yaml)
    metadata_keys = [
        "contact",
        "email",
        "created",
        "description",
        "notes",
        "url",
        "keywords",
    ]
    contact = Column(String)
    email = Column(String)
    created = Column(String)
    #: Experimental configuration control repository, e.g. GitHub payu control repo
    url = Column(String)
    #: Human-readable experiment description
    description = Column(Text)
    #: Any other notes
    notes = Column(Text)
    #: Short, categorical keywords
    kw = relationship(
        "Keyword",
        secondary=keyword_assoc_table,
        back_populates="experiments",
        cascade="merge",  # allow unique constraints on uncommitted session
        collection_class=set,
    )
    # add an association proxy to the keyword column of the keywords table
    # this lets us add keywords as strings rather than Keyword objects
    keywords = association_proxy("kw", "keyword")

    #: Files in this experiment
    ncfiles = relationship(
        "NCFile", back_populates="experiment", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return "<NCExperiment('{e.experiment}', '{e.root_dir}', {} files)".format(
            len(self.ncfiles), e=self
        )


class Keyword(UniqueMixin, Base):
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True)
    # enable sqlite case-insensitive string collation
    _keyword = Column(
        String(collation="NOCASE"), nullable=False, unique=True, index=True
    )

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

    experiments = relationship(
        "NCExperiment", secondary=keyword_assoc_table, back_populates="kw"
    )

    def __init__(self, keyword):
        self.keyword = keyword

    @classmethod
    def unique_hash(cls, keyword):
        return keyword

    @classmethod
    def unique_filter(cls, query, keyword):
        return query.filter(Keyword.keyword == keyword)


class NCAttributeString(Base):
    # unique-ified table of strings (keys/values) encountered in attribute processing
    #
    # this doesn't use the usual unique mixin pattern because it's hard to enforce
    # uniqueness through the dictionary mapped collection on NCFile and NCVar
    __tablename__ = "ncattribute_strings"

    id = Column(Integer, primary_key=True)
    value = Column(String, unique=True)


def _setup_ncattribute(session, attr_object):
    """
    Return the existing NCAttributeString with the same value as attr_object
    if it already exists in session, else return attr_object.
    """

    # create a cache on the session just for attributes (ignoring the global
    # cache for UniqueMixin, for example)
    cache = getattr(session, "_ncattribute_cache", None)
    if cache is None:
        session._ncattribute_cache = cache = {}

    if attr_object.value in cache:
        return cache[attr_object.value]

    with session.no_autoflush:
        r = (
            session.query(NCAttributeString)
            .filter_by(value=attr_object.value)
            .one_or_none()
        )
        if r is not None:
            return r

    cache[attr_object.value] = attr_object
    return attr_object


class NCAttribute(Base):
    __tablename__ = "ncattributes"

    id = Column(Integer, primary_key=True)

    # an NCAttribute may belong to an NCVar, or an NCFile directly
    ncvar_id = Column(Integer, ForeignKey("ncvars.id"), index=True)
    ncfile_id = Column(Integer, ForeignKey("ncfiles.id"))

    name_id = Column(Integer, ForeignKey("ncattribute_strings.id"))
    value_id = Column(Integer, ForeignKey("ncattribute_strings.id"))

    _name = relationship("NCAttributeString", foreign_keys=name_id)
    _value = relationship("NCAttributeString", foreign_keys=value_id)

    #: Attribute name
    name = association_proxy(
        "_name", "value", creator=lambda value: NCAttributeString(value=value)
    )
    #: Attribute value (cast to a string)
    value = association_proxy(
        "_value", "value", creator=lambda value: NCAttributeString(value=value)
    )

    def __init__(self, name, value):
        self.name = name
        self.value = value

    @validates("_name", "_value")
    def _validate_name(self, key, value):
        # called when the _name or _value objects are modified: look to see if
        # an existing attribute key/value with the same string already exists
        # in the session

        sess = object_session(self)
        if sess is not None:
            return _setup_ncattribute(sess, value)

        return value


@event.listens_for(Session, "transient_to_pending")
def _validate_ncattribute(session, object_):
    # this is the second part of the attribute uniqueness constraint: the transient -> pending
    # event is fired when an object is added to the session, so at this point we'll have access
    # to all the other attribute key/value pairs on objects already in the session

    if (
        isinstance(object_, NCAttribute)
        and object_._name is not None
        and object_._name.id is None
    ):
        old_name = object_._name

        # make sure to expunge this NCATtributeString from the session
        # so we don't accidentally add it and create an (unreferenced)
        # duplicate
        if old_name in session:
            session.expunge(old_name)

        object_._name = _setup_ncattribute(session, old_name)

    if (
        isinstance(object_, NCAttribute)
        and object_._value is not None
        and object_._value.id is None
    ):
        old_value = object_._value
        if old_value in session:
            session.expunge(old_value)

        object_._value = _setup_ncattribute(session, old_value)


class NCFile(Base):
    __tablename__ = "ncfiles"
    __table_args__ = (
        Index("ix_ncfiles_experiment_ncfile", "experiment_id", "ncfile", unique=True),
    )

    id = Column(Integer, primary_key=True)

    #: When this file was indexed
    index_time = Column(DateTime)
    #: The file name
    ncfile = Column(String, index=True)
    #: Is the file actually present on the filesystem?
    present = Column(Boolean)
    #: The experiment to which the file belongs
    experiment_id = Column(
        Integer, ForeignKey("experiments.id"), nullable=False, index=True
    )
    experiment = relationship("NCExperiment", back_populates="ncfiles")
    #: Start time of data in the file
    time_start = Column(String)
    #: End time of data in the file
    time_end = Column(String)
    #: Temporal frequency of the file
    frequency = Column(String)

    #: variables in this file
    ncvars = relationship(
        "NCVar",
        collection_class=attribute_mapped_collection("varname"),
        cascade="all, delete-orphan",
        back_populates="ncfile",
    )

    #: file-level attributes
    ncfile_attrs = relationship(
        "NCAttribute",
        collection_class=attribute_mapped_collection("name"),
        cascade="all, delete-orphan",
    )
    attrs = association_proxy("ncfile_attrs", "value", creator=NCAttribute)

    _model_map = {
        "ocean": ("ocn", "ocean"),
        "land": ("lnd", "land"),
        "atmosphere": ("atm", "atmos", "atmosphere"),
        "ice": ("ice",),
    }

    def __repr__(self):
        return """<NCFile('{e.ncfile}' in {e.experiment}, {} variables, \
from {e.time_start} to {e.time_end}, {e.frequency} frequency, {}present)>""".format(
            len(self.ncvars), "" if self.present else "not ", e=self
        )

    @property
    def ncfile_path(self):
        return Path(self.experiment.root_dir) / Path(self.ncfile)

    @hybrid_property
    def model(self):
        """
        Heuristic to guess type of model. Look for exact strings in subdirectories
        in path of a file. Match is case-insensitive. Returns model type as string.
        Either 'ocean', 'land', 'atmosphere', 'ice', or 'none' if no match found
        """

        for m in self._model_map:
            if any(
                x in map(str.lower, Path(self.ncfile).parent.parts)
                for x in self._model_map[m]
            ):
                return m
        return "none"

    @model.expression
    def model(cls):
        """
        SQL version of the model property
        """
        return case(
            [
                (
                    func.lower(cls.ncfile).contains(f"/{substr}/"),
                    literal_column(f"'{model}'"),
                )
                for model, substrs in cls._model_map.items()
                for substr in substrs
            ]
            + [
                (
                    func.lower(cls.ncfile).startswith(f"{substr}/"),
                    literal_column(f"'{model}'"),
                )
                for model, substrs in cls._model_map.items()
                for substr in substrs
            ],
            else_=literal_column("'none'"),
        )

    @hybrid_property
    def is_restart(self):
        """
        Heuristic to guess if this is a restart file, returns True if restart file,
        False otherwise
        """
        return any(
            p.startswith("restart")
            for p in map(str.lower, Path(self.ncfile).parent.parts)
        )

    @is_restart.expression
    def is_restart(cls):
        """
        SQL version of the is_restart property
        """
        return case(
            [
                (
                    func.lower(cls.ncfile).like("restart%/%"),
                    literal_column("1", Boolean),
                ),
            ],
            else_=literal_column("0", Boolean),
        )


class CFVariable(UniqueMixin, Base):
    __tablename__ = "variables"
    __table_args__ = (
        Index(
            "ix_variables_name_long_name_units",
            "name",
            "long_name",
            "units",
            unique=True,
        ),
    )

    id = Column(Integer, primary_key=True)

    #: Attributes associated with the variable that should
    #: be stored in the database
    attributes = ["long_name", "standard_name", "units"]

    #: The variable name
    name = Column(String, nullable=False, index=True)
    #: The variable long name (CF Conventions §3.2)
    long_name = Column(String)
    #: The variable standard name (CF Conventions §3.3)
    standard_name = Column(String)
    #: The variable units (CF Conventions §3.1)
    units = Column(String)

    #: Back-populate a list of ncvars that use this variable
    ncvars = relationship("NCVar", back_populates="variable")

    def __init__(self, name, long_name=None, standard_name=None, units=None):
        self.name = name
        self.long_name = long_name
        self.standard_name = standard_name
        self.units = units

    def __repr__(self):
        return "<CFVariable('{e.name}', in {} NCVars)>".format(len(self.ncvars), e=self)

    @classmethod
    def unique_hash(cls, name, long_name, standard_name, units, *arg):
        return "{}_{}_{}".format(name, long_name, units)

    @classmethod
    def unique_filter(cls, query, name, long_name, standard_name, units, *arg):
        return (
            query.filter(CFVariable.name == name)
            .filter(CFVariable.long_name == long_name)
            .filter(CFVariable.units == units)
        )

    @hybrid_property
    def is_coordinate(self):
        """
        Heuristic to guess if this is a coordinate variable based on units. Returns
        True if coordinate variable, False otherwise
        """
        if self.units is not None or self.units != "" or self.units.lower() != "none":
            coord_units = {r".*degrees_.*", r".*since.*", r"radians", r".*days.*"}
            for u in coord_units:
                if re.search(u, self.units):
                    return True
        return False

    @is_coordinate.expression
    def is_coordinate(cls):
        """
        SQL version of the is_coordinate property
        """
        return case(
            [
                (
                    func.lower(cls.units).contains("degrees_", autoescape=True),
                    literal_column("1", Boolean),
                ),
                (func.lower(cls.units).contains("since"), literal_column("1", Boolean)),
                (func.lower(cls.units).contains("days"), literal_column("1", Boolean)),
                (func.lower(cls.units).like("radians"), literal_column("1", Boolean)),
            ],
            else_=literal_column("0", Boolean),
        )


class NCVar(Base):
    __tablename__ = "ncvars"

    id = Column(Integer, primary_key=True)

    #: The ncfile to which this variable belongs
    ncfile_id = Column(Integer, ForeignKey("ncfiles.id"), nullable=False, index=True)
    ncfile = relationship("NCFile", back_populates="ncvars")
    #: The generic form of this variable (name and attributes)
    variable_id = Column(Integer, ForeignKey("variables.id"), nullable=False)
    variable = relationship(
        "CFVariable",
        back_populates="ncvars",
        uselist=False,
    )
    #: Proxy for the variable name
    varname = association_proxy("variable", "name")
    #: Serialised tuple of variable dimensions
    dimensions = Column(String)
    #: Serialised tuple of chunking along each dimension
    chunking = Column(String)

    #: A dictionary of NCAttributes
    ncvar_attrs = relationship(
        "NCAttribute",
        collection_class=attribute_mapped_collection("name"),
        cascade="all, delete-orphan",
    )
    attrs = association_proxy("ncvar_attrs", "value", creator=NCAttribute)

    def __repr__(self):
        return "<NCVar('{e.varname}' in '{e.ncfile.ncfile_path}', attrs: {})>".format(
            set(self.attrs.keys()), e=self
        )

    @property
    def cell_methods(self):
        """
        Return cell_methods attribute if it exists, otherwise None
        """
        return self.attrs.get("cell_methods", None)


def create_session(db=None, debug=False):
    """Create a session for the specified database file.

    If debug=True, the session will output raw SQL whenever it is executed on the database.
    """

    if db is None:
        db = os.getenv("COSIMA_COOKBOOK_DB", __DEFAULT_DB__)

    # File might be a symlink, so we make sure to resolve it before proceeding
    db_path = Path(db).resolve()

    engine = create_engine("sqlite:///" + str(db_path), echo=debug)

    # if database version is 0, we've created it anew
    conn = engine.connect()
    ver = conn.execute("PRAGMA user_version").fetchone()[0]
    if ver == 0:
        # seems we can't use usual SQL parameter strings, so we'll just format the version in...
        conn.execute("PRAGMA user_version={}".format(__DB_VERSION__))
    elif ver < __DB_VERSION__:
        raise Exception(
            "Incompatible database versions, expected {}, got {}".format(
                ver, __DB_VERSION__
            )
        )

    Base.metadata.create_all(conn)
    conn.close()

    Session = sessionmaker(bind=engine, autoflush=False)
    return Session()


class EmptyFileError(Exception):
    pass


def update_timeinfo(f, ncfile):
    """Extract time information from a single netCDF file: start time, end time, and frequency."""

    with netCDF4.Dataset(f, "r") as ds:
        # we assume the record dimension corresponds to time
        time_dim = netcdf_utils.find_time_dimension(ds)
        if time_dim is None:
            return None

        time_var = ds.variables[time_dim]
        has_bounds = hasattr(time_var, "bounds")

        if len(time_var) == 0:
            raise EmptyFileError(
                "{} has a valid unlimited dimension, but no data".format(f)
            )

        if not hasattr(time_var, "units") or not hasattr(time_var, "calendar"):
            # non CF-compliant file -- don't process further
            return

        # Helper function to get a date
        def todate(t):
            return cftime.num2date(t, time_var.units, calendar=time_var.calendar)

        if has_bounds:
            bounds_var = ds.variables[time_var.bounds]
            ncfile.time_start = todate(bounds_var[0, 0])
            ncfile.time_end = todate(bounds_var[-1, 1])
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
                next_time = todate(bounds_var[0, 1])
            else:
                next_time = todate(time_var[1])

            dt = next_time - ncfile.time_start
            if dt.days >= 365:
                years = round(dt.days / 365)
                ncfile.frequency = "{} yearly".format(years)
            elif dt.days >= 28:
                months = round(dt.days / 30)
                ncfile.frequency = "{} monthly".format(months)
            elif dt.days >= 1:
                ncfile.frequency = "{} daily".format(dt.days)
            else:
                ncfile.frequency = "{} hourly".format(dt.seconds // 3600)
        else:
            # single time value in this file and no averaging
            ncfile.frequency = "static"

        # convert start/end times to timestamps
        ncfile.time_start = format_datetime(ncfile.time_start)
        ncfile.time_end = format_datetime(ncfile.time_end)


def index_file(ncfile_name, experiment, session):
    """Index a single netCDF file within an experiment by retrieving all variables, their dimensions
    and chunking.
    """

    # construct absolute path to file
    f = str(Path(experiment.root_dir) / ncfile_name)

    # try to index this file, and mark it 'present' if indexing succeeds
    ncfile = NCFile(
        index_time=datetime.now(),
        ncfile=ncfile_name,
        present=False,
        experiment=experiment,
    )
    try:
        with netCDF4.Dataset(f, "r") as ds:
            for v in ds.variables.values():
                # create the generic cf variable structure
                cfvar = {
                    "name": v.name,
                    "long_name": None,
                    "standard_name": None,
                    "units": None,
                }

                # check for other attributes
                for att in CFVariable.attributes:
                    if att in v.ncattrs():
                        cfvar[att] = v.getncattr(att)

                cfvar = CFVariable.as_unique(session, **cfvar)

                # fill in the specifics for this file: dimensions and chunking
                ncvar = NCVar(
                    variable=cfvar,
                    dimensions=str(v.dimensions),
                    chunking=str(v.chunking()),
                )

                # we'll add all attributes to the ncvar itself
                for att in v.ncattrs():
                    ncvar.attrs[att] = str(v.getncattr(att))

                ncfile.ncvars[v.name] = ncvar

            # add file-level attributes
            for att in ds.ncattrs():
                ncfile.attrs[att] = str(ds.getncattr(att))

        update_timeinfo(f, ncfile)
        ncfile.present = True
    except FileNotFoundError:
        logging.info("Unable to find file: %s", f)
    except Exception as e:
        logging.error("Error indexing %s: %s", f, e)

    return ncfile


def update_metadata(experiment, session):
    """Look for a metadata.yaml for a given experiment, and populate
    the row with any data found."""

    metadata_file = Path(experiment.root_dir) / "metadata.yaml"
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
        logging.warning("Error reading metadata file %s: %s", metadata_file, e)

    # update keywords to be unique
    experiment.kw = {Keyword.as_unique(session, kw.keyword) for kw in experiment.kw}


class IndexingError(Exception):
    pass


def find_files(searchdir, matchstring="*.nc", followsymlinks=False):
    """Return netCDF files under search directory"""

    # find all netCDF files in the hierarchy below searchdir
    options = []
    if followsymlinks:
        options.append("-L")

    cmd = ["find", *options, searchdir, "-name", matchstring]
    proc = subprocess.run(
        cmd, encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if proc.returncode != 0:
        warnings.warn(
            "Some files or directories could not be read while finding output files: %s",
            UserWarning,
        )

    # make all files relative to the search directory and return as set
    return {str(Path(s).relative_to(searchdir)) for s in proc.stdout.split()}


def find_experiment(session, expt_path):
    """Return experiment if it already exists in this DB session"""

    q = (
        session.query(NCExperiment)
        .filter(NCExperiment.experiment == str(expt_path.name))
        .filter(NCExperiment.root_dir == str(expt_path.resolve()))
    )

    return q.one_or_none()


def index_experiment(files, session, expt, client=None):
    """Index specified files for an experiment."""

    if client is not None:
        warnings.warn(
            "client is no longer a supported argument", DeprecationWarning, stacklevel=2
        )

    update_metadata(expt, session)

    results = []

    results = [index_file(f, experiment=expt, session=session) for f in tqdm(files)]

    session.add_all(results)
    return len(results)


def build_index(
    directories,
    session,
    client=None,
    prune="delete",
    force=False,
    followsymlinks=False,
):
    """Index all netcdf files contained within experiment directories.

    Requires a session for the database that's been created with the
    create_session() function. If client is not None, use a distributed
    client for processing files in parallel. If prune is set to 'flag' files
    that are already in the database but are missing from the filesystem will
    be flagged as missing, or if set to 'delete' they will be removed from the
    database. If force is False only files that are not in the database will
    be indexed, othewise if True all files will be indexed and their database
    entries updated. Symbolically linked files and/or directories will be
    indexed if followsymlinks is True.

    Returns the number of new files that were indexed.
    """

    if client is not None:
        warnings.warn(
            "client is no longer a supported argument", DeprecationWarning, stacklevel=2
        )

    if not isinstance(directories, list):
        directories = [directories]

    prune = prune.lower()
    if not prune in {"flag", "delete"}:
        print(
            "build_index :: ERROR :: Value for option prune is incorrect,\n "
            "must be flag or delete: {}\n"
            "Resetting to 'flag'".format(prune)
        )
        prune = "flag"

    indexed = 0
    for directory in [Path(d) for d in directories]:

        expt = find_experiment(session, directory)
        if expt is None:
            expt = NCExperiment(
                experiment=str(directory.name), root_dir=str(directory.resolve())
            )

        print("Indexing experiment: {}".format(directory.name))

        # find all netCDF files in the experiment directory
        files = find_files(directory, followsymlinks=followsymlinks)

        if force:
            # Prune all files to force re-indexing data
            _prune_files(expt, session, {}, delete=True)
        else:
            # Prune files that exist in the database but are not present on disk
            _prune_files(expt, session, files, delete=(prune == "delete"))

        if len(files) > 0:
            if len(expt.ncfiles) > 0:
                # Remove files that are already in the DB
                files.difference_update(
                    {
                        f
                        for f, in session.query(NCFile.ncfile)
                        .with_parent(expt)
                        .filter(NCFile.ncfile.in_(files))
                    }
                )

            indexed += index_experiment(files, session, expt)

            # if everything went smoothly, commit these changes to the database
            session.commit()

    return indexed


def _prune_files(expt, session, files, delete=True):
    """Delete or mark as not present the database entries that are
    not present in the list of files
    """
    if len(expt.ncfiles) == 0:
        # No entries in DB, special case just return as there is nothing
        # to prune and trying to do so will raise errors
        return

    # Find ids of all files newer than the time last indexed. Only valid
    # for delete=True as entries cannot be updated if they already exist
    # in the DB
    oldids = [
        f.id
        for f in (
            session.query(NCFile)
            .with_parent(expt)
            .filter(NCFile.ncfile.in_(files) & (NCFile.present == True))
        )
        if f.index_time < datetime.fromtimestamp(f.ncfile_path.stat().st_mtime)
    ]
    if not delete:
        oldids = []
        logging.warning(
            "Data files have been updated since they were last indexed. "
            "Prune has been set to 'flag' so they will not be reindexed. "
            "Set prune to 'delete' to reindex updated files"
        )

    # Missing are physically missing from disk, or where marked as not
    # present previously. Can also be a broken file which didn't index.
    missing_ncfiles = (
        session.query(NCFile)
        .with_parent(expt)
        .filter(
            NCFile.ncfile.notin_(files)
            | (NCFile.present == False)
            | (NCFile.id.in_(oldids))
        )
    )

    session.expire_all()
    if delete:
        missing_ncfiles.delete(synchronize_session=False)
    else:
        missing_ncfiles.update({NCFile.present: False}, synchronize_session=False)


def prune_experiment(experiment, session, delete=True, followsymlinks=False):
    """Delete or mark as not present the database entries for files
    within the given experiment that no longer exist or were broken at
    index time. Experiment can be either an NCExperiment object, or the
    name of an experiment available within the current session.
    """

    if isinstance(experiment, NCExperiment):
        expt = experiment
        experiment = expt.experiment
    else:
        expt = (
            session.query(NCExperiment)
            .filter(NCExperiment.experiment == experiment)
            .one_or_none()
        )
        if not expt:
            raise RuntimeError("No such experiment: {}".format(experiment))

    files = find_files(expt.root_dir, followsymlinks=followsymlinks)

    # Prune files that exist in the database but are not present
    # on the filesystem
    _prune_files(expt, session, files, delete)


def delete_experiment(experiment, session):
    """Completely delete an experiment from the database.

    This removes the experiment entry itself, as well as all
    of its associated files.
    """

    expt = (
        session.query(NCExperiment)
        .filter(NCExperiment.experiment == experiment)
        .one_or_none()
    )

    if expt is not None:
        session.delete(expt)
        session.commit()
