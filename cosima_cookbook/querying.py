"""querying.py

Functions for data discovery.

"""

import logging
import os.path
import pandas as pd
from sqlalchemy import func, distinct
import warnings
import xarray as xr

from . import database
from .database import NCExperiment, NCFile, CFVariable, NCVar, Keyword


class VariableNotFoundError(Exception):
    pass

def get_experiments(session, experiment=True, keywords=None, all=False, **kwargs):
    """
    Returns a DataFrame of all experiments and the number of netCDF4 files contained 
    within each experiment.

    Optionally one or more keywords can be specified, and only experiments with all the
    specified keywords will be return.

    All metadata fields will be returned if all=True, or individual metadata fields
    can be selected by passing field=True, where available fields are: 
    contact, email, created, description, notes, and root_dir
    """

    # Determine which attributes to return. Special case experiment
    # as this is the only one that defaults to True
    columns = []
    if experiment:
        columns.append(NCExperiment.experiment)

    for f in NCExperiment.metadata_keys + ['root_dir']:
        # Explicitly don't support returning keyword metadata
        if f == 'keywords': 
            continue
        if kwargs.get(f, all):
            columns.append(getattr(NCExperiment, f))

    q = (session
        .query(*columns,
                func.count(NCFile.experiment_id).label('ncfiles'))
        .join(NCFile.experiment)
        .group_by(NCFile.experiment_id))

    if keywords is not None:

        if isinstance(keywords, str):
            keywords = [ keywords ]

        q = q.filter(*(NCExperiment.keywords == k for k in keywords))

    return pd.DataFrame(q)

def get_ncfiles(session, experiment):
    """
    Returns a DataFrame of all netcdf files for a given experiment.
    """

    q = (session
         .query(NCFile.ncfile, NCFile.index_time)
         .join(NCFile.experiment)
         .filter(NCExperiment.experiment == experiment)
         .order_by(NCFile.ncfile))

    return pd.DataFrame(q)

def get_keywords(session, experiment=None):
    """
    Returns a set of all keywords, and optionally only for a given experiment
    """

    if experiment is not None:
        q = (session
            .query(NCExperiment)
            .filter(NCExperiment.experiment == experiment))
        return q.scalar().keywords
    else:
        q = (session
            .query(Keyword))
        return {r.keyword for r in q}

def get_variables(session, experiment, frequency=None):
    """
    Returns a DataFrame of variables for a given experiment and optionally
    a given diagnostic frequency.
    """

    q = (session
         .query(CFVariable.name,
                CFVariable.long_name,
                NCFile.frequency,
                NCFile.ncfile,
                func.count(NCFile.ncfile).label('# ncfiles'),
                func.min(NCFile.time_start).label('time_start'),
                func.max(NCFile.time_end).label('time_end'))
         .join(NCFile.experiment)
         .join(NCFile.ncvars)
         .join(NCVar.variable)
         .filter(NCExperiment.experiment == experiment)
         .order_by(NCFile.frequency,
                   CFVariable.name,
                   NCFile.time_start,
                   NCFile.ncfile)
         .group_by(CFVariable.name, NCFile.frequency))

    if frequency is not None:
        q = q.filter(NCFile.frequency == frequency)

    return pd.DataFrame(q)

def get_frequencies(session, experiment=None):
    """
    Returns a DataFrame with all diagnostics frequencies and optionally
    for a given experiment.
    """

    if experiment is None:
        q = (session
             .query(NCFile.frequency)
             .group_by(NCFile.frequency))
    else:
        q = (session
             .query(NCFile.frequency)
             .join(NCFile.experiment)
             .filter(NCExperiment.experiment == experiment)
             .group_by(NCFile.frequency))

    return pd.DataFrame(q)


def getvar(expt, variable, session, ncfile=None,
           start_time=None, end_time=None, n=None, **kwargs):
    """For a given experiment, return an xarray DataArray containing the
    specified variable.

    expt - text string indicating the name of the experiment
    variable - text string indicating the name of the variable to load
    session - a database session created by cc.database.create_session()
    ncfile -  an optional text string indicating the pattern for filenames
              to load. All filenames containing this string will match, so
              be specific. '/' can be used to match the start of the
              filename, and '%' is a wildcard character.
    start_time - only load data after this date. specify as a text string,
                 e.g. '1900-01-01'
    end_time - only load data before this date. specify as a text string,
               e.g. '1900-01-01'
    n - after all other queries, restrict the total number of files to the
        first n. pass a negative value to restrict to the last n

    Note that if start_time and/or end_time are used, the time range
    of the resulting dataset may not be bounded exactly on those
    values, depending on where the underlying files start/end. Use
    dataset.sel() to exactly select times from the dataset.

    Other kwargs are passed through to xarray.open_mfdataset, including:

    chunks - Override any chunking by passing a chunks dictionary.
    decode_times - Time decoding can be disabled by passing decode_times=False

    """

    ncfiles = _ncfiles_for_variable(expt, variable, session, ncfile, start_time, end_time, n)

    # chunking -- use first row/file and assume it's the same across the whole dataset
    xr_kwargs = {"chunks": _parse_chunks(ncfiles[0].NCVar)}
    xr_kwargs.update(kwargs)

    ds = xr.open_mfdataset(
        (str(f.NCFile.ncfile_path) for f in ncfiles),
        parallel=True,
        combine="by_coords",
        preprocess=lambda d: d[variable].to_dataset()
        if variable not in d.coords
        else d,
        **xr_kwargs
    )

    return ds[variable]


def _ncfiles_for_variable(expt, variable, session,
                          ncfile=None, start_time=None, end_time=None, n=None):
    """Return a list of (NCFile, NCVar) pairs corresponding to the
    database objects for a given variable.

    Optionally, pass ncfile, start_time, end_time or n for additional
    disambiguation (see getvar documentation for their semantics).
    """

    f, v = database.NCFile, database.NCVar
    q = (
        session.query(f, v)
        .join(f.ncvars)
        .join(f.experiment)
        .filter(v.varname == variable)
        .filter(database.NCExperiment.experiment == expt)
        .filter(f.present)
        .order_by(f.time_start)
    )

    # additional disambiguation
    if ncfile is not None:
        q = q.filter(f.ncfile.like("%" + ncfile))
    if start_time is not None:
        q = q.filter(f.time_end >= start_time)
    if end_time is not None:
        q = q.filter(f.time_start <= end_time)
    ncfiles = q.all()

    if n is not None:
        if n > 0:
            ncfiles = ncfiles[:n]
        else:
            ncfiles = ncfiles[n:]

    # ensure we actually got a result
    if not ncfiles:
        raise VariableNotFoundError(
            "No files were found containing '{}' in the '{}' experiment".format(
                variable, expt
            )
        )

    # check whether the results are unique
    unique_files = set(os.path.basename(f.NCFile.ncfile) for f in ncfiles)
    if len(unique_files) > 1:
        warnings.warn(
            f"Your query gets a variable from differently-named files: {unique_files}. "
            "This could lead to unexpected behaviour! Disambiguate by passing "
            "ncfile= to getvar, specifying the desired file."
        )

    return ncfiles

def _parse_chunks(ncvar):
    """Parse an NCVar, returning a dictionary mapping dimensions to chunking along that dimension."""

    try:
        # this should give either a list, or 'None' (other values will raise an exception)
        var_chunks = eval(ncvar.chunking)
        if var_chunks is not None:
            return dict(zip(eval(ncvar.dimensions), var_chunks))

        return None

    except NameError:
        # chunking could be 'contiguous', which doesn't evaluate
        return None
