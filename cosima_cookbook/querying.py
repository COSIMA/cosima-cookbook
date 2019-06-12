import logging
import os.path

import xarray as xr

from . import database

class VariableNotFoundError(Exception):
    pass

def getvar(expt, variable, session, ncfile=None, n=None,
           start_time=None, end_time=None, chunks=None,
           time_units=None, offset=None, decode_times=True,
           check_present=False):
    """For a given experiment, return an xarray DataArray containing the
    specified variable.
    
    expt - text string indicating the name of the experiment
    variable - text string indicating the name of the variable to load
    session - a database session created by cc.database.create_session()

    ncfile - If disambiguation based on filename is required, pass the ncfile
    argument.
    n - A subset of output data can be obtained by restricting the number of 
        netcdf files to load (use a negative value of n to get the last n 
        files, or a positive n to get the first n files).
    start_time - Only load data after this date. Specify the date as a text string
        (e.g. '1900-1-1')
    start_time - Only load data before this date. Specify the date as a text string
        (e.g. '1900-1-1')
    chunks - Override any chunking by passing a chunks dictionary.
    offset - A time offset (in an integer number of days) can also be applied.
    decode_times - Time decoding can be disabled by passing decode_times=False
    check_present - indicates whether to check the presence of the file before 
        loading.
    """

    f, v = database.NCFile, database.NCVar
    q = (session
         .query(f, v)
         .join(f.ncvars).join(f.experiment)
         .filter(v.varname == variable)
         .filter(database.NCExperiment.experiment == expt)
         .filter(f.present)
         .order_by(f.time_start))

    # further constraints
    if ncfile is not None:
        q = q.filter(f.ncfile.like('%' + ncfile))
    if start_time is not None:
        q = q.filter(f.time_end >= start_time)
    if end_time is not None:
        q = q.filter(f.time_start <= end_time)

    ncfiles = q.all()

    # ensure we actually got a result
    if not ncfiles:
        raise VariableNotFoundError("No files were found containing {} in the '{}' experiment".format(variable, expt))

    if check_present:
        ncfiles_full = ncfiles
        ncfiles = []

        for f in ncfiles_full:
            # check whether file exists
            if f.NCFile.ncfile_path.exists():
                ncfiles.append(f)
                continue

            # doesn't exist, update in database
            session.delete(f.NCFile)

        session.commit()

    # restrict number of files directly
    if n is not None:
        if n > 0:
            ncfiles = ncfiles[:n]
        else:
            ncfiles = ncfiles[n:]

    file_chunks = None

    # chunking -- use first row/file
    try:
        file_chunks = dict(zip(eval(ncfiles[0].NCVar.dimensions), eval(ncfiles[0].NCVar.chunking)))
        # apply caller overrides
        if chunks is not None:
            file_chunks.update(chunks)
    except NameError:
        # chunking could be 'contiguous', which doesn't evaluate
        pass

    # the "dreaded" open_mfdata can actually be quite efficient
    # I found that it was important to "preprocess" to select only
    # the relevant variable, because chunking doesn't apply to
    # all variables present in the file
    ds = xr.open_mfdataset((str(f.NCFile.ncfile_path) for f in ncfiles), parallel=True,
                           chunks=file_chunks,
                           decode_times=False,
                           preprocess=lambda d: d[variable].to_dataset() if variable not in d.coords else d)

    # handle time offsetting and decoding
    # TODO: use helper function to find the time variable name
    if 'time' in (c.lower() for c in ds.coords) and decode_times:
        calendar = ncfiles[0].NCFile.calendar
        tvar = 'time'
        # if dataset uses capitalised variant
        if 'Time' in ds.coords:
            tvar = 'Time'

        # first rebase times onto new units if required
        if time_units is not None:
            dates = xr.conventions.times.decode_cf_datetime(ds[tvar], ncfiles[0].NCFile.timeunits, calendar)
            times = xr.conventions.times.encode_cf_datetime(dates, time_units, calendar)
            ds[tvar] = times[0]
        else:
            time_units = ncfiles[0].NCFile.timeunits

        # time offsetting - mimic one aspect of old behaviour by adding
        # a fixed number of days
        if offset is not None:
            ds[tvar] += offset


        # decode time - we assume that we're getting units and a calendar from a file
        try:
            decoded_time = xr.conventions.times.decode_cf_datetime(ds[tvar], time_units, calendar)
            ds[tvar] = decoded_time
        except Exception as e:
            logging.error('Unable to decode time: %s', e)

    return ds[variable]
