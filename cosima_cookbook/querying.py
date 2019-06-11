import logging
import os.path

from sqlalchemy import select, bindparam
import xarray as xr

from . import database

class VariableNotFoundError(Exception):
    pass

def getvar(expt, variable, db, ncfile=None, n=None,
           start_time=None, end_time=None, chunks=None,
           time_units=None, offset=None, decode_times=True,
           rebase_times=True,
           calendar=None, check_present=False):
    """For a given experiment, return an xarray DataArray containing the
    specified variable.
    
    expt - text string indicating the name of the experiment
    variable - text string indicating the name of the variable to load
    db - text string indicating the file path of the database. The default 
        database includes many available experiments, and is usually kept
        up to data

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

    check_present - indicates whether to check the presence of the file before 
        loading.

    decode_times - Time decoding can be disabled by passing decode_times=False
    rebase_times - If True (default), and time_units is specified, rebase times
        to time_units before applying offset. Otherwise, time_units is simply
        overridden for decoding of times from the file.
    time_units - Either a time unit on which to rebase time coordinate variable,
        or an override for the units attribute (see above).
    calendar - Override the calendar specified in the attributes of the time
        coordinate variable.
    offset - A time offset (in an integer number of days) can also be applied.
    """

    conn, tables = database.create_database(db)

    # find candidate vars -- base query
    s = select([tables['ncfiles'].c.ncfile,
                tables['ncvars'].c.dimensions,
                tables['ncvars'].c.chunking,
                tables['ncfiles'].c.timeunits,
                tables['ncfiles'].c.calendar,
                tables['ncfiles'].c.id]) \
            .select_from(tables['ncvars'].join(tables['ncfiles'])) \
            .where(tables['ncvars'].c.variable == variable) \
            .where(tables['ncfiles'].c.experiment == expt) \
            .where(tables['ncfiles'].c.present) \
            .order_by(tables['ncfiles'].c.time_start)

    # further constraints
    if ncfile is not None:
        s = s.where(tables['ncfiles'].c.ncfile.like('%' + ncfile))
    if start_time is not None:
        s = s.where(tables['ncfiles'].c.time_end >= start_time)
    if end_time is not None:
        s = s.where(tables['ncfiles'].c.time_start <= end_time)

    ncfiles = conn.execute(s).fetchall()

    # ensure we actually got a result
    if not ncfiles:
        raise VariableNotFoundError("No files were found containing {} in the '{}' experiment".format(variable, expt))

    if check_present:
        u = tables['ncfiles'].update().where(tables['ncfiles'].c.id == bindparam('ncfile_id')).values(present=False)

        for f in ncfiles.copy():
            # check whether file exists
            if os.path.isfile(f[0]):
                continue

            # doesn't exist, update in database
            conn.execute(u, ncfile_id=f[-1])
            ncfiles.remove(f)

    # restrict number of files directly
    if n is not None:
        if n > 0:
            ncfiles = ncfiles[:n]
        else:
            ncfiles = ncfiles[n:]

    file_chunks = None

    # chunking -- use first row/file
    try:
        file_chunks = dict(zip(eval(ncfiles[0][1]), eval(ncfiles[0][2])))
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
    ds = xr.open_mfdataset((f[0] for f in ncfiles), parallel=True,
                           chunks=file_chunks,
                           decode_times=False,
                           preprocess=lambda d: d[variable].to_dataset() if variable not in d.coords else d)

    # handle time offsetting and decoding
    # TODO: use helper function to find the time variable name
    if 'time' in (c.lower() for c in ds.coords) and decode_times:
        if calendar is None:
            calendar = ncfiles[0][4]

        tvar = 'time'
        # if dataset uses capitalised variant
        if 'Time' in ds.coords:
            tvar = 'Time'

        # first rebase times onto new units if required
        if time_units is not None and rebase_times:
            dates = xr.conventions.times.decode_cf_datetime(ds[tvar], ncfiles[0][3], calendar)
            times = xr.conventions.times.encode_cf_datetime(dates, time_units, calendar)
            ds[tvar] = times[0]

        # after rebasing, just use time units from file unless specified
        if time_units is None:
            time_units = ncfiles[0][3]

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
