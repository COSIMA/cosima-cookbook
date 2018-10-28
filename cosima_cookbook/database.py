import logging
from pathlib import Path
import re
import subprocess

import cftime
from dask.distributed import as_completed
import netCDF4
from sqlalchemy import create_engine, select, exists, sql
from sqlalchemy import Table, Column, Integer, String, Boolean, MetaData, ForeignKey

from . import netcdf_utils

def create_database(db, debug=False):
    """Create new database file with the target schema.

    We create a foreign key constraint on the ncfile column in
    the ncvars table, but it won't be enforced without `PRAGMA
    foreign_keys = 1' in sqlite."""

    engine = create_engine('sqlite:///' + db, echo=debug)
    metadata = MetaData()

    ncfiles = Table('ncfiles', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('ncfile', String, index=True, unique=True),
                    Column('present', Boolean),
                    Column('experiment', String, index=True),
                    Column('run', Integer),
                    Column('timeunits', String),
                    Column('calendar', String),
                    Column('time_start', String),
                    Column('time_end', String),
                    Column('frequency', String),
                    )

    ncvars = Table('ncvars', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('ncfile', None, ForeignKey('ncfiles.id'), nullable=False, index=True),
                   Column('variable', String),
                   Column('dimensions', String),
                   Column('chunking', String))

    metadata.create_all(engine)
    return engine.connect(), {'ncfiles': ncfiles, 'ncvars': ncvars}

def file_timeinfo(f):
    """Extract time information from a single netCDF file: units, calendar,
    start time, end time, and frequency."""

    timeinfo = {}

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
            timeinfo['timeunits'] = time_var.units
        else:
            timeinfo['timeunits'] = None
        if hasattr(time_var, 'calendar'):
            timeinfo['calendar'] = time_var.calendar
        else:
            timeinfo['calendar'] = None

        if timeinfo['timeunits'] is None or timeinfo['calendar'] is None:
            # non CF-compliant file
            timeinfo['time_start'] = None
            timeinfo['time_end'] = None
            timeinfo['frequency'] = None
            return timeinfo

        def todate(t):
            return cftime.num2date(t, time_var.units, calendar=time_var.calendar)

        if has_bounds:
            bounds_var = ds.variables[time_var.bounds]
            timeinfo['time_start'] = todate(bounds_var[0,0])
            timeinfo['time_end'] = todate(bounds_var[-1,1])
        else:
            timeinfo['time_start'] = todate(time_var[0])
            timeinfo['time_end'] = todate(time_var[-1])

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

            dt = next_time - timeinfo['time_start']
            if dt.days >= 365:
                years = round(dt.days / 365)
                timeinfo['frequency'] = '{} yearly'.format(years)
            elif dt.days >= 28:
                months = round(dt.days / 30)
                timeinfo['frequency'] = '{} monthly'.format(months)
            elif dt.days >= 1:
                timeinfo['frequency'] = '{} daily'.format(dt.days)
            else:
                timeinfo['frequency'] = '{} hourly'.format(dt.seconds // 3600)
        else:
            # single time value in this file and no averaging
            timeinfo['frequency'] = 'static'

        # convert start/end times to timestamps
        # strftime doesn't like years with fewer than 4 digits (pads with spaces
        # instead of zeroes)...
        def zeropad(s):
            ss = s.lstrip()
            return (len(s)-len(ss))*'0' + ss

        timeinfo['time_start'] = zeropad(timeinfo['time_start'].strftime('%Y-%m-%d %H:%M:%S'))
        timeinfo['time_end'] = zeropad(timeinfo['time_end'].strftime('%Y-%m-%d %H:%M:%S'))

    return timeinfo

def index_file(f):
    """Index a single netCDF file by retrieving all variables, their dimensions
    and chunking.

    Returns a list of dictionaries."""

    with netCDF4.Dataset(f, 'r') as ds:
        ncvars = [{'variable': v.name,
                   'dimensions': str(v.dimensions),
                   'chunking': str(v.chunking())}
                  for v in ds.variables.values()]

    return ncvars

def index_run(run_dir, db, update=False):
    """Index all output files for a single run (i.e. an outputNNN directory).

    Pass update=True to try to optimise by not touching files already in the
    database (may run into sqlite concurrency errors)."""

    # find all netCDF files in the hierarchy below this directory
    files = []
    try:
        results = subprocess.check_output(['find', run_dir, '-name', '*.nc'])
        results = [s for s in results.decode('utf-8').split()]
        files.extend(results)
    except Exception as e:
        logging.error('Error occurred while finding output files: %s', e)

    # extract experiment and run from path
    expt = Path(run_dir).parent.name
    run = 0
    m = re.search(r'\d+$', Path(run_dir).name)
    if m:
        run = int(m.group())
    else:
        logging.warning('Unconvention run directory: %s', run_dir)

    results = []
    if update:
        query_conn, tables = create_database(db)

    for f in files:
        # skip processing file if it's already in the database
        # (since we treat files atomically, it's either there or not)
        if update and query_conn.execute(select([tables['ncfiles'].c.id]) \
                                         .where(tables['ncfiles'].c.ncfile == f)).fetchone() is not None:
            continue

        # try to index this file, and mark it 'present' if indexing succeeds
        file_present = False
        timeinfo = None
        try:
            ncvars = index_file(f)
            file_present = True
            timeinfo = file_timeinfo(f)
        except FileNotFoundError:
            logging.info('Unable to find file: %s', f)
        except Exception as e:
            logging.error('Error indexing %s: %s', f, e)

        info = {'ncfile': f,
                'experiment': expt,
                'run': run,
                'present': file_present,
                'vars': ncvars}
        if timeinfo is not None:
            info.update(timeinfo)

        results.append(info)

    if update:
        query_conn.close()

    return results

def build_index(directories, client, db, update=False, debug=False):
    """Index all runs contained within a directory. Requires a distributed client for processing,
    and the filename of a database that's been created with the create_database() function.

    May scan for only new entries to add to database with the update flag."""

    # make the assumption that everything has been created with payu -- all output data is a
    # child of an `output???' directory
    runs = []
    if not isinstance(directories, list):
        directories = [directories]

    for directory in directories:
        try:
            results = subprocess.check_output(['find', directory, '-maxdepth', '3', '-type', 'd',
                                               '-name', 'output???', '-prune'])
            results = [s for s in results.decode('utf-8').split()]
            runs.extend(results)
        except Exception as e:
            logging.error('Error occurred while finding output directories: %s', e)
            return None

    conn, tables = create_database(db, debug=debug)

    if update:
        # this is a bit of a crazy optimisation:
        # pop all the output directories in a temporary table in the database
        # select out only those that haven't already had a file indexed
        # instead of using a 'like' clause, we manually do the string comparison,
        # because sqlite doesn't realise it can use our index on the ncfile column...
        metadata = MetaData()
        cand = Table('candidates', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('rundir', Text),
                     Column('rundir2', Text))
        conn.execute('CREATE TEMP TABLE candidates (id INTEGER PRIMARY KEY, rundir TEXT, rundir2 TEXT)')
        conn.execute(cand.insert(), [{'rundir': run, 'rundir2': run[:-1] + chr(ord(run[-1]) + 1)} for run in runs])
        s = select([cand.c.rundir]).where(sql.not_(exists()
                                                   .where(tables['ncfiles'].c.ncfile >= cand.c.rundir)
                                                   .where(tables['ncfiles'].c.ncfile < cand.c.rundir2)))
        r = conn.execute(s)
        runs = [run[0] for run in r.fetchall()]

    # perform the indexing on the client that we've been provided
    futures = client.map(index_run, runs, db=db, update=update)
    i = 0
    n = len(runs)
    print('{}/{}'.format(i, n), end='', flush=True)

    for future, result in as_completed(futures, with_results=True):
        i += 1
        print('\r{}/{}'.format(i, n), end='', flush=True)

        if result is None: continue

        for ncfile in result:
            r = conn.execute(tables['ncfiles'].insert(), ncfile)

            if not ncfile['present']: continue
            file_id = r.inserted_primary_key[0]

            ncvars = ncfile['vars']
            for v in ncvars:
                v.update(ncfile=file_id)
            conn.execute(tables['ncvars'].insert(), ncvars)
