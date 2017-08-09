"""
Common tools for accessing NetCDF4 variables
"""

__all__ = ['build_index', 'get_nc_variable', 'get_experiments']

import netCDF4
import dataset
import re
import os
import fnmatch
import dask.bag
import distributed
from distributed.diagnostics.progressbar import progress
import xarray as xr
import subprocess

directoriesToSearch = ['/g/data3/hh5/tmp/cosima/',
                       '/g/data1/v45/APE-MOM',
                      ]

cosima_cookbook_dir = '/g/data1/v45/cosima-cookbook'
database_file = '{}/cosima-cookbook.db'.format(cosima_cookbook_dir)
database_url = 'sqlite:///{}'.format(database_file)

def build_index():
    """
    An experiment is a collection of outputNNN directories.  Each directory
    represents the output of a single job submission script. These directories
    are created by the *payu* tool.

    This function creates and/or updates an index cache of variables names
    found in all NetCDF4 files.

    We can also examine the .nc files directly to infer their contents.
    for each .nc file, get variables -> dimensions

    .ncfile, varname, dimensions, chunksize

    """

    # Build index of all NetCDF files found in directories to search.

    ncfiles = []
    for directoryToSearch in directoriesToSearch:
        print('Searching {}'.format(directoryToSearch))
        results = subprocess.check_output(['find', directoryToSearch, '-name', '*.nc'])
        results = [s for s in results.decode('utf-8').split()]
        ncfiles.extend(results)

    print('Found {} .nc files'.format(len(ncfiles)))

    # We can persist this index by storing it in a sqlite database placed in a centrally available location.

    # The use of the `dataset` module hides the details of working with SQL directly.
    # In this database is a single table listing all variables in NetCDF4 seen previously.
    print('Using database {}'.format(database_url))
    db = dataset.connect(database_url)

    files_already_seen = set([_['ncfile'] for _ in db['ncfiles'].distinct('ncfile')])

    print('Files already indexed: {}'.format(len(files_already_seen)))

    # NetCDF files found on disk not seen before:
    files_to_add = set(ncfiles) - set(files_already_seen)

    print('Files found but not yet indexed: {}'.format(len(files_to_add)))

    # For these new files, we can determine their configuration, experiment, and run.
    # Using NetCDF4 to get list of all variables in each file.

    # output* directories
    # match the parent and grandparent directory to configuration/experiment
    m = re.compile('(.*)/([^/]*)/([^/]*)/(output\d+)/.*\.nc')

    def index_variables(ncfile):

        matched = m.match(ncfile)
        if matched is None:
            return []

        if not os.path.exists(ncfile):
            return []

        try:
            with netCDF4.Dataset(ncfile) as ds:
                ncvars = [ {'ncfile': ncfile,
                   'rootdir': matched.group(1),
                   'configuration': matched.group(2),
                   'experiment' : matched.group(3),
                   'run' : matched.group(4),
                   'basename' : os.path.basename(ncfile),
                   'variable' : v.name,
                   'dimensions' : str(v.dimensions),
                   'chunking' : str(v.chunking()),
                   } for v in ds.variables.values()]
        except:
            print ('Exception occurred while trying to read {}'.format(ncfile))
            ncvars = []

        return ncvars

    print('Indexing new .nc files...')
    with distributed.default_client() as client:
        bag = dask.bag.from_sequence(files_to_add)
        bag = bag.map(index_variables).flatten()
        futures = client.compute(bag)
        progress(futures)
        ncvars = futures.result()

    print('')
    print('Found {} new variables'.format(len(ncvars)))

    print('Saving results in database...')
    db['ncfiles'].insert_many(ncvars)

    print('Indexing complete.')

    return True

def get_experiments(configuration):
    """
    Returns list of all experiments for the given configuration
    """
    db = dataset.connect(database_url)
    
    rows = db.query('SELECT DISTINCT experiment FROM ncfiles '
                'WHERE configuration = "{configuration}"'.format(configuration=configuration), )
    expts = [row['experiment'] for row in rows]
    
    return expts

def get_nc_variable(expt, ncfile, variable, chunks={}, n=None,
                   op=None,
                   time_units=None):
    """
    For a given experiment, concatenate together variable over all time
    given a basename ncfile.

    By default, xarray is set to use the same chunking pattern that is
    stored in the ncfile. This can be overwritten by passing in a dictionary
    chunks or setting chunks=None for no chunking (load directly into memory).

    n > 0 means only use the last n ncfiles files. Useful for testing.

    op() is function to apply to each variable before concatenating.

    time_units (e.g. "days since 1600-01-01") can be used to override
    the original time.units

    """

    if '/' in expt:
        configuration, experiment = expt.split('/')
    else:
        experiment = expt

    db = dataset.connect(database_url)

    res = db.query('SELECT ncfile, dimensions, chunking \
                    FROM ncfiles \
                    WHERE experiment = "{}" \
                    AND basename LIKE "%{}%" \
                    AND variable = "{}" \
                    ORDER BY ncfile'.format(experiment, ncfile, variable))

    rows = list(res)

    ncfiles = [row['ncfile'] for row in rows]

    #print('Found {} ncfiles'.format(len(ncfiles)))

    dimensions = eval(rows[0]['dimensions'])
    chunking = eval(rows[0]['chunking'])

    #print ('chunking info', dimensions, chunking)
    if chunking is not None:
        default_chunks = dict(zip(dimensions, chunking))
    else:
        default_chunks = {}

    if chunks is not None:
        default_chunks.update(chunks)
        chunks = default_chunks

    if n is not None:
        #print('using last {} ncfiles only'.format(n))
        ncfiles = ncfiles[-n:]

    if op is None:
        op = lambda x: x

    b = dask.bag.from_sequence(ncfiles)
    b = b.map(lambda fn : op(xr.open_dataset(fn, chunks=chunks,
                                             decode_times=False)[variable]) )

    dataarrays = b.compute()

    dataarray = xr.concat(dataarrays, dim='time', coords='all')

    if 'time' in dataarray.coords:
        if time_units is None:
            time_units = dataarray.time.units

        decoded_time = xr.conventions.decode_cf_datetime(dataarray.time, time_units)
        dataarray.coords['time'] = ('time', decoded_time,
                                    {'long_name' : 'time', 'decoded_using' : time_units }
                                   )

    return dataarray

def get_scalar_variables(configuration):
    db = dataset.connect(database_url)
    
    rows = db.query('SELECT DISTINCT variable FROM ncfiles '
         'WHERE basename = "ocean_scalar.nc" '
         'AND dimensions = "(\'time\', \'scalar_axis\')" '
         'AND configuration = "{configuration}"'.format(configuration=configuration))
    variables = [row['variable'] for row in rows]
    
    return variables
