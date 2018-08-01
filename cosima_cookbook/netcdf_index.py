"""
Common tools for accessing NetCDF4 variables
"""

print('netcdf_index loaded.')

__all__ = ['build_index', 'get_nc_variable',
           'get_experiments', 'get_configurations',
           'get_variables', 'get_ncfiles']

import netCDF4
import dataset
import re
import os
import sys
import fnmatch
import dask.bag
import distributed
from distributed.diagnostics.progressbar import progress
import xarray as xr
import subprocess
import tqdm
import IPython.display

import logging
logging.basicConfig(level=logging.INFO)

directoriesToSearch = ['/g/data3/hh5/tmp/cosima/',
                       '/g/data1/v45/APE-MOM',
                      ]

cosima_cookbook_dir = '/g/data3/hh5/tmp/cosima/cosima-cookbook'
database_file = '{}/cosima-cookbook.db'.format(cosima_cookbook_dir)
database_url = 'sqlite:///{}'.format(database_file)

def build_index(use_bag=False):
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
    runs_available = []

    print('Finding runs on disk... ', end='')
    for directoryToSearch in directoriesToSearch:
        #print('Searching {}'.format(directoryToSearch))

        # find all subdirectories
        try:
            results = subprocess.check_output(['find', directoryToSearch, '-maxdepth', '3', '-type', 'd',
                '-name', 'output???'])
            results = [s for s in results.decode('utf-8').split()]
            runs_available.extend(results)
        except:
            print ('{0} exception occurred while finding output directories in {1}'.format(sys.exc_info()[0], directoryToSearch))

        
    print('found {} run directories'.format( len(runs_available)))

    #ncfiles.extend(results)
#
#    results = subprocess.check_output(['find', directoryToSearch, '-name', '*.nc'])
#
#    print('Found {} .nc files'.format(len(ncfiles)))

    # We can persist this index by storing it in a sqlite database placed in a centrally available location.

    # The use of the `dataset` module hides the details of working with SQL directly.

    # In this database is a single table listing all variables in NetCDF4 seen previously.
    print('Using database {}'.format(database_url))
    print('Querying database... ', end='')

    db = dataset.connect(database_url)

    # find list of all run directories
    r = db.query('SELECT DISTINCT rootdir, configuration, experiment, run FROM ncfiles')

    runs_already_seen = [os.path.join(*row.values())
                         for row in r]

    print('runs already indexed: {}'.format(len(runs_already_seen)))

    runs_to_index = list(set(runs_available) - set(runs_already_seen))

    if len(runs_to_index) == 0:
        print("No new runs found.")
        return

    print('{} new run directories found including... '.format(len(runs_to_index)))

    for i in range(min(3, len(runs_to_index))):
        print(runs_to_index[i])
    if len(runs_to_index) > 3:
        print('...')

    print('Finding files on disk... ')
    ncfiles = []
    for run in tqdm.tqdm_notebook(runs_to_index, leave=True):
        try:
            results = subprocess.check_output(['find', run, '-name', '*.nc'])
            results = [s for s in results.decode('utf-8').split()]
            ncfiles.extend(results)
        except:
            print ('{0} exception occurred while finding *.nc in {1}'.format(sys.exc_info()[0], run))

    IPython.display.clear_output(wait=True)
    
    # NetCDF files found on disk not seen before:
    #files_to_add = set(ncfiles) - set(files_already_seen)

    files_to_add = ncfiles

    print('Files found but not yet indexed: {}'.format(len(files_to_add)))

    # For these new files, we can determine their configuration, experiment, and run.
    # Using NetCDF4 to get list of all variables in each file.

    # output* directories
    # match the parent and grandparent directory to configuration/experiment
    find_output = re.compile('(.*)/([^/]*)/([^/]*)/(output\d+)/.*\.nc')

    # determine general pattern for ncfile names
    find_basename_pattern = re.compile('(?P<root>[^\d]+)(?P<index>__\d+_\d+)?(?P<indexice>\.\d+\-\d+)?(?P<ext>\.nc)')

    def index_variables(ncfile):

        matched = find_output.match(ncfile)
        if matched is None:
            return []

        if not os.path.exists(ncfile):
            return []

        basename = os.path.basename(ncfile)
        m = find_basename_pattern.match(basename)
        if m is None:
            basename_pattern = basename
        else:
            basename_pattern = m.group('root') + ('__\d+_\d+' if m.group('index') else '') + ('.\d+-\d+' if m.group('indexice') else '')+ m.group('ext')

        try:
            with netCDF4.Dataset(ncfile) as ds:
                ncvars = [ {'ncfile': ncfile,
                   'rootdir': matched.group(1),
                   'configuration': matched.group(2),
                   'experiment' : matched.group(3),
                   'run' : matched.group(4),
                   'basename' : basename,
                   'basename_pattern' : basename_pattern,
                   'variable' : v.name,
                   'dimensions' : str(v.dimensions),
                   'chunking' : str(v.chunking()),
                   } for v in ds.variables.values()]
        except:
            print ('{0} exception occurred while trying to read {1}'.format(sys.exc_info()[0], ncfile))
            ncvars = []

        return ncvars

    if len(files_to_add) == 0:
        print("No new .nc files found.")
        return True

    print('Indexing new .nc files... ')

    if use_bag:
        with distributed.Client() as client:
            bag = dask.bag.from_sequence(files_to_add)
            bag = bag.map(index_variables).flatten()

            futures = client.compute(bag)
            progress(futures, notebook=False)

            ncvars = futures.result()
    else:
        ncvars = []
        for file_to_add in tqdm.tqdm_notebook(files_to_add, leave=False):
            ncvars.extend(index_variables(file_to_add))
        IPython.display.clear_output()
        
    print('')
    print('Found {} new variables'.format(len(ncvars)))

    print('Saving results in database... ')
    db['ncfiles'].insert_many(ncvars)

    print('Indexing complete.')

    return True


def get_configurations():
    """
    Returns list of all configurations
    """
    db = dataset.connect(database_url)

    rows = db.query('SELECT DISTINCT configuration FROM ncfiles')
    configurations = [row['configuration'] for row in rows]

    return configurations


def get_experiments(configuration):
    """
    Returns list of all experiments for the given configuration
    """
    db = dataset.connect(database_url)

    rows = db.query('SELECT DISTINCT experiment FROM ncfiles '
                'WHERE configuration = "{configuration}" ORDER BY experiment'.format(configuration=configuration), )
    expts = [row['experiment'] for row in rows]

    return expts

def get_ncfiles(expt):
    """
    Returns list of ncfiles for the given experiment
    """
    with dataset.connect(database_url) as db:
        rows = db.query('SELECT DISTINCT basename_pattern '
                        'FROM ncfiles '
                        f'WHERE experiment = "{expt}"')
        ncfiles = [row['basename_pattern'] for row in rows]

    return ncfiles


def get_variables(expt, ncfile):
    """
    Returns list of variables available in given experiment
    and ncfile basename pattern
    
    
    ncfile can use glob syntax http://www.sqlitetutorial.net/sqlite-glob/
    and regular expressions also work in some limited cases.
    """

    with dataset.connect(database_url) as db:
        rows = db.query('SELECT DISTINCT variable '
                        'FROM ncfiles '
                        f'WHERE experiment = "{expt}" '
                        f'AND (basename_pattern = "{ncfile}" '
                        f'OR basename GLOB "{ncfile}")'
                        )
        variables = [row['variable'] for row in rows]

    return variables
    
def get_nc_variable(expt, ncfile,
                    variable, chunks={}, n=None,
                    op=None, 
                    time_units="days since 1900-01-01",
                    use_bag = False):
    """
    For a given experiment, concatenate together
    variable over all time given a basename ncfile.

    Since some NetCDF4 files have trailing integers (e.g. ocean_123_456.nc)
    ncfile can use glob syntax http://www.sqlitetutorial.net/sqlite-glob/
    and regular expressions also work in some limited cases.

    By default, xarray is set to use the same chunking pattern that is
    stored in the ncfile. This can be overwritten by passing in a dictionary
    chunks or setting chunks=None for no chunking (load directly into memory).

    n < 0 means only use the last n ncfiles files. 
    n > 0 means only use the first ncfiles files.

    op() is function to apply to each variable before concatenating.

    time_units (e.g. "days since 1600-01-01") can be used to override
    the original time.units.  If time_units=None, no overriding is performed.

    if variable is a list, then return a dataset for all given variables
    """

    if '/' in expt:
        configuration, experiment = expt.split('/')
    else:
        experiment = expt

    if not isinstance(variable, list):
        variables = [variable]
        return_dataarray = True
    else:
        variables = variable
        return_dataarray = False

    db = dataset.connect(database_url)

    var_list = ",".join(['"{}"'.format(v) for v in variables])

    sql = " ".join(['SELECT DISTINCT ncfile, dimensions, chunking ',
                    'FROM ncfiles',
                    f'WHERE experiment = "{experiment}"',
                    'AND (',
                    f'basename_pattern = "{ncfile}"',
                    f'OR basename GLOB "{ncfile}"',
                    ')',
                    f'AND variable in ({var_list})',
                    'ORDER BY ncfile'])

    logging.debug(sql)

    res = db.query(sql)
    rows = list(res)

    ncfiles = [row['ncfile'] for row in rows]
    
    #res.close()
    
    if len(ncfiles) == 0:
        raise ValueError("No variable {} found for {} in {}".format(variable, expt, ncfile))

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
        if n<0:
            ncfiles = ncfiles[n:]
        else:
            ncfiles = ncfiles[:n]

    if op is None:
        op = lambda x: x


    #print ('Opening {} ncfiles...'.format(len(ncfiles)))
    logging.debug(f'Opening {len(ncfiles)} ncfiles...')

    if use_bag:
        bag = dask.bag.from_sequence(ncfiles)
        
        load_variable = lambda ncfile: xr.open_dataset(ncfile, 
                           chunks=chunks, 
                           decode_times=False)[variables]
        #bag = bag.map(load_variable, chunks, time_units, variables)
        bag = bag.map(load_variable)
        
        dataarrays = bag.compute()
    else:
        dataarrays = []
        for ncfile in tqdm.tqdm_notebook(ncfiles,
            desc='get_nc_variable:', leave=False):
            dataarray = xr.open_dataset(ncfile, chunks=chunks, decode_times=False)[variables]

            #dataarray = op(dataarray)

            dataarrays.append(dataarray)

    #print ('Building dataarray.')

    dataarray = xr.concat(dataarrays,
                          dim='time', coords='all', )

    
    if 'time' in dataarray.coords:
        if time_units is None:
            time_units = dataarray.time.units
        if dataarray.time[0] > 6.e+5:           ## AH: This is a brazen hack ... sorry!!
            time_units = dataarray.time.units
        try:
            decoded_time = xr.conventions.times.decode_cf_datetime(dataarray.time, time_units)
        except:  # for compatibility with older xarray (pre-0.10.2 ?)
            decoded_time = xr.conventions.decode_cf_datetime(dataarray.time, time_units)
        dataarray.coords['time'] = ('time', decoded_time,
                                    {'long_name' : 'time', 'decoded_using' : time_units }
                                   )

    #print ('Dataarray constructed.')

    if return_dataarray:
        return dataarray[variable]
    else:
        return dataarray

def get_scalar_variables(configuration):
    db = dataset.connect(database_url)

    rows = db.query('SELECT DISTINCT variable FROM ncfiles '
         'WHERE basename = "ocean_scalar.nc" '
         'AND dimensions = "(\'time\', \'scalar_axis\')" '
         'AND configuration = "{configuration}"'.format(configuration=configuration))
    variables = [row['variable'] for row in rows]

    return variables
