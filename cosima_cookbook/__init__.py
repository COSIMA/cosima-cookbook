# -*- coding: utf-8 -*-
"""
Common tools for working with COSIMA model output
"""

import numpy as np
import os

import dask
import dask.bag
from dask.distributed import Client

import pandas as pd
import xarray as xr
from glob import glob
import re
import netCDF4

import tempfile
from joblib import Memory

from tqdm import tqdm_notebook, tqdm

cachedir = tempfile.gettempdir()
memory = Memory(cachedir=cachedir, verbose=0)

import warnings 
warnings.filterwarnings("ignore", 
                        message="Unable to decode time axis into full numpy.datetime64 objects")

# hardcoded -- this is meant to be run on a NCI system
DataDir = '/g/data3/hh5/tmp/cosima'

# find all experiments with at least one outputNNN subdirectory
expts = sorted({ re.search(DataDir + '/' + '(.*)' + '/output', d).group(1) 
	         for d in glob(os.path.join(DataDir , '*/*/output*') )})

def get_expt():
    cwd = os.getcwd()
    _, expt = cwd.split('cosima-cookbook/configurations/')
    return expt


@memory.cache
def index_ncfile(ncpath):
    """
    Give an ncfile, create an index of all the variables, dimensions, chunking information
    along with the metadata for that file.
    """

    dataset = netCDF4.Dataset(ncpath)
 
    variables = [ { 'name' : v.name,
                     'dimensions' : v.dimensions,
                     'chunking' : tuple(v.chunking()), }
		  for v in dataset.variables.values() ]

    dataset.close()   

    ncfile = os.path.basename(ncpath)
 
    # extract out experiment from path
    expt = re.search(DataDir + '/' + '(.*)' + '/output', ncpath).group(1) 

    index = {'ncfile': ncfile,
             'ncpath': ncpath,
             'configuration' : '',
             'experiment' : expt,
             'run' : '', 
             'variables': variables,
            }

    return index

@memory.cache    
def build_index():
    """
    An experiment is a collection of outputNNN directories.  Each directory 
    represents the output of a single job submission script. These directories 
    are created by the *payu* tool.   

    An experiment is given by a modelconfiguration/experimentname
 
    The file `diag_table` identifies which fields should be in the output directory.
 
    But we can also examine the .nc files directly to infer their contents.
    for each .nc file, get variables -> dimensions
 
    .ncfile, varname, dimensions, chunksize

    Generate an index for all netCDF4 files. The results are cached, so 
    this needs only to be done once.
    """

    ncfiles = glob(os.path.join(DataDir, '*/*/output*/*.nc'))
    ncfiles.sort()
    b = dask.bag.from_sequence(ncfiles)
    index = b.map(index_ncfiles)
   
  #  index = list(index)

    index = pd.DataFrame.from_records(index)

    return index


def get_nc_variable(expt, ncfile, variable, chunks={}, n=None,
                   op= lambda x: x):
    """
    For a given experiment, concatenate together variable over all time
    given a basename ncfile.
    
    By default, xarray is set to use the same chunking pattern that is
    stored in the ncfile. This can be overwritten by passing in a dictionary
    chunks or setting chunks=None for no chunking (load directly into memory).
    
    n > 0 means only use the last n ncfiles files. Useful for testing.
    
    op() is function to apply to each variable before concatenating.
    
    """
    
    df = build_index(expt)
    var = df[(df.ncfile.str.contains(ncfile)) & (df.variable == variable)]
    
    chunking = var.chunking.iloc[0]
    dimensions = var.dimensions.iloc[0]  
    default_chunks = dict(zip(dimensions, chunking))
    
    if chunks is not None:
        default_chunks.update(chunks)
        chunks = default_chunks
        
    ncfiles = sorted(list(var.path))
    
    if n is not None:
        ncfiles = ncfiles[-n:]

    b = dask.bag.from_sequence(ncfiles)
    b = b.map(lambda fn : op(xr.open_dataset(fn, chunks=chunks)[variable]) )
    datasets = b.compute()
    
    dsx = xr.concat(datasets, dim='time', coords='all')

    return dsx

class Configuration():
    pass

class Experiment():
    def __init__():
        pass
