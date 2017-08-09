import dask
#import dask.bag
#from dask.distributed import Client

#import pandas as pd
#import xarray as xr
#from glob import glob
#import re
#import netCDF4

#print('starting distributed client...')
#client = Client()
#print(client)

from joblib import Memory

cachedir = '/g/data1/v45/cosima-cookbook'
memory = Memory(cachedir=cachedir, verbose=0)

from ..netcdf_index import get_nc_variable

@memory.cache
def mean_tau_x(expt):
    """
    10-year zonal average of horizontal wind stress.
    """
    tau_x = get_nc_variable(expt, 
                            'ocean_month.nc', 
                            'tau_x',
                            time_units = 'days since 2000-01-01',
                            n=10)

    mean_tau_x = tau_x.mean('xu_ocean').mean('time')
    mean_tau_x = mean_tau_x.compute()
    mean_tau_x.name = 'mean_tau_x'
    
    return mean_tau_x