import netCDF4
import numpy as np
import pandas as pd
import xarray as xr
import dask

dask.set_options(get=dask.multiprocessing.get)

filename = '/g/data1/v45/mom01_comparison/KDS75/output165/ocean.nc'

dsx = xr.open_dataset(filename, decode_times=False, engine='h5netcdf')

print(dsx)

chunked = dsx.temp.chunk({  'time' : 1, 
                            'st_ocean' : 7, 
                            'yt_ocean' : 300,
                            'xt_ocean' : 400})
print(chunked)
t = chunked.values
#df = chunked.to_dataframe()

