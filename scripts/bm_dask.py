import netCDF4
from dask.delayed import delayed
import dask.array as da
import dask
import numpy as np
import pandas as pd
import xarray as xr

filename = '/g/data1/v45/mom01_comparison/KDS75/output165/ocean.nc'

dsx = xr.open_dataset(filename, decode_times=False, chunks={'st_ocean':1})
print (dsx.temp)

dsx.temp.to_dataframe()

