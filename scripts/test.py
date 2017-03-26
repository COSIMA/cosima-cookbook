import xarray as xr
import dask

dask.set_options(get=dask.multiprocessing.get)

filename = '/g/data1/v45/mom01_comparison/KDS75/output165/ocean.nc'

dsx = xr.open_dataset(filename, decode_times=False, lock=True)

chunked = dsx.temp.chunk((1,7,300,400))
print(chunked)
chunked.load()
#temp = chunked.values
