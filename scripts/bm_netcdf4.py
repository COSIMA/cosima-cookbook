import netCDF4

filename = '/g/data1/v45/mom01_comparison/KDS75/output165/ocean.nc'

dataset = netCDF4.Dataset(filename)
temp = dataset.variables['temp']

get_slice = lambda i: temp[0, i, :, :].mean()

a = [get_slice(i) for i in range(75)]

dataset.close()

