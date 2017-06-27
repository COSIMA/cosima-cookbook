import netCDF4 as nc
import numpy as np

dataset = nc.Dataset('ocean_dataset.nc', 'w')

dataset.createDimension('xu', size=3600)
dataset.createDimension('yu', size=2700)
dataset.createDimension('xt', size=3600)
dataset.createDimension('yt', size=2700)
dataset.createDimension('z', size=75)
dataset.createDimension('time', size=1)

T = dataset.createVariable('T', "f8", ('time', 'z', 'yt', 'xt'))
u = dataset.createVariable('u', "f8", ('time', 'z', 'yu', 'xu'))
v = dataset.createVariable('v', "f8", ('time', 'z', 'yu', 'xu'))

u[0, :, :, :] = np.random.rand(75, 2700, 3600)
v[0, :, :, :] = np.random.rand(75, 2700, 3600)
T[0, :, :, :] = np.random.rand(75, 2700, 3600)

dataset.close()
