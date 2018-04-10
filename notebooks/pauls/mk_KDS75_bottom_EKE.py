# calculate terms of K, L, M, N
# written by Kial Stewart

import netCDF4 as nc
import numpy as np
import pylab as pl
import sys
import glob
import scipy.io
import os
import matplotlib
import matplotlib.pyplot as plt
import gsw
from matplotlib.colors import LinearSegmentedColormap

gravity = 9.81
omega = 7.2921e-5
num_dirs = 4
dir_num_1 = 258

# Load in the dimension and variables
gridfile = nc.Dataset('/g/data1/v45/pas561/mom/archive/KDS75/botvel_kds75_258.nc','r') # '+repr(dir_num_1)+'
xu_ocean = gridfile.variables['xu_ocean'][:]
yu_ocean = gridfile.variables['yu_ocean'][:]
hu = gridfile.variables['hu'][:,:]
gridfile.close()

xn = len(xu_ocean) #3600 # number of longitude points
yn = len(yu_ocean) #2700 # number of latitude points

bot_u_sum = np.ma.zeros([yn,xn])
bot_v_sum = np.ma.zeros([yn,xn])
bot_u2_sum = np.ma.zeros([yn,xn])
bot_v2_sum = np.ma.zeros([yn,xn])
bot_uv_sum = np.ma.zeros([yn,xn])
counter = 0

for dd in range(num_dirs):
	gridfile = nc.Dataset('/g/data1/v45/pas561/mom/archive/KDS75/botvel_kds75_'+repr(dir_num_1+dd)+'.nc','r') #
	file_time = gridfile.variables['time'][:]
	for tt in range(len(file_time)):
		bot_u_sum += gridfile.variables['ubot'][tt,:,:]
		bot_v_sum += gridfile.variables['vbot'][tt,:,:]
		bot_u2_sum += gridfile.variables['ubot'][tt,:,:]**2
		bot_v2_sum += gridfile.variables['vbot'][tt,:,:]**2
		bot_uv_sum += gridfile.variables['ubot'][tt,:,:]*gridfile.variables['vbot'][tt,:,:]
		counter += 1
		print "day ", tt, " of dir", dd
	gridfile.close()



up2_bot = np.ma.zeros([yn,xn])
vp2_bot = np.ma.zeros([yn,xn])
upvp_bot = np.ma.zeros([yn,xn])

up2_bot = (bot_u2_sum/counter) - (bot_u_sum/counter)*(bot_u_sum/counter)
vp2_bot = (bot_v2_sum/counter) - (bot_v_sum/counter)*(bot_v_sum/counter)
upvp_bot = (bot_uv_sum/counter) - (bot_u_sum/counter)*(bot_v_sum/counter)


K_bot = np.ma.zeros([yn,xn])
L_bot = np.ma.zeros([yn,xn])
M_bot = np.ma.zeros([yn,xn])
N_bot = np.ma.zeros([yn,xn])

K_bot = (up2_bot + vp2_bot)/2.0
M_bot = (up2_bot - vp2_bot)/2.0
N_bot = upvp_bot
L_bot = np.sqrt((M_bot**2) + (N_bot**2))


## make the netcdf file to save the terms

eddyfile = nc.Dataset('/g/data1/v45/kxs157/analysis/KDS75_bottom_terms_for_Paul.nc', 'w', format='NETCDF4')
eddyfile.createDimension('time',None)
time = eddyfile.createVariable('time','f8',('time',)) #time
time.long_name = 'time'
time.units = 'days since 0001-01-01 00:00:00'
time.cartesian_axis = 'T'
time.calendar_type = 'NOLEAP'
time.calendar = 'NOLEAP'
time.bounds = 'time_bounds'
eddyfile.createDimension('yu_ocean',yn)
eddyfile.createDimension('xu_ocean',xn)
lat = eddyfile.createVariable('yu_ocean','f8',('yu_ocean',)) #yu_ocean
lat.long_name = 'ucell latitude'
lat.units = 'degrees_N'
lat.cartesian_axis = 'Y'
eddyfile.variables['yu_ocean'][:] = yu_ocean
lon = eddyfile.createVariable('xu_ocean','f8',('xu_ocean',)) #xu_ocean
lon.long_name = 'ucell longitude'
lon.units = 'degrees_E'
lon.cartesian_axis = 'X'
eddyfile.variables['xu_ocean'][:] = xu_ocean
Fssh_bar = eddyfile.createVariable('bot_K','f8',('time','yu_ocean','xu_ocean',),fill_value=1.e+20)
Fssh_bar.long_name = 'EKE_bottom'
Fssh_bar.units = 'm2s-2'
Fssh_bar.valid_range = -1000., 1000.
Fssh_bar.missing_value = 1.e+20
Fssh_bar.cell_methods = 'time: mean'
Fssh_bar.coordinates = 'lon lat'
Fssh_bar.standard_name = 'EKE_bottom'
Fssh_bar = eddyfile.createVariable('bot_L','f8',('time','yu_ocean','xu_ocean',),fill_value=1.e+20)
Fssh_bar.long_name = 'L_bottom'
Fssh_bar.units = 'm2s-2'
Fssh_bar.valid_range = -1000., 1000.
Fssh_bar.missing_value = 1.e+20
Fssh_bar.cell_methods = 'time: mean'
Fssh_bar.coordinates = 'lon lat'
Fssh_bar.standard_name = 'L_bottom'
Fssh_bar = eddyfile.createVariable('bot_M','f8',('time','yu_ocean','xu_ocean',),fill_value=1.e+20)
Fssh_bar.long_name = 'M_bottom'
Fssh_bar.units = 'm2s-2'
Fssh_bar.valid_range = -1000., 1000.
Fssh_bar.missing_value = 1.e+20
Fssh_bar.cell_methods = 'time: mean'
Fssh_bar.coordinates = 'lon lat'
Fssh_bar.standard_name = 'M_bottom'
Fssh_bar = eddyfile.createVariable('bot_N','f8',('time','yu_ocean','xu_ocean',),fill_value=1.e+20)
Fssh_bar.long_name = 'N_bottom'
Fssh_bar.units = 'm2s-2'
Fssh_bar.valid_range = -1000., 1000.
Fssh_bar.missing_value = 1.e+20
Fssh_bar.cell_methods = 'time: mean'
Fssh_bar.coordinates = 'lon lat'
Fssh_bar.standard_name = 'N_bottom'
eddyfile.close()



eddyfile = nc.Dataset('/g/data1/v45/kxs157/analysis/KDS75_bottom_terms_for_Paul.nc','a')
eddyfile.variables['bot_K'][0,:,:] = K_bot
eddyfile.variables['bot_L'][0,:,:] = L_bot
eddyfile.variables['bot_M'][0,:,:] = M_bot
eddyfile.variables['bot_N'][0,:,:] = N_bot
eddyfile.close()







