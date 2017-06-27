# calculate EKE and eddy heat transports from KDS75
# written by Kial Stewart

import netCDF4 as nc
import numpy as np
import sys
import glob
import scipy.io
import os

gravity = 9.81
omega = 7.2921e-5

member = 1
start_quart = 146 + ((member-1)*4)
model_year = 40 + (member-1)

pentdays_by_quart = [18,19,19,19]
startday_by_quart = [1, 91, 182, 274]

gridfile = nc.Dataset('/short/v45/pas561/mom/archive/kds75_cp/output146/rregionoceankerg__0040_001.nc','r')
xt_ocean = gridfile.variables['xt_ocean_sub01'][:]
yt_ocean = gridfile.variables['yt_ocean_sub01'][:]
xu_ocean = gridfile.variables['xu_ocean_sub01'][:]
yu_ocean = gridfile.variables['yu_ocean_sub01'][:]
st_ocean = gridfile.variables['st_ocean'][:]
sw_ocean = gridfile.variables['sw_ocean'][:]
gridfile.close()

xn = len(xu_ocean) # number of longitude points
yn = len(yu_ocean) # number of latitude points
zn = len(sw_ocean) # number of depth levels

#kk=0
for kk in np.arange(zn):
	u_cum = np.ma.zeros([yn,xn])
	u_squared_cum = np.ma.zeros([yn,xn])
	v_cum = np.ma.zeros([yn,xn])
	v_squared_cum = np.ma.zeros([yn,xn])
	uv_cum = np.ma.zeros([yn,xn])
	t_cum = np.ma.zeros([yn,xn])
	ut_cum = np.ma.zeros([yn,xn])
	vt_cum = np.ma.zeros([yn,xn])
	count = 0
	for qq in np.arange(4):
		for pp in np.arange(pentdays_by_quart[qq]):
			gridfile = nc.Dataset('/short/v45/pas561/mom/archive/kds75_cp/output'+(repr(start_quart)).zfill(3)+'/rregionoceankerg__'+(repr(model_year)).zfill(4)+'_'+(repr(startday_by_quart[qq]+(pp*5))).zfill(3)+'.nc','r')
			num_o_days = len(gridfile.variables['time'][:])
			for dd in np.arange(num_o_days):
				print "day number ", dd+startday_by_quart[qq]+(pp*5)
#				u_tmp = np.ma.zeros([yn,xn])
				u_tmp = gridfile.variables['u'][dd,kk,:,:]
				u_cum += u_tmp
				u_squared_cum += (u_tmp)**2.0
#				v_tmp = np.ma.zeros([yn,xn])
				v_tmp = gridfile.variables['v'][dd,kk,:,:]
				v_cum += v_tmp
				v_squared_cum += (v_tmp)**2.0
				uv_cum += (u_tmp)*(v_tmp)
#				t_tmp_t = np.ma.zeros([yn,xn])
				t_tmp_t = gridfile.variables['temp'][dd,kk,:,:]
#				t_tmp_u = np.ma.zeros([yn,xn])
				t_tmp_u[1:,1:] = (t_tmp_t[:-1,:-1]+t_tmp_t[:-1,1:]+t_tmp_t[1:,:-1]+t_tmp_t[1:,1:])/4.0
				t_cum += t_tmp_u
				ut_cum += (u_tmp)*(t_tmp_u)
				vt_cum += (v_tmp)*(t_tmp_u)
				count += 1
			gridfile.close()
	eddyfile = nc.Dataset('/g/data1/v45/kxs157/analysis/ANT_WARM/KDS75_cp_EKE_member_'+(repr(member)).zfill(2)+'.nc', 'a')
	eddyfile.variables['u_bar'][0,kk,:,:] = u_cum/count
	eddyfile.variables['v_bar'][0,kk,:,:] = v_cum/count
	eddyfile.variables['uv_bar'][0,kk,:,:] = uv_cum/count
	eddyfile.variables['u_squared_bar'][0,kk,:,:] = u_squared_cum/count
	eddyfile.variables['v_squared_bar'][0,kk,:,:] = v_squared_cum/count
	eddyfile.variables['u_prime_squared_bar'][0,kk,:,:] = (u_squared_cum/count)-(u_cum/count)*(u_cum/count)
	eddyfile.variables['v_prime_squared_bar'][0,kk,:,:] = (v_squared_cum/count)-(v_cum/count)*(v_cum/count)
	eddyfile.variables['u_prime_v_prime_bar'][0,kk,:,:] = (uv_cum/count)-(u_cum/count)*(v_cum/count)
	eddyfile.variables['u_prime_t_prime_bar'][0,kk,:,:] = (ut_cum/count)-(u_cum/count)*(t_cum/count)
	eddyfile.variables['v_prime_t_prime_bar'][0,kk,:,:] = (vt_cum/count)-(v_cum/count)*(t_cum/count)
	eddyfile.variables['eke_bar'][0,kk,:,:] = 0.5*(((u_squared_cum/count)-(u_cum/count)*(u_cum/count))+((v_squared_cum/count)-(v_cum/count)*(v_cum/count)))
	eddyfile.variables['tke_bar'][0,kk,:,:] = 0.5*((u_squared_cum/count)+(v_squared_cum/count))
	eddyfile.variables['ut_bar'][0,kk,:,:] = ut_cum/count
	eddyfile.variables['vt_bar'][0,kk,:,:] = vt_cum/count
	eddyfile.close()
# 
# eddyfile = nc.Dataset('/g/data1/v45/kxs157/analysis/ANT_WARM/KDS75_cp_EKE_member_'+(repr(member)).zfill(2)+'.nc', 'w', format='NETCDF4')
# eddyfile.createDimension('time',None)
# time = eddyfile.createVariable('time','f8',('time',)) #time
# time.long_name = 'time'
# time.units = 'days since 0001-01-01 00:00:00'
# time.cartesian_axis = 'T'
# time.calendar_type = 'NOLEAP'
# time.calendar = 'NOLEAP'
# time.bounds = 'time_bounds'
# eddyfile.createDimension('sw_ocean',zn)
# depth = eddyfile.createVariable('sw_ocean','f8',('sw_ocean',)) #depth
# depth.long_name = 'depth'
# depth.units = 'meters'
# depth.cartesian_axis = 'Z'
# depth.positive = 'down'
# eddyfile.variables['sw_ocean'][:] = sw_ocean
# eddyfile.createDimension('yu_ocean',yn)
# eddyfile.createDimension('xu_ocean',xn)
# lat = eddyfile.createVariable('yu_ocean','f8',('yu_ocean',)) #yu_ocean
# lat.long_name = 'latitude'
# lat.units = 'degrees_N'
# lat.cartesian_axis = 'Y'
# eddyfile.variables['yu_ocean'][:] = yu_ocean
# lon = eddyfile.createVariable('xu_ocean','f8',('xu_ocean',)) #xu_ocean
# lon.long_name = 'longitude'
# lon.units = 'degrees_E'
# lon.cartesian_axis = 'X'
# eddyfile.variables['xu_ocean'][:] = xu_ocean
# ussh_bar = eddyfile.createVariable('u_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# ussh_bar.long_name = 'u_mean'
# ussh_bar.units = 'ms-1'
# ussh_bar.valid_range = -1000., 1000.
# ussh_bar.missing_value = 1.e+20
# ussh_bar.cell_methods = 'time: mean'
# ussh_bar.coordinates = 'lon lat'
# ussh_bar.standard_name = 'u_mean'
# vssh_bar = eddyfile.createVariable('v_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# vssh_bar.long_name = 'v_mean'
# vssh_bar.units = 'ms-1'
# vssh_bar.valid_range = -1000., 1000.
# vssh_bar.missing_value = 1.e+20
# vssh_bar.cell_methods = 'time: mean'
# vssh_bar.coordinates = 'lon lat'
# vssh_bar.standard_name = 'v_mean'
# uvssh_bar = eddyfile.createVariable('uv_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# uvssh_bar.long_name = 'uv_mean'
# uvssh_bar.units = 'm2s-2'
# uvssh_bar.valid_range = -0., 1000.
# uvssh_bar.missing_value = 1.e+20
# uvssh_bar.cell_methods = 'time: mean'
# uvssh_bar.coordinates = 'lon lat'
# uvssh_bar.standard_name = 'uv_mean'
# uussh_bar = eddyfile.createVariable('u_squared_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# uussh_bar.long_name = 'u_squared_mean'
# uussh_bar.units = 'm2s-2'
# uussh_bar.valid_range = -0., 1000.
# uussh_bar.missing_value = 1.e+20
# uussh_bar.cell_methods = 'time: mean'
# uussh_bar.coordinates = 'lon lat'
# uussh_bar.standard_name = 'u_squared_mean'
# vvssh_bar = eddyfile.createVariable('v_squared_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# vvssh_bar.long_name = 'v_squared_mean'
# vvssh_bar.units = 'm2s-2'
# vvssh_bar.valid_range = -0., 1000.
# vvssh_bar.missing_value = 1.e+20
# vvssh_bar.cell_methods = 'time: mean'
# vvssh_bar.coordinates = 'lon lat'
# vvssh_bar.standard_name = 'v_squared_mean'
# up2ssh_bar = eddyfile.createVariable('u_prime_squared_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# up2ssh_bar.long_name = 'u_prime_squared_mean'
# up2ssh_bar.units = 'm2s-2'
# up2ssh_bar.valid_range = -0., 1000.
# up2ssh_bar.missing_value = 1.e+20
# up2ssh_bar.cell_methods = 'time: mean'
# up2ssh_bar.coordinates = 'lon lat'
# up2ssh_bar.standard_name = 'u_prime_squared_mean'
# vp2ssh_bar = eddyfile.createVariable('v_prime_squared_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# vp2ssh_bar.long_name = 'v_prime_squared_mean'
# vp2ssh_bar.units = 'm2s-2'
# vp2ssh_bar.valid_range = -0., 1000.
# vp2ssh_bar.missing_value = 1.e+20
# vp2ssh_bar.cell_methods = 'time: mean'
# vp2ssh_bar.coordinates = 'lon lat'
# vp2ssh_bar.standard_name = 'v_prime_squared_mean'
# upvpssh_bar = eddyfile.createVariable('u_prime_v_prime_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# upvpssh_bar.long_name = 'u_prime_v_prime_mean'
# upvpssh_bar.units = 'm2s-2'
# upvpssh_bar.valid_range = -1000., 1000.
# upvpssh_bar.missing_value = 1.e+20
# upvpssh_bar.cell_methods = 'time: mean'
# upvpssh_bar.coordinates = 'lon lat'
# upvpssh_bar.standard_name = 'u_prime_v_prime_mean'
# uptpssh_bar = eddyfile.createVariable('u_prime_t_prime_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# uptpssh_bar.long_name = 'u_prime_t_prime_mean'
# uptpssh_bar.units = 'oCms-1'
# uptpssh_bar.valid_range = -1000., 1000.
# uptpssh_bar.missing_value = 1.e+20
# uptpssh_bar.cell_methods = 'time: mean'
# uptpssh_bar.coordinates = 'lon lat'
# uptpssh_bar.standard_name = 'u_prime_t_prime_mean'
# vptpssh_bar = eddyfile.createVariable('v_prime_t_prime_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# vptpssh_bar.long_name = 'v_prime_t_prime_mean'
# vptpssh_bar.units = 'oCms-1'
# vptpssh_bar.valid_range = -1000., 1000.
# vptpssh_bar.missing_value = 1.e+20
# vptpssh_bar.cell_methods = 'time: mean'
# vptpssh_bar.coordinates = 'lon lat'
# vptpssh_bar.standard_name = 'v_prime_t_prime_mean'
# ekessh_bar = eddyfile.createVariable('eke_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# ekessh_bar.long_name = 'eke_mean'
# ekessh_bar.units = 'm2s-2'
# ekessh_bar.valid_range = -0., 1000.
# ekessh_bar.missing_value = 1.e+20
# ekessh_bar.cell_methods = 'time: mean'
# ekessh_bar.coordinates = 'lon lat'
# ekessh_bar.standard_name = 'eke_mean'
# tkessh_bar = eddyfile.createVariable('tke_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# tkessh_bar.long_name = 'tke_mean'
# tkessh_bar.units = 'm2s-2'
# tkessh_bar.valid_range = -0., 1000.
# tkessh_bar.missing_value = 1.e+20
# tkessh_bar.cell_methods = 'time: mean'
# tkessh_bar.coordinates = 'lon lat'
# tkessh_bar.standard_name = 'tke_mean'
# utssh_bar = eddyfile.createVariable('ut_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# utssh_bar.long_name = 'ut_mean'
# utssh_bar.units = 'oCms-1'
# utssh_bar.valid_range = -0., 1000.
# utssh_bar.missing_value = 1.e+20
# utssh_bar.cell_methods = 'time: mean'
# utssh_bar.coordinates = 'lon lat'
# utssh_bar.standard_name = 'ut_mean'
# vtssh_bar = eddyfile.createVariable('vt_bar','f8',('time','sw_ocean','yu_ocean','xu_ocean',),fill_value=1.e+20)
# vtssh_bar.long_name = 'vt_mean'
# vtssh_bar.units = 'oCms-1'
# vtssh_bar.valid_range = -0., 1000.
# vtssh_bar.missing_value = 1.e+20
# vtssh_bar.cell_methods = 'time: mean'
# vtssh_bar.coordinates = 'lon lat'
# vtssh_bar.standard_name = 'vt_mean'
# eddyfile.close()
# 
# 































