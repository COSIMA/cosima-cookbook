from ..netcdf_index import get_nc_variable
from ..memory import memory

import logging

@memory.cache
def annual_scalar(expt, variables):
    """
    """
    
    logging.debug('Building dataset')
    darray = get_nc_variable(expt,
                             'ocean_scalar.nc',
                             variables,
                             time_units='days since 1700-01-01',
                             use_bag=True,
                             )
    
    logging.debug('Resampling in time')
    annual_average = darray.resample('A', 'time').compute()
    
    for v in annual_average.data_vars:

        avar = annual_average.variables[v]
        dvar = darray.variables[v]
        avar.attrs['long_name'] = dvar.attrs['long_name'] + ' (annual average)'
        avar.attrs['units'] = dvar.attrs['units']

    return annual_average


@memory.cache
def drake_passage(expt):
    "Calculate transport through Drake Passage"
    
    tx = get_nc_variable(expt,
                         'ocean_month.nc',
                         'tx_trans_int_z',
                         chunks={'yt_ocean':200},
                         time_units = 'days since 1700-01-01',
                         use_bag=False)
    
    tx_trans = tx.sel(xu_ocean=-69,method='nearest').sel(yt_ocean=slice(-72,-52))
    
    if tx_trans.units == 'Sv (10^9 kg/s)':
        transport = tx_trans.sum('yt_ocean').resample('A','time')
    else:
        #print('WARNING: Changing units for ', expt)
        transport = tx_trans.sum('yt_ocean').resample('A','time')*1.0e-9

    transport.load()
    
    return transport

@memory.cache
def bering_strait(expt):
    ty = get_nc_variable(expt,'ocean_month.nc',
                         'ty_trans_int_z',
                         chunks={'yu_ocean':200},
                         time_units = 'days since 1700-01-01')
    ty_trans = ty.sel(yu_ocean=67,method='nearest').sel(xt_ocean=slice(-171,-167))
    if ty_trans.units == 'Sv (10^9 kg/s)':
        transport = ty_trans.sum('xt_ocean').resample('A','time')
    else:
        #print('WARNING: Changing units for ', expt)
        transport = ty_trans.sum('xt_ocean').resample('A','time')*1.0e-9

    transport.load()
    
    return transport

@memory.cache
def sea_surface_temperature(expt):
    ## Load SST from expt - last 10 outputs (TODO: would prefer to do this by year)
    SST = get_nc_variable(expt, 'ocean_month.nc', 'surface_temp',n=10, time_units = 'days since 1900-01-01')
    #SSS = get_nc_variable(expt, 'ocean.nc', 'temp',n=10,time_units = 'days since 1900-01-01').isel(st_ocean=0)

    if SST.units == 'degrees K':
        SST = SST - 273.15

    # Annual Average  WOA13 long-term climatology.
    ## TODO: Need to generalise this to other resolutions!!
    SST_WOA13 = get_nc_variable('woa13/10', 'woa13_ts_\d+_mom10.nc', 'temp',time_units = 'days since 1900-01-01').isel(ZT=0)

    # Average
    SST = SST.mean('time')
    SSTdiff = SST - SST_WOA13.mean('time').values

    return SST, SSTdiff

@memory.cache
def sea_surface_salinity(expt):
    ## Load SST from expt - last 10 outputs (TODO: would prefer to do this by year)
    SSS = get_nc_variable(expt, 'ocean_month.nc', 'surface_salt',n=10,time_units = 'days since 1900-01-01')
    #SSS = get_nc_variable(expt, 'ocean.nc', 'salt',n=10,time_units = 'days since 1900-01-01').isel(st_ocean=0)

    # Annual Average  WOA13 long-term climatology.
    ## TODO: Need to generalise this to other resolutions!!
    SSS_WOA13 = get_nc_variable('woa13/10', 'woa13_ts_\d+_mom10.nc', 'salt',time_units = 'days since 1900-01-01').isel(ZT=0)


    # Average over last 10 time slices - prefer to do this by year.
    SSS = SSS.mean('time')
    SSSdiff = SSS - SSS_WOA13.mean('time').values

    return SSS, SSSdiff
