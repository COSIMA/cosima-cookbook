from ..netcdf_index import get_nc_variable
from ..memory import memory


@memory.cache
def annual_scalar(expt, variable):
    darray = get_nc_variable(expt, 'ocean_scalar.nc', variable,
                              time_units='days since 1900-01-01')
    annual_average = darray.resample('A', 'time').load()
    annual_average.attrs['long_name'] = darray.long_name + ' (annual average)'
    annual_average.attrs['units'] = darray.units

    return annual_average


@memory.cache
def drake_passage(expt):
    tx = get_nc_variable(expt,'ocean_month.nc','tx_trans_int_z',chunks={'yt_ocean':200},
                         time_units = 'days since 1900-01-01')
    tx_trans = tx.sel(xu_ocean=-69).sel(yt_ocean=slice(-72,-52))
    if tx_trans.units == 'Sv (10^9 kg/s)':
        transport = tx_trans.sum('yt_ocean').resample('A','time')
    else:
        print('WARNING: Changing units for ', expt)
        transport = tx_trans.sum('yt_ocean').resample('A','time')*1.0e-9

    return transport

@memory.cache
def sea_surface_temperature(expt):
    SST = get_nc_variable(expt, 'ocean.nc', 'temp',time_units = 'days since 1900-01-01').isel(st_ocean=0)

    # Average over first year. We would prefer to compare with WOA13 long-term average.
    SST0 = SST.sel(time=slice('1900-01-01','1901-01-01')).mean('time')

    # Average over last 10 time slices - prefer to do this by year.
    SST = SST.isel(time=slice(-10,None)).mean('time')
    SSTdiff = SST - SST0

    return SST, SSTdiff

@memory.cache
def sea_surface_salinity(expt):
    SSS = get_nc_variable(expt, 'ocean.nc', 'salt',time_units = 'days since 1900-01-01').isel(st_ocean=0)

    # Average over first year. We would prefer to compare with WOA13 long-term average.
    SSS0 = SSS.sel(time=slice('1900-01-01','1901-01-01')).mean('time')

    # Average over last 10 time slices - prefer to do this by year.
    SSS = SSS.isel(time=slice(-10,None)).mean('time')
    SSSdiff = SSS - SSS0

    return SSS, SSSdiff
