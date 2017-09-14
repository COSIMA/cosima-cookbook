from joblib import Memory

cachedir = None
memory = Memory(cachedir=cachedir, verbose=0)

from ..netcdf_index import get_nc_variable

@memory.cache
def annual_scalar(expt, variable):
    darray = get_nc_variable(expt, 'ocean_scalar.nc', variable,
                              time_units='days since 2000-01-01')
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

