from joblib import Memory

cachedir = '/g/data1/v45/cosima-cookbook'
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