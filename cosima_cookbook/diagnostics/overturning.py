from joblib import Memory

cachedir = None
memory = Memory(cachedir=cachedir, verbose=0)

from ..netcdf_index import get_nc_variable

@memory.cache
def psi_avg(expt, n=10):

    # modified to remove op by AH - 20/9/17
    #op = lambda p: p.sum('grid_xt_ocean').cumsum('potrho')
    #psi = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho',
    #                      op=op,
    #                      chunks={'potrho': None}, n=25)
    
    psi = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho',
                          chunks={'potrho': None}, n=n,
                          time_units = 'days since 1900-01-01')
    
    if psi.units == 'kg/s':
        #print('WARNING: Changing units for ', expt)
        psi = psi*1.0e-9
    
    psi_avg = psi.sum('grid_xt_ocean').cumsum('potrho').mean('time') - \
                psi.sum('grid_xt_ocean').sum('potrho').mean('time')
    psi_avg = psi_avg.compute()

    return psi_avg


@memory.cache
def psiGM_avg(expt, n=10):

    # modified to remove op by AH - 20/9/17
    #op = lambda p: p.sum('grid_xt_ocean').cumsum('potrho')
    #psi = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho',
    #                      op=op,
    #                      chunks={'potrho': None}, n=25)
    
    psiGM = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho_gm',
                            chunks={'potrho': None}, n=n,
                            time_units = 'days since 1900-01-01')
    
    if psiGM.units == 'kg/s':
        #print('WARNING: Changing units for ', expt)
        psiGM = psiGM*1.0e-9
    
    psiGM_avg = psiGM.sum('grid_xt_ocean').mean('time')
    psiGM_avg = psiGM_avg.compute()

    return psiGM_avg

@memory.cache
def zonal_mean(expt, variable, n=10):
        
    zonal_var = get_nc_variable(expt, 'ocean.nc', variable,
                                chunks={'st_ocean': None},
                                time_units = 'days since 1900-01-01')
    
    # Average over first year. We would prefer to compare with WOA13 long-term average.
    zonal_var0 = zonal_var.sel(time=slice('1900-01-01','1901-01-01')).mean('xt_ocean').mean('time') 
    zonal_var0.compute()
    
    zonal_mean = zonal_var.isel(time=slice(-n,None)).mean('xt_ocean').mean('time')
    zonal_mean.compute()
    zonal_diff = zonal_mean - zonal_var0
    
    return zonal_mean, zonal_diff