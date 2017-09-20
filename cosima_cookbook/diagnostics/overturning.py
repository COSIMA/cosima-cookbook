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
