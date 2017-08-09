from joblib import Memory

cachedir = '/g/data1/v45/cosima-cookbook'
memory = Memory(cachedir=cachedir, verbose=0)

from ..netcdf_index import get_nc_variable

@memory.cache
def psi_avg(expt):

    op = lambda p: p.sum('grid_xt_ocean').cumsum('potrho')

    psi = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho',
                          op=op,
                          chunks={'potrho': None}, n=25)

    psi_avg = psi.mean('time')
    psi_avg = psi_avg.compute()
    
    return psi_avg