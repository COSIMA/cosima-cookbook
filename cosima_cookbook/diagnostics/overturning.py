from ..netcdf_index import get_nc_variable
from ..memory import memory

@memory.cache
def psi_avg(expt, n=10, GM = False):

    def op(p):
        summed_p = p.sum('grid_xt_ocean')
        #summed_p.attrs['units'] = p.units
        return summed_p

    psi = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho',
                       #   op=op,
                          chunks={'potrho': None}, n=n,
                          time_units = 'days since 1700-01-01')
    psi = psi.sum('grid_xt_ocean')

    if GM:
        psiGM = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho_gm',
                          #    op=op,
                              chunks={'potrho': None}, n=n,
                              time_units = 'days since 1700-01-01')
        psiGM = psiGM.sum('grid_xt_ocean')

    #if psi.units == 'kg/s':
        #print('WARNING: Changing units for ', expt)
    psi = psi*1.0e-9
    if GM:
        psiGM = psiGM*1.0e-9

    psi_avg = psi.cumsum('potrho').mean('time') - \
                psi.sum('potrho').mean('time')
    if GM:
        psi_avg = psi_avg + psiGM.mean('time')

    psi_avg.load()

    return psi_avg


@memory.cache
def calc_aabw(expt, GM = False):
    print('Calculating {} timeseries of AABW transport at 55S '.format(expt))

    def op(p):
        summed_p = p.sum('grid_xt_ocean')
        #summed_p.attrs['units'] = p.units
        return summed_p

    psi = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho',
                          op=op,
                          chunks={'potrho': None},
                          time_units = 'days since 1900-01-01')
    if GM:
        psiGM = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho_gm',
                              op=op,
                              chunks={'potrho': None},
                              time_units = 'days since 1900-01-01')

    #if psi.units == 'kg/s':
        #print('WARNING: Changing units for ', expt)
    psi = psi*1.0e-9
    if GM:
        psiGM = psiGM*1.0e-9

    psi_sum = psi.cumsum('potrho') - psi.sum('potrho')
    if GM:
        psi_sum = psi_sum + psiGM

    psi_aabw = psi_sum.sel(method='Nearest',grid_yu_ocean=-55).sel(potrho=slice(1036,None))\
                .min('potrho').resample('3A',dim='time')
    psi_aabw = psi_aabw.compute()

    return psi_aabw


@memory.cache
def calc_amoc(expt, GM = False):
    print('Calculating {} timeseries of AMOC transport at 26N '.format(expt))

    def op(p):
        summed_p = p.sum('grid_xt_ocean')
        #summed_p.attrs['units'] = p.units
        return summed_p

    psi = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho',
                          op=op,
                          chunks={'potrho': None},
                          time_units = 'days since 1900-01-01')

    if GM:
        psiGM = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho_gm',
                              op=op,
                              chunks={'potrho': None},
                              time_units = 'days since 1900-01-01')

    #if psi.units == 'kg/s':
        #print('WARNING: Changing units for ', expt)
    psi = psi*1.0e-9
    if GM:
        psiGM = psiGM*1.0e-9

    psi_sum = psi.cumsum('potrho') - psi.sum('potrho')
    if GM:
        psi_sum = psi_sum + psiGM

    psi_amoc = psi_sum.sel(method='Nearest',grid_yu_ocean=26).sel(potrho=slice(1035.5,None))\
                .max('potrho').resample('3A',dim='time')
    psi_amoc = psi_amoc.compute()

    return psi_amoc


@memory.cache
def calc_amoc_south(expt, GM = False):
    print('Calculating {} timeseries of AMOC transport at 35S '.format(expt))

    def op(p):
        summed_p = p.sum('grid_xt_ocean')
        #summed_p.attrs['units'] = p.units
        return summed_p

    psi = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho',
                          op=op,
                          chunks={'potrho': None},
                          time_units = 'days since 1900-01-01')
    if GM:
        psiGM = get_nc_variable(expt, 'ocean.nc', 'ty_trans_rho_gm',
                              op=op,
                              chunks={'potrho': None},
                              time_units = 'days since 1900-01-01')

    #if psi.units == 'kg/s':
        #print('WARNING: Changing units for ', expt)
    psi = psi*1.0e-9
    if GM:
        psiGM = psiGM*1.0e-9

    psi_sum = psi.cumsum('potrho') - psi.sum('potrho')
    if GM:
        psi_sum = psi_sum + psiGM

    psi_amoc_south = psi_sum.sel(method='Nearest',grid_yu_ocean=-35).sel(potrho=slice(1035.5,None))\
                .max('potrho').resample('3A',dim='time')
    psi_amoc_south = psi_amoc_south.compute()

    return psi_amoc_south

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
