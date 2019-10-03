from ..memory import memory
from ..querying import getvar

@memory.cache
def mean_tau_x(expt):
    """
    10-year zonal average of horizontal wind stress.
    """
    tau_x = get_nc_variable(expt,
                            'ocean_month.nc',
                            'tau_x',
                            time_units = 'days since 1900-01-01',
                            n=10)

    mean_tau_x = tau_x.mean('xu_ocean').mean('time')
    mean_tau_x = mean_tau_x.compute()
    mean_tau_x.name = 'mean_tau_x'

    return mean_tau_x
