import matplotlib.pyplot as plt
import cosima_cookbook as cc
from tqdm import tqdm_notebook


def wind_stress(expts=[]):
    """
    Plot zonally averaged wind stress.

    Parameters
    ----------
    expts : str or list of str
        Experiment name(s).
    """

    plt.figure(figsize=(12, 6))

    if not isinstance(expts, list):
        expts = [expts]

    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        mean_tau_x = cc.diagnostics.mean_tau_x(expt)
        plt.plot(mean_tau_x, mean_tau_x.yu_ocean,
                 linewidth=2,
                 label=expt)

    plt.ylim([-70, 65])
    plt.xlim([-0.08, 0.20])
    plt.ylabel('Latitude ($^\circ$N)')
    plt.xlabel('Stress (N m$^{-2}$)')
    plt.legend(fontsize=10, loc='best')


def annual_scalar(expts=[], variables=[]):
    """
    Calculate and plot annual average of variable(s) for experiment(s).

    Parameters
    ----------
    expts : str or list of str
        Experiment name(s).
    variable : str or list of str
        Variable name(s).
    """

    plt.figure(figsize=(12, 6))

    if not isinstance(expts, list):
        expts = [expts]

    if not isinstance(variables, list):
        variables = [variables]

    for variable in tqdm_notebook(variables, leave=False, desc='variables',
                                  position=0):
        for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
            annual_average = cc.diagnostics.annual_scalar(expt, variable)
            if len(variables) > 1:
                lbl = annual_average.long_name + \
                    ' ({})'.format(annual_average.units)
                if len(expts) > 1:  # if false, title displays expt
                    lbl = expt + ' ' + lbl
            else:
                lbl = expt  # title displays variable instead
            annual_average.plot(label=lbl)

    if len(variables) > 0 and len(expts) > 0:
        if len(variables) == 1:
            plt.title(annual_average.long_name)
            plt.ylabel(annual_average.name
                       + ' ({})'.format(annual_average.units))
            plt.legend(loc='best')
        else:
            if len(expts) == 1:
                plt.title(expts[0])
            else:
                plt.title('')  # legend displays this info instead
            plt.ylabel('')  # legend displays this info instead
            plt.legend(fontsize=10, bbox_to_anchor=(1, 1), loc='best',
                       borderaxespad=0.)  # puts long legend outside plot
    plt.xlabel('Time')


def drake_passage(expts=[]):
    """
    Plot Drake Passage transport.

    Parameters
    ----------
    expts : str or list of str
        Experiment name(s).
    """

    plt.figure(figsize=(12, 6))

    if not isinstance(expts, list):
        expts = [expts]

    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        transport = cc.diagnostics.drake_passage(expt)
        transport.plot(label=expt)
    plt.title('Drake Passage Transport')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')
    

def bering_strait(expts=[]):
    """
    Plot Bering Strait transport.

    Parameters
    ----------
    expts : str or list of str
        Experiment name(s).
    """

    plt.figure(figsize=(12, 6))

    if not isinstance(expts, list):
        expts = [expts]

    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        transport = cc.diagnostics.bering_strait(expt)
        transport.plot(label=expt)
    plt.title('Bering Strait Transport')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')
    
def aabw(expts=[], GM=False):
    """
    Plot timeseries of AABW transport measured at 55S.

    Parameters
    ----------
    expts : str or list of str
        Experiment name(s).
    """

    plt.figure(figsize=(12, 6))

    if not isinstance(expts, list):
        expts = [expts]

    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        psi_aabw = cc.diagnostics.calc_aabw(expt, GM)
        psi_aabw.plot(label=expt)
        
    plt.title('AABW Transport')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')
    

def amoc(expts=[], GM=False):
    """
    Plot timeseries of AMOC transport measured at 26N.

    Parameters
    ----------
    expts : str or list of str
        Experiment name(s).
    """

    plt.figure(figsize=(12, 6))

    if not isinstance(expts, list):
        expts = [expts]

    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        psi_amoc = cc.diagnostics.calc_amoc(expt, GM)
        psi_amoc.plot(label=expt)
        
    plt.title('AMOC Transport at 26N')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')

def amoc_south(expts=[], GM=False):
    """
    Plot timeseries of AMOC transport measured at 35S.

    Parameters
    ----------
    expts : str or list of str
        Experiment name(s).
    """

    plt.figure(figsize=(12, 6))

    if not isinstance(expts, list):
        expts = [expts]

    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        psi_amoc_south = cc.diagnostics.calc_amoc_south(expt, GM)
        psi_amoc_south.plot(label=expt)
        
    plt.title('AMOC Transport at 35S')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')