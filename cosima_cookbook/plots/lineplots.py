import matplotlib.pyplot as plt
import cosima_cookbook as cc
from tqdm import tqdm_notebook
import IPython.display

def wind_stress(expts=[]):
    """
    Plot zonally averaged wind stress.

    Parameters
    ----------
    expts : str or list of str
        Experiment name(s).
    """

    if not isinstance(expts, list):
        expts = [expts]

    # computing
    results = []
    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        result = {'mean_tau_x': cc.diagnostics.mean_tau_x(expt),
                  'expt': expt }
        results.append(result)
            
    IPython.display.clear_output()
    
    plt.figure(figsize=(12, 6))
    
    # plotting
    for result in results:
        mean_tau_x = result['mean_tau_x']
        expt = result['expt']
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

    if not isinstance(expts, list):
        expts = [expts]

    if not isinstance(variables, list):
        variables = [variables]

    # computing
    results = []
    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        annual_average = cc.diagnostics.annual_scalar(expt, variables)
            
        result = {'annual_average': annual_average,
                  'expt': expt}
        results.append(result)
    
    IPython.display.clear_output()
    
    # plotting each variable in a separate plot
    for variable in variables:
        
        plt.figure(figsize=(12, 6))
        
        for result in results:
            annual_average = result['annual_average']
            expt = result['expt']

            annual_average[variable].plot(label=expt)
            
        plt.title(annual_average[variable].long_name)
        plt.legend(fontsize=10, bbox_to_anchor=(1, 1), 
                   loc='best', borderaxespad=0.)
          
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

    # computing
    results = []
    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        transport = cc.diagnostics.drake_passage(expt)
            
        result = {'transport': transport,
                  'expt': expt}
        results.append(result)
    
    IPython.display.clear_output()
    
    # plotting
    for result in results:
        transport = result['transport']
        expt = result['expt']
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
        
    IPython.display.clear_output()
    
    plt.title('Bering Strait Transport')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')
    
def aabw(expts=[]):
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
        psi_aabw = cc.diagnostics.calc_aabw(expt)
        psi_aabw.plot(label=expt)
    
    IPython.display.clear_output()
        
    plt.title('AABW Transport at 40S')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')
    

def amoc(expts=[]):
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
        psi_amoc = cc.diagnostics.calc_amoc(expt)
        psi_amoc.plot(label=expt)
    
    
    IPython.display.clear_output()
        
    plt.title('AMOC Transport at 26N')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')

def amoc_south(expts=[]):
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
        psi_amoc_south = cc.diagnostics.calc_amoc_south(expt)
        psi_amoc_south.plot(label=expt)
    
    IPython.display.clear_output()
    
    plt.title('AMOC Transport at 35S')
    plt.xlabel('Time')
    plt.ylabel('Transport (Sv)')
    plt.legend(fontsize=10, loc='best')