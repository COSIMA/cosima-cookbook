import matplotlib.pyplot as plt
import cosima_cookbook as cc
from tqdm import tqdm_notebook

import IPython.display

def sea_surface_temperature(expts=[],resolution=1):
    """
    Plot a map of SST from last decade of run.
    """
               
    if not isinstance(expts, list):
        expts = [expts]
    
    # computing
    results = []
    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        SST, SSTdiff = cc.diagnostics.sea_surface_temperature(expt, resolution)
            
        result = {'SST': SST,
                  'SSTdiff': SSTdiff,
                  'expt': expt}
        results.append(result)
        
    IPython.display.clear_output()
   
    # plotting
    for result in results:
        SST = result['SST']
        SSTdiff = result['SSTdiff']
        expt = result['expt']
        
        plt.figure(figsize=(12,4))
        plt.subplot(121)
        SST.plot()
        plt.title(expt)
        plt.subplot(122)
        SSTdiff.plot()
        plt.title(expt)


def sea_surface_salinity(expts=[], resolution=1):
    """
    Plot a map of SSS from last decade of run.
    """
               
    if not isinstance(expts, list):
        expts = [expts]
    
    # computing
    results = []
    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        SSS, SSSdiff = cc.diagnostics.sea_surface_salinity(expt, resolution)
            
        result = {'SSS': SSS,
                  'SSSdiff': SSSdiff,
                  'expt': expt}
        results.append(result)
        
    IPython.display.clear_output()
   
    # plotting
    for result in results:
        SSS = result['SSS']
        SSSdiff = result['SSSdiff']
        expt = result['expt']
        
        plt.figure(figsize=(12,4))
        plt.subplot(121)
        SSS.plot()
        plt.title(expt)
        plt.subplot(122)
        SSSdiff.plot(robust=True)
        plt.title(expt)
