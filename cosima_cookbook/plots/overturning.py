import cosima_cookbook as cc
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm_notebook

import IPython.display
    
def psi_avg(expts, n=10, GM=False, clev=np.arange(-20,20,2)):
    
    if not isinstance(expts, list):
        expts = [expts]
        
    # computing
    results = []
    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        psi_avg = cc.diagnostics.psi_avg(expt, n, GM)
            
        result = {'psi_avg': psi_avg,
                  'expt': expt}
        results.append(result)
        
    IPython.display.clear_output()
   
    # plotting
    for result in results:
        psi_avg = result['psi_avg']
        expt = result['expt']
        
        plt.figure(figsize=(10, 5)) 
        plt.contourf(psi_avg.grid_yu_ocean, 
                 psi_avg.potrho, psi_avg, 
                 cmap=plt.cm.PiYG,levels=clev,extend='both')
        cb=plt.colorbar(orientation='vertical', shrink = 0.7)
    
        cb.ax.set_xlabel('Sv')
        plt.contour(psi_avg.grid_yu_ocean, psi_avg.potrho, psi_avg, levels=clev, colors='k', linewidths=0.25)
        plt.contour(psi_avg.grid_yu_ocean, psi_avg.potrho, psi_avg, levels=[0.0,], colors='k', linewidths=0.5)
        plt.gca().invert_yaxis()
    
        plt.ylim((1037.5,1034))
        plt.ylabel('Potential Density (kg m$^{-3}$)')
        plt.xlabel('Latitude ($^\circ$N)')
        plt.xlim([-75,85])
        plt.title('Overturning in %s' % expt)
    
def zonal_mean(expts,variable,n=10):
               
    if not isinstance(expts, list):
        expts = [expts]
    
    
    # computing
    results = []
    for expt in tqdm_notebook(expts, leave=False, desc='experiments'):
        zonal_mean, zonal_diff = cc.diagnostics.zonal_mean(expt,variable,n)
            
        result = {'zonal_mean': zonal_mean,
                  'zonal_diff': zonal_diff,
                  'expt': expt}
        results.append(result)
        
    IPython.display.clear_output()
    
    # plotting
    for result in results:
        zonal_mean = result['zonal_mean']
        zonal_diff = result['zonal_diff']
        expt = result['expt']
        
        plt.figure(figsize=(12,5))
        plt.subplot(121)
        zonal_mean.plot()
        plt.title(expt)
        plt.gca().invert_yaxis()
        plt.title('{}: Zonal Mean {}'.format(expt, variable))
        plt.subplot(122)
        zonal_diff.plot()
        plt.title(expt)
        plt.gca().invert_yaxis()
        plt.title('{}: Zonal Mean {} Change'.format(expt, variable))
        
