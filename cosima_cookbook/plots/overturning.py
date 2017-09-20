import cosima_cookbook as cc
import matplotlib.pyplot as plt
import numpy as np

def psi_avg(expt, n=10, GM=False, clev=np.arange(-20,20,2)):
    
    psi_avg = cc.diagnostics.psi_avg(expt, n)
    if GM:
        #print('Adding GM component')
        psi_avg = psi_avg + cc.diagnostics.psiGM_avg(expt, n)
    
    
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