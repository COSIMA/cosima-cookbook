import matplotlib.pyplot as plt
import cosima_cookbook as cc

def sea_surface_temperature(expts=[]):
    """
    Plot a map of SST from last decade of run.
    """
               
    if not isinstance(expts, list):
        expts = [expts]
    
    for expt in expts:
        SST, SSTdiff = cc.diagnostics.sea_surface_temperature(expt)
        plt.figure(figsize=(12,4))
        plt.subplot(121)
        SST.plot()
        plt.title(expt)
        plt.subplot(122)
        SSTdiff.plot()
        plt.title(expt)


def sea_surface_salinity(expts=[]):
    """
    Plot a map of SSS from last decade of run.
    """
               
    if not isinstance(expts, list):
        expts = [expts]
    
    for expt in expts:
        SSS, SSSdiff = cc.diagnostics.sea_surface_salinity(expt)
        plt.figure(figsize=(12,4))
        plt.subplot(121)
        SSS.plot()
        plt.title(expt)
        plt.subplot(122)
        SSSdiff.plot()
        plt.title(expt)
