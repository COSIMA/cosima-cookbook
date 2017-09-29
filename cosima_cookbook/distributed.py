import os, socket, getpass
from dask.distributed import Client, LocalCluster

from itertools import product
import numpy as np
import xarray as xr

from tqdm import tqdm_notebook

def start_cluster(diagnostics_port=0):
    "Set up a LocalCluster for distributed"
    
    hostname = socket.gethostname()
    n_workers = os.cpu_count() // 2
    cluster = LocalCluster(ip='localhost',
                       n_workers=n_workers,
                       diagnostics_port=bokeh_port,
                       memory_limit=6e9)
    client = Client(cluster)

    params = { 'bokeh_port': cluster.scheduler.services['bokeh'].port,
           'user': getpass.getuser(),
           'scheduler_ip': cluster.scheduler.ip,
           'hostname': hostname, }

    print("Run this command on a local terminal to set up SSH tunnels for dashboard:")
    print()
    print("  ssh -N -L {bokeh_port}:{scheduler_ip}:{bokeh_port} {hostname}.nci.org.au -l {user}".format(**params) )
    
    return client


def compute_by_block(dsx):
    """
    
    """
    
    # determine index key for each chunk
    slices = []
    for chunks in dsx.chunks:
        L  = [0,] + list(np.cumsum(chunks))
        slices.append( [slice(a, b) 
                        for a,b in (zip(L[:-1], L[1:]))]  )
    indexes = list(product(*slices))
    
    # allocate memory to receive result
    if isinstance(dsx, xr.DataArray):
        result = xr.zeros_like(dsx).load()
    else:
        result = np.zeros(dsx.shape)
    
    #evaluate each chunk one at a time
    for index in tqdm_notebook(indexes, leave=False):
        block = dsx.__getitem__(index).compute()
        result.__setitem__(index, block)
    
    return result 