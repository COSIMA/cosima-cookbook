import os, socket, getpass
from dask.distributed import Client, LocalCluster

def start_cluster():
    "Set up a LocalCluster for distributed"
    
    hostname = socket.gethostname()
    n_workers = os.cpu_count() // 2
    cluster = LocalCluster(ip='localhost',
                       n_workers=n_workers,
                       diagnostics_port=0,
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