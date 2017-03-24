#!/usr/bin/python3
"""
Script to launch a VDI session (or connect to already running session)
and start a Jupyter server on the VDI

A ssh tunnel from the local machine to the VDI is set up and the local
webbrowser is spawned.
"""

import subprocess
import re
import sys
import webbrowser
import time

params = {'User' : 'jm0634',
          'IdentityFile' : '~/.ssh/MassiveLauncherKey',
          'JupyterPort' : '8890',
         }

def ssh(func):
    cmd = "ssh vdi " + func
    ret = subprocess.run(cmd.split(), stdout=subprocess.PIPE)
    return ret

def session(func):
    cmd = '/opt/vdi/bin/session-ctl --configver=20151620513 '
    cmd += func
    return ssh(cmd)

print("Verifying SSH keys to VDI are configured...", end='' )
r = session('hello --partition main')
if r.returncode != 0:
    print("Error with ssh keys and VDI. Please edit params dictionary in script.")
    sys.exit(1)
print("OK")

print("Determine if VDI session is already running...", end='')
r = session('list-avail --partition main')
m = re.match('^#~#id=(?P<jobid>(?P<jobidNumber>.*?))#~#state=(?P<state>.*?)(?:#~#time_rem=(?P<remainingWalltime>.*?))?#~#', r.stdout.decode())
if m is not None:
    params.update(m.groupdict())
    w = int(params['remainingWalltime'])
    remainingWalltime = '{:02}:{:02}:{:02}'.format(w // 3600, w % 3600 // 60, w % 60)
    print(remainingWalltime, 'time remaining')
else:
    print('No')
    print("Launching new VDI session...", end='')
    r = session('launch --partition main')
    m = re.match('^#~#id=(?P<jobid>(?P<jobidNumber>.*?))#~#', r.stdout.decode())
    params.update(m.groupdict())
    time.sleep(2) # instead of waiting, should check for confirmation

print("Determine jobid for VDI session...{jobid}".format(**params))

print("Get execHost for VDI session...", end='')
r = session('get-host --jobid {jobid}'.format(**params))
m = re.search('^#~#host=(?P<execHost>.*?)#~#', r.stdout.decode())
params.update(m.groupdict())
print('{execHost}'.format(**params))

print ("Running Jupyter on VDI...")
cmd = 'ssh -t {execHost} -l {User} -L {JupyterPort}:localhost:{JupyterPort} bash -c " module use /g/data3/hh5/public/modules && module load conda && jupyter notebook --no-browser --port {JupyterPort} "'.format(**params)

webbrowser_started = False
p = subprocess.Popen(cmd.split(), bufsize=1, stdout=subprocess.PIPE, universal_newlines=True)
for line in iter(lambda: p.stdout.readline(), ''):
    print(line, end='\r')
    m = re.search('The Jupyter Notebook is running at: (?P<url>.*)', line)
    if not webbrowser_started and m is not None:
        params.update(m.groupdict())
        # open browser
        webbrowser.open(params['url'])
        webbrowser_started = True

