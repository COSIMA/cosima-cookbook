#!/usr/bin/python3
"""
Script to launch a VDI session (or connect to already running session)
and start an ssh tunnel to that session.
"""

import subprocess
import re
import sys
import webbrowser
import time

params = {'User' : 'jm0634',
          'IdentityFile' : '~/.ssh/MassiveLauncherKey',
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

    # should give use option of starting a new session of the remaining walltime is short
else:
    print('No')
    print("Launching new VDI session...", end='')
    r = session('launch --partition main')
    m = re.match('^#~#id=(?P<jobid>(?P<jobidNumber>.*?))#~#', r.stdout.decode())
    params.update(m.groupdict())
    time.sleep(2) # instead of waiting, should check for confirmation
    # use has-started


print("Determine jobid for VDI session...{jobid}".format(**params))

print("Get execHost for VDI session...", end='')
r = session('get-host --jobid {jobid}'.format(**params))
m = re.search('^#~#host=(?P<execHost>.*?)#~#', r.stdout.decode())
params.update(m.groupdict())
print('{execHost}'.format(**params))

print ("Openning SSH to VDI...")
cmd = 'ssh -Y -t {execHost} -l {User}'.format(**params)

p = subprocess.Popen(cmd.split())
p.wait()
