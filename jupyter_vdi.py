#!/usr/bin/env python
"""
Script to launch a VDI session (or connect to already running session)
and start a Jupyter server on the VDI

A ssh tunnel from the local machine to the VDI is set up and the local
webbrowser is spawned.

This is a python3 script (uses unicode strings).  If you don't have
python3 on your local machine, try installing Miniconda3

The only external module is pexpect which may need to be installed
using conda or pip.

Usage:
- edit params dictionary below with your NCI user id (replace jm0634)
- if you use a password, the script will prompt you for your password when needed
- if you have already set up SSH public key with Strudel, try running
    $ ssh-add ~/.ssh/MassiveLauncherKey
  to add your public key to the ssh key agent.

Author: James Munroe, 2017
"""

from __future__ import print_function

import re
import sys
import webbrowser
import time
import getpass

import pexpect

params = {'user' : 'jm0634',
          'JupyterPort' : '8890',
          'execHost' :  'vdi.nci.org.au',
         }

def ssh(cmd, params, login_timeout=10):
    """
    Run a remote command via SSH
    """

    cmd = ("ssh -l {user} {execHost} " + cmd).format(**params)
    s = pexpect.spawn(cmd)

    # SSH pexpect logic taken from pxshh:
    i = s.expect(["(?i)are you sure you want to continue connecting", "(?i)(?:password)|(?:passphrase for key)",
        "(?i)permission denied", "(?i)connection closed by remote host", pexpect.EOF, pexpect.TIMEOUT], timeout=login_timeout)

    # First phase
    if i==0:
	# New certificate -- always accept it.
	# This is what you get if SSH does not have the remote host's
	# public key stored in the 'known_hosts' cache.
        s.sendline("yes")
        i = s.expect(["(?i)are you sure you want to continue connecting", "(?i)(?:password)|(?:passphrase for key)",
          "(?i)permission denied", "(?i)connection closed by remote host", pexpect.EOF, pexpect.TIMEOUT], timeout=login_timeout)

    if i==1: # password or passphrase
        if 'password' not in params:
            params['password'] = getpass.getpass('password: ')

        s.sendline(params['password'])
        i = s.expect(["(?i)are you sure you want to continue connecting", "(?i)(?:password)|(?:passphrase for key)",
              "(?i)permission denied", "(?i)connection closed by remote host", pexpect.EOF, pexpect.TIMEOUT], timeout=login_timeout)

    # TODO: check if ssh connection is successful

    return s

def session(func, *args, **kwargs):
    """wrapper for sending session-ctl commands"""
    cmd = '/opt/vdi/bin/session-ctl --configver=20151620513 ' + func
    s = ssh(cmd, *args, **kwargs)
    s.close()
    return s

print("Checking SSH keys to VDI are configured...", end='' )
r = session('hello --partition main', params)
if r.exitstatus != 0:
    # suggest setting up SSH keys
    # TODO: get user id from user instead of being hard coded
    print("Error with ssh keys/password and VDI. Edit params dictionary in the script")
    sys.exit(1)
print("OK")

print("Determine if VDI session is already running...", end='')
r = session('list-avail --partition main', params)
m = re.search('#~#id=(?P<jobid>(?P<jobidNumber>.*?))#~#state=(?P<state>.*?)(?:#~#time_rem=(?P<remainingWalltime>.*?))?#~#', r.before.decode())
if m is not None:
    params.update(m.groupdict())
    w = int(params['remainingWalltime'])
    remainingWalltime = '{:02}:{:02}:{:02}'.format(w // 3600, w % 3600 // 60, w % 60)
    print(remainingWalltime, 'time remaining')

    # TODO: should give use option of starting a new session of the remaining walltime is short
else:
    print('No')
    print("Launching new VDI session...", end='')
    r = session('launch --partition main', params)
    m = re.search('#~#id=(?P<jobid>(?P<jobidNumber>.*?))#~#', r.before.decode())
    params.update(m.groupdict())
    time.sleep(2) # TODO: instead of waiting, should check for confirmation
    # use has-started

print("Determine jobid for VDI session...{jobid}".format(**params))

print("Get execHost for VDI session...", end='')
r = session('get-host --jobid {jobid}', params)
m = re.search('#~#host=(?P<execHost>.*?)#~#', r.before.decode())
params.update(m.groupdict())
print('{execHost}'.format(**params))

# wait for jupyter to start running and launch web browser locally
webbrowser_started = False
def start_jupyter(s):
    global webbrowser_started

    if not webbrowser_started:
        m = re.search('The Jupyter Notebook is running at: (?P<url>.*)', s.decode())
        if m is not None:
            params.update(m.groupdict())
            # open browser locally
            webbrowser.open(params['url'])
            webbrowser_started = True

    return s

print ("Running Jupyter on VDI...")
cmd = '-t -L {JupyterPort}:localhost:{JupyterPort} bash -c "echo &&  module use /g/data3/hh5/public/modules && module load conda && source activate analysis3  && jupyter notebook --no-browser --port {JupyterPort} "'
s = ssh(cmd, params, login_timeout=2)

print ("Waiting for Jupyter to start...")

# give control over to user
s.interact(output_filter=start_jupyter)

# optional: terminate to close the vdi session?
