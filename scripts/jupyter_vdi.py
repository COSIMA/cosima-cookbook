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
- if you use a password, the script will ask for your password when needed
- if you have already set up SSH public key with Strudel, try running
    $ ssh-add ~/.ssh/MassiveLauncherKey
  to add your public key to the ssh key agent.

Author: James Munroe, 2017
"""

from __future__ import print_function

import re
import sys
import time
import getpass
import pexpect
import os
import configparser
# Requires future module https://pypi.org/project/future/
from builtins import input
import argparse

import logging
logging.basicConfig(format='[%(asctime)s jupyter_vdi.py] %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
try:
    import appscript
except ImportError:
    import webbrowser
    is_mac = False
else:
    is_mac = True

DEFAULTS = {
    'user' : getpass.getuser(),
    'JupyterPort' : '8889',
    'BokehPort' : '8787',
    'execHost' :  'vdi.nci.org.au'
}

verbose = 0

config_path = os.path.expanduser('~/cosima_cookbook.conf')
parser = configparser.ConfigParser(defaults=DEFAULTS)

if os.path.exists(config_path):
    logging.info('Using config file: {}'.format(config_path))
    parser.read(config_path)
else:
    logging.warn('No config file found. Creating default {} file.'.format(config_path))
    logging.warn('*** Please edit this file as needed. ***')
    while DEFAULTS['user']==getpass.getuser() or DEFAULTS['user']=="":
        DEFAULTS['user']=input('What is your NCI username? ')
    parser = configparser.ConfigParser(defaults=DEFAULTS)

    with open(config_path, 'w') as f:
        parser.write(f)

params = parser.defaults()

def parse_args(args):

    parser = argparse.ArgumentParser(description="Log into the VDI, start a jupyter notebook session and ssh tunnel to local machine")
    parser.add_argument("-v","--verbose", help="Increase verbosity", action='count', default=0)

    return parser.parse_args(args)

def clean_params(params):

    for key, value in params.items():
        try:
            params[key] = value.decode()
        except AttributeError:
            pass

def ssh(cmd, params, login_timeout=10):
    """
    Run a remote command via SSH
    """

    clean_params(params)

    cmd = ("ssh -x -l {user} {exechost} " + cmd).format(**params)
    if verbose > 0: logging.info(cmd)
    s = pexpect.spawn(cmd)

    # SSH pexpect logic taken from pxshh:
    i = s.expect(["(?i)are you sure you want to continue connecting",
                  "(?i)(?:password)|(?:passphrase for key)",
                  "(?i)permission denied",
                  "(?i)connection closed by remote host",
                  pexpect.EOF, pexpect.TIMEOUT], timeout=login_timeout)

    # First phase
    if i == 0:
        # New certificate -- always accept it.
        # This is what you get if SSH does not have the remote host's
        # public key stored in the 'known_hosts' cache.
        s.sendline("yes")
        i = s.expect(["(?i)are you sure you want to continue connecting",
                      "(?i)(?:password)|(?:passphrase for key)",
                      "(?i)permission denied",
                      "(?i)connection closed by remote host",
                      pexpect.EOF, pexpect.TIMEOUT], timeout=login_timeout)

    if i == 1:  # password or passphrase
        if 'password' not in params:
            params['password'] = getpass.getpass('password: ')

        s.sendline(params['password'])
        i = s.expect(["(?i)are you sure you want to continue connecting",
                      "(?i)(?:password)|(?:passphrase for key)",
                      "(?i)permission denied",
                      "(?i)connection closed by remote host",
                      pexpect.EOF, pexpect.TIMEOUT], timeout=login_timeout)

    # TODO: check if ssh connection is successful

    return s

def session(func, *args, **kwargs):
    """wrapper for sending session-ctl commands"""
    cmd = '/opt/vdi/bin/session-ctl --configver=20151620513 ' + func
    s = ssh(cmd, *args, **kwargs)
    s.close()
    return s

def open_jupyter_url(params):
    # Open browser locally
    status = ''
    url = 'http://localhost:{jupyterport}/?token={token}'.format(**params)
    if is_mac:
        status = "Using appscript to open {}".format(url)
        safari = appscript.app("Safari")
        safari.make(new=appscript.k.document, with_properties={appscript.k.URL: url})
    else:
        status = "Opening {}".format(url)
        webbrowser.open(url)

    return status

tunnel_started = False
tunnel = None

def start_tunnel(params):

    # Create ssh tunnel for local access to jupyter notebook
    cmd = ' '.join(['-N -f -L {jupyterport}:localhost:{jupyterport}',
        '-L {bokehport}:localhost:{bokehport}'])

    # This print statement is needed as there are /r/n line endings from
    # the jupyter notebook output that are difficult to suppress
    logging.info("Starting ssh tunnel...")
    tunnel = ssh(cmd, params, login_timeout=2)
    tunnel.expect (pexpect.EOF)

    # Open web browser and log result
    logging.info(open_jupyter_url(params))

def main(args):

    # global verbose means it doesn't need to be passed to every routine
    global verbose

    verbose = args.verbose

    logging.info("Checking SSH keys to VDI are configured...")
    r = session('hello --partition main', params)
    if r.exitstatus != 0:
        # suggest setting up SSH keys
        logging.error("Error with ssh keys/password and VDI.")
        logging.error("  Incorrect user name in ~/cosima_cookbook.conf file?")
        logging.error("  Edit ~/cosima_cookbook.conf before continuing.")
        sys.exit(1)
    logging.info("SSH keys configured OK")

    logging.info("Determine if VDI session is already running...")
    r = session('list-avail --partition main', params)
    m = re.search('#~#id=(?P<jobid>(?P<jobidNumber>.*?))#~#state=(?P<state>.*?)(?:#~#time_rem=(?P<remainingWalltime>.*?))?#~#', r.before.decode())
    if m is not None:
        params.update(m.groupdict())
        w = int(params['remainingWalltime'])
        remainingWalltime = '{:02}:{:02}:{:02}'.format(
            w // 3600, w % 3600 // 60, w % 60)
        logging.info('Time remaining: %s', remainingWalltime)
        # TODO: should give user option of starting a new session if the remaining walltime is short
    else:
        logging.info('No VDI session found')
        logging.info("Launching a new VDI session...")
        r = session('launch --partition main', params)
        m = re.search('#~#id=(?P<jobid>(?P<jobidNumber>.*?))#~#',
                  r.before.decode())
        if m is None:
            logging.info('Unable to launch new VDI session:\n'+r.before.decode())

        params.update(m.groupdict())
        time.sleep(2)   # TODO: instead of waiting, should check for confirmation 
                        # use has-started

    logging.info("Determine jobid for VDI session...{jobid}".format(**params))

    logging.info("Get exechost for VDI session...")
    r = session('get-host --jobid {jobid}', params)
    m = re.search('#~#host=(?P<exechost>.*?)#~#', r.before.decode())
    params.update(m.groupdict())
    logging.info('exechost: {exechost}'.format(**params))

    logging.info("Running Jupyter on VDI...")

    setupconda = params.get('setupconda',
              """module use /g/data3/hh5/public/modules
                 && module load conda/analysis3
              """.replace('\n', ' '))

    jupyterapp = params.get('jupyterapp',  "notebook")
    run_jupyter = "jupyter %s --no-browser --port {jupyterport}" % jupyterapp
    run_jupyter = setupconda + ' && ' + run_jupyter

    cmd = ' '.join(['-t', """'bash -l -c "%s"'""" % run_jupyter])

    logging.info("Waiting for Jupyter to start...")

    # Launch jupyter on VDI
    s = ssh(cmd, params, login_timeout=2)
    ret = s.expect('http://\S*:(?P<jupyterport>\d+)/\?token=(?P<token>[a-zA-Z0-9]+)')

    if s.match:
        params.update(s.match.groupdict())
        start_tunnel(params)
    else:
        logging.info("Could not find url information in jupyter output")
        sys.exit(1)

    # Grab all the output up to the incorrect URL -- uses the token twice, which is unhelpful
    ret = s.expect('http://.*')

    logging.info("Use Control-C to stop the Notebook server and shut down all kernels (twice to skip confirmation)\n\n")

    # give control over to user
    s.interact()

    logging.info('end of script')
    # optional: terminate to close the vdi session?

def main_argv():
    
    args = parse_args(sys.argv[1:])

    main(args)

if __name__ == "__main__":

    main_argv()
