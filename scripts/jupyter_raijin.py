#!/usr/bin/env python
"""
Script to launch a raijin session
and start a Jupyter server on it.

A ssh tunnel from the local machine to the VDI is set up and the local
webbrowser is spawned.

This is a python3 script (uses unicode strings).  If you don't have
python3 on your local machine, try installing Miniconda3
The only external module is pexpect which may need to be installed
using conda or pip.

Usage:
- if you use a password, the script will ask for your password when needed
- if you have already set up SSH public key, try running
    $ ssh-add ~/.ssh/FILENAME
  to add your public key to the ssh key agent.

Based on VDI script by James Munroe, 2017
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
    'user' : 'jm5970',
    'JupyterPort' : '8889',
    'BokehPort' : '8787',
    'execHost' :  'raijin.nci.org.au',
    'path' : 'data'
}

verbose = 1

config_path = os.path.expanduser('~/.raijin_cosima_cookbook.conf')
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

    parser = argparse.ArgumentParser(description="Log into Raijin, start a jupyter notebook session and ssh tunnel to local machine")
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

def open_jupyter_url(params):
    # Open browser locally
    status = ''
    url = 'http://{session}.nci.org.au:{jupyterport}/?token={token}'.format(**params)
    urldask = 'http://{session}.nci.org.au:{bokehport}/status'.format(**params)
    if is_mac:
        status = "Using appscript to open {}".format(url)
        safari = appscript.app("Safari")
        safari.make(new=appscript.k.document, with_properties={appscript.k.URL: url})
        safari.make(new=appscript.k.document, with_properties={appscript.k.URL: urldask})
    else:
        status = "Opening {}".format(url)
        webbrowser.open(url)
        webbrowser.open(urldask)

    return status

def session(func, *args, **kwargs):
    """wrapper for sending session-ctl commands"""
    cmd = ' ' + func
    s = ssh(cmd, *args, **kwargs)
    s.close()
    return s

tunnel_started = False

def start_connection(params):

    # This print statement is needed as there are /r/n line endings from
    # the jupyter notebook output that are difficult to suppress
    logging.info("Starting webfront...")

    # Open web browser and log result
    logging.info(open_jupyter_url(params))

def main(args):

    # global verbose means it doesn't need to be passed to every routine
    global verbose

    verbose = args.verbose

    logging.info("Connect ot session...")
    logging.info('exechost: {exechost}'.format(**params))

    logging.info("Running Jupyter on Raijin...")

    setupconda = params.get('setupconda',
              """module use /g/data3/hh5/public/modules
                 && module load conda/analysis3
              """.replace('\n', ' '))

    jupyterapp = params.get('jupyterapp',  "notebook")
    run_jupyter = "jupyter %s --no-browser --port {jupyterport} --ip 0.0.0.0" % jupyterapp
    run_jupyter = setupconda + ' && cd {path}' + ' && ' + run_jupyter
    r = session('hostname',params)
    m = r.before.decode().rstrip()
    params.update({'session':m})

    cmd = ' '.join(['-t', """'bash -l -c "%s"'""" % run_jupyter])
    logging.info("Waiting for Jupyter to start...")

    # Launch jupyter on Raijin
    s = ssh(cmd, params, login_timeout=2)
    ret = s.expect('http://\((?P<session>\w+).*\):(?P<jupyterport>\d+)/\?token=(?P<token>\w+)')
    if s.match:
        params.update(s.match.groupdict())
        clean_params(params)
        start_connection(params)
    else:
        logging.info("Could not find url information in jupyter output")
        sys.exit(1)

    # Grab all the output up to the incorrect URL -- uses the token twice, which is unhelpful

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
