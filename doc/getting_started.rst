===============
Getting Started
===============

Some users may find it sufficient to browse through the sample diagnostics
and model configurations provided.  In order to reproduce these results
on your own, you will a Python 3 development environment.

The cookbook itself includes a Python 3 package that contains the
diagnostics themselves and some utility functions.  The Jupyter IPython
notebooks that can be downloaded from the cookbook need this package
(called cosima_cookbook) to be installed.

Choosing up your platform
==========================

COSIMA ocean and ice models are typically run on `NCI <nci.org.au>`_ computing
platform.  The output data is very large and it is assumed that this
data resides on a NCI storage system.

The cookbook may be used on several different platforms:

#. `Virtual Desktop Infrastructure (VDI) <http://nci.org.au/services/vdi/>`_
#. `Raijin raijin.nci.org.au <http://nci.org.au/systems-services/peak-system/raijin/>`_ using qsub
#. `Tenjin tenjin.nci.org.au <http://nci.org.au/systems-services/cloud-computing/tenjin/>`_ using virtual machines and NFS mounted directories
#. Local workstation or laptop with data accessible over sshfs or OPeNDAP

For this documentation, we will assume you are running your analysis using
the VDI.  Use the
`VDI User Guide <https://opus.nci.org.au/display/Help/VDI+User+Guide>`_
to get connect to the VDI.

Once connected, open a terminal (Applications -> System Tools -> Terminal).

(If your connection to the VDI is too slow, can can also also ssh directly to a VDI
node and work over ssh tunnels. Ask James Munroe for more information. )

Clone the cosima-cookbook repository::

    $ git clone https://github.com/OceansAus/cosima-cookbook.git

The next assumption we make it that you have a Python development environment
setup.  Options include either miniconda or modules.  In either case,
it is recommend to create a user-writable environment to be able to install
additional Python packages.

Clone the cosima-cookbook repository::

    $ git clone https://github.com/OceansAus/cosima-cookbook.git

Within the cosima-cookbook directory, run::

    $ pip install -e .

This installs the cosima-cookbook so that it is available in your
current Python environment.  The '-e' switch means editable; changes to
the cosima_cookbook project can be made without having to reinstall.
(Eventually, the cosima_cookbook package will be made available through
PyPi and as a conda package but this is still in development).

The cookbook requires a number of other packages including

 jupyter joblib tqdm matplotlib pandas numpy dask distributed xarray netcdf4
 bokeh seaborn datashader python-graphviz basemap cartopy

Note, just installing all of this packages in your /home directory will
probably use more than your allocated quota.  Options include installing these
files somewhere on /g/data or on /local.  You may wish to examine this script
to see one way of setting things up::

    scripts/bootstrap.sh

Finally, run the Jupyter notebook::

    $ jupyter notebook

You can also connect to this Jupyter notebook using an SSH tunnel. See
`scripts/jupyter_vdi.py`_.
