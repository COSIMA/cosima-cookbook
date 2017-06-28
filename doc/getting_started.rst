===============
Getting Started
===============

Some users may find it sufficient to browse through the sample diagnostics
and model configurations provided.  In order to reproduce these results
on your own, you will a Python development environment.

The cookbook itself includes a Python package that contains the
diagnostics themselves and some utility functions.  The Jupyter IPython
notebooks that can be downloaded from the cookbook need this package
(called cosima_cookbook) to be installed to be able to work.

Choosing up your environment
============================

COSIMA ocean and ice models are typically run on `NCI <nci.org.au>`_ computing
platform.  The output data is very large and it is assumed that this
data resides on a NCI storage system.

The cookbook may be used on several different platforms:

#. `Virtual Desktop Infrastructure (VDI) <http://nci.org.au/services/vdi/>`_
#. `Raijin raijin.nci.org.au <http://nci.org.au/systems-services/peak-system/raijin/>`_ using qsub
#. `Tenjin tenjin.nci.org.au <http://nci.org.au/systems-services/cloud-computing/tenjin/>`_ using virtual machines and NFS mounted directories
#. Local workstation or laptop with data accessible over sshfs or OPeNDAP

The assumption is that these notebooks will be run on either
raijin.nci.org.au, the VDI (http://nci.org.au/services/vdi/) or on a virtual machine with NFS mounted directories on tenjin.nci.org.au. The notebooks can also be run locally  by using something like sshfs.

On raijin, use::
    module use conda

On your on machine, install miniconda

Create an a conda environment 'cosima'

Clone the cosima-cookbook repository::

    git clone https://github.com/OceansAus/cosima-cookbook.git

install the cosima_cookbook utilities. From the cosima-cookbook directory, run::

    pip install -e .

Run the Jupyter notebook::

    jupyter notebook --no-browser --ip='*'

Use::

    scripts/jupyter_vdi.py

[NCI Virtual Desktop Infrastructure (VDI)](http://nci.org.au/services/vdi/)

These notebooks are designed to run on tenjin.nci.org.au which includes the VDI or on raijin.nci.org.au.

This cookbook assumes a Python computational environment and access to
the NCI infrastructure.
