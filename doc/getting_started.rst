===============
Getting Started
===============

`COSIMA: Consortium for Ocean-Sea Ice Modelling in Australia
<http://cosima.org.au>`_

This repository is a gallery of self contained analysis examples.

Setting up a development environment
====================================

The assumption is that these notebooks will be run on either raijin.nci.org.au, the VDI (http://nci.org.au/services/vdi/) or on a virtual machine with NFS mounted directories on tenjin.nci.org.au. The notebooks can also be run locally  by using something like sshfs.

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
