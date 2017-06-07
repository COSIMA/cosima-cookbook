===============
Getting Started
===============

`COSIMA: Consortium for Ocean-Sea Ice Modelling in Australia
<http://cosima.org.au>`_

This repository is a gallery of self contained analysis examples.

Setting up a development environment
====================================

clone the cosima-cookbook repository::

    git clone

install the cosima_cookbook utilities. From the cosima-cookbook directory, run::

    pip install -e .


Run Jupyter notebook::

    jupyter notebook --no-browser --ip='*'

Use::

    scripts/jupyter_vdi.py

[NCI Virtual Desktop Infrastructure (VDI)](http://nci.org.au/services/vdi/)

These notebooks are designed to run on tenjin.nci.org.au which includes the VDI or on raijin.nci.org.au.

This cookbook assumes a Python computational environment and access to 
the NCI infrastructure.
