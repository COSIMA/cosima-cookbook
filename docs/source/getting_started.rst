===============
Getting Started
===============

The cookbook consists of a Python 3 package that contains infrastructure
for indexing COSIMA model output and convenient methods for searching for
and loading the data into `xarray <http://xarray.pydata.org/>`_ datastructures.

Some users may find it sufficient to browse through the examples and tutorials
in the `COSIMA recipes <http://cosima-recipes.readthedocs.io/>`_ repository.
The Jupyter notebooks that can be downloaded from COSIMA recipes need this package
(called cosima_cookbook) to be installed.

Choosing your platform
======================

COSIMA ocean and ice models are typically run on `NCI <nci.org.au>`_, a HPC
computing centre in Australia.  The output data is very large and it is 
assumed that this data resides on a NCI storage system.

The cookbook is supported on two NCI systems

#. `Virtual Desktop Infrastructure (VDI) <http://nci.org.au/services/vdi/>`_
#. `gadi (gadi.nci.org.au) <http://nci.org.au/systems-services/peak-system/gadi/>`_

Connecting
==========

For both VDI and gadi scripts are used to start a `jupyter notebook <http://jupyter-notebook.readthedocs.io>`_ 
or `jupyter lab <http://jupyterlab.readthedocs.io>`_ session on the chosen system 
and automatically create an `ssh tunnel <https://www.ssh.com/ssh/tunneling/>`_ 
such that the jupyter session can be opened in your local browser using a url
like <http://localhost:8888> that appears to be on your own local machine.

Scripts for this purpose are provided by the CLEX CMS team in this repository

https://github.com/coecms/nci_scripts

Clone the repository to your local computer. There are instructions in the repository 
on the requirements for each script and how to use them.

Alternatively if you are using the VDI Strudel environment and accessing the VDI
through a virtual desktop you can load the same python conda environment that is
used in the scripts above and start a jupyter notebook session like so:
::

    module use /g/data3/hh5/public/modules
    module load conda/analysis3

    jupyter notebook

Finding data
============

Most of the infrastructure the COSIMA Cookbook provides revolves around indexing
data output from COSIMA models and providing a python based API to access the 
data in a convenient and straight forward way.

There are graphical user interface (GUI) tools to help with data discovering and
exploration. There is a 
`tutorial <https://nbviewer.jupyter.org/github/COSIMA/cosima-recipes/blob/master/Tutorials/Using_Explorer_tools.ipynb>`_
in the COSIMA recipes repository which demonstrates the available tools.

Tutorials and examples
======================

COSIMA recipes provides `tutorials <https://cosima-recipes.readthedocs.io/en/latest/tutorials.html>`_
and `documented examples <https://cosima-recipes.readthedocs.io/en/latest/documented_examples.html>`_ 
which can be used to learn how to use the Cookbook and for ideas and inspiration for your own analysis.
