# cosima-cookbook

The COSIMA Cookbook is a framework for analysing output from ocean-sea ice models. The focus is on the ACCESS-OM2 suite of models being developed and run by members of [COSIMA: Consortium for Ocean-Sea Ice Modelling in Australia](http://cosima.org.au). But this framework is suited to analysing any MOM5/MOM6 output, as well as output from other models.

The cookbook is structured as follows:
 * This repository includes boiler-plate code and scripts that underpin the cookbook.
 * The [COSIMA recipes](https://github.com/OceansAus/cosima-recipes) repository includes example notebooks on which you can base your analyses.
 * The [COSIMA recipes template](https://github.com/OceansAus/cosima-recipe-template) provides you with a template if you want to contribute your own scripts to the analysis.

## Getting Started
The easiest way to use the COSIMA Cookbook is via the [NCI Virtual Desktop Infrastructure (VDI)](http://nci.org.au/services/vdi/). The cookbook is preinstalled in the latest conda implemntation on the VDI. 

Once you have an account on the VDI, you should:
 1. Clone the [COSIMA recipes](https://github.com/OceansAus/cosima-recipes) repository to your local file space.
 2. Start a jupyter notebook session using the following commands:
```
>> module use /g/data3/hh5/public/modules/
>> module load conda/analysis3-unstable
>> jupyter notebook
```
 3. Navigate to one of the COSIMA recipes and run the analysis.
 
 Alternatively, you might prefer to download our [jupyter VDI](https://github.com/OceansAus/cosima-cookbook/blob/master/scripts/jupyter_vdi.py) script to your local machine to open a jupyer notebook in your local browser window.
 
## Using the Cookbook
The COSIMA Cookbook relies on several components:
 1. There needs to be a database of simulations -- on the NCI system, model output that is stored in the COSIMA space on the hh5 directory.
 2. Once you have access to data, the best place to start is the [cosima-recipes](https://github.com/OceansAus/cosima-recipes) repository. Here, there are a series of jupyter notebooks containing examples which explain how you can use the cookbook. There are also some example notebooks [here](https://github.com/OceansAus/ACCESS-OM2-1-025-010deg-report/tree/master/figures). There's some documentation [here](http://cosima-cookbook.readthedocs.io) but it's  very out-of-date - caveat lector.

## Contributing to the Cookbook
If you like the cookbook, you may like to interact more closely with us:
 * Contributions of new notebooks or analysis scripts are always welcome. Please check out the [cosima-recipes](https://github.com/OceansAus/cosima-recipes) repository.
 * If you find a problem, or have a suggestion for improvement, please log an issue.

| Travis CI | Read the Docs | 
|:---------:|:-------------:|
| [![Travis CI Build Status](https://travis-ci.org/COSIMA/cosima-cookbook.svg?branch=master)](https://travis-ci.org/COSIMA/cosima-cookbook) | [![Documentation Status](https://readthedocs.org/projects/cosima-cookbook/badge/?version=latest)](https://cosima-cookbook.readthedocs.org/en/latest) |
