<img src="https://github.com/COSIMA/logo/blob/master/png/logo_word.png" width="800"/>
<br/> <br/>

<a href="https://cosima-recipes.readthedocs.io/en/latest">
    <img alt="latest docs" src="https://img.shields.io/badge/docs-latest-blue.svg">
</a>

<a href="https://anaconda.org/coecms/cosima-cookbook">
    <img src="https://anaconda.org/coecms/cosima-cookbook/badges/version.svg" />
</a>

# cosima-cookbook

The COSIMA Cookbook is a framework for analysing output from ocean-sea ice models. The focus is on the ACCESS-OM2 suite of models being developed and run by members of [COSIMA: Consortium for Ocean-Sea Ice Modelling in Australia](http://cosima.org.au). But this framework is suited to analysing any MOM5/MOM6 output, as well as output from other models.

The cookbook is structured as follows:
 * This repository includes boiler-plate code and scripts that underpin the cookbook.
 * The [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) repository includes example notebooks on which you can base your analyses.
 * The [`cosima-recipes` template](https://github.com/COSIMA/cosima-recipes/blob/master/Tutorials/Template_For_Notebooks.ipynb) provides you with a template if you want to contribute your own scripts to the analysis.


## Getting Started

The easiest way to use the COSIMA Cookbook is through NCI's HPC systems (either via Open OnDemand (OOD) or on Gadi). The cookbook is preinstalled in the latest `conda/analysis3` environment.

Once you have an NCI account you can connect via the Open OnDemand (OOD) following instructions at instructions at https://ood.nci.org.au/ and also at https://opus.nci.org.au/display/DAE/COSIMA+Cookbook.

Alternatively, you might prefer to download the `gadi_jupyter` scripts hosted in the CLEx CMS Github Repository [coecms/nci_scripts](https://github.com/coecms/nci_scripts). Running the script will open a Jupyter notebook in your local browser window.


## Using the Cookbook

The COSIMA Cookbook relies on several components:
 1. There needs to be a database of simulations -- on the NCI system, model output that is stored in the COSIMA space on the `/g/data/ik11/` directory.
 2. Once you have access to data, the best place to start is the [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) repository which includes a series of jupyter notebooks containing examples that guide you through to use the cookbook to load model output and then proceed doing simple (or elaborate) computations. The best starting point of exploring the [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) is the [Tutorials](https://cosima-recipes.readthedocs.io/en/latest/tutorials/index.html) and the [Documented Examples](https://cosima-recipes.readthedocs.io/en/latest/documented_examples/index.html). A collection of useful examples leveraging the `cosima-cookbook` that were used in the [ACCESS-OM2 model announcement paper](https://doi.org/10.5194/gmd-13-401-2020) can be found at [COSIMA/ACCESS-OM2-1-025-010deg-report](https://github.com/COSIMA/ACCESS-OM2-1-025-010deg-report/tree/master/figures) repository.


## Contributing to the Cookbook

If you like the cookbook, you may like to interact more closely with us:
 * Contributions of new notebooks or analysis scripts are always welcome. Please check out the [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) repository.
 * If you find a problem, or have a suggestion for improvement, please log an issue.
 * All code submitted as part of the `cosima-cookbook` itself must be formatted with [black](https://github.com/psf/black)


[![Documentation Status](https://readthedocs.org/projects/cosima-cookbook/badge/?version=latest)](https://cosima-cookbook.readthedocs.org/en/latest)
