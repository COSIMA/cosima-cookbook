<img src="https://github.com/COSIMA/logo/blob/master/png/logo_word.png" width="800"/>
<br/> <br/>

<a href="https://cosima-recipes.readthedocs.io/en/latest">
    <img alt="latest docs" src="https://img.shields.io/badge/docs-latest-blue.svg">
</a>

# cosima-cookbook

The COSIMA Cookbook is a framework for analysing output from ocean-sea ice models. The focus is on the ACCESS-OM2 suite of models being developed and run by members of [COSIMA: Consortium for Ocean-Sea Ice Modelling in Australia](http://cosima.org.au). But this framework is suited to analysing any MOM5/MOM6 output, as well as output from other models.

The cookbook is structured as follows:
 * This repository includes boiler-plate code and scripts that underpin the cookbook.
 * The [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) repository includes example notebooks on which you can base your analyses.
 * The [`cosima-recipes` template](https://github.com/COSIMA/cosima-recipes/blob/master/Tutorials/Template_For_Notebooks.ipynb) provides you with a template if you want to contribute your own scripts to the analysis.


## Getting Started

The easiest way to use the COSIMA Cookbook is through NCI's HPC systems (either VDI or Gadi). The cookbook is preinstalled in the latest `conda/analysis3` environment.

Once you have an account on the VDI, you should:
 1. Clone the [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) repository to your local file space.
 2. Start a jupyter notebook session using the following commands:
```
>> module use /g/data/hh5/public/modules/
>> module load conda/analysis3-unstable
>> jupyter notebook
```
 3. Navigate to one of the COSIMA recipes and run the analysis.

Alternatively, you might prefer to download `vdi_jupyter` or the `gadi_jupyter` scripts hosted in the CLEx CMS Github Repository [coecms/nci_scripts](https://github.com/coecms/nci_scripts). These scripts will allow you to open a Jupyter notebook in your local browser window.


## Using the Cookbook

The COSIMA Cookbook relies on several components:
 1. There needs to be a database of simulations -- on the NCI system, model output stored in the COSIMA directories `/g/data/ik11/` and `/g/data/cj50` is indexed in a default shared database which is [manually updated](https://github.com/COSIMA/master_index) on request. Alternatively you can [make your own database](https://cosima-recipes.readthedocs.io/en/latest/Tutorials/Make_Your_Own_Database.html). 
 2. Once you have access to data, the best place to start is the [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) repository which includes a series of jupyter notebooks containing examples that guide you through to use the cookbook to load model output and then proceed doing simple (or elaborate) computations. The best starting point of exploring the [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) is the [Documented Examples](https://cosima-recipes.readthedocs.io/en/latest/documented_examples.html). A collection of useful examples leveraging the `cosima-cookbook` is also found [here](https://github.com/COSIMA/ACCESS-OM2-1-025-010deg-report/tree/master/figures).


## Contributing to the Cookbook

If you like the cookbook, you may like to interact more closely with us:
 * Contributions of new notebooks or analysis scripts are always welcome. Please check out the [`cosima-recipes`](https://github.com/COSIMA/cosima-recipes) repository.
 * If you find a problem, or have a suggestion for improvement, please log an issue.
 * All code submitted as part of the `cosima-cookbook` itself must be formatted with [black](https://github.com/psf/black)

## Conditions of use for ACCESS-OM2 data

We request that users of ACCESS-OM2 model [code](https://github.com/COSIMA/access-om2) or output data:
1. consider citing Kiss et al. (2020) ([http://doi.org/10.5194/gmd-13-401-2020](http://doi.org/10.5194/gmd-13-401-2020))
2. include an acknowledgement such as the following:

   *The authors thank the Consortium for Ocean-Sea Ice Modelling in Australia (COSIMA; [http://www.cosima.org.au](http://www.cosima.org.au)) for making the ACCESS-OM2 suite of models available at [https://github.com/COSIMA/access-om2](https://github.com/COSIMA/access-om2).*
3. let us know of any publications which use these models or data so we can add them to [our list](https://scholar.google.com/citations?hl=en&user=inVqu_4AAAAJ).

[![Documentation Status](https://readthedocs.org/projects/cosima-cookbook/badge/?version=latest)](https://cosima-cookbook.readthedocs.org/en/latest)
