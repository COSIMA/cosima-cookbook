# cosima-cookbook

The COSIMA Cookbook is a framework for analysing output from ocean-sea ice models. The focus is on the ACCESS-OM2 suite of models being developed and run by members of [COSIMA: Consortium for Ocean-Sea Ice Modelling in Australia](http://cosima.org.au), but the framework is suitable for other model output.

## Getting Started
To use the COSIMA Cookbook you will need to install your own copy of the code -- most likely you will be doing this on the [NCI Virtual Desktop Infrastructure (VDI)](http://nci.org.au/services/vdi/). See [Getting Started](http://cosima-cookbook.readthedocs.io/en/latest/getting_started.html) for detailed information on how to install the Cookbook, and requirements for running analyses.

## Using the Cookbook
The COSIMA Cookbook relies on several components:
 1. There needs to be a database of simulations -- on the NCI system, model output that is stored in the COSIMA space on the hh5 directory.
 2. Once you have access to data, the best place to start the **DocumentedExamples** directory. Here, there are a series of jupyter notebooks containing well-documented examples, which explain how you can use the cookbook.
 3. There is another **ContributedExamples** directory, which includes additional jupyter notebooks. This code has less documentation and comes with no warranty, but may be helpful in doing analyses.
 4. Some of our standard code has been functionised, and there are example notebooks in the **configurations** directory which show analysis of some recent model output.


## Contributing to the Cookbook
If you like the cookbook, you may like to interact more closely with us:
 * Contributions of new notebooks or analysis scripts are always welcome. Please put them in **ContributedExamples** and send us a pull request.
 * Alternatively, you might like to improve and/or better document existing Examples, or develop **DocumentedExamples**.
 * If you find a problem, or have a suggestion for improvement, please log an issue.

| Travis CI | Jenkins | Read the Docs | 
|:---------:|:-------:|:-------------:|
| [![Travis CI Build Status](https://travis-ci.org/OceansAus/cosima-cookbook.svg?branch=master)](https://travis-ci.org/OceansAus/cosima-cookbook) | [![Build Status](https://accessdev.nci.org.au/jenkins/job/COSIMA/job/OceansAus/job/cosima-cookbook/job/master/badge/icon)](https://accessdev.nci.org.au/jenkins/job/COSIMA/job/OceansAus/job/cosima-cookbook/job/master/) | [![Documentation Status](https://readthedocs.org/projects/cosima-cookbook/badge/?version=latest)](https://cosima-cookbook.readthedocs.org/en/latest) |


