#!/usr/bin/env python

"""
Copyright 2017 ARC Centre of Excellence for Climate Systems Science
author: Aidan Heerdegen <aidan.heerdegen@anu.edu.au>
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import print_function

import pytest
import sys, os, time, glob
import shutil
import pdb # Add pdb.set_trace() to set breakpoints

verbose = True

def setup_module(module):
    if verbose: print ("setup_module      module:%s" % module.__name__)
    if verbose: print ("Python version: {}".format(sys.version))
    # Put any setup code in here, like making temporary files
 
def teardown_module(module):
    if verbose: print ("teardown_module   module:%s" % module.__name__)
    # Put any taerdown code in here, like deleting temporary files

def test_something():

    # Put tests in functions which begin with 'test_'

    # User assert to test conditions
    assert(True)

