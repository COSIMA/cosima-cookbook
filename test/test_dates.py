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
import xarray as xr
import numpy as np
import cftime
from datetime import datetime, timedelta

from cosima_cookbook.date_utils import rebase_times, rebase_dataset, \
    rebase_variable, rebase_shift_attr

from xarray.testing import assert_equal

verbose = True

times = []

def setup_module(module):
    if verbose: print ("setup_module      module:%s" % module.__name__)
    if verbose: print ("Python version: {}".format(sys.version))
    # Put any setup code in here, like making temporary files
    # Make 5 years of a noleap calendar on the first of each month
    global times
    for y in range(1,6):
        for m in range(1,13):
            times.append(cftime.date2num(cftime.datetime(y,m,1),units='days since 01-01-01',calendar='noleap'))
    times = np.array(times)

def teardown_module(module):
    if verbose: print ("teardown_module   module:%s" % module.__name__)
    # Put any taerdown code in here, like deleting temporary files

def test_rebase_times():

    # Should be a 10 year offset between original times and rebased times
    assert(not np.any((times + 365*10) - rebase_times(times,'days since 1980-01-01','noleap','days since 1970-01-01')))

    # Should be a -10 year offset between original times and rebased times
    assert(not np.any((times - 365*10) - rebase_times(times,'days since 1980-01-01','noleap','days since 1990-01-01')))

def test_rebase_variable():

    timesvar = xr.DataArray(times,attrs={'units':'days since 1980-01-01','calendar':'noleap'})

    print("att:",timesvar.attrs)

    # Test we can rebase with and without explicitly setting a calendar
    timesvar_rebased = rebase_variable(timesvar, target_units='days since 1970-01-01')
    assert(timesvar_rebased.equals(rebase_variable(timesvar, 'noleap', target_units='days since 1970-01-01')))

    assert(not timesvar.equals(timesvar_rebased))

    # Should be a 10 year offset between original times and rebased times
    assert(not np.any((times + 365*10) - timesvar_rebased.values))
    # assert(not np.any((times + 365*10) - rebase_variable(timesvar, 'noleap', target_units='days since 1970-01-01').values))

    with pytest.raises(ValueError):
        timesvar_rebased = rebase_variable(timesvar, 'noleap', target_units='days since 1990-01-01')

    # Rebase with an offset otherwise would have negative dates
    timesvar_rebased = rebase_variable(timesvar, 'noleap', target_units='days since 1990-01-01', offset=365*10)

    # Values should be the same
    assert(not np.any(times - timesvar_rebased.values))

    # But the rebase_shift_attr should be set to 10 years
    assert(timesvar_rebased.attrs[rebase_shift_attr] == 365*10)

    # Check we get back timesvar if rebased again with no arguments (rebases to previous
    # units and applies offset if required in this instance)
    assert(timesvar.equals(rebase_variable(timesvar_rebased)))


def test_matching_time_units():

    testfile = 'test/data/ocean_sealevel.nc'

    ds = xr.open_dataset(testfile,decode_times=False)
    target_units = 'days since 1800-01-01'

    ds1 = rebase_dataset(ds, target_units)
    # s1.to_netcdf('tmp.nc')

    ds2 = rebase_dataset(ds1)
    # ds2.to_netcdf('tmp2.nc')

    # Rebasing again without target_units specified should
    # un-do previous rebase
    assert(ds.equals(ds2))

    # An offset is required as the target units are ahead of the data in time
    target_units = 'days since 2000-01-01'

    # Offset can be automatically generated as difference between target and src units
    ds1 = rebase_dataset(ds, target_units,offset='auto')
    ds2 = rebase_dataset(ds1)

    assert(ds.equals(ds2))

    # Offset can be an integer, but need to know what units are being used, days, hours etc
    ds1 = rebase_dataset(ds, target_units,offset=100*365)
    ds2 = rebase_dataset(ds1)

    assert(ds.equals(ds2))

    # Offset can be a datetime.timedelta object, but this would need some knowledge of
    # the calendar
    ds1 = rebase_dataset(ds, target_units,offset=timedelta(days=100*365))
    ds2 = rebase_dataset(ds1)

    # A different offset will yield a different dataset, but upon rebasing a second time
    # should still be the same as the original regardless of offset.
    ds3 = rebase_dataset(ds, target_units,offset=timedelta(days=200*365))
    ds4 = rebase_dataset(ds3)

    assert(ds.equals(ds4))
    assert(not ds1.equals(ds3))

    # Test graceful recovery if time_bounds missing.
    del(ds['time_bounds'])
    ds3 = rebase_dataset(ds, target_units,offset=timedelta(days=200*365))
    ds4 = rebase_dataset(ds3)

    assert(ds.equals(ds4))
    assert(not ds1.equals(ds3))

    ds = xr.open_dataset(testfile,decode_times=False)[['sea_level']]
    target_units = 'days since 1800-01-01'

    ds1 = rebase_dataset(ds, target_units)

def test_chunking():

    # An offset is required as the target units are ahead of the data in time
    target_units = 'days since 2000-01-01'

    testfile = 'test/data/ocean_sealevel.nc'

    ds = xr.open_dataset(testfile,decode_times=False, chunks={'time':10})
    target_units = 'days since 1800-01-01'

    ds1 = rebase_dataset(ds, target_units)
