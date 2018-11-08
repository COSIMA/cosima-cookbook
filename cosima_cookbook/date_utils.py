
#!/usr/bin/env python

"""
Copyright 2018 ARC Centre of Excellence for Climate Systems Science
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

import xarray as xr
from cftime import num2date, date2num
import numpy as np
import datetime

rebase_attr = '_rebased_units'
rebase_shift_attr = '_rebased_shift'
bounds = 'bounds'
boundsvar = 'bounds_var'

# Code adapted from https://github.com/spencerahill/aospy/issues/212

def date2num_round(dates, units, calendar):
    return np.round(date2num(dates, units, calendar),8)

def rebase_times(values, input_units, calendar, output_units):
    dates = num2date(values, input_units, calendar)
    return date2num_round(dates, output_units, calendar)

def is_bounds(var):
    """
    Return True if the xarray variable has been flagged as a bounds
    variable (has a bounds_var attribute)
    """
    return boundsvar in var.attrs

def set_bounds(var, varname):
    """
    Set the bounds_var attribute to the name of the dimension for which
    it is the bounds
    """
    var.attrs[boundsvar] = varname

def flag_bounds(ds):
    """
    Cycle through all the variables in a dataset and mark variables which
    are bounds as such by adding a bounds_var attribute
    """
    for name in ds.variables:
        if is_bounds(ds[name]):
            # This is a bounds variable and has been flagged as such
            continue
        if bounds in ds[name].attrs:
            # Flag bounds variable as such
            try:
                set_bounds(ds[ds[name].attrs[bounds]],name)
            except KeyError:
                # Ignore if bounds variable not present
                pass
            

def unflag_bounds(ds):
    """
    Cycle through all the variables in a dataset and unflag variables which
    are bounds by deleting any bounds_var
    """
    for name in ds.variables:
        try:
            del(ds[name].attrs[boundsvar])
        except KeyError:
            pass

def rebase_variable(var, calendar=None, target_units=None, src_units=None, offset=None):
    """
    Create rebased time variable
    """
    attributes = var.attrs

    # If no target_units are specified check if the variable has been previously
    # rebased and use this as the target, which will undo the previous rebasing
    if calendar == None:
        try:
            calendar = var.attrs['calendar']
        except KeyError:
            try:
                calendar = var.encoding['calendar']
            except KeyError:
                raise AttributeError('No calendar attribute found and none specified')

    # Default to src_units being the units for the variable (bounds variables
    # may not have correct units so in this case it has to be specified)
    if src_units is None:
        src_units = attributes["units"]

    # If no target_units are specified check if the variable has been previously
    # rebased and use this as the target, which will undo the previous rebasing
    if target_units == None:
        try:
            target_units = attributes[rebase_attr]
        except KeyError:
            raise AttributeError('No existing rebase found and target_units not specified')
        finally:
            del attributes[rebase_attr]
    else:
        attributes[rebase_attr] = src_units

    # Rebase
    newvar = xr.apply_ufunc(rebase_times, var, src_units, calendar, target_units, dask='allowed')

    if rebase_shift_attr in attributes:
        newvar = newvar - attributes[rebase_shift_attr]
        del attributes[rebase_shift_attr]
    else:
        if offset is not None:
            # Offset can be an integer, 'auto', or datetime.delta

            if offset == 'auto':
                # Generate a timedelta offset based on the calendars of src 
                # and target
                offset = num2date(0,target_units,calendar) - num2date(0,src_units,calendar)

            if isinstance(offset,datetime.timedelta):
                # Add delta to src calendar origin and convert to integer offset
                offset = date2num_round(num2date(0,src_units,calendar)+offset,src_units,calendar)

            newvar = newvar + offset
            attributes[rebase_shift_attr] = offset

    if  newvar.min() < 0:
        raise ValueError("Rebase creates negative dates, specify offset=auto to shift dates appropriately")

    # Save the values back into the variable, put back the attributes and update
    # the units
    newvar.attrs = attributes
    newvar.attrs['units'] = target_units

    return newvar

def rebase_dataset(ds, target_units=None, timevar='time', offset=None):
    """
    Rebase the time dimension variable in a dataset to a different start date.
    This is useful to overcome limitations in pandas datetime indices used in 
    xarray, and to place two datasets with different date indices onto a common
    date index
    """

    # The units are defined as the units used by timevar
    units = ds[timevar].attrs['units']
    calendar = ds[timevar].attrs['calendar']

    newds = ds.copy()

    # Cycle through all variables, setting a flag if they are a bounds variable
    flag_bounds(newds)

    for name in newds.variables:
        if is_bounds(newds[name]):
            # This is a bounds variable and has been flagged as such so ignore
            # as it will be processed by the variable for which it is the bounds
            continue
        if newds[name].attrs['units'] == units:
            newds[name] = rebase_variable(newds[name], calendar, target_units, offset=offset)
            if bounds in newds[name].attrs:
                # Must make the same adjustment to the bounds variable
                bvarname = newds[name].attrs[bounds]
                try:
                    newds[bvarname] = rebase_variable(newds[bvarname], calendar, target_units, src_units=units, offset=offset)
                except KeyError:
                    # Ignore if bounds_var missing
                    pass

    # Unset bounds flags
    unflag_bounds(newds)

    # newds = xr.decode_cf(newds, decode_coords=False, decode_times=True)

    return newds

def shift_time(ds):
    """
    Apply time shift to un-decoded time axis, to align datasets and 
    """
    pass