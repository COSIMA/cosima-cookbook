def find_record_dimension(d):
    """Find the record dimension (i.e. time) in a netCDF4 Dataset."""

    for dim in d.dimensions:
        if d.dimensions[dim].isunlimited():
            return dim

    return None


def find_dimension_with_attribute(d, attribute, value):
    """Find a matching dimension with attribute=value, or None."""

    for dim in d.dimensions:
        if dim not in d.variables:
            continue

        if getattr(d.variables[dim], attribute, None) == value:
            return dim

    return None


def find_time_dimension(d):
    """Find a time dimension in a netCDF4 Dataset."""

    # this is a bit heuristic, but we cascade through some checks, guided by
    # the CF conventions

    dim = find_dimension_with_attribute(d, "standard_name", "time")
    if dim is not None:
        return dim

    dim = find_dimension_with_attribute(d, "axis", "T")
    if dim is not None:
        return dim

    dim = find_record_dimension(d)
    if dim is not None:
        return dim

    for dim in d.dimensions:
        if dim.lower() == "time":
            return dim

    # CF conventions also suggests the units attribute,
    # but time_bounds may have the same units, and a false positive
    # here could be very confusing...
    return None
