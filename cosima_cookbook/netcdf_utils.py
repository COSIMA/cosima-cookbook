def find_record_dimension(d):
    """Find the record dimension (i.e. time) in a netCDF4 Dataset."""

    for dim in d.dimensions:
        if d.dimensions[dim].isunlimited():
            return dim

    return None
