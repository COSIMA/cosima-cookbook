"""
Common tools for accessing NetCDF4 variables
"""

__all__ = [
    "build_index",
]

import netCDF4
import re
import os
import sys
import fnmatch
import dask.bag
import distributed
from distributed.diagnostics.progressbar import progress
import xarray as xr
import subprocess
import tqdm
import IPython.display
import pickle

import logging

logging.basicConfig(level=logging.INFO)

from .date_utils import rebase_dataset


def database_url_from_path(path):
    return "sqlite:///" + os.path.join(path, "cosima-cookbook.db")


directoriesToSearch = [
    "/g/data3/hh5/tmp/cosima/",
    "/g/data1/v45/APE-MOM",
]

database_url = database_url_from_path("/g/data3/hh5/tmp/cosima/cosima-cookbook")


def build_index(use_bag=False, careful=False, expt_dir_list=None):
    """
    An experiment is a collection of outputNNN directories.  Each directory
    represents the output of a single job submission script. These directories
    are created by the *payu* tool.

    This function creates and/or updates an index cache of variables names
    found in all NetCDF4 files.

    Optional arguments:

        careful: if True, use slower but more thorough method. Use this if some
             files are missing from index. Default is False.

        use_bag: Default is False.

        expt_dir_list: list of experiment directories in which cosima-cookbook.db
             will be created/updated. At present this is used only in get_nc_variable.
             You must have write access to all these directories.
             If expt_dir_list=None (the default), a central database is used.

    We can also examine the .nc files directly to infer their contents.
    for each .nc file, get variables -> dimensions

    .ncfile, varname, dimensions, chunksize

    """

    # Build index of all NetCDF files found in directories to search.

    # dir_dbs dict keys are directories to search, values are corresponding db url
    if expt_dir_list is None:  # one db for all expts
        dir_dbs = {d: database_url for d in directoriesToSearch}
        maxdepth = 3
    else:  # a separate db at root of each expt dir
        if not isinstance(expt_dir_list, list):
            expt_dir_list = [expt_dir_list]
        dir_dbs = {d: database_url_from_path(d) for d in expt_dir_list}
        maxdepth = 1

    prev_db_url = ""
    for directoryToSearch, db_url in dir_dbs.items():
        print("Finding runs in {}... ".format(directoryToSearch), end="")
        ncfiles = []
        runs_available = []
        # find all output subdirectories
        try:
            results = subprocess.check_output(
                [
                    "find",
                    directoryToSearch,
                    "-maxdepth",
                    str(maxdepth),
                    "-type",
                    "d",
                    "-name",
                    "output???",
                ]
            )
            results = [s for s in results.decode("utf-8").split()]
            runs_available.extend(results)
        except:
            print(
                "{0} exception occurred while finding output directories in {1}".format(
                    sys.exc_info()[0], directoryToSearch
                )
            )

        runs_available = set(runs_available)
        print("found {} run directories".format(len(runs_available)))

        # ncfiles.extend(results)
        #
        #    results = subprocess.check_output(['find', directoryToSearch, '-name', '*.nc'])
        #
        #    print('Found {} .nc files'.format(len(ncfiles)))

        # We can persist this index by storing it in a sqlite database placed in a centrally available location.

        # The use of the `dataset` module hides the details of working with SQL directly.

        # In this database is a single table listing all variables in NetCDF4 seen previously.
        print("Using database {}".format(db_url))
        db = dataset.connect(db_url)
        db["ncfiles"]
        # db.create_table('ncfiles')  # has no effect if 'ncfiles' table already exists

        if not (
            db_url == prev_db_url
        ):  # avoid repeating db query when expt_dir_list is None
            runs_already_seen = set([])
            files_already_seen = set([])
            if (
                db["ncfiles"].count() > 0
            ):  # this also creates 'ncfiles' table if db is new
                if careful:  # filter by filename rather than dir
                    print("Querying database for files... ", end="")
                    rf = db.query("SELECT DISTINCT ncfile FROM ncfiles")
                    files_already_seen = set([row["ncfile"] for row in rf])
                    # files_already_seen = dict.fromkeys(rf) # use a dict for fast lookup
                    # files_already_seen = {n['ncfile']: None for n in rf}  # use a dict for fast lookup
                    print("files already indexed: {}".format(len(files_already_seen)))
                else:  # filter by dir rather than filename
                    # BUG: this can skip dirs even if they contain un-indexed .nc files - see https://github.com/OceansAus/cosima-cookbook/issues/95
                    print("Querying database for directories... ", end="")
                    # find list of all run directories
                    r = db.query(
                        "SELECT DISTINCT rootdir, configuration, experiment, run FROM ncfiles"
                    )
                    runs_already_seen = set([os.path.join(*row.values()) for row in r])
                    print(
                        "run directories already indexed: {}".format(
                            len(runs_already_seen)
                        )
                    )
        prev_db_url = db_url

        runs_to_index = list(runs_available - runs_already_seen)
        if len(runs_to_index) == 0:
            print("No new runs found in {}".format(directoryToSearch))
            continue
        #
        # print('{} new run directories found including... '.format(len(runs_to_index)))
        #
        # for i in range(min(3, len(runs_to_index))):
        #     print(runs_to_index[i])
        # if len(runs_to_index) > 3:
        #     print('...')

        print("Finding files in {} run directories... ".format(len(runs_to_index)))
        ncfiles = []
        for run in tqdm.tqdm_notebook(runs_to_index, leave=False):
            try:
                results = subprocess.check_output(["find", run, "-name", "*.nc"])
                results = [s for s in results.decode("utf-8").split()]
                ncfiles.extend(results)
            except:
                print(
                    "{0} exception occurred while finding *.nc in {1}".format(
                        sys.exc_info()[0], run
                    )
                )

        # IPython.display.clear_output(wait=True)
        # NetCDF files found on disk not seen before:
        files_to_add = set(ncfiles) - files_already_seen

        print("Files found but not yet indexed: {}".format(len(files_to_add)))

        # For these new files, we can determine their configuration, experiment, and run.
        # Using NetCDF4 to get list of all variables in each file.

        # output* directories
        # match the parent and grandparent directory to configuration/experiment
        find_output = re.compile("(.*)/([^/]*)/([^/]*)/(output\d+)/.*\.nc")

        # determine general pattern for ncfile names
        find_basename_pattern = re.compile(
            "(?P<root>[^\d]+)(?P<index>__\d+_\d+)?(?P<indexice>\.\d+\-\d+)?(?P<ext>\.nc)"
        )

        def index_variables(ncfile):
            matched = find_output.match(ncfile)
            if matched is None:
                return []

            if not os.path.exists(ncfile):
                return []

            # TODO: also exit here if ncfile is already in database - use [NOT] EXISTS ??
            # but this is super slow
            # module load sqlite
            # sqlite3 /g/data3/hh5/tmp/cosima/cosima-cookbook/cosima-cookbook.db
            # select * from ncfiles where ncfile == '/g/data3/hh5/tmp/cosima/access-om2-025/025deg_jra55v13_ryf8485_KDS50/output024/ocean/ocean_scalar.nc';

            basename = os.path.basename(ncfile)
            m = find_basename_pattern.match(basename)
            if m is None:
                basename_pattern = basename
            else:
                basename_pattern = (
                    m.group("root")
                    + ("__\d+_\d+" if m.group("index") else "")
                    + (".\d+-\d+" if m.group("indexice") else "")
                    + m.group("ext")
                )

            try:
                with netCDF4.Dataset(ncfile) as ds:
                    ncvars = [
                        {
                            "ncfile": ncfile,
                            "rootdir": matched.group(1),
                            "configuration": matched.group(2),
                            "experiment": matched.group(3),
                            "run": matched.group(4),
                            "basename": basename,
                            "basename_pattern": basename_pattern,
                            "variable": v.name,
                            "dimensions": str(v.dimensions),
                            "chunking": str(v.chunking()),
                        }
                        for v in ds.variables.values()
                    ]
            except:
                print(
                    "{0} exception occurred while trying to read {1}".format(
                        sys.exc_info()[0], ncfile
                    )
                )
                ncvars = []

            return ncvars

        if len(files_to_add) == 0:
            print("No new .nc files found in {}".format(directoryToSearch))
            continue

        print("Indexing {} new .nc files... ".format(len(files_to_add)))

        if use_bag:
            with distributed.Client() as client:
                bag = dask.bag.from_sequence(files_to_add)
                bag = bag.map(index_variables).flatten()

                futures = client.compute(bag)
                progress(futures, notebook=False)

                ncvars = futures.result()
        else:
            ncvars = []
            for file_to_add in tqdm.tqdm_notebook(files_to_add, leave=False):
                ncvars.extend(index_variables(file_to_add))
            IPython.display.clear_output()

        print("")
        print("Indexed {} variables found in new files".format(len(ncvars)))

        print("Saving results in database {}... ".format(db_url))
        db["ncfiles"].insert_many(ncvars)

    print("Indexing complete.")

    return True


def decode_time(dataset, time_units, offset):
    """
    Decode and offset time axis for a single dataset (preprocessing step for open_mfdataset).

    See get_nc_variable for documentation on the arguments.
    """

    if "time" in dataset.coords:
        calendar = None
        if time_units is None:
            time_units = dataset.time.units
            if "calendar" in dataset.time.attrs:
                calendar = dataset.time.calendar
            elif "calendar_type" in dataset.time.attrs:
                calendar = dataaray.time.calendar_type
        if offset is not None:
            dataset = rebase_dataset(dataset, time_units, offset=offset)
        try:
            decoded_time = xr.conventions.times.decode_cf_datetime(
                dataset.time, time_units, calendar=calendar
            )
        except:  # for compatibility with older xarray (pre-0.10.2 ?)
            decoded_time = xr.conventions.decode_cf_datetime(dataset.time, time_units)
        dataset.coords["time"] = (
            "time",
            decoded_time,
            {"long_name": "time", "decoded_using": time_units},
        )

    return dataset


def get_nc_variable(
    expt,
    ncfile,
    variable,
    chunks={},
    n=None,
    op=None,
    time_units="days since 1900-01-01",
    offset=None,
    use_bag=False,
    use_cache=False,
    **kwargs,
):
    """
    For a given experiment, concatenate together
    variable over all time given a basename ncfile.
    If variable is a list, then return a dataset for all given variables.

    If expt is an experiment name, a central database is used.
    If expt is the absolute path of the experiment directory, a separate
    database in that directory will be used instead. This must be
    created beforehand (and updated) via build_index(expt_dir_list=expt).

    Since some NetCDF4 files have trailing integers (e.g. ocean_123_456.nc)
    ncfile can use glob syntax http://www.sqlitetutorial.net/sqlite-glob/
    and regular expressions also work in some limited cases.

    By default, xarray is set to use the same chunking pattern that is
    stored in the ncfile. This can be overwritten by passing in a dictionary
    chunks or setting chunks=None for no chunking (load directly into memory).

    n < 0 means only use the last |n| ncfiles files.
    n > 0 means only use the first n ncfiles files.

    op() is function to apply to each variable before concatenating.
    TODO: implement this - currently does nothing.

    time_units (e.g. "days since 1600-01-01") can be used to override
    the time units specified in the .nc files.
    Default is "days since 1900-01-01".
    If time_units=None, no overriding is performed.
    NB: The effect of time_units depends on whether offset=None.
    If offset=None, time_units alters the interpretation of numerical time data
    in terms of dates, i.e. dates are changed if time_units differs from that
    in the .nc files.
    If offset!=None, the time data is altered to use time_units, so time_units
    no longer alters the dates if time_units differs from that
    in the .nc files. In particular, offset=None (the default) is not
    equivalent to offset=0 unless time_units matches what is in the .nc files.

    offset shifts the data by the specified number of days, to allow different
    experiments to be aligned in time and/or to work within the 2^64 nanosecond
    pandas time range. Valid values are None, a number, or 'auto'.
    Use with care ...
    NB: offset=None (the default) is not equivalent to offset=0 since it alters
    the interpretation of time_units (see above).

    use_cache determines whether to return a cached result, which is faster,
    but is not kept up to date with the .nc files. The cache file is persistent
    across kernel restarts. It can be deleted to save space or force an update.
    Switching to use_cache=False will also delete the cache file if it exists.
    The default is use_cache=False.

    """

    if expt.endswith("/"):
        expt = expt[:-1]  # assumes only one trailing slash...
    experiment = os.path.basename(expt)

    if not isinstance(variable, list):
        variables = [variable]
        return_dataarray = True
    else:
        variables = variable
        return_dataarray = False

    if time_units is None:
        tunits = str(time_units)
    else:
        tunits = time_units.replace(" ", "-")
    # BUG: cachefname doesn't include chunks or op
    # TODO: use all args in filename, perhaps via args = locals()  ...?
    cachefname = (
        "cache_get_nc_variable_"
        + "_".join(
            [
                experiment,
                ncfile,
                "_".join(variables),
                str(n),
                tunits,
                str(offset),
                str(use_bag),
            ]
        )
        + ".pkl"
    )

    if use_cache and os.path.isfile(cachefname):
        print("Reading from cache file {}".format(cachefname))
        with open(cachefname, "rb") as cachefile:
            return pickle.load(cachefile)
    else:
        if os.path.isabs(expt):
            db_url = database_url_from_path(expt)
        else:
            db_url = database_url
        print("Using database {}".format(db_url))
        db = dataset.connect(db_url)

        var_list = ",".join(['"{}"'.format(v) for v in variables])

        sql = " ".join(
            [
                "SELECT DISTINCT ncfile, dimensions, chunking ",
                "FROM ncfiles",
                f'WHERE experiment = "{experiment}"',
                "AND (",
                f'basename_pattern = "{ncfile}"',
                f'OR basename GLOB "{ncfile}"',
                ")",
                f"AND variable in ({var_list})",
                "ORDER BY ncfile",
            ]
        )

        logging.debug(sql)

        res = db.query(sql)
        rows = list(res)

        ncfiles = [row["ncfile"] for row in rows]

        # res.close()

        if len(ncfiles) == 0:
            raise ValueError(
                "No variable {} found for {} in {}".format(variable, expt, ncfile)
            )

        # print('Found {} ncfiles'.format(len(ncfiles)))

        dimensions = eval(rows[0]["dimensions"])
        chunking = eval(rows[0]["chunking"])

        # print ('chunking info', dimensions, chunking)
        if chunking is not None:
            default_chunks = dict(zip(dimensions, chunking))
        else:
            default_chunks = {}

        if chunks is not None:
            default_chunks.update(chunks)
            chunks = default_chunks

        if n is not None:
            # print('using last {} ncfiles only'.format(n))
            if n < 0:
                ncfiles = ncfiles[n:]
            else:
                ncfiles = ncfiles[:n]

        if op is None:
            op = lambda x: x

        # print ('Opening {} ncfiles...'.format(len(ncfiles)))
        logging.debug(f"Opening {len(ncfiles)} ncfiles...")

        if use_bag:
            bag = dask.bag.from_sequence(ncfiles)

            load_variable = lambda ncfile: xr.open_dataset(
                ncfile, chunks=chunks, decode_times=False
            )[variables]
            # bag = bag.map(load_variable, chunks, time_units, variables)
            bag = bag.map(load_variable)

            dataarrays = bag.compute()
            dataarray = xr.concat(
                dataarrays,
                dim="time",
                coords="all",
            )
            dataarray = decode_time(dataarray, time_units, offset)
        else:
            dataarray = xr.open_mfdataset(
                ncfiles,
                parallel=True,
                chunks=chunks,
                autoclose=True,
                decode_times=False,
                preprocess=lambda d: decode_time(d[variables], time_units, offset),
                **kwargs,
            )

        if return_dataarray:
            out = dataarray[variable]
        else:
            out = dataarray
        if use_cache:
            print("Saving cache file {}".format(cachefname))
            with open(cachefname, "wb") as cachefile:
                pkl = pickle.dump(out, cachefile, protocol=-1)
        else:
            if os.path.exists(cachefname):
                print("Deleting cache file {}".format(cachefname))
                os.remove(cachefname)
        return out


def get_scalar_variables(configuration):
    db = dataset.connect(database_url)

    rows = db.query(
        "SELECT DISTINCT variable FROM ncfiles "
        'WHERE basename = "ocean_scalar.nc" '
        "AND dimensions = \"('time', 'scalar_axis')\" "
        'AND configuration = "{configuration}"'.format(configuration=configuration)
    )
    variables = [row["variable"] for row in rows]

    return variables
