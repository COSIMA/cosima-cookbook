import warnings

from datetime import datetime

import pytest

import xarray as xr
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import numpy as np

import cosima_cookbook as cc
from cosima_cookbook.querying import QueryWarning
from cosima_cookbook.database import NCFile, CFVariable


@pytest.fixture(scope="module")
def session(tmpdir_factory):
    # index test directory into temp database
    d = tmpdir_factory.mktemp("database")
    db = d.join("test.db")
    session = cc.database.create_session(str(db))

    # build index for entire module
    cc.database.build_index(
        ["test/data/querying", "test/data/querying_disambiguation"], session
    )

    # force all files to be marked as present, even though they're empty
    ncfiles = session.query(cc.database.NCFile).all()
    for f in ncfiles:
        f.present = True
    session.commit()

    return session


def test_valid_query(session):
    with cc.querying.getvar("querying", "temp", session, decode_times=False) as v:
        assert isinstance(v, xr.DataArray)
        assert len(v.attrs["ncfiles"]) == 1
        assert v.attrs["ncfiles"][0].endswith("test/data/querying/output000/ocean.nc")
        # Make sure other fields aren't included in attributes
        assert "index" not in v.attrs
        assert "root_dir" not in v.attrs
        # Make sure empty metadata fields haven't been included as attributes
        assert "contact" not in v.attrs
        assert "notes" not in v.attrs
        assert "description" not in v.attrs
        assert "email" not in v.attrs


def test_invalid_query(session):
    with pytest.raises(cc.querying.VariableNotFoundError):
        cc.querying.getvar("querying", "notfound", session, decode_times=False)


def test_warning_on_ambiguous_attr(session):

    with pytest.warns(QueryWarning) as record:
        cc.querying._ncfiles_for_variable(
            "querying_disambiguation",
            "v",
            session,
            attrs_unique={"cell_methods": "bar"},
        )

    assert len(record) == 1
    assert (
        record[0]
        .message.args[0]
        .startswith(
            "Your query returns variables from files with different cell_methods"
        )
    )

    with pytest.warns(QueryWarning) as record:
        files = cc.querying._ncfiles_for_variable(
            "querying_disambiguation",
            "u",
            session,
            attrs_unique={"cell_methods": "time: no_valid"},
        )

    assert len(files) == 2
    assert len(record) == 1
    assert (
        record[0]
        .message.args[0]
        .startswith(
            "Your query returns variables from files with different cell_methods"
        )
    )

    # Raise an exception if QueryWarning set to error
    warnings.simplefilter("error", QueryWarning)
    with pytest.raises(QueryWarning) as record:
        cc.querying._ncfiles_for_variable(
            "querying_disambiguation",
            "v",
            session,
            attrs_unique={"cell_methods": "bar"},
        )

    with warnings.catch_warnings(record=True) as record:
        # Turn off warnings, will run without exception
        # and record will be empty
        warnings.simplefilter("ignore", QueryWarning)

        cc.querying._ncfiles_for_variable(
            "querying_disambiguation",
            "v",
            session,
            attrs_unique={"cell_methods": "bar"},
        )

    assert len(record) == 0


def test_disambiguation_on_default_attr(session):

    files = cc.querying._ncfiles_for_variable(
        "querying_disambiguation",
        "v",
        session,
        attrs_unique={"cell_methods": "mean_pow(02)"},
    )

    assert len(files) == 1
    assert files[0].NCVar.attrs["cell_methods"] == "mean_pow(02)"

    files = cc.querying._ncfiles_for_variable(
        "querying_disambiguation",
        "v",
        session,
        attrs_unique={"cell_methods": "time: mean"},
    )

    assert len(files) == 1
    assert files[0].NCVar.attrs["cell_methods"] == "time: mean"

    # One file has no cell_methods attribute
    files = cc.querying._ncfiles_for_variable(
        "querying_disambiguation",
        "u",
        session,
        attrs_unique={"cell_methods": "time: mean"},
    )

    assert len(files) == 1
    assert files[0].NCVar.attrs["cell_methods"] == "time: mean"

    # Add another unique attribute not present (should be ignored)
    files = cc.querying._ncfiles_for_variable(
        "querying_disambiguation",
        "v",
        session,
        attrs_unique={"cell_methods": "time: mean", "foo": "bar"},
    )

    assert len(files) == 1
    assert files[0].NCVar.attrs["cell_methods"] == "time: mean"


def test_query_times(session):
    with cc.querying.getvar("querying", "ty_trans", session) as v:
        assert isinstance(v, xr.DataArray)


def test_chunk_parsing_chunked(session):
    var = (
        session.query(cc.database.NCVar)
        .filter(cc.database.NCVar.varname == "salt")
        .first()
    )

    chunk_dict = {
        "time": 1,
        "st_ocean": 15,
        "yt_ocean": 216,
        "xt_ocean": 288,
    }

    assert cc.querying._parse_chunks(var) == chunk_dict


def test_chunk_parsing_contiguous(session):
    var = (
        session.query(cc.database.NCVar)
        .filter(cc.database.NCVar.varname == "potrho")
        .first()
    )

    assert var.chunking == "contiguous"
    assert cc.querying._parse_chunks(var) is None


def test_chunk_parsing_unchunked(session):
    var = (
        session.query(cc.database.NCVar)
        .filter(cc.database.NCVar.varname == "hi_m")
        .first()
    )

    assert var.chunking == "None"
    assert cc.querying._parse_chunks(var) is None


def test_get_experiments(session):
    r = cc.querying.get_experiments(session)

    df = pd.DataFrame.from_dict(
        {"experiment": ["querying", "querying_disambiguation"], "ncfiles": [3, 2]}
    )
    assert_frame_equal(r, df)

    metadata_keys = [
        "root_dir",
        "contact",
        "email",
        "created",
        "url",
        "description",
        "notes",
    ]

    # Won't try and match everything, there is not much useful metadata, just
    # check dimensions are correct. Metadata correctness checked in test_metadata
    for k in metadata_keys:
        r = cc.querying.get_experiments(session, **{k: True})
        assert k == r.columns[1]
        assert r.shape == (2, 3)

    # Test all = True to select all available metadata
    r = cc.querying.get_experiments(session, all=True)
    assert r.shape == (2, 9)

    # Functionally equivalent to above
    r = cc.querying.get_experiments(session, **{k: True for k in metadata_keys})
    assert r.shape == (2, 9)

    # Functionally equivalent to above
    r = cc.querying.get_experiments(
        session, experiment=False, exptname="querying", all=True
    )
    assert r.shape == (1, 8)
    assert "experiment" not in r

    # Test for filtering by variables
    in_both = {"potrho_edges", "age_global", "tx_trans_rho"}
    only_in_querying = {"hi_m", "ty_trans"}

    r = cc.querying.get_experiments(session, variables=in_both)
    assert r.shape == (2, 2)

    r = cc.querying.get_experiments(session, variables=(in_both | only_in_querying))
    assert r.shape == (1, 2)

    r = cc.querying.get_experiments(
        session, variables=(in_both | only_in_querying | {"none"})
    )
    assert r.shape == (0, 0)


def test_get_ncfiles(session):
    r = cc.querying.get_ncfiles(session, "querying")

    df = pd.DataFrame.from_dict(
        {
            "ncfile": [
                "output000/hi_m.nc",
                "output000/ocean.nc",
                "restart000/ty_trans.nc",
            ],
            "index_time": [
                pd.Timestamp("2019-08-09 21:51:12.090930"),
                pd.Timestamp("2019-08-09 21:51:12.143794"),
                pd.Timestamp("2019-08-09 21:51:12.148942"),
            ],
        }
    )

    # The Timestamps will not be the same so check only that the ncfiles are correct
    assert_series_equal(r["ncfile"], df["ncfile"])


def test_get_variables(session):
    r = cc.querying.get_variables(session, "querying", "1 monthly")

    df = pd.DataFrame.from_dict(
        {
            "name": ["TLAT", "TLON", "hi_m", "tarea", "time", "time_bounds"],
            "long_name": [
                "T grid center latitude",
                "T grid center longitude",
                "grid cell mean ice thickness",
                "area of T grid cells",
                "model time",
                "boundaries for time-averaging interval",
            ],
            "units": [
                "degrees_north",
                "degrees_east",
                "m",
                "m^2",
                "days since 1900-01-01 00:00:00",
                "days since 1900-01-01 00:00:00",
            ],
            "frequency": ["1 monthly"] * 6,
            "ncfile": ["output000/hi_m.nc"] * 6,
            "cell_methods": [None, None, "time: mean", None, None, None],
            "# ncfiles": [1] * 6,
            "time_start": ["1900-01-01 00:00:00"] * 6,
            "time_end": ["1900-02-01 00:00:00"] * 6,
        }
    )

    assert_frame_equal(r, df)

    r = cc.querying.get_variables(session, "querying", search="temp")

    df = pd.DataFrame.from_dict(
        {
            "name": ["diff_cbt_t", "temp", "temp_xflux_adv", "temp_yflux_adv"],
            "long_name": [
                "total vert diff_cbt(temp) (w/o neutral included)",
                "Potential temperature",
                "cp*rho*dzt*dyt*u*temp",
                "cp*rho*dzt*dxt*v*temp",
            ],
            "units": ["m^2/s", "degrees K", "Watts", "Watts"],
            "frequency": [None] * 4,
            "ncfile": ["output000/ocean.nc"] * 4,
            "cell_methods": ["time: mean"] * 4,
            "# ncfiles": [1] * 4,
            "time_start": [None] * 4,
            "time_end": [None] * 4,
        }
    )

    assert_frame_equal(r, df)

    r = cc.querying.get_variables(session, search="temp")

    df = pd.DataFrame.from_dict(
        {
            "name": ["diff_cbt_t", "temp", "temp_xflux_adv", "temp_yflux_adv"],
            "long_name": [
                "total vert diff_cbt(temp) (w/o neutral included)",
                "Potential temperature",
                "cp*rho*dzt*dyt*u*temp",
                "cp*rho*dzt*dxt*v*temp",
            ],
            "units": ["m^2/s", "degrees K", "Watts", "Watts"],
        }
    )

    assert_frame_equal(r, df)

    r = cc.querying.get_variables(session, search=("temp", "velocity"))

    df = pd.DataFrame.from_dict(
        {
            "name": [
                "diff_cbt_t",
                "temp",
                "temp_xflux_adv",
                "temp_yflux_adv",
                "u",
                "v",
                "wt",
            ],
            "long_name": [
                "total vert diff_cbt(temp) (w/o neutral included)",
                "Potential temperature",
                "cp*rho*dzt*dyt*u*temp",
                "cp*rho*dzt*dxt*v*temp",
                "i-current",
                "j-current",
                "dia-surface velocity T-points",
            ],
            "units": [
                "m^2/s",
                "degrees K",
                "Watts",
                "Watts",
                "m/sec",
                "m/sec",
                "m/sec",
            ],
        }
    )

    r = cc.querying.get_variables(session, search=("temp", "velocity"))

    df = pd.DataFrame.from_dict(
        {
            "name": [
                "diff_cbt_t",
                "temp",
                "temp_xflux_adv",
                "temp_yflux_adv",
                "u",
                "v",
                "wt",
            ],
            "long_name": [
                "total vert diff_cbt(temp) (w/o neutral included)",
                "Potential temperature",
                "cp*rho*dzt*dyt*u*temp",
                "cp*rho*dzt*dxt*v*temp",
                "i-current",
                "j-current",
                "dia-surface velocity T-points",
            ],
            "units": [
                "m^2/s",
                "degrees K",
                "Watts",
                "Watts",
                "m/sec",
                "m/sec",
                "m/sec",
            ],
            "frequency": [None] * 7,
            "ncfile": ["output000/ocean.nc"] * 7,
            "# ncfiles": [1] * 7,
            "time_start": [None] * 7,
            "time_end": [None] * 7,
        }
    )


def test_model_property(session):

    filename_map = {
        "ocean": (
            "output/ocean/ice.nc",
            "output/ocn/land.nc",
            "output/ocean/atmos.nc",
            "ocean/ocean_daily.nc",
            "output/ocean/ocean_daily.nc.0000",
            "ocean/atmos.nc",
        ),
        "atmosphere": (
            "output/atm/fire.nc",
            "output/atmos/ice.nc",
            "output/atmosphere/ice.nc",
            "atmosphere/ice.nc",
            "atmos/ice.nc",
        ),
        "land": (
            "output/land/fire.nc",
            "output/lnd/ice.nc",
            "land/fire.nc",
            "lnd/ice.nc",
        ),
        "ice": (
            "output/ice/fire.nc",
            "output/ice/in/here/land.nc",
            "ice/fire.nc",
            "ice/in/here/land.nc",
        ),
        "none": (
            "output/ocean.nc",  # only a model if part of path, not filename
            "someotherpath/ocean_daily.nc",
            "lala/land_daily.nc.0000",
            "output/atmosphere_ice.nc",
            "output/noice/in/here/land.nc",
        ),
    }
    for model in filename_map:
        for fpath in filename_map[model]:
            ncfile = NCFile(
                index_time=datetime.now(),
                ncfile=fpath,
                present=True,
            )
            assert ncfile.model == model


def test_is_restart_property(session):

    filename_map = {
        True: (
            "output/restart/ice.nc",
            "output/restart000/land.nc",
            "restart/land.nc",
        ),
        False: (
            "output/restartice.nc",
            "output/lastrestart/land.nc",
        ),
    }
    for isrestart in filename_map:
        for fpath in filename_map[isrestart]:
            ncfile = NCFile(
                index_time=datetime.now(),
                ncfile=fpath,
                present=True,
            )
            assert ncfile.is_restart == isrestart

    # Grab all variables and ensure the SQL classification matches the python version
    # May be some holes, as not ensured all cases covered
    for index, row in cc.querying.get_variables(
        session, "querying", inferred=True
    ).iterrows():
        ncfile = NCFile(
            index_time=datetime.now(),
            ncfile=row.ncfile,
            present=True,
        )
        assert ncfile.is_restart == row.restart


def test_is_coordinate_property(session):

    units_map = {
        True: (
            "degrees_",
            "degrees_E",
            "degrees_N",
            "degrees_east",
            "hours since a long time ago",
            "radians",
            "days",
            "days since a while ago",
        ),
        False: ("degrees K",),
    }

    for iscoord in units_map:
        for units in units_map[iscoord]:
            assert CFVariable(name="bogus", units=units).is_coordinate == iscoord

    # Grab all variables and ensure the SQL classification matches the python version
    # May be some holes, as not ensured all cases covered
    for index, row in cc.querying.get_variables(session, inferred=True).iterrows():
        assert (
            CFVariable(name=row["name"], units=row.units).is_coordinate
            == row.coordinate
        )


def test_get_frequencies(session):
    r = cc.querying.get_frequencies(session, "querying")

    df = pd.DataFrame.from_dict({"frequency": [None, "1 monthly", "1 yearly"]})

    assert_frame_equal(r, df)


def test_disambiguation_by_frequency(session):

    with pytest.warns(UserWarning) as record:
        assert len(cc.querying._ncfiles_for_variable("querying", "time", session)) == 3

    if len(record) != 1:
        raise ValueError("|".join([r.message.args[0] for r in record]))

    assert len(record) == 1
    assert (
        record[0]
        .message.args[0]
        .startswith("Your query returns files with differing frequencies:")
    )

    assert (
        len(
            cc.querying._ncfiles_for_variable(
                "querying", "time", session, frequency="1 monthly"
            )
        )
        == 1
    )
    assert (
        len(
            cc.querying._ncfiles_for_variable(
                "querying", "time", session, frequency="1 yearly"
            )
        )
        == 1
    )

    # Both of these select a single file and successfully return an xarray object
    assert cc.querying.getvar(
        "querying", "time", session, frequency="1 monthly"
    ).shape == (1,)
    assert cc.querying.getvar(
        "querying", "time", session, frequency="1 yearly"
    ).shape == (2,)


def test_time_bounds_on_dataarray(session):
    var_salt = cc.querying.getvar("querying", "salt", session, decode_times=False)

    # we should have added time_bounds into the DataArray's attributes
    assert "time_bounds" in var_salt.attrs

    # and time_bounds should itself be a DataArray
    assert isinstance(var_salt.attrs["time_bounds"], xr.DataArray)


def test_query_with_attrs(session):
    attrs = {
        "long_name": "Practical Salinity",
        "units": "psu",
    }

    # a valid set of attributes
    var_salt = cc.querying.getvar(
        "querying", "salt", session, decode_times=False, attrs=attrs
    )

    for attr, val in attrs.items():
        assert var_salt.attrs[attr] == val

    # make sure that this is actually applied as an additional filter
    # by making failing queries
    # first: incorrect attribute value
    with pytest.raises(cc.querying.VariableNotFoundError):
        cc.querying.getvar(
            "querying",
            "salt",
            session,
            decode_times=False,
            attrs={"units": "degrees K"},
        )

    # second: non-present attribute name
    with pytest.raises(cc.querying.VariableNotFoundError):
        cc.querying.getvar(
            "querying", "salt", session, decode_times=False, attrs={"not_found": "psu"}
        )


def test_query_chunks(session, caplog):
    with cc.querying.getvar(
        "querying", "ty_trans", session, chunks={"invalid": 99}
    ) as v:
        assert "chunking along dimensions {'invalid'} is not possible" in caplog.text
