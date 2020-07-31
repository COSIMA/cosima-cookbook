import pytest

import os.path
import xarray as xr
import pandas as pd
from pandas.util.testing import assert_frame_equal, assert_series_equal

import cosima_cookbook as cc

@pytest.fixture(scope='module')
def session(tmpdir_factory):
    # index test directory into temp database
    d = tmpdir_factory.mktemp('database')
    db = d.join('test.db')
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
    with cc.querying.getvar('querying', 'temp', session, decode_times=False) as v:
        assert(isinstance(v, xr.DataArray))

def test_invalid_query(session):
    with pytest.raises(cc.querying.VariableNotFoundError):
        cc.querying.getvar('querying', 'notfound', session, decode_times=False)

def test_warning_on_ambiguous(session):
    with pytest.warns(UserWarning):
        cc.querying._ncfiles_for_variable("querying_disambiguation", "temp", session)

def test_query_times(session):
    with cc.querying.getvar('querying', 'ty_trans', session) as v:
        assert(isinstance(v, xr.DataArray))

def test_chunk_parsing_chunked(session):
    var = (session
           .query(cc.database.NCVar)
           .filter(cc.database.NCVar.varname == 'salt')
           .first())

    chunk_dict = {'time': 1,
                  'st_ocean': 15,
                  'yt_ocean': 216,
                  'xt_ocean': 288,}

    assert(cc.querying._parse_chunks(var) == chunk_dict)

def test_chunk_parsing_contiguous(session):
    var = (session
           .query(cc.database.NCVar)
           .filter(cc.database.NCVar.varname == 'potrho')
           .first())

    assert(var.chunking == 'contiguous')
    assert(cc.querying._parse_chunks(var) is None)

def test_chunk_parsing_unchunked(session):
    var = (session
           .query(cc.database.NCVar)
           .filter(cc.database.NCVar.varname == 'hi_m')
           .first())

    assert(var.chunking == 'None')
    assert(cc.querying._parse_chunks(var) is None)

def test_get_experiments(session):
    r = cc.querying.get_experiments(session)

    df = pd.DataFrame.from_dict(
        {"experiment": ["querying", "querying_disambiguation"], "ncfiles": [3, 2]}
    )
    assert_frame_equal(r, df)

    metadata_keys = ['root_dir', 'contact', 'email', 'created', 'description', 'notes']

    # Won't try and match everything, there is not much useful metadata, just 
    # check dimensions are correct. Metadata correctness checked in test_metadata
    for k in metadata_keys:
        r = cc.querying.get_experiments(session, **{k: True})
        assert(k == r.columns[1])
        assert(r.shape == (2,3))

    # Test all = True to select all available metadata
    r = cc.querying.get_experiments(session, all=True)
    assert(r.shape == (2,8))

    # Functionally equivalent to above
    r = cc.querying.get_experiments(session, **{k: True for k in metadata_keys})
    assert(r.shape == (2,8))

def test_get_ncfiles(session):
    r = cc.querying.get_ncfiles(session, 'querying')

    df = pd.DataFrame.from_dict(
        {
            "ncfile": [
                "output000/hi_m.nc",
                "output000/ocean.nc",
                "output000/ty_trans.nc",
            ],
            "index_time": [
                pd.Timestamp("2019-08-09 21:51:12.090930"),
                pd.Timestamp("2019-08-09 21:51:12.143794"),
                pd.Timestamp("2019-08-09 21:51:12.148942"),
            ],
        }
    )

    # The Timestamps will not be the same so check only that the ncfiles are correct
    assert_series_equal(r['ncfile'], df['ncfile'])

def test_get_variables(session):
    r = cc.querying.get_variables(session, 'querying', '1 monthly')

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
            "frequency": ["1 monthly"] * 6,
            "ncfile": ["output000/hi_m.nc"] * 6,
            "# ncfiles": [1] * 6,
            "time_start": ["1900-01-01 00:00:00"] * 6,
            "time_end": ["1900-02-01 00:00:00"] * 6,
        }
    )

    assert_frame_equal(r, df)

def test_get_frequencies(session):
    r = cc.querying.get_frequencies(session, 'querying')

    df = pd.DataFrame.from_dict({"frequency": [None, "1 monthly", "1 yearly"]})

    assert_frame_equal(r, df)

def test_disambiguation_by_frequency(session):

    with pytest.warns(UserWarning):
        assert(len(cc.querying._ncfiles_for_variable("querying", "time", session)) == 3)

    assert(len(cc.querying._ncfiles_for_variable("querying", "time", session, frequency='1 monthly')) == 1)
    assert(len(cc.querying._ncfiles_for_variable("querying", "time", session, frequency='1 yearly')) == 1)

    # Both of these select a single file and successfully return an xarray object
    assert(cc.querying.getvar("querying", "time", session, frequency='1 monthly').shape == (1,))
    assert(cc.querying.getvar("querying", "time", session, frequency='1 yearly').shape == (2,))
