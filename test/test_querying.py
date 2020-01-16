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
