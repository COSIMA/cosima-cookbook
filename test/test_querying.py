import pytest
import os.path
import xarray as xr
import pandas as pd
from pandas.util.testing import assert_frame_equal, assert_series_equal
import cosima_cookbook as cc
from dask.distributed import Client

@pytest.fixture(scope='module')
def session(tmpdir_factory):
    # create dask client to index
    # index test directory into temp database
    d = tmpdir_factory.mktemp('database')
    db = d.join('test.db')
    session = cc.database.create_session(str(db))

    # build index for entire module
    cc.database.build_index('test/data/querying', session)

    return session

def test_valid_query(session):
    with cc.querying.getvar('querying', 'temp', session, decode_times=False) as v:
        assert(isinstance(v, xr.DataArray))

def test_invalid_query(session):
    with pytest.raises(cc.querying.VariableNotFoundError):
        cc.querying.getvar('querying', 'notfound', session, decode_times=False)

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

    df = pd.DataFrame.from_dict({'experiment': {0: 'querying'}, 'ncfiles': {0: 3}})
    assert_frame_equal(r, df)

def test_get_ncfiles(session):
    r = cc.querying.get_ncfiles(session, 'querying')

    df = pd.DataFrame.from_dict(
        {
            "ncfile": {
                0: "output000/hi_m.nc",
                1: "output000/ocean.nc",
                2: "output000/ty_trans.nc",
            },
            "index_time": {
                0: pd.Timestamp("2019-08-09 21:51:12.090930"),
                1: pd.Timestamp("2019-08-09 21:51:12.143794"),
                2: pd.Timestamp("2019-08-09 21:51:12.148942"),
            },
        }
    )

    # The Timestamps will not be the same so check only that the ncfiles are correct
    assert_series_equal(r['ncfile'], df['ncfile'])

def test_get_variables(session):
    r = cc.querying.get_variables(session, 'querying', '1 monthly')

    df = pd.DataFrame.from_dict({'name': {0: 'TLAT', 1: 'TLON', 2: 'hi_m', 3: 'tarea', 4: 'time', 5: 'time_bounds'},
                       'frequency': {0: '1 monthly', 1: '1 monthly', 2: '1 monthly', 3: '1 monthly', 4: '1 monthly', 5: '1 monthly'},
                       'ncfile': {0: 'output000/hi_m.nc', 1: 'output000/hi_m.nc', 2: 'output000/hi_m.nc', 3: 'output000/hi_m.nc', 4: 'output000/hi_m.nc', 5: 'output000/hi_m.nc'},
                       '# ncfiles': {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
                       'time_start': {0: '1900-01-01 00:00:00', 1: '1900-01-01 00:00:00', 2: '1900-01-01 00:00:00', 3: '1900-01-01 00:00:00', 4: '1900-01-01 00:00:00', 5: '1900-01-01 00:00:00'}, 'time_end': {0: '1900-02-01 00:00:00', 1: '1900-02-01 00:00:00', 2: '1900-02-01 00:00:00', 3: '1900-02-01 00:00:00', 4: '1900-02-01 00:00:00', 5: '1900-02-01 00:00:00'}})

    assert_frame_equal(r, df)

def test_get_frequencies(session):
    r = cc.querying.get_frequencies(session, 'querying')

    df = pd.DataFrame.from_dict({'frequency': {0: None, 1: '1 monthly', 2: '1 yearly'}})

    assert_frame_equal(r, df)
