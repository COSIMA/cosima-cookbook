import pytest
import os.path
import xarray as xr
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
    expts = cc.querying.get_experiments(session)

    assert(expts == [('querying', 3)])

def test_get_variables(session):
    vars = cc.querying.get_variables('querying', session)

    assert(len(vars) == 45)

    names = [v[0] for v in vars]
    assert('temp' in names)
    assert('notfound' not in names)

def test_get_time_range(session):
    time_range = cc.querying.get_time_range('querying', 'ty_trans', session)

    assert(time_range == ('0166-01-01 00:00:00', '0168-01-01 00:00:00'))
