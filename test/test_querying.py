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
