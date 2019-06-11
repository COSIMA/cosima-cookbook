import pytest
import os.path
import xarray as xr
import cosima_cookbook as cc
from dask.distributed import Client

@pytest.fixture(scope='module')
def database(client, tmpdir_factory):
    # create dask client to index
    # index test directory into temp database
    d = tmpdir_factory.mktemp('database')
    db = d.join('test.db')
    cc.database.build_index('test/data/querying', client, str(db))

    return str(db)

def test_valid_query(database):
    with cc.querying.getvar('querying', 'temp', database, decode_times=False) as v:
        assert(isinstance(v, xr.DataArray))
    
def test_invalid_query(database):
    with pytest.raises(cc.querying.VariableNotFoundError):
        cc.querying.getvar('querying', 'notfound', database, decode_times=False)

def test_time_attribute_args(database):
    with cc.querying.getvar('querying', 'Heat', database,
                            time_units='days since 0001-01-01 00:00:00',
                            calendar='JULIAN',
                            rebase_times=False) as v:
        assert(isinstance(v, xr.DataArray))
