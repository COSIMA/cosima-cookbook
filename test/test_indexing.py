import pytest
import os
import shutil
import xarray as xr
from cosima_cookbook import database
from dask.distributed import Client

from sqlalchemy import select, func

@pytest.fixture(scope='module')
def client():
    client = Client()
    yield client
    client.close()

def test_broken(client, tmp_path):
    db = tmp_path / 'test.db'
    database.build_index('test/data/indexing/broken_file', client, str(db))

    # make sure the database was created
    assert(db.exists())

    conn, schema = database.create_database(str(db))

    # query ncfiles table
    q = select([func.count()]).select_from(schema['ncfiles'])
    r = conn.execute(q)

    assert(r.first()[0] == 0)

    q = select([func.count()]).select_from(schema['ncvars'])
    r = conn.execute(q)

    assert(r.first()[0] == 0)

def test_single_broken(client, tmp_path):
    db = tmp_path / 'test.db'
    database.build_index('test/data/indexing/single_broken_file', client, str(db))

    # make sure the database was created
    assert(db.exists())

    conn, schema = database.create_database(str(db))

    # query ncfiles table
    q = select([func.count()]).select_from(schema['ncfiles'])
    r = conn.execute(q)

    assert(r.first()[0] == 1)
