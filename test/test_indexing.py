import pytest
import os
import shutil
import xarray as xr
from cosima_cookbook import database

from sqlalchemy import select, func

def test_broken(client, tmpdir):
    db = tmpdir.join('test.db')
    database.build_index('test/data/indexing/broken_file', client, str(db))

    # make sure the database was created
    assert(db.check())

    conn, schema = database.create_database(str(db))

    # query ncfiles table
    q = select([func.count()]).select_from(schema['ncfiles'])
    r = conn.execute(q)

    assert(r.first()[0] == 0)

    q = select([func.count()]).select_from(schema['ncvars'])
    r = conn.execute(q)

    assert(r.first()[0] == 0)

def test_single_broken(client, tmpdir):
    db = tmpdir.join('test.db')
    database.build_index('test/data/indexing/single_broken_file', client, str(db))

    # make sure the database was created
    assert(db.exists())

    conn, schema = database.create_database(str(db))

    # query ncfiles table
    q = select([func.count()]).select_from(schema['ncfiles'])
    r = conn.execute(q)

    assert(r.first()[0] == 1)
