import pytest
import os
import shutil
import xarray as xr
from cosima_cookbook import database
from sqlalchemy import func

@pytest.fixture
def session_db(tmpdir):
    db = tmpdir.join('test.db')
    s = database.create_session(str(db))
    yield s, db

    s.close()

def test_broken(session_db):
    session, db = session_db
    indexed = database.build_index('test/data/indexing/broken_file', session)

    # make sure the database was created
    assert(db.check())

    # we indexed a single file
    assert(indexed == 1)

    # query ncfiles table -- should have a single file, marked as empty
    q = session.query(database.NCFile)
    r = q.all()
    assert(len(r) == 1)
    assert(not r[0].present)

    # query ncvars table -- should be empty
    q = session.query(func.count(database.NCVar.id))
    assert(q.scalar() == 0)

def test_update(client, session_db):
    session, db = session_db
    database.build_index('test/data/indexing/broken_file', session)
    assert(db.check())

    # re-run the index, make sure we don't re-index anything
    reindexed = database.build_index('test/data/indexing/broken_file', session, update=True)
    assert(reindexed == 0)

def test_single_broken(session_db):
    session, db = session_db
    database.build_index('test/data/indexing/single_broken_file', session)

    # query ncfiles table -- should have two entries
    q = session.query(func.count(database.NCFile.id))
    assert(q.scalar() == 2)

    # query ncvars table -- should have a single entry
    q = session.query(func.count(database.NCVar.id))
    assert(q.scalar() == 1)

def test_longnames(session_db):
    session, db = session_db
    database.build_index('test/data/indexing/longnames', session)

    # query ncvars table -- should have two entries
    q = session.query(func.count(database.NCVar.id))
    assert(q.scalar() == 2)

    # query generic table -- should only be a single variable
    q = session.query(database.CFVariable)
    r = q.all()
    assert(len(r) == 1)
    assert(r[0].long_name == 'Test Variable')

def test_distributed(client, session_db):
    session, db = session_db
    database.build_index('test/data/indexing/broken_file', session, client)

    assert(db.check())
    q = session.query(database.NCExperiment)
    r = q.all()
    assert(len(r) == 1)
