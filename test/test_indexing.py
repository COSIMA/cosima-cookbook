import pytest
from datetime import datetime
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

def test_multiple_experiments(session_db):
    session, db = session_db
    # index multiple experiments, which have duplicate data and therefore push
    # against some unique constraints
    database.build_index(['test/data/indexing/multiple/experiment_a', 'test/data/indexing/multiple/experiment_b'], session)

    q = session.query(database.NCExperiment)
    assert(q.count() == 2)

def test_same_expt_name(session_db):
    session, db = session_db
    # index multiple experiments with different root directories, but the same
    # final path component (experiment name)
    database.build_index(['test/data/indexing/multiple/experiment_a', 'test/data/indexing/alternate/experiment_a'], session)

    # the indexing shouldn't fail, and we should have two distinct experiments
    # with the same name

    q = (session
         .query(database.NCExperiment)
         .filter(database.NCExperiment.experiment == 'experiment_a'))
    r = q.all()
    assert(len(r) == 2)
    assert(r[0].root_dir != r[1].root_dir)

def test_metadata(session_db):
    session, db = session_db
    database.build_index('test/data/indexing/metadata', session)

    # query metadata
    q = session.query(database.NCExperiment.contact,
                      database.NCExperiment.created,
                      database.NCExperiment.description)
    r = q.one()
    assert(r[0] == 'The ACCESS Oracle')
    assert(r[1] == datetime(2018, 1, 1))
    assert(len(r[2]) > 0)

def test_broken_metadata(session_db):
    session, db = session_db
    indexed = database.build_index('test/data/indexing/broken_metadata', session)

    assert(indexed == 1)

def test_distributed(client, session_db):
    session, db = session_db
    database.build_index('test/data/indexing/broken_file', session, client)

    assert(db.check())
    q = session.query(database.NCExperiment)
    r = q.all()
    assert(len(r) == 1)
