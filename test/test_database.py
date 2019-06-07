import pytest
import os
from cosima_cookbook import database

@pytest.fixture
def db_env(tmpdir):
    old_db = os.getenv('COSIMA_COOKBOOK_DB')
    db = tmpdir.join('test.db')
    os.environ['COSIMA_COOKBOOK_DB'] = str(db)

    yield db

    # clean up by resetting the env var
    if old_db:
        os.environ['COSIMA_COOKBOOK_DB'] = old_db
    else:
        del os.environ['COSIMA_COOKBOOK_DB']

def test_default(tmpdir):
    db = tmpdir.join('test.db')
    # override the NCI-specific default
    database.__DEFAULT_DB__ = str(db)
    
    s = database.create_session()
    
    assert(db.check())

def test_env_var(db_env):
    # make sure we use the environment variable
    # override with no arguments supplied
    s = database.create_session()
    assert(db_env.check())

def test_arg_override(tmpdir, db_env):
    # check that if we supply an argument, that
    # is used rather than the environment variable
    db = tmpdir.join('test_other.db')
    s = database.create_session(str(db))
    
    assert(not db_env.check())
    assert(db.check())
