import pytest
import os
import sqlalchemy as sa
from cosima_cookbook import database


@pytest.fixture
def db_env(tmp_path):
    old_db = os.getenv("COSIMA_COOKBOOK_DB")
    db = tmp_path / "test.db"
    os.environ["COSIMA_COOKBOOK_DB"] = str(db)

    yield db

    # clean up by resetting the env var
    if old_db:
        os.environ["COSIMA_COOKBOOK_DB"] = old_db
    else:
        del os.environ["COSIMA_COOKBOOK_DB"]


def test_default(tmp_path):
    db = tmp_path / "test.db"
    # override the NCI-specific default
    database.__DEFAULT_DB__ = str(db)

    s = database.create_session()

    assert db.exists()


def test_env_var(db_env):
    # make sure we use the environment variable
    # override with no arguments supplied
    s = database.create_session()
    assert db_env.exists()


def test_arg_override(tmp_path, db_env):
    # check that if we supply an argument, that
    # is used rather than the environment variable
    db = tmp_path / "test_other.db"
    s = database.create_session(str(db))

    assert not db_env.exists()
    assert db.exists()


def test_creation(session_db):
    """Test that a database file is created with a session
    when the session file doesn't exist."""

    s, db = session_db
    assert db.exists()

    # we should be able to query against a table that exists
    # with no error
    s.execute("SELECT * FROM ncfiles")

    # but not a non-existent table
    with pytest.raises(sa.exc.OperationalError, match="no such table"):
        s.execute("SELECT * FROM ncfiles_notfound")


def test_reopen(tmp_path):
    """Test that we can reopen a database of the correct version."""

    db = tmp_path / "test.db"
    s = database.create_session(str(db))

    s.close()
    s = database.create_session(str(db))
    s.close()


def test_outdated(tmp_path):
    """Test that we can't use an outdated database"""

    db = tmp_path / "test.db"
    s = database.create_session(str(db))

    # check that the current version matches that defined in the module
    ver = s.execute("PRAGMA user_version").fetchone()[0]
    assert ver == database.__DB_VERSION__

    # reset version to one prior
    s.execute("PRAGMA user_version={}".format(database.__DB_VERSION__ - 1))
    s.close()

    # recreate the session
    with pytest.raises(Exception, match="Incompatible database versions"):
        s = database.create_session(str(db))


def test_outdated_notmodified(tmp_path):
    """Test that we don't try to modify an outdated database.
    This includes adding tables that don't yet exist because
    it's a previous version.
    """

    # set up an empty database with a previous version
    db = tmp_path / "test.db"
    conn = sa.create_engine("sqlite:///" + str(db)).connect()
    conn.execute("PRAGMA user_version={}".format(database.__DB_VERSION__ - 1))
    conn.close()

    # try to create the session
    # this should fail and not modify the existing database
    with pytest.raises(Exception):
        s = database.create_session(str(db))

    # reopen the connection and ensure tables weren't created
    conn = sa.create_engine("sqlite:///" + str(db)).connect()
    with pytest.raises(sa.exc.OperationalError, match="no such table"):
        conn.execute("SELECT * FROM ncfiles")


def test_delete_experiment(session_db):
    """Test that we can completely delete an experiment
    and its associated data.
    """

    session, db = session_db
    database.build_index("test/data/indexing/longnames", session)

    # make sure we actually did index something
    expt = (
        session.query(database.NCExperiment)
        .filter(database.NCExperiment.experiment == "longnames")
        .one_or_none()
    )
    assert expt is not None

    database.delete_experiment("longnames", session)
    expt = (
        session.query(database.NCExperiment)
        .filter(database.NCExperiment.experiment == "longnames")
        .one_or_none()
    )
    assert expt is None

    # check that all files are removed
    files = session.query(sa.func.count(database.NCFile.id)).scalar()
    assert files == 0

    # make sure all ncvars are removed
    vars = session.query(sa.func.count(database.NCVar.id)).scalar()
    assert vars == 0
