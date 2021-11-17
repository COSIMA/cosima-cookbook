import logging
import os
import pytest
import shutil
import time
import xarray as xr
from pathlib import Path
from cosima_cookbook import database
from sqlalchemy import func, inspect

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def unreadable_dir(tmpdir):
    expt_path = tmpdir / "expt_dir"
    expt_path.mkdir()
    idx_dir = expt_path / "unreadable"
    idx_dir.mkdir()
    idx_dir.chmod(0o300)

    yield idx_dir

    idx_dir.chmod(0o700)
    expt_path.remove(ignore_errors=True)


def test_find_files():

    files = database.find_files("test/data/indexing/")
    assert len(files) == 16

    for f in files:
        assert Path(f).suffix == ".nc"

    # No python source files in data subdirectory
    assert len(database.find_files("test/data/indexing/", "*.py")) == 0

    # Test works with alternative suffix
    files = database.find_files("test/", "*.py")
    assert len(files) == 8

    for f in files:
        assert Path(f).suffix == ".py"


def test_find_experiment(session_db):
    session, db = session_db

    directory = Path("test/data/indexing/broken_file")

    assert None == database.find_experiment(session, directory)

    expt = database.NCExperiment(
        experiment=str(directory.name), root_dir=str(directory.resolve())
    )
    session.add(expt)

    assert expt == database.find_experiment(session, directory)


def test_index_experiment(session_db):
    session, db = session_db

    directory = Path("test/data/indexing/longnames")
    expt = database.NCExperiment(
        experiment=str(directory.name), root_dir=str(directory.resolve())
    )

    files = database.find_files(directory)

    # Index just one file
    database.index_experiment(set(list(files)[:1]), session, expt)

    assert expt == database.find_experiment(session, directory)
    assert len(database.find_experiment(session, directory).ncfiles) == 1

    # Index the other file
    database.index_experiment(set(list(files)[1:]), session, expt)

    assert expt == database.find_experiment(session, directory)
    assert len(database.find_experiment(session, directory).ncfiles) == 2


def test_unreadable(session_db, unreadable_dir):
    session, db = session_db

    with pytest.warns(UserWarning, match="Some files or directories could not be read"):
        indexed = database.build_index(str(unreadable_dir), session)


def test_broken(session_db):
    session, db = session_db
    indexed = database.build_index("test/data/indexing/broken_file", session)

    # make sure the database was created
    assert db.check()

    # we indexed a single file
    assert indexed == 1

    # query ncfiles table -- should have a single file, marked as empty
    q = session.query(database.NCFile)
    r = q.all()
    assert len(r) == 1
    assert not r[0].present

    # query ncvars table -- should be empty
    q = session.query(func.count(database.NCVar.id))
    assert q.scalar() == 0


def test_empty_file(session_db):
    session, db = session_db
    indexed = database.build_index("test/data/indexing/empty_file", session)

    # as with test_broken, we should have seen a single file,
    # but it should be marked as empty
    assert db.check()
    assert indexed == 1
    q = session.query(database.NCFile)
    r = q.all()
    assert len(r) == 1
    assert not r[0].present

    # but there should be a valid variable
    q = session.query(func.count(database.NCVar.id)).filter(
        database.NCVar.varname == "ty_trans_rho"
    )
    assert q.scalar() == 1


def test_update_nonew(session_db):
    session, db = session_db
    database.build_index("test/data/indexing/broken_file", session)
    assert db.check()

    # re-run the index, make sure we don't re-index anything
    reindexed = database.build_index(
        "test/data/indexing/broken_file", session, prune="flag"
    )
    assert reindexed == 0


def test_reindex_force(session_db):
    session, db = session_db
    database.build_index("test/data/indexing/broken_file", session)
    assert db.check()

    # re-run the index, make sure re-index
    reindexed = database.build_index(
        "test/data/indexing/broken_file", session, force=True
    )
    assert reindexed == 1


def test_update_newfile(session_db, tmpdir):
    session, db = session_db
    shutil.copy(
        "test/data/indexing/longnames/output000/test1.nc", str(tmpdir / "test1.nc")
    )
    database.build_index(str(tmpdir), session)

    # add another file
    shutil.copy(
        "test/data/indexing/longnames/output000/test2.nc", str(tmpdir / "test2.nc")
    )
    reindexed = database.build_index(str(tmpdir), session)
    assert reindexed == 1


def test_updated_file(session_db, tmpdir, caplog):
    session, db = session_db

    # Make tmpdir a concrete path otherwise filesystem ops won't work
    tmpdir = Path(tmpdir)

    ncfile = "test1.nc"
    ncpath = Path("test/data/indexing/longnames/output000/") / ncfile
    shutil.copy(str(ncpath), str(tmpdir / ncfile))
    indexed = database.build_index(str(tmpdir), session)
    assert indexed == 1

    # Should not reindex
    reindexed = database.build_index(str(tmpdir), session)
    assert reindexed == 0

    # Should reindex as file is updated
    time.sleep(1)
    (tmpdir / ncfile).touch()
    reindexed = database.build_index(str(tmpdir), session)
    assert reindexed == 1

    # Should not reindex as flagging as missing will not remove
    # file from the database, so will not be reindexed
    time.sleep(1)
    (tmpdir / ncfile).touch()
    with caplog.at_level(logging.WARNING):
        reindexed = database.build_index(str(tmpdir), session, prune="flag")
        assert reindexed == 0
        assert "Set prune to 'delete' to reindex updated files" in caplog.text


def test_single_broken(session_db):
    session, db = session_db
    database.build_index("test/data/indexing/single_broken_file", session)

    # query ncfiles table -- should have two entries
    q = session.query(func.count(database.NCFile.id))
    assert q.scalar() == 2

    # query ncvars table -- should have a single entry
    q = session.query(func.count(database.NCVar.id))
    assert q.scalar() == 1


def test_longnames(session_db):
    session, db = session_db
    database.build_index("test/data/indexing/longnames", session)

    # query ncvars table -- should have two entries
    q = session.query(func.count(database.NCVar.id))
    assert q.scalar() == 2

    # query generic table -- should only be a single variable
    q = session.query(database.CFVariable)
    r = q.all()
    assert len(r) == 1
    assert r[0].long_name == "Test Variable"


def test_multiple_experiments(session_db):
    session, db = session_db
    # index multiple experiments, which have duplicate data and therefore push
    # against some unique constraints
    database.build_index(
        [
            "test/data/indexing/multiple/experiment_a",
            "test/data/indexing/multiple/experiment_b",
        ],
        session,
    )

    q = session.query(database.NCExperiment)
    assert q.count() == 2


def test_same_expt_name(session_db):
    session, db = session_db
    # index multiple experiments with different root directories, but the same
    # final path component (experiment name)
    database.build_index(
        [
            "test/data/indexing/multiple/experiment_a",
            "test/data/indexing/alternate/experiment_a",
        ],
        session,
    )

    # the indexing shouldn't fail, and we should have two distinct experiments
    # with the same name

    q = session.query(database.NCExperiment).filter(
        database.NCExperiment.experiment == "experiment_a"
    )
    r = q.all()
    assert len(r) == 2
    assert r[0].root_dir != r[1].root_dir


def test_following_symlinks(session_db):
    session, db = session_db

    # Indexing symlinked experiment should fail with default arguments
    database.build_index("test/data/indexing/symlinked/experiment_a", session)

    q = session.query(database.NCExperiment)
    assert q.count() == 0

    # Now specify to follow symlinks
    database.build_index(
        "test/data/indexing/symlinked/experiment_a", session, followsymlinks=True
    )

    q = session.query(database.NCExperiment)
    assert q.count() == 1


def test_broken_metadata(session_db):
    session, db = session_db
    indexed = database.build_index("test/data/indexing/broken_metadata", session)

    assert indexed == 1


def test_time_dimension(session_db):
    session, db = session_db
    database.build_index("test/data/indexing/time", session)

    q = session.query(database.NCFile.time_start, database.NCFile.time_end)
    assert q.count() == 5  # should pick up 5 files

    q = q.filter(
        (database.NCFile.time_start is None) | (database.NCFile.time_end is None)
    )
    assert q.count() == 0  # but all of them should have times populated

    # there should be 5 separate time variables
    q = session.query(database.CFVariable)
    assert q.count() == 5

    # each file should have exactly one time dimension
    q = (
        session.query(func.count(database.NCFile.ncvars))
        .join(database.NCFile.ncvars)
        .group_by(database.NCFile.id)
    )
    for r in q.all():
        assert r[0] == 1


def test_index_attributes(session_db):
    session, db = session_db
    database.build_index("test/data/querying", session)

    inspector = inspect(session.get_bind())
    assert inspector.get_indexes("ncattributes")[0] == {
        "name": "ix_ncattributes_ncvar_id",
        "column_names": ["ncvar_id"],
        "unique": 0,
    }

    ncfile = "output000/ocean.nc"

    # check that we have the right attributes for a file (just use a subset)
    f = session.query(database.NCFile).filter(database.NCFile.ncfile == ncfile).one()

    file_attrs = {
        "filename": "ocean.nc",
        "title": "MOM5",
        "grid_type": "mosaic",
        "grid_tile": "1",
    }
    for attr, attr_val in file_attrs.items():
        assert attr in f.attrs and f.attrs[attr] == attr_val

    # and check a particular variable
    v = (
        session.query(database.NCVar)
        .join(database.NCFile)
        .filter(database.NCFile.ncfile == ncfile)
        .filter(database.NCVar.varname == "temp")
        .one()
    )
    var_attrs = {
        "long_name": "Potential temperature",
        "cell_methods": "time: mean",
        "coordinates": "geolon_t geolat_t",
    }
    for attr, attr_val in var_attrs.items():
        assert attr in v.attrs and v.attrs[attr] == attr_val


def test_distributed(client, session_db):
    session, db = session_db
    database.build_index("test/data/indexing/broken_file", session, client)

    assert db.check()
    q = session.query(database.NCExperiment)
    r = q.all()
    assert len(r) == 1


def test_prune_broken(session_db):
    session, db = session_db
    database.build_index("test/data/indexing/broken_file", session)

    assert db.check()

    # check that we have one file
    q = session.query(database.NCFile)
    r = q.all()
    assert len(r) == 1

    # prune experiment
    database.prune_experiment("broken_file", session)

    # now the database should be empty
    q = session.query(database.NCFile)
    r = q.all()
    assert len(r) == 0


def test_prune_missing_experiment(session_db):
    session, db = session_db
    database.build_index("test/data/indexing/broken_file", session)

    assert db.check()

    # check that we have one file
    q = session.query(database.NCFile)
    r = q.all()
    assert len(r) == 1

    # prune experiment
    experiment = "incorrect_experiment"
    with pytest.raises(RuntimeError, match="No such experiment: ".format(experiment)):
        database.prune_experiment(experiment, session)


def test_prune_nodelete(session_db, tmpdir):
    session, db = session_db
    expt_dir = tmpdir / "expt"
    expt_dir.mkdir()

    # copy the file to a new experiment directory and index
    shutil.copy(
        "test/data/indexing/longnames/output000/test1.nc", str(expt_dir / "test1.nc")
    )
    database.build_index(str(expt_dir), session)

    # check that we have a valid file
    q = session.query(database.NCFile).filter(database.NCFile.present)
    r = q.all()
    assert len(r) == 1

    # remove the file and prune
    os.remove(expt_dir / "test1.nc")
    database.prune_experiment("expt", session, delete=False)

    # now we should still have one file, but now not present
    q = session.query(database.NCFile)
    r = q.one_or_none()
    assert r is not None
    assert not r.present


def test_prune_delete(session_db, tmpdir):
    session, db = session_db
    expt_dir = tmpdir / "expt"
    expt_dir.mkdir()

    # copy the file to a new experiment directory and index
    shutil.copy(
        "test/data/indexing/longnames/output000/test1.nc", str(expt_dir / "test1.nc")
    )
    database.build_index(str(expt_dir), session)

    # check that we have a valid file
    q = session.query(database.NCFile).filter(database.NCFile.present)
    r = q.all()
    assert len(r) == 1

    # remove the file and prune
    os.remove(expt_dir / "test1.nc")
    database.prune_experiment("expt", session)

    # now we should still have no files
    q = session.query(database.NCFile)
    r = q.one_or_none()
    assert r is None


def test_index_with_prune_nodelete(session_db, tmpdir):
    session, db = session_db
    expt_dir = tmpdir / "expt"
    expt_dir.mkdir()

    # copy the file to a new experiment directory and index
    shutil.copy(
        "test/data/indexing/longnames/output000/test1.nc", str(expt_dir / "test1.nc")
    )
    database.build_index(str(expt_dir), session)

    # check that we have a valid file
    q = session.query(database.NCFile).filter(database.NCFile.present)
    r = q.all()
    assert len(r) == 1

    # remove the file and build with pruning
    os.remove(expt_dir / "test1.nc")
    database.build_index(str(expt_dir), session, prune="flag")

    # now we should still have one file, but now not present
    q = session.query(database.NCFile)
    r = q.one_or_none()
    assert r is not None
    assert not r.present


def test_index_with_prune_delete(session_db, tmpdir):
    session, db = session_db
    expt_dir = tmpdir / "expt"
    expt_dir.mkdir()

    # copy the file to a new experiment directory and index
    shutil.copy(
        "test/data/indexing/longnames/output000/test1.nc", str(expt_dir / "test1.nc")
    )
    database.build_index(str(expt_dir), session)

    # check that we have a valid file
    q = session.query(database.NCFile).filter(database.NCFile.present)
    r = q.all()
    assert len(r) == 1

    # remove the file and build with pruning
    os.remove(expt_dir / "test1.nc")
    database.build_index(str(expt_dir), session, prune="delete")

    # now we should still have no files
    q = session.query(database.NCFile)
    r = q.one_or_none()
    assert r is None
