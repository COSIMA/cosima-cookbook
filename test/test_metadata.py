import pytest
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from cosima_cookbook import database, querying


def metadata_for_experiment(path, session, name="test", commit=True):
    """Method to read metadata for an experiment without requiring
    the rest of the indexing infrastructure.
    """

    expt = database.NCExperiment(experiment=name, root_dir=path)
    database.update_metadata(expt, session)

    if commit:
        session.add(expt)
        session.commit()
    else:
        return expt


def test_metadata(session_db):
    """Test that metadata.yaml is read for an experiment during indexing"""

    session, db = session_db
    database.build_index("test/data/indexing/metadata", session)

    # query metadata
    q = session.query(
        database.NCExperiment.contact,
        database.NCExperiment.created,
        database.NCExperiment.description,
    )
    r = q.one()
    assert r[0] == "The ACCESS Oracle"
    assert r[1] == "2018-01-01"
    assert len(r[2]) > 0


def test_get_experiments_metadata(session_db):
    """Test that get_experiments returns metadata correctly"""

    session, db = session_db
    database.build_index("test/data/indexing/metadata", session)

    r = querying.get_experiments(session, contact=True)
    df = pd.DataFrame.from_dict(
        {"experiment": ["metadata"], "contact": ["The ACCESS Oracle"], "ncfiles": [1]}
    )
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, email=True)
    df = pd.DataFrame.from_dict(
        {"experiment": ["metadata"], "email": ["oracle@example.com"], "ncfiles": [1]}
    )
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, url=True)
    df = pd.DataFrame.from_dict(
        {
            "experiment": ["metadata"],
            "url": ["https://github.com/COSIMA/oracle"],
            "ncfiles": [1],
        }
    )
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, description=True)
    df = pd.DataFrame.from_dict(
        {
            "experiment": ["metadata"],
            "description": [
                (
                    "Attempted spinup, using salt flux fix "
                    "https://arccss.slack.com/archives/C6PP0GU9Y/p1515460656000124 "
                    "and https://github.com/mom-ocean/MOM5/pull/208/commits/9f4ee6f8b72b76c96a25bf26f3f6cdf773b424d2 "
                    "from the start. Used mushy ice from July year 1 onwards to avoid vertical thermo error in cice "
                    "https://arccss.slack.com/archives/C6PP0GU9Y/p1515842016000079"
                )
            ],
            "ncfiles": [1],
        }
    )
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, notes=True)
    df = pd.DataFrame.from_dict(
        {
            "experiment": ["metadata"],
            "notes": [
                (
                    "Stripy salt restoring: "
                    "https://github.com/OceansAus/access-om2/issues/74 tripole seam bug: "
                    "https://github.com/OceansAus/access-om2/issues/86 requires dt=300s "
                    "in May, dt=240s in Aug to maintain CFL in CICE near tripoles (storms "
                    "in those months in 8485RYF); all other months work with dt=400s"
                )
            ],
            "ncfiles": [1],
        }
    )
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, created=True)
    df = pd.DataFrame.from_dict(
        {"experiment": ["metadata"], "created": ["2018-01-01"], "ncfiles": [1]}
    )
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, root_dir=True)
    # Won't try and match a path that can change on different platforms
    # assert_frame_equal(r, df)
    assert r.shape == (1, 3)

    r = querying.get_experiments(session, all=True)
    # Won't try and match everything, just check dimensions are correct
    assert r.shape == (1, 9)

    # Test turning off returning experiment (bit dumb, but hey ...)
    r = querying.get_experiments(session, experiment=False)
    df = pd.DataFrame.from_dict({"ncfiles": [1]})
    assert_frame_equal(r, df)


def test_keywords(session_db):
    """Test that keywords are read for an experiment"""

    session, db = session_db
    metadata_for_experiment("test/data/metadata/keywords", session)

    q = session.query(database.NCExperiment).filter(
        database.NCExperiment.experiment == "test"
    )
    r = q.one()
    assert len(r.keywords) == 3
    assert "cosima" in r.keywords
    assert "not-a-keyword" not in r.keywords


def test_duplicate_keywords_commit(session_db):
    """Test that the uniqueness constraint works across experiments.
    This simulates separate index calls, where the session is committed in between.
    """

    session, db = session_db
    metadata_for_experiment("test/data/metadata/keywords", session, name="e1")
    metadata_for_experiment("test/data/metadata/keywords2", session, name="e2")

    q = session.query(database.Keyword)
    r = q.all()
    assert len(r) == 4


def test_duplicate_keywords_nocommit(session_db):
    """Test that the uniqueness constraint works across experiments.
    This simulates multiple experiments being added in a single call.
    """

    session, db = session_db
    e1 = metadata_for_experiment(
        "test/data/metadata/keywords", session, name="e1", commit=False
    )
    e2 = metadata_for_experiment(
        "test/data/metadata/keywords2", session, name="e2", commit=False
    )
    session.add_all([e1, e2])
    session.commit()

    q = session.query(database.Keyword)
    r = q.all()
    assert len(r) == 4


def test_keyword_upcast(session_db):
    """Test that a string keyword is added correctly."""

    session, db = session_db
    metadata_for_experiment("test/data/metadata/string_keyword", session)

    q = session.query(database.NCExperiment).filter(
        database.NCExperiment.experiment == "test"
    )
    r = q.one()
    assert "cosima" in r.keywords
    assert "c" not in r.keywords  # make sure it wasn't added as a string


def test_keyword_case_sensitivity(session_db):
    """Test that keywords are treated in a case-insensitive manner,
    both for metadata retrieval and querying.
    """

    session, db = session_db
    metadata_for_experiment("test/data/metadata/keywords", session, name="e1")
    metadata_for_experiment("test/data/metadata/upcase", session, name="e2")

    # we should be able to find the keyword in lowercase
    q = session.query(database.Keyword).filter(database.Keyword.keyword == "cosima")
    k1 = q.one_or_none()
    assert k1 is not None

    # and in uppercase
    q = session.query(database.Keyword).filter(database.Keyword.keyword == "COSIMA")
    k2 = q.one_or_none()
    assert k2 is not None

    # but they should resolve to the same keyword
    assert k1 is k2

    # finally, the set of keywords should all be lowercase
    q = session.query(database.NCExperiment).filter(
        database.NCExperiment.experiment == "e2"
    )
    r = q.one()
    for kw in r.keywords:
        assert kw == kw.lower()


def test_get_keywords(session_db):
    """Test retrieval of keywords"""

    session, db = session_db
    metadata_for_experiment("test/data/metadata/keywords", session, name="e1")
    metadata_for_experiment("test/data/metadata/keywords2", session, name="e2")

    # Grab keywords for individual experiments
    r = querying.get_keywords(session, "e1")
    assert r == {"access-om2-01", "ryf9091", "cosima"}

    r = querying.get_keywords(session, "e2")
    assert r == {"another-keyword", "cosima"}

    # Test retrieving all keywords
    r = querying.get_keywords(session)
    assert r == {"access-om2-01", "ryf9091", "another-keyword", "cosima"}


def test_get_experiments_with_keywords(session_db):
    """Test retrieval of experiments with keyword filtering"""
    session, db = session_db
    database.build_index("test/data/metadata/keywords", session)
    database.build_index("test/data/metadata/keywords2", session)

    # Test keyword common to both experiments
    r = querying.get_experiments(session, keywords="cosima")
    df = pd.DataFrame.from_dict(
        {"experiment": ["keywords", "keywords2"], "ncfiles": [1, 1]}
    )
    assert_frame_equal(r, df)

    # Test keyword common to both experiments using wildcard
    r = querying.get_experiments(session, keywords="cos%")
    df = pd.DataFrame.from_dict(
        {"experiment": ["keywords", "keywords2"], "ncfiles": [1, 1]}
    )
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, keywords="%-%")
    df = pd.DataFrame.from_dict(
        {"experiment": ["keywords", "keywords2"], "ncfiles": [1, 1]}
    )
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, keywords="access-om2%")
    df = pd.DataFrame.from_dict({"experiment": ["keywords"], "ncfiles": [1]})
    assert_frame_equal(r, df)

    # Test keyword in only one experiment
    r = querying.get_experiments(session, keywords="another-keyword")
    df = pd.DataFrame.from_dict({"experiment": ["keywords2"], "ncfiles": [1]})
    assert_frame_equal(r, df)

    r = querying.get_experiments(session, keywords="ryf9091")
    df = pd.DataFrame.from_dict({"experiment": ["keywords"], "ncfiles": [1]})
    assert_frame_equal(r, df)

    # Test passing an array of keywords that match only one experiment
    r = querying.get_experiments(session, keywords=["cosima", "another-keyword"])
    df = pd.DataFrame.from_dict({"experiment": ["keywords2"], "ncfiles": [1]})
    assert_frame_equal(r, df)

    # Test passing an array of keywords that will not match any one experiment
    r = querying.get_experiments(session, keywords=["another-keyword", "ryf9091"])
    df = pd.DataFrame()
    assert_frame_equal(r, df)

    # Test passing a non-existent keyword along with one present. Should return
    # nothing as no experiment contains it
    r = querying.get_experiments(session, keywords=["ryf9091", "not-a-keyword"])
    df = pd.DataFrame()
    assert_frame_equal(r, df)

    # Test passing only a non-existent keyword
    r = querying.get_experiments(session, keywords=["not-a-keyword"])
    df = pd.DataFrame()
    assert_frame_equal(r, df)

    # Test passing only a non-existent wildcard keyword
    r = querying.get_experiments(session, keywords=["z%"])
    df = pd.DataFrame()
    assert_frame_equal(r, df)


def test_getvar_with_metadata(session_db):

    session, db = session_db
    database.build_index("test/data/indexing/metadata", session)

    with querying.getvar("metadata", "test", session, decode_times=False) as v:
        assert v.attrs["long_name"] == "Test Variable"
        assert v.attrs["contact"] == "The ACCESS Oracle"
        assert v.attrs["email"] == "oracle@example.com"
        assert v.attrs["created"] == "2018-01-01"
        assert "description" in v.attrs
