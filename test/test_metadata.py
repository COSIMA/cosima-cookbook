import pytest
from datetime import datetime

from cosima_cookbook import database

def metadata_for_experiment(path, session, name='test', commit=True):
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
    database.build_index('test/data/indexing/metadata', session)

    # query metadata
    q = session.query(database.NCExperiment.contact,
                      database.NCExperiment.created,
                      database.NCExperiment.description)
    r = q.one()
    assert(r[0] == 'The ACCESS Oracle')
    assert(r[1] == datetime(2018, 1, 1))
    assert(len(r[2]) > 0)


def test_keywords(session_db):
    """Test that keywords are read for an experiment"""

    session, db = session_db
    metadata_for_experiment('test/data/metadata/keywords', session)

    q = session.query(database.NCExperiment).filter(database.NCExperiment.experiment == 'test')
    r = q.one()
    assert(len(r.keywords) == 3)
    assert('cosima' in r.keywords)
    assert('not-a-keyword' not in r.keywords)


def test_duplicate_keywords_commit(session_db):
    """Test that the uniqueness constraint works across experiments.
    This simulates separate index calls, where the session is committed in between.
    """

    session, db = session_db
    metadata_for_experiment('test/data/metadata/keywords', session, name='e1')
    metadata_for_experiment('test/data/metadata/keywords2', session, name='e2')

    q = session.query(database.Keyword)
    r = q.all()
    assert(len(r) == 4)


def test_duplicate_keywords_nocommit(session_db):
    """Test that the uniqueness constraint works across experiments.
    This simulates multiple experiments being added in a single call.
    """

    session, db = session_db
    e1 = metadata_for_experiment('test/data/metadata/keywords', session, name='e1', commit=False)
    e2 = metadata_for_experiment('test/data/metadata/keywords2', session, name='e2', commit=False)
    session.add_all([e1, e2])
    session.commit()

    q = session.query(database.Keyword)
    r = q.all()
    assert(len(r) == 4)


def test_keyword_upcast(session_db):
    """Test that a string keyword is added correctly."""

    session, db = session_db
    metadata_for_experiment('test/data/metadata/string_keyword', session)

    q = session.query(database.NCExperiment).filter(database.NCExperiment.experiment == 'test')
    r = q.one()
    assert('cosima' in r.keywords)
    assert('c' not in r.keywords) # make sure it wasn't added as a string

def test_keyword_case_sensitivity(session_db):
    """Test that keywords are treated in a case-insensitive manner,
    both for metadata retrieval and querying.
    """

    session, db = session_db
    metadata_for_experiment('test/data/metadata/keywords', session, name='e1')
    metadata_for_experiment('test/data/metadata/upcase', session, name='e2')

    # we should be able to find the keyword in lowercase
    q = session.query(database.Keyword).filter(database.Keyword.keyword == 'cosima')
    k1 = q.one_or_none()
    assert(k1 is not None)

    # and in uppercase
    q = session.query(database.Keyword).filter(database.Keyword.keyword == 'COSIMA')
    k2 = q.one_or_none()
    assert(k2 is not None)

    # but they should resolve to the same keyword
    assert(k1 is k2)

    # finally, the set of keywords should all be lowercase
    q = session.query(database.NCExperiment).filter(database.NCExperiment.experiment == 'e2')
    r = q.one()
    for kw in r.keywords:
        assert(kw == kw.lower())
