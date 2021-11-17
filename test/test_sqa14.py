import pytest
from cosima_cookbook.database import *


def test_empty_file(session_db):
    session, db = session_db

    exp = NCExperiment(experiment="a", root_dir="b")
    file = NCFile()

    file.experiment = exp

    session.add(exp)
    session.commit()

    assert session.query(NCFile).count() == 1
    assert session.query(NCExperiment).count() == 1


def test_file_one_var(session_db):
    session, db = session_db

    exp = NCExperiment(experiment="a", root_dir="b")
    file = NCFile()
    cfvar = CFVariable(name="c")
    var = NCVar()

    file.experiment = exp
    var.ncfile = file
    var.variable = cfvar

    session.add(exp)
    session.commit()

    assert session.query(NCFile).count() == 1
    assert session.query(NCVar).count() == 1


def test_file_attr(session_db):
    session, db = session_db

    exp = NCExperiment(experiment="a", root_dir="b")
    file = NCFile()
    cfvar = CFVariable(name="c")
    var = NCVar()

    file.experiment = exp
    file.attrs["x"] = "y"

    session.add(exp)
    session.commit()

    assert session.query(NCFile).count() == 1
    assert session.query(NCAttribute).count() == 1
    assert session.query(NCAttributeString).count() == 2

    # Add another attribute with duplicate string
    file.attrs["z"] = "y"

    session.add(exp)
    session.commit()

    assert session.query(NCFile).count() == 1
    assert session.query(NCAttribute).count() == 2
    assert session.query(NCAttributeString).count() == 3


def test_var_attr(session_db):
    session, db = session_db

    exp = NCExperiment(experiment="a", root_dir="b")
    file = NCFile()
    cfvar = CFVariable(name="c")
    var = NCVar()

    file.experiment = exp
    var.ncfile = file
    var.variable = cfvar
    var.attrs["x"] = "y"

    session.add(exp)
    session.commit()

    assert session.query(NCFile).count() == 1
    assert session.query(NCAttribute).count() == 1
    assert session.query(NCAttributeString).count() == 2

    # Add another attribute with duplicate string
    var.attrs["z"] = "y"

    session.add(exp)
    session.commit()

    assert session.query(NCAttribute).count() == 2
    assert session.query(NCAttributeString).count() == 3

    # Add an attribute to the file
    file.attrs["y"] = "x"

    session.add(exp)
    session.commit()

    assert session.query(NCAttribute).count() == 3
    assert session.query(NCAttributeString).count() == 3


def test_index_file(session_db):
    session, db = session_db

    exp = NCExperiment(experiment="a", root_dir="test/data/querying")

    file = index_file("output000/ocean.nc", exp, session)

    session.add(exp)
    session.commit()

    assert session.query(NCFile).count() == 1
    assert session.query(CFVariable).count() == 38
    assert session.query(NCVar).count() == 38
    assert session.query(NCAttribute).count() == 243 - 18

    var = session.query(NCVar).filter(NCVar.varname == "temp").one()
    assert var.attrs["long_name"] == "Potential temperature"
