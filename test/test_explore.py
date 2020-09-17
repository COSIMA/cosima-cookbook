import pytest

import os.path
import xarray as xr
import pandas as pd
from pandas.util.testing import assert_frame_equal, assert_series_equal

import cosima_cookbook as cc

from cosima_cookbook.database import NCExperiment


def metadata_for_experiment(
    path, session, metadata_file=None, name="test", commit=True
):
    """Method to read metadata for an experiment without requiring
    the rest of the indexing infrastructure.
    """

    expt = NCExperiment(experiment=name, root_dir=path)

    # look for this experiment in the database
    q = (
        session.query(NCExperiment)
        .filter(NCExperiment.experiment == expt.experiment)
        .filter(NCExperiment.root_dir == expt.root_dir)
    )
    r = q.one_or_none()
    if r is not None:
        expt = r

    cc.database.update_metadata(expt, session, metadata_file)

    if commit:
        session.add(expt)
        session.commit()
    else:
        return expt


@pytest.fixture(scope="module")
def session(tmpdir_factory):
    # index test directory into temp database
    d = tmpdir_factory.mktemp("database")
    db = d.join("test.db")
    session = cc.database.create_session(str(db))

    # build index for entire module
    cc.database.build_index(["test/data/explore/one", "test/data/explore/two"], session)

    # force all files to be marked as present, even if they're empty
    ncfiles = session.query(cc.database.NCFile).all()
    for f in ncfiles:
        f.present = True
    session.commit()

    return session


def test_database_extension(session):

    # DatabaseExtension adds a layer of logic, inferrs model type, identifies
    # coordinate and restart variables, and creates a mapping from variables
    # to experiment
    de = cc.explore.DatabaseExtension(session=session)

    assert de.experiments.shape == (2, 8)
    assert de.expt_variable_map.shape == (108, 5)
    assert de.expt_variable_map[de.expt_variable_map.restart].shape == (12, 5)
    assert de.expt_variable_map[de.expt_variable_map.coordinate].shape == (44, 5)
    assert de.keywords == ["access-om2-01", "another-keyword", "cosima", "ryf9091"]
    # All unique variables contained in the database
    assert de.variables.shape == (68, 5)
    # Check restart and coordinate variables correctly assigned
    assert de.variables[~de.variables.restart & ~de.variables.coordinate].shape == (
        32,
        5,
    )
    # Check model assignment
    assert de.variables[de.variables.model == "ocean"].shape == (38, 5)
    assert de.variables[de.variables.model == "atmosphere"].shape == (12, 5)
    assert de.variables[de.variables.model == "ice"].shape == (6, 5)
    assert de.variables[de.variables.model == ""].shape == (12, 5)

    # Now specify only one experiment, which is what happens in ExperimentExplorer
    de = cc.explore.DatabaseExtension(
        session=session,
        experiments=[
            "one",
        ],
    )

    assert de.experiments.shape == (1, 8)
    assert de.allexperiments.shape == (2, 8)
    assert de.expt_variable_map.shape == (52, 5)
    assert de.expt_variable_map[de.expt_variable_map.restart].shape == (6, 5)
    assert de.expt_variable_map[de.expt_variable_map.coordinate].shape == (22, 5)
    assert de.keywords == ["access-om2-01", "another-keyword", "cosima", "ryf9091"]
    # All unique variables contained in the database
    assert de.variables.shape == (52, 5)
    # Check restart and coordinate variables correctly assigned
    assert de.variables[~de.variables.restart & ~de.variables.coordinate].shape == (
        24,
        5,
    )
    assert de.variables[de.variables.model == "ocean"].shape == (34, 5)
    assert de.variables[de.variables.model == "atmosphere"].shape == (6, 5)
    assert de.variables[de.variables.model == "ice"].shape == (6, 5)
    assert de.variables[de.variables.model == ""].shape == (6, 5)

    # Now specify only one experiment, which is what happens in ExperimentExplorer
    de = cc.explore.DatabaseExtension(
        session=session,
        experiments=[
            "two",
        ],
    )

    assert de.experiments.shape == (1, 8)
    assert de.allexperiments.shape == (2, 8)
    assert de.expt_variable_map.shape == (56, 5)
    assert de.expt_variable_map[de.expt_variable_map.restart].shape == (6, 5)
    assert de.expt_variable_map[de.expt_variable_map.coordinate].shape == (22, 5)
    assert de.keywords == ["access-om2-01", "another-keyword", "cosima", "ryf9091"]
    # All unique variables contained in the database
    assert de.variables.shape == (56, 5)
    # Check restart and coordinate variables correctly assigned
    assert de.variables[~de.variables.restart & ~de.variables.coordinate].shape == (
        28,
        5,
    )
    assert de.variables[de.variables.model == "ocean"].shape == (38, 5)
    assert de.variables[de.variables.model == "atmosphere"].shape == (6, 5)
    assert de.variables[de.variables.model == "ice"].shape == (0, 5)
    assert de.variables[de.variables.model == ""].shape == (12, 5)


def test_database_explorer(session):

    dbx = cc.explore.DatabaseExplorer(session=session)

    assert dbx.session is session

    # Experiment selector
    assert dbx.expt_selector.options == ("one", "two")

    # Keyword filter selector
    assert dbx.filter_widget.options == tuple(dbx.de.keywords)

    # The variable filter box
    variables = dbx.var_filter.selector.variables
    assert dbx.var_filter.selector.selector.options == dict(
        zip(variables[variables.visible].name, variables[variables.visible].long_name)
    )


def test_experiment_explorer(session):

    ee1 = cc.explore.ExperimentExplorer(session=session)

    # Experiment selector
    assert ee1.expt_selector.options == ("one", "two")

    assert len(ee1.var_selector.selector.options) == 23
    assert "pot_rho_0" in ee1.var_selector.selector.options
    assert "ty_trans_rho" not in ee1.var_selector.selector.options

    # Simulate selecting a different experiment from menu
    ee1._load_experiment("two")
    assert len(ee1.var_selector.selector.options) == 27
    assert "pot_rho_0" in ee1.var_selector.selector.options
    assert "ty_trans_rho" in ee1.var_selector.selector.options

    # Check frequency drop down changes when variable selector assigned a value
    assert ee1.frequency.options == ()
    ee1.var_selector.selector.label = "tx_trans"
    ee1.var_selector._set_frequency_selector("tx_trans")
    assert ee1.frequency.options == (None,)
    ee1.var_selector._set_daterange_selector("ty_trans", "1 yearly")
    assert ee1.frequency.options == (None,)

    ee2 = cc.explore.ExperimentExplorer(session=session)

    assert id(ee1.var_selector) != id(ee2.var_selector)


def test_get_data(session):

    ee = cc.explore.ExperimentExplorer(session=session)

    assert ee.data is None

    ee._load_experiment("one")
    ee.var_selector.selector.label = "ty_trans"
    ee.var_selector._set_frequency_selector("ty_trans")
    ee.var_selector._set_daterange_selector("ty_trans", "1 yearly")
    ee._load_data(None)

    assert ee.frequency.options == ("1 yearly",)
    assert ee.daterange.options[0][0] == " 166/12/31"
    assert ee.daterange.options[1][0] == " 167/12/31"

    assert ee.data is not None
    assert ee.data.shape == (2, 1, 1, 1)
