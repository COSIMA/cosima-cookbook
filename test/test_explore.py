import pytest

import os.path
import shutil
import xarray as xr
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

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
    cc.database.build_index(
        [
            "test/data/explore/one",
            "test/data/explore/two",
            "test/data/explore/duplicate/one",
        ],
        session,
    )

    # force all files to be marked as present, even if they're empty
    ncfiles = session.query(cc.database.NCFile).all()
    for f in ncfiles:
        f.present = True
    session.commit()

    return session


def test_database_explorer(session):

    dbx = cc.explore.DatabaseExplorer(session=session)

    assert dbx.session is session

    # Experiment selector
    assert dbx.expt_selector.options == ("one", "two")

    # Keyword filter selector
    assert dbx.filter_widget.options == tuple(dbx.keywords)

    in_one = set(cc.querying.get_variables(session, "one").name)
    in_two = set(cc.querying.get_variables(session, "two").name)

    # The variable filter box
    assert len(dbx.var_filter.selector.variables) == len((in_one | in_two))

    # Turn off filtering so all variables are present in the filter selector
    dbx.var_filter.selector._filter_variables(coords=False, restarts=False, model="")

    truth = {
        "age_global": "Age (global) (yr)",
        "diff_cbt_t": "total vert diff_cbt(temp) (w/o neutral included) (m^2/s)",
        "dzt": "t-cell thickness (m)",
        "hi_m": "grid cell mean ice thickness (m)",
        "neutral": "neutral density (kg/m^3)",
        "neutralrho_edges": "neutral density edges (kg/m^3)",
        "nv": "vertex number",
        "pot_rho_0": "potential density referenced to 0 dbar (kg/m^3)",
        "pot_rho_2": "potential density referenced to 2000 dbar (kg/m^3)",
        "salt": "Practical Salinity (psu)",
        "st_edges_ocean": "tcell zstar depth edges (meters)",
        "st_ocean": "tcell zstar depth (meters)",
    }

    for var, label in truth.items():
        assert dbx.var_filter.selector.selector.options[var] == label

    # Add all variables common to both experiments and ensure after filter
    # experiment selector still contains both
    for var in in_one & in_two:
        dbx.var_filter.selector.selector.label = var
        dbx.var_filter._add_var_to_selected(None)

    dbx._filter_experiments(None)
    assert dbx.expt_selector.options == ("one", "two")

    dbx.var_filter.delete(in_one & in_two)
    assert len(dbx.var_filter.var_filter_selected.options) == 0

    # Now all variables only in experiment two and ensure after filter
    # experiment selector only contains two
    for var in in_two - in_one:
        dbx.var_filter.selector.selector.label = var
        dbx.var_filter._add_var_to_selected(None)

    dbx._filter_experiments(None)
    assert dbx.expt_selector.options == ("two",)


def test_experiment_explorer(session):

    ee1 = cc.explore.ExperimentExplorer(session=session)

    # Experiment selector
    assert ee1.expt_selector.options == ("one", "two")

    assert len(ee1.var_selector.selector.options) == 24
    assert "pot_rho_0" in ee1.var_selector.selector.options
    assert "ty_trans_rho" not in ee1.var_selector.selector.options

    # Simulate selecting a different experiment from menu
    ee1._load_experiment("two")
    assert len(ee1.var_selector.selector.options) == 28
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
    assert ee.daterange.options[0][0] == "0166/12/31"
    assert ee.daterange.options[1][0] == "0167/12/31"

    assert ee.data is not None
    assert ee.data.shape == (2, 1, 1, 1)
