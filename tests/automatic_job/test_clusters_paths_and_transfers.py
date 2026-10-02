"""Tests for ``u19_pipeline.automatic_job.clusters_paths_and_transfers.get_cluster_vars``.

At import time the module pulls in ``params_config`` (which declares DataJoint
schemas) and reads ``dj.config["custom"]["root_data_dir"]``. ``get_cluster_vars``
needs neither, so both are stubbed and the tests run without a database.
"""

import importlib
import sys
import types

import pytest

pytest.importorskip("element_interface")  # comes from the "pipeline" extra

MODULE = "u19_pipeline.automatic_job.clusters_paths_and_transfers"


@pytest.fixture
def cpt(monkeypatch):
    dj = pytest.importorskip("datajoint")
    custom = dict(dj.config.get("custom") or {}, root_data_dir="/tmp/root")
    monkeypatch.setitem(dj.config, "custom", custom)
    monkeypatch.setitem(
        sys.modules,
        "u19_pipeline.automatic_job.params_config",
        types.ModuleType("params_config"),
    )
    monkeypatch.delitem(sys.modules, MODULE, raising=False)
    module = importlib.import_module(MODULE)
    yield module
    sys.modules.pop(MODULE, None)


@pytest.mark.parametrize("cluster", ["tiger", "spock"])
def test_get_cluster_vars_known(cpt, cluster):
    assert cpt.get_cluster_vars(cluster) is cpt.cluster_vars[cluster]


@pytest.mark.parametrize("cluster", ["della", "", None, 0, "Spock"])
def test_get_cluster_vars_unknown_raises_value_error(cpt, cluster):
    with pytest.raises(ValueError, match="Non existing cluster"):
        cpt.get_cluster_vars(cluster)
