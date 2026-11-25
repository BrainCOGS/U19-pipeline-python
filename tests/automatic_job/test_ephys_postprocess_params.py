"""The post-processing step list of an ephys job: from the database rows to the file."""

import ast
import importlib.util
import math
import pathlib
import sys
import types
from unittest import mock

import pytest

from u19_pipeline.automatic_job import ephys_postprocess_params as epp

BOMBCELL = {"thresholds": {"mua": {"snr": {"greater": 3}}}}
SI = {"extensions": {"quality_metrics": {}}}


def step(job_id, number, method, params):
    return dict(
        job_id=job_id, step_number=number, postcluster_method=method, params=params
    )


def test_steps_are_grouped_per_job_in_step_order():
    rows = [
        step(12, 2, "bombcell", BOMBCELL),
        step(11, 1, "bombcell", {}),
        step(12, 1, "spikeinterface", SI),
    ]
    assert epp.group_postparams(rows) == {
        11: [{"bombcell": {}}],
        12: [{"spikeinterface": SI}, {"bombcell": BOMBCELL}],
    }


def test_a_job_without_a_list_gets_an_empty_one():
    rows = [step(12, 1, "bombcell", BOMBCELL)]
    assert epp.postparams_for_jobs([11, 12], rows) == [[], [{"bombcell": BOMBCELL}]]
    assert epp.postparams_for_jobs([11], []) == [[]]


@pytest.mark.parametrize("cell", [None, math.nan, "x", {}])
def test_a_missing_dataframe_cell_is_an_empty_list(cell):
    assert epp.as_step_list(cell) == []
    assert epp.as_step_list([{"bombcell": {}}]) == [{"bombcell": {}}]


# ------------------------------------------------------------------ the parameter file


AUTOMATIC_JOB = (
    pathlib.Path(__file__).resolve().parents[2] / "u19_pipeline" / "automatic_job"
)
CLUSTERS = "u19_pipeline.automatic_job.clusters_paths_and_transfers"


def filename_constants():
    """The ``default_*_filename`` strings of params_config, which imports the database."""
    constants = {}
    for node in ast.parse((AUTOMATIC_JOB / "params_config.py").read_text()).body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id.endswith("_filename")
            and isinstance(node.value, ast.Constant)
        ):
            constants[node.targets[0].id] = node.value.value
    return constants


@pytest.fixture
def paramfiles(monkeypatch, tmp_path):
    """parameter_file_creator, loaded on its own with its database-bound imports stubbed.

    It is loaded from its file under a private name, so the stubs never reach the
    ``u19_pipeline.automatic_job.parameter_file_creator`` other tests import.
    """
    config = types.SimpleNamespace(
        **filename_constants(),
        parameter_files_filepath=str(tmp_path),
        system_process={"SUCCESS": 0},
    )
    stubs = {
        CLUSTERS: mock.MagicMock(),
        "u19_pipeline.automatic_job.params_config": config,
        "u19_pipeline.utils.file_utils": mock.MagicMock(),
        "scipy.io": mock.MagicMock(),
    }
    for name, stub in stubs.items():
        monkeypatch.setitem(sys.modules, name, stub)
    spec = importlib.util.spec_from_file_location(
        "parameter_file_creator_under_test", AUTOMATIC_JOB / "parameter_file_creator.py"
    )
    pfc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pfc)

    written = []
    monkeypatch.setattr(
        pfc, "write_file", lambda path, text: written.append((path, text))
    )
    return pfc, written


@pytest.mark.parametrize(
    ("type_params", "filename"),
    [
        ("params", "process_paramset_42.json"),
        ("preparams", "preprocess_paramset_42.json"),
        ("postparams", "postprocess_paramset_42.json"),
    ],
)
def test_each_kind_of_params_gets_its_file(paramfiles, tmp_path, type_params, filename):
    pfc, written = paramfiles
    status = pfc.generate_parameter_file(
        42, [{"bombcell": {}}], type_params, {"local_or_cluster": "local"}
    )
    assert status == pfc.config.system_process["SUCCESS"]
    assert written == [(str(tmp_path / filename), '[{"bombcell": {}}]')]


def test_the_postprocess_file_goes_to_the_cluster_too(paramfiles, monkeypatch):
    pfc, _ = paramfiles
    sent = []
    pfc.ft.get_cluster_vars.return_value = {
        "params_files_dir": "/scratch/ParameterFiles",
        "user": "u19prod",
        "hostname": "spock",
    }
    monkeypatch.setattr(
        pfc, "transfer_parameter_file", lambda *args: sent.append(args) or "ok"
    )
    status = pfc.generate_parameter_file(
        42,
        [],
        "postparams",
        {"local_or_cluster": "cluster", "process_cluster": "spock"},
    )
    assert status == "ok"
    assert sent == [
        (
            42,
            "postprocess_paramset_%s.json",
            "/scratch/ParameterFiles",
            "u19prod@spock",
        )
    ]
