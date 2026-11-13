"""Planning of the lookup rows for SpikeInterface ephys jobs.

``scripts/insert_spikeinterface_paramsets.py`` only talks to the database inside
``main``; what it would insert is worked out by ``plan`` from the tables' current
content, which is what these tests feed it.
"""

import importlib.util
import json
import pathlib

import pytest

SCRIPT = (
    pathlib.Path(__file__).resolve().parents[1]
    / "scripts"
    / "insert_spikeinterface_paramsets.py"
)
DATE = "2026-11-13"

CATGT = {
    "apfilter": ["biquad", 2, 300, 0],
    "gfix": [0.4, 0.1, 0.02],
    "extras": ["prb_fld", "t_miss_ok", "ap", "gblcar", "out_prb_fld"],
}


@pytest.fixture(scope="module")
def script():
    spec = importlib.util.spec_from_file_location("insert_si_paramsets", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def production():
    """The tables as they were on 2026-11-13."""

    return dict(
        clustering_rows=[
            {"paramset_idx": 0, "params": {"fs": 30000, "NchanTOT": 385}},
            {"paramset_idx": 1, "params": {"fs": 30000, "NchanTOT": 385, "GPU": 2}},
            {"paramset_idx": 2, "params": {"fs": 30000, "nblocks": 5}},
            {"paramset_idx": 3, "params": {"n_chan_bin": 385, "nblocks": 5}},
            {"paramset_idx": 4, "params": {"n_chan_bin": 385}},
        ],
        precluster_rows=[
            {"paramset_idx": 0, "params": CATGT},
            {"paramset_idx": 1, "params": {**CATGT, "apfilter": ["biquad", 2, 301, 0]}},
            {"paramset_idx": 2, "params": {"preset": "dredge", "n_jobs": 1}},
        ],
        precluster_methods=["catgt", "dredge", "Nothing"],
        steps_rows=[
            {"precluster_param_steps_id": 0, "paramset_idx": 0},
            {"precluster_param_steps_id": 1, "paramset_idx": 1},
            {"precluster_param_steps_id": 2, "paramset_idx": 2},
            {"precluster_param_steps_id": 3, "paramset_idx": 2},
            {"precluster_param_steps_id": 4, "paramset_idx": 2},
        ],
    )


def test_rows_planned_for_the_production_tables(script, production):
    todo = script.plan(**production, date=DATE)

    full, sort_only = todo["ClusteringParamSet"]
    assert [row["paramset_idx"] for row in (full, sort_only)] == [5, 6]
    assert {row["clustering_method"] for row in (full, sort_only)} == {"kilosort4"}
    assert [s["name"] for s in full["params"]["preprocessing"]["steps"]] == [
        "phase_shift",
        "highpass_filter",
        "common_reference",
    ]
    assert "preprocessing" not in sort_only["params"]

    assert todo["PreClusterMethod"] == [
        {
            "precluster_method": "spikeinterface",
            "precluster_method_desc": script.PRECLUSTER_METHOD_DESC,
        }
    ]
    (precluster,) = todo["PreClusterParamSet"]
    assert precluster["paramset_idx"] == 3
    assert precluster["params"] == script.PREPROCESSING_STEPS
    assert todo["PreClusterParamSteps"][0]["precluster_param_steps_id"] == 5
    assert todo["PreClusterParamSteps.Step"] == [
        {"precluster_param_steps_id": 5, "step_number": 1, "paramset_idx": 3}
    ]


def test_clustering_paramsets_are_the_namespaced_shape_the_sorters_expect(script):
    """BrainCogsEphysSorters job_spec: ``sorter_params`` marks a SpikeInterface paramset,
    only these keys may sit next to it, and ``clustering_method`` is a column that
    recording_process_handler adds to the file, not part of the stored params."""

    allowed = {
        "sorter_version",
        "sorter_params",
        "si_params",
        "preprocessing",
        "postprocessing",
    }
    for _, params in script.clustering_paramsets(DATE):
        assert "sorter_params" in params
        assert set(params) <= allowed
        assert "clustering_method" not in params
        assert "n_jobs" not in params["si_params"]  # sized from the slurm job instead
        json.dumps(params)  # what parameter_file_creator will do with it


def test_text_columns_fit(script):
    assert len(script.CLUSTERING_METHOD) <= script.METHOD_MAX_LENGTH
    assert len(script.PRECLUSTER_METHOD) <= script.METHOD_MAX_LENGTH
    descs = [desc for desc, _ in script.clustering_paramsets(DATE)]
    descs.append(script.precluster_paramset(DATE)[0])
    assert all(len(desc) <= script.PARAMSET_DESC_MAX_LENGTH for desc in descs)
    name, desc = script.precluster_steps(DATE)
    assert len(name) <= script.STEPS_NAME_MAX_LENGTH
    assert len(desc) <= script.STEPS_DESC_MAX_LENGTH


def test_second_run_adds_nothing(script, production):
    todo = script.plan(**production, date=DATE)
    after = dict(
        clustering_rows=production["clustering_rows"] + todo["ClusteringParamSet"],
        precluster_rows=production["precluster_rows"] + todo["PreClusterParamSet"],
        precluster_methods=production["precluster_methods"] + ["spikeinterface"],
        steps_rows=production["steps_rows"] + todo["PreClusterParamSteps.Step"],
    )
    # a later date changes the descriptions, not the params the rows are matched on
    again = script.plan(**after, date="2027-01-01")
    assert not any(again.values())


def test_existing_precluster_paramset_is_reused_for_the_step_list(script, production):
    production["precluster_rows"].append(
        {"paramset_idx": 7, "params": script.PREPROCESSING_STEPS}
    )
    production["precluster_methods"].append("spikeinterface")
    todo = script.plan(**production, date=DATE)
    assert todo["PreClusterMethod"] == []
    assert todo["PreClusterParamSet"] == []
    assert todo["PreClusterParamSteps.Step"][0]["paramset_idx"] == 7


def test_key_order_does_not_make_a_new_paramset(script, production):
    _, params = script.clustering_paramsets(DATE)[1]
    production["clustering_rows"].append(
        {"paramset_idx": 5, "params": dict(reversed(list(params.items())))}
    )
    todo = script.plan(**production, date=DATE)
    assert [row["paramset_idx"] for row in todo["ClusteringParamSet"]] == [6]


def test_empty_tables(script):
    todo = script.plan([], [], [], [], DATE)
    assert [row["paramset_idx"] for row in todo["ClusteringParamSet"]] == [0, 1]
    assert todo["PreClusterParamSet"][0]["paramset_idx"] == 0
    assert todo["PreClusterParamSteps"][0]["precluster_param_steps_id"] == 0


def test_step_list_without_steps_is_not_mistaken_for_ours(script, production):
    production["steps_rows"].append(
        {"precluster_param_steps_id": 9, "paramset_idx": None}
    )
    todo = script.plan(**production, date=DATE)
    assert todo["PreClusterParamSteps"][0]["precluster_param_steps_id"] == 10
