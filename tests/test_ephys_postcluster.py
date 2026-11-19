"""The ephys post-processing tables and the one-time script that creates them.

Nothing here needs a database: the tables are defined on schemas that are only activated
on request, and ``scripts/create_ephys_postcluster_tables.py`` works out what it would
insert with ``plan``, which these tests feed.
"""

import importlib.util
import pathlib

import pytest

from u19_pipeline import ephys_postcluster

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
DATE = "2026-11-19"


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script():
    return load_script("create_ephys_postcluster_tables")


# ---------------------------------------------------------------------------- tables


def attributes(definition):
    lines = [line.split("#")[0].strip() for line in definition.splitlines()]
    return [line for line in lines if line and line != "---"]


def test_importing_the_module_declares_nothing():
    for schema in (ephys_postcluster.element_schema, ephys_postcluster.job_schema):
        assert schema.database is None


def test_tables_mirror_the_precluster_ones():
    assert attributes(ephys_postcluster.PostClusterMethod.definition) == [
        "postcluster_method: varchar(16)",
        "postcluster_method_desc: varchar(1000)",
    ]
    assert attributes(ephys_postcluster.PostClusterParamSet.definition) == [
        "paramset_idx: smallint",
        "-> PostClusterMethod",
        "paramset_desc: varchar(128)",
        "param_set_hash: uuid",
        "unique index (param_set_hash)",
        "params: longblob",
    ]
    assert attributes(ephys_postcluster.PostClusterParamSteps.definition) == [
        "postcluster_param_steps_id: smallint",
        "postcluster_param_steps_name: varchar(32)",
        "postcluster_param_steps_desc: varchar(128)",
    ]
    assert attributes(ephys_postcluster.PostClusterParamSteps.Step.definition) == [
        "-> master",
        "step_number: smallint",
        "-> PostClusterParamSet",
    ]


def test_a_job_selects_a_list_in_a_table_of_its_own():
    """A new table keyed on the job: Processing and EphysParams are not altered."""
    assert attributes(ephys_postcluster.EphysPostParams.definition) == [
        "-> recording_process.Processing",
        "-> PostClusterParamSteps",
    ]


def test_schema_names():
    assert ephys_postcluster.schema_names("u19_") == (
        "u19_pipeline_ephys_element",
        "u19_recording_process",
    )


def test_tables_are_unavailable_without_a_database(monkeypatch):
    monkeypatch.setattr(ephys_postcluster, "_tables", None)
    monkeypatch.setitem(ephys_postcluster.dj.config, "custom", {})
    assert ephys_postcluster.tables() is None


# -------------------------------------------------------------------- creation script


def test_rows_planned_for_new_tables(script):
    todo = script.plan([], [], [], DATE)

    assert [m["postcluster_method"] for m in todo["PostClusterMethod"]] == [
        "spikeinterface",
        "bombcell",
    ]
    si_metrics, bombcell = todo["PostClusterParamSet"]
    assert (si_metrics["paramset_idx"], si_metrics["postcluster_method"]) == (
        1,
        "spikeinterface",
    )
    assert "quality_metrics" in si_metrics["params"]["extensions"]
    assert (bombcell["paramset_idx"], bombcell["postcluster_method"]) == (2, "bombcell")
    assert bombcell["params"] == {}

    assert [
        (s["postcluster_param_steps_id"], s["postcluster_param_steps_name"])
        for s in todo["PostClusterParamSteps"]
    ] == [(1, "SpikeInterface metrics"), (2, "bombcell")]
    assert todo["PostClusterParamSteps.Step"] == [
        {"postcluster_param_steps_id": 1, "step_number": 1, "paramset_idx": 1},
        {"postcluster_param_steps_id": 2, "step_number": 1, "paramset_idx": 2},
    ]


def test_rows_fit_the_columns(script):
    todo = script.plan([], [], [], DATE)
    for row in todo["PostClusterMethod"]:
        assert len(row["postcluster_method"]) <= script.METHOD_MAX_LENGTH
    for row in todo["PostClusterParamSet"]:
        assert len(row["paramset_desc"]) <= script.PARAMSET_DESC_MAX_LENGTH
        # "<user>_<date>_<text>": the website splits on "_" into exactly three parts
        assert len(row["paramset_desc"].split("_")) == 3
    for row in todo["PostClusterParamSteps"]:
        assert len(row["postcluster_param_steps_name"]) <= script.STEPS_NAME_MAX_LENGTH
        assert len(row["postcluster_param_steps_desc"]) <= script.STEPS_DESC_MAX_LENGTH
        assert len(row["postcluster_param_steps_desc"].split("_")) == 3


def test_a_second_run_adds_nothing(script):
    first = script.plan([], [], [], DATE)
    methods = [m["postcluster_method"] for m in first["PostClusterMethod"]]
    paramsets = first["PostClusterParamSet"]
    steps = first["PostClusterParamSteps.Step"]
    again = script.plan(methods, paramsets, steps, "2027-01-01")
    assert again == {
        "PostClusterMethod": [],
        "PostClusterParamSet": [],
        "PostClusterParamSteps": [],
        "PostClusterParamSteps.Step": [],
    }


def test_existing_rows_are_reused_and_new_ids_follow_them(script):
    """Someone already registered bombcell defaults as paramset 7, in a list with SI."""
    todo = script.plan(
        ["spikeinterface", "bombcell"],
        [{"paramset_idx": 7, "postcluster_method": "bombcell", "params": {}}],
        [
            {"postcluster_param_steps_id": 4, "paramset_idx": 7},
            {"postcluster_param_steps_id": 5, "paramset_idx": None},
        ],
        DATE,
    )
    assert todo["PostClusterMethod"] == []
    assert [r["paramset_idx"] for r in todo["PostClusterParamSet"]] == [8]
    # [7] on its own is not a list yet (list 4 is [7]: it is), so only the SI one is new
    assert [s["postcluster_param_steps_id"] for s in todo["PostClusterParamSteps"]] == [
        6
    ]
    assert todo["PostClusterParamSteps.Step"] == [
        {"postcluster_param_steps_id": 6, "step_number": 1, "paramset_idx": 8}
    ]


def test_paramset_hash_is_the_elements(script):
    other = load_script("insert_spikeinterface_paramsets")
    for params in ({}, script.SI_METRICS):
        assert script.dict_to_uuid(params) == other.dict_to_uuid(params)
