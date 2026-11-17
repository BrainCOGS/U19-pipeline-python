"""Tests for active jobs that have no ``Processing.ImagingParams``/``EphysParams`` row.

Job 1446 (imaging) was cloned without an ``ImagingParams`` row. When it was the only
active imaging job, the params join in ``get_imaging_params_jobs`` was empty, so the
DataFrame built from it had no columns and ``drop("paramset_idx")`` raised a KeyError.
That stopped the whole cron job, so the active ephys jobs never ran either.

``recording_process_handler`` imports the DataJoint schemas (which connect at import)
and the pipeline extras, so it is imported once with those stubbed, and
``sys.modules`` is restored afterwards. DataJoint queries are replaced by
``FakeQuery``, whose ``*`` is a natural join and ``&`` is ignored.
"""

import sys
from unittest import mock

import pandas as pd
import pytest

STUBBED = [
    "u19_pipeline.recording",
    "u19_pipeline.recording_process",
    "u19_pipeline.ephys_pipeline",
    "u19_pipeline.imaging_pipeline",
    "u19_pipeline.utility",
    "u19_pipeline.utils.ephys_utils",
    "u19_pipeline.utils.slack_utils",
    "u19_pipeline.automatic_job.recording_handler",
    "u19_pipeline.automatic_job.clusters_paths_and_transfers",
    "u19_pipeline.automatic_job.slurm_creator",
    "u19_pipeline.automatic_job.parameter_file_creator",
    "u19_pipeline.automatic_job.params_config",
    "u19_pipeline.automatic_job.ephys_element_populate",
    "u19_pipeline.automatic_job.imaging_element_populate",
    "ecephys_spike_sorting",
    "ecephys_spike_sorting.common",
    "ecephys_spike_sorting.common.SGLXMetaToCoords",
]


def _import_handler():
    stubs = {name: mock.MagicMock() for name in STUBBED}
    saved_modules = {name: sys.modules.get(name) for name in stubs}
    loaded_before = set(sys.modules)
    sys.modules.update(stubs)
    try:
        from u19_pipeline.automatic_job import recording_process_handler

        return recording_process_handler
    finally:
        for name in set(sys.modules) - loaded_before:
            if name.startswith("u19_pipeline."):
                del sys.modules[name]
        for name, module in saved_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


rph = _import_handler()
Handler = rph.RecProcessHandler


class FakeQuery:
    def __init__(self, rows=()):
        self.rows = [dict(r) for r in rows]

    def proj(self, *args, **kwargs):
        return self

    def __mul__(self, other):
        """Natural join: pairs of rows that agree on every shared field."""
        return FakeQuery(
            {**a, **b}
            for a in self.rows
            for b in other.rows
            if all(a[k] == b[k] for k in a.keys() & b.keys())
        )

    def __and__(self, restriction):
        return self

    def fetch(self, *args, **kwargs):
        return [dict(r) for r in self.rows]


class FakeTable(FakeQuery):
    """A table whose part tables are attributes, e.g. ``Processing.ImagingParams``."""

    def __init__(self, rows=(), **parts):
        super().__init__(rows)
        for name, part in parts.items():
            setattr(self, name, part)


def imaging_tables(imaging_params_rows):
    element = mock.MagicMock()
    element.ProcessingParamSet = FakeQuery(
        [dict(paramset_idx=10, params={"tau": 1.0}, processing_method="suite2p")]
    )
    element.PreprocessParamSteps = FakeTable(
        [dict(preprocess_param_steps_id=0)], Step=FakeQuery()
    )
    element.PreprocessParamSet = FakeQuery()
    return element, FakeQuery(imaging_params_rows)


def ephys_tables(ephys_params_rows):
    element = mock.MagicMock()
    element.ClusteringParamSet = FakeQuery(
        [dict(paramset_idx=1, params={"Th": [10, 4]}, clustering_method="kilosort4")]
    )
    element.PreClusterParamSteps = FakeTable(
        [dict(precluster_param_steps_id=1)], Step=FakeQuery()
    )
    element.PreClusterParamSet = FakeQuery()
    return element, FakeQuery(ephys_params_rows)


@pytest.fixture
def db(monkeypatch):
    """Point the handler at fake tables; returns a function that sets them up."""
    monkeypatch.setattr(rph.utility, "smart_dj_join", lambda a, b: a * b)

    def setup(*, jobs=(), imaging_params=(), ephys_params=()):
        imaging_element, imaging_params_part = imaging_tables(imaging_params)
        ephys_element, ephys_params_part = ephys_tables(ephys_params)
        monkeypatch.setattr(rph.imaging_pipeline, "imaging_element", imaging_element)
        monkeypatch.setattr(rph.ephys_pipeline, "ephys_element", ephys_element)
        processing = FakeTable(
            jobs, ImagingParams=imaging_params_part, EphysParams=ephys_params_part
        )
        monkeypatch.setattr(rph.recording_process, "Processing", processing)
        monkeypatch.setattr(rph.recording, "Recording", FakeQuery(jobs))

    return setup


@pytest.mark.parametrize("modality", ["imaging", "electrophysiology"])
def test_no_params_rows_gives_empty_frame_keyed_on_job_id(db, modality):
    db()
    get_params = {
        "imaging": Handler.get_imaging_params_jobs,
        "electrophysiology": Handler.get_ephys_params_jobs,
    }[modality]

    params_df = get_params([dict(job_id=1446)])

    assert params_df.empty
    assert {"job_id", "params", "preparams"} <= set(params_df.columns)


def test_imaging_job_with_params_keeps_them(db):
    db(imaging_params=[dict(job_id=1416, preprocess_param_steps_id=0, paramset_idx=10)])

    params_df = Handler.get_imaging_params_jobs([dict(job_id=1416)])

    assert params_df["job_id"].tolist() == [1416]
    assert params_df.loc[0, "params"] == {"tau": 1.0, "processing_method": "suite2p"}


def job(job_id, modality):
    return dict(
        job_id=job_id,
        recording_id=865,
        recording_modality=modality,
        location="182-recording",
        recording_directory="efonseca/ef932",
        status_processing_id=0,
        fragment_number=1,
    )


def test_active_jobs_survive_an_imaging_job_without_params(db, monkeypatch):
    """The cron state of 2026-11-17: job 1446 has no params, and 2 ephys jobs wait."""
    monkeypatch.setattr(rph.config, "JOB_STATUS_ERROR_ID", -1)
    monkeypatch.setattr(rph.config, "JOB_STATUS_PROCESSED", 7)
    monkeypatch.setattr(
        rph.dj_short, "get_primary_key_fields", lambda table: ["job_id"]
    )
    monkeypatch.setattr(
        Handler,
        "get_program_selection_params",
        staticmethod(
            lambda modality: pd.DataFrame(
                dict(recording_modality=[modality], program_selection_params=[{}])
            )
        ),
    )
    monkeypatch.setattr(
        Handler,
        "get_ephys_params_jobs",
        staticmethod(
            lambda keys: pd.DataFrame(
                dict(
                    job_id=[k["job_id"] for k in keys],
                    params=[{}] * len(keys),
                    preparams=[[]] * len(keys),
                )
            )
        ),
    )
    db(
        jobs=[
            job(1446, "imaging"),
            job(1467, "electrophysiology"),
            job(1469, "electrophysiology"),
        ]
    )

    df = Handler.get_active_process_jobs()

    assert sorted(df["job_id"]) == [1446, 1467, 1469]
    imaging_row = df.loc[df["job_id"] == 1446].iloc[0]
    assert pd.isna(imaging_row["params"])


STATUSES = pd.DataFrame(
    [
        dict(Value=-1, ProcessFunction=None, UpdateField=None, SlackMessage=None),
        dict(Value=0, ProcessFunction=None, UpdateField=None, SlackMessage=None),
        dict(
            Value=1,
            ProcessFunction="transfer_request",
            UpdateField=None,
            SlackMessage=None,
        ),
    ]
)


@pytest.fixture
def handler_run(monkeypatch):
    """Run pipeline_handler_main on the given jobs; record status writes and Slack."""
    calls = dict(processed=[], status=[], log=[], slack=[])
    monkeypatch.setattr(rph.config, "recording_process_status_df", STATUSES)
    monkeypatch.setattr(
        rph.config,
        "status_update_idx",
        {"NEXT_STATUS": 1, "NO_CHANGE": 0, "ERROR_STATUS": -1},
    )
    monkeypatch.setattr(rph.config, "RECORDING_STATUS_ERROR_ID", -1)
    monkeypatch.setattr(
        rph.config,
        "default_update_value_dict",
        {
            "value_update": None,
            "error_info": {"error_message": None, "error_exception": None},
        },
    )
    monkeypatch.setattr(rph.time, "sleep", lambda s: None)

    def transfer_request(series, next_status):
        calls["processed"].append(series["job_id"])
        return 0, {
            "value_update": None,
            "error_info": {"error_message": None, "error_exception": None},
        }

    monkeypatch.setattr(Handler, "transfer_request", staticmethod(transfer_request))
    monkeypatch.setattr(
        Handler,
        "update_status_pipeline",
        staticmethod(lambda key, status, *a: calls["status"].append((key, status))),
    )
    monkeypatch.setattr(
        Handler,
        "update_job_id_log",
        staticmethod(lambda *args: calls["log"].append(args)),
    )
    monkeypatch.setattr(
        rph.slack_utils,
        "send_slack_error_notification",
        lambda webhook, error_info, series: calls["slack"].append(
            (error_info, series["job_id"])
        ),
    )

    def run(jobs):
        df = pd.DataFrame(jobs)
        df["query_key"] = [dict(job_id=j) for j in df["job_id"]]
        monkeypatch.setattr(
            Handler, "get_active_process_jobs", staticmethod(lambda: df)
        )
        Handler.pipeline_handler_main()
        return calls

    return run


def test_job_without_params_is_failed_with_a_clear_slack_message(handler_run):
    calls = handler_run(
        [
            {**job(1446, "imaging"), "params": float("nan")},
            {**job(1467, "electrophysiology"), "params": {"Th": [10, 4]}},
        ]
    )

    # 1446 never reaches its transfer and goes to error, 1467 is processed as usual
    assert calls["processed"] == [1467]
    assert calls["status"] == [(dict(job_id=1446), -1)]
    (log,) = calls["log"]
    job_id, old_status, new_status, error_info = log
    assert (job_id, old_status, new_status) == (1446, 0, -1)
    assert "Processing.ImagingParams" in error_info["error_message"]
    assert len(error_info["error_message"]) < 256
    ((slack_info, slack_job),) = calls["slack"]
    assert slack_job == 1446
    assert "no row for job_id 1446" in slack_info["error_exception"]


@pytest.mark.parametrize(
    "modality, params, expected_part",
    [
        ("imaging", float("nan"), "ImagingParams"),
        ("imaging", None, "ImagingParams"),
        ("electrophysiology", float("nan"), "EphysParams"),
        ("imaging", {"tau": 1.0}, None),
        ("electrophysiology", {}, None),
        ("video_pupillometry", float("nan"), None),
    ],
)
def test_missing_params_error(monkeypatch, modality, params, expected_part):
    monkeypatch.setattr(
        rph.config,
        "default_update_value_dict",
        {
            "value_update": None,
            "error_info": {"error_message": None, "error_exception": None},
        },
    )
    monkeypatch.setattr(
        rph.config,
        "status_update_idx",
        {"NEXT_STATUS": 1, "NO_CHANGE": 0, "ERROR_STATUS": -1},
    )
    series = pd.Series({**job(1446, modality), "params": params})

    result = Handler.missing_params_error(series)

    if expected_part is None:
        assert result is None
    else:
        status, update_dict = result
        assert status == -1
        assert expected_part in update_dict["error_info"]["error_message"]
