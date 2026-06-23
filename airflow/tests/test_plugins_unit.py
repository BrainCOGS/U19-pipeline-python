"""Unit tests for the u19.* Airflow plugins (no database, dry-run mode)."""

from __future__ import annotations

import pytest

# --- transfers -------------------------------------------------------------

def test_request_transfer_dry_run_to_cluster():
    from u19 import transfers

    result = transfers.request_transfer(42, "some/rel/path", "electrophysiology", "to_cluster")
    assert result["status"] == 0  # SUCCESS
    assert result["task_id"].startswith("dryrun-task-42-to_cluster")


def test_request_transfer_unknown_direction():
    from u19 import transfers

    with pytest.raises(ValueError):
        transfers.request_transfer(1, "p", "electrophysiology", "sideways")


def test_check_transfer_status_dry_run_completed():
    from u19 import transfers

    result = transfers.check_transfer_status("dryrun-task-1-to_cluster")
    assert transfers.transfer_succeeded(result)
    assert not transfers.transfer_failed(result)


# --- slurm -----------------------------------------------------------------

def test_submit_slurm_job_dry_run():
    from u19 import slurm

    result = slurm.submit_slurm_job(7, {"process_cluster": "spock"}, "raw", "proc", "electrophysiology")
    assert result["status"] == 0
    assert result["slurm_id"] == "dryrun-slurm-7"


def test_poll_slurm_job_dry_run_terminal_success():
    from u19 import slurm

    result = slurm.poll_slurm_job("dryrun-slurm-7", {"process_cluster": "spock"})
    assert slurm.is_terminal(result)
    assert slurm.is_success(result)


def test_slurm_state_helpers():
    from u19 import slurm

    assert slurm.is_terminal({"pipeline_status": 1}) is True
    assert slurm.is_terminal({"pipeline_status": -1}) is True
    assert slurm.is_terminal({"pipeline_status": 0}) is False
    assert slurm.is_success({"pipeline_status": 1}) is True
    assert slurm.is_success({"pipeline_status": -1}) is False


# --- slurm sensor / trigger ------------------------------------------------

def test_slurm_trigger_serialize_roundtrip():
    from u19.slurm_sensor import SlurmJobTrigger

    trig = SlurmJobTrigger("123", {"process_cluster": "spock"}, poll_interval=30)
    classpath, kwargs = trig.serialize()
    assert classpath == "u19.slurm_sensor.SlurmJobTrigger"
    assert kwargs == {"slurm_id": "123", "program_selection_params": {"process_cluster": "spock"}, "poll_interval": 30}


@pytest.mark.asyncio
async def test_slurm_trigger_fires_on_completion(monkeypatch):
    from u19.slurm_sensor import SlurmJobTrigger

    # poll_slurm_job (dry-run) returns terminal success, so the trigger should
    # emit exactly one TriggerEvent with success=True.
    trig = SlurmJobTrigger("dryrun-slurm-1", {"process_cluster": "spock"}, poll_interval=0)
    gen = trig.run()
    event = await gen.__anext__()
    assert event.payload["success"] is True
    assert event.payload["slurm_id"] == "dryrun-slurm-1"


def test_slurm_sensor_execute_complete_raises_on_failure():
    from u19.slurm_sensor import SlurmJobSensor

    sensor = SlurmJobSensor(task_id="s", slurm_id="9", program_selection_params={})
    with pytest.raises(RuntimeError):
        sensor.execute_complete(context={}, event={"success": False, "slurm_id": "9", "error": "TIMEOUT"})
    # success path must not raise
    sensor.execute_complete(context={}, event={"success": True, "slurm_id": "9"})


# --- params ----------------------------------------------------------------

def test_program_selection_params_unknown_modality(monkeypatch):
    import sys
    import types

    import pandas as pd

    # Stub the full module chain so the leaf import resolves without the heavy
    # real u19_pipeline (which pulls scipy etc.).
    pkg = types.ModuleType("u19_pipeline")
    pkg.__path__ = []  # mark as package
    aj = types.ModuleType("u19_pipeline.automatic_job")
    aj.__path__ = []
    cfg = types.ModuleType("u19_pipeline.automatic_job.params_config")
    cfg.recording_modality_df = pd.DataFrame(
        [{"recording_modality": "electrophysiology", "process_cluster": "spock"}]
    )
    monkeypatch.setitem(sys.modules, "u19_pipeline", pkg)
    monkeypatch.setitem(sys.modules, "u19_pipeline.automatic_job", aj)
    monkeypatch.setitem(sys.modules, "u19_pipeline.automatic_job.params_config", cfg)

    from u19 import params

    assert params.program_selection_params_for("electrophysiology")["process_cluster"] == "spock"
    with pytest.raises(ValueError):
        params.program_selection_params_for("nope")
