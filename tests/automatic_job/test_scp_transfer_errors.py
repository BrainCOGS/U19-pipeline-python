"""A failed scp to the cluster has to say why in the job's error info.

The failure reaches ``slurm_job_queue`` the way it does in production: a fake ``scp``
executable on PATH exits non-zero and writes to stderr. The handler's database and
sorter imports are stubbed, so no database or cluster is needed.
"""

import stat
import sys
import types
from unittest import mock

import datajoint as dj
import pytest

STUBBED_MODULES = (
    "ecephys_spike_sorting",
    "ecephys_spike_sorting.common",
    "ecephys_spike_sorting.common.SGLXMetaToCoords",
    "u19_pipeline.lab",
    "u19_pipeline.recording",
    "u19_pipeline.recording_process",
    "u19_pipeline.ephys_pipeline",
    "u19_pipeline.imaging_pipeline",
    "u19_pipeline.utility",
    "u19_pipeline.acquisition",
    "u19_pipeline.utils.ephys_utils",
    "u19_pipeline.utils.scp_transfers",
    "u19_pipeline.automatic_job.ephys_element_ingest",
    "u19_pipeline.automatic_job.ephys_element_populate",
    "u19_pipeline.automatic_job.imaging_element_populate",
)


def _import_handler():
    element_interface = types.ModuleType("element_interface")
    element_interface_utils = types.ModuleType("element_interface.utils")
    element_interface_utils.dict_to_uuid = lambda d: None
    stubs = {
        "element_interface": element_interface,
        "element_interface.utils": element_interface_utils,
        **{name: mock.MagicMock() for name in STUBBED_MODULES},
    }
    saved_custom = dj.config.get("custom")
    dj.config["custom"] = {**(saved_custom or {}), "root_data_dir": "/tmp"}
    saved_modules = {name: sys.modules.get(name) for name in stubs}
    loaded_before = set(sys.modules)
    sys.modules.update(stubs)
    try:
        from u19_pipeline.automatic_job import recording_process_handler

        return recording_process_handler
    finally:
        dj.config["custom"] = saved_custom
        for name in set(sys.modules) - loaded_before:
            if name.startswith("u19_pipeline."):
                del sys.modules[name]
        for name, module in saved_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


handler = _import_handler()
config = handler.config
ft = handler.ft
slurmlib = handler.slurmlib
ERROR_STATUS = config.status_update_idx["ERROR_STATUS"]
SUCCESS = config.system_process["SUCCESS"]
DENIED = "u19prod@spockmk2-loginvm.pni.princeton.edu: Permission denied (publickey)."


@pytest.fixture
def fake_scp(tmp_path, monkeypatch):
    """Put an ``scp`` on PATH. Destinations containing a pattern in ``fail_on`` fail."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "scp_calls"
    state = types.SimpleNamespace(calls=calls)

    def install(exit_code=255, stderr=DENIED, fail_on=""):
        script = bin_dir / "scp"
        script.write_text(
            "#!/bin/sh\n"
            f'echo "$@" >> "{calls}"\n'
            "for last; do :; done\n"
            f'case "$last" in *"{fail_on}"*) ;; *) exit 0 ;; esac\n'
            f"printf '%s' '{stderr}' >&2\n"
            f"exit {exit_code}\n"
        )
        script.chmod(script.stat().st_mode | stat.S_IEXEC)
        return state

    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin:/bin")
    monkeypatch.setattr(config, "parameter_files_filepath", str(tmp_path))
    monkeypatch.setattr(config, "chanmap_files_filepath", str(tmp_path))
    return install


def rec_series(job_id=1465, modality="imaging"):
    return {
        "job_id": job_id,
        "params": {"a": 1},
        "preparams": {"b": 2},
        "recording_modality": modality,
        "program_selection_params": {
            "local_or_cluster": "cluster",
            "process_cluster": "spock",
        },
    }


@pytest.mark.parametrize(
    "job_id, modality", [(1465, "imaging"), (1467, "electrophysiology")]
)
def test_failed_parameter_file_scp_reports_the_scp_error(fake_scp, job_id, modality):
    fake_scp()

    status, update = handler.RecProcessHandler.slurm_job_queue(
        rec_series(job_id, modality), None
    )

    assert status == ERROR_STATUS
    error_info = update["error_info"]
    assert "Permission denied (publickey)" in error_info["error_message"]
    assert f"process_paramset_{job_id}.json" in error_info["error_message"]
    assert "255" in error_info["error_message"]
    assert "Permission denied (publickey)" in error_info["error_exception"]


def test_failed_scp_without_stderr_still_reports_the_exit_code(fake_scp):
    fake_scp(exit_code=1, stderr="")

    status, update = handler.RecProcessHandler.slurm_job_queue(rec_series(), None)

    assert status == ERROR_STATUS
    assert "exit code 1" in update["error_info"]["error_message"]


def test_failed_preparams_scp_is_not_blamed_on_the_slurm_file(fake_scp):
    fake_scp(fail_on="preprocess_paramset_")

    status, update = handler.RecProcessHandler.slurm_job_queue(rec_series(), None)

    assert status == ERROR_STATUS
    assert "preprocess_paramset_1465.json" in update["error_info"]["error_message"]
    assert "slurm" not in update["error_info"]["error_message"].lower()


def test_scp_not_installed_is_reported(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(config, "parameter_files_filepath", str(tmp_path))

    status, update = handler.RecProcessHandler.slurm_job_queue(rec_series(), None)

    assert status == ERROR_STATUS
    assert "scp" in update["error_info"]["error_message"]
    assert update["error_info"]["error_exception"]


def test_failed_slurm_file_scp_reports_the_scp_error(fake_scp, tmp_path):
    fake_scp()

    with pytest.raises(ft.ScpTransferError, match="Permission denied"):
        slurmlib.transfer_slurm_file(
            str(tmp_path / "slurm.slurm"),
            "/cluster/slurm.slurm",
            {"user": "u19prod", "hostname": "spock"},
        )


def test_successful_scp_returns_success(fake_scp, tmp_path):
    fake_scp(fail_on="never-matches")

    assert ft.scp_file_transfer_or_raise(str(tmp_path / "a"), "h:/b") == SUCCESS


def test_local_jobs_do_not_scp(fake_scp, tmp_path):
    state = fake_scp()

    status = handler.paramfilelib.generate_parameter_file(
        1465, {"a": 1}, "params", {"local_or_cluster": "local"}
    )

    assert status == SUCCESS
    assert not state.calls.exists()


# scp_file_transfer's callers (cluster log copies) compare an exit code, and a missing
# log is not a failure of the job
@pytest.mark.parametrize("exit_code", [1, 255])
def test_scp_file_transfer_returns_the_exit_code(fake_scp, tmp_path, exit_code):
    fake_scp(exit_code=exit_code)

    assert ft.scp_file_transfer(str(tmp_path / "a"), "h:/b") == exit_code
