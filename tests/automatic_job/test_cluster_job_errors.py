"""Tests for what is reported when a cluster (slurm) job fails.

``slurm_creator`` imports ``params_config`` (which queries ``lab.SlackWebhooks`` at
import time) and ``clusters_paths_and_transfers`` (which needs ``element_interface``
and ``dj.config['custom']['root_data_dir']``). The module is imported once with those
dependencies stubbed, and ``sys.modules`` / ``dj.config`` are restored afterwards so no
stub leaks into other test modules. There is no slurm here: ``sacct`` and ``scp`` are
faked.
"""

import pathlib
import subprocess
import sys
import types
from unittest import mock

import datajoint as dj
import pytest

from u19_pipeline.utils.slurm_utils import sacct_format


SACCT_HEADER = "|".join(sacct_format)


def sacct(*rows):
    return "\n".join([SACCT_HEADER, *rows]) + "\n"


# JobID|State|ExitCode|DerivedExitCode|Reason|Elapsed|Timelimit|MaxRSS|NodeList
OOM_JOB = sacct(
    "1415|OUT_OF_MEMORY|0:125|0:0|None|01:02:03|1-00:00:00||spock-g3",
    "1415.batch|OUT_OF_MEMORY|0:125|||01:02:03||31457280K|spock-g3",
    "1415.extern|COMPLETED|0:0|||01:02:03||1024K|spock-g3",
)
FAILED_JOB = sacct(
    "200|FAILED|1:0|0:0|None|00:00:05|02:00:00||spock-c1",
    "200.batch|FAILED|1:0|||00:00:05||1500M|spock-c1",
)
CANCELLED_JOB = sacct(
    "300|CANCELLED by 12345|0:0|0:0|None|00:10:00|02:00:00||spock-c2",
    "300.batch|CANCELLED|0:15|||00:10:00||2G|spock-c2",
)
RUNNING_JOB = sacct("500|RUNNING|0:0|0:0|None|00:01:00|02:00:00||spock-g1")


def _import_slurm_creator():
    element_interface = types.ModuleType("element_interface")
    element_interface_utils = types.ModuleType("element_interface.utils")
    element_interface_utils.dict_to_uuid = lambda d: None
    stubs = {
        "element_interface": element_interface,
        "element_interface.utils": element_interface_utils,
        "u19_pipeline.lab": mock.MagicMock(),
    }
    saved_custom = dj.config.get("custom")
    dj.config["custom"] = {**(saved_custom or {}), "root_data_dir": "/tmp"}
    saved_modules = {name: sys.modules.get(name) for name in stubs}
    loaded_before = set(sys.modules)
    sys.modules.update(stubs)
    try:
        from u19_pipeline.automatic_job import slurm_creator

        return slurm_creator
    finally:
        dj.config["custom"] = saved_custom
        # Drop what was imported against the stubs (third party modules such as numpy
        # stay: they cannot be loaded twice in one process)
        for name in set(sys.modules) - loaded_before:
            if name.startswith("u19_pipeline."):
                del sys.modules[name]
        for name, module in saved_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


sc = _import_slurm_creator()
ft = sc.ft
config = sc.config
SUCCESS = config.system_process["SUCCESS"]
ERROR_STATUS = config.status_update_idx["ERROR_STATUS"]
NEXT_STATUS = config.status_update_idx["NEXT_STATUS"]
NO_CHANGE = config.status_update_idx["NO_CHANGE"]


class FakeSacct:
    """Stands in for ``subprocess.run``, answering the detailed and the state-only query."""

    def __init__(self, detailed=None, state_only=None):
        # Each answer is (returncode, stdout, stderr) or an exception to raise
        self.answers = {"detailed": detailed, "state_only": state_only}
        self.commands = []

    def __call__(self, command, **kwargs):
        self.commands.append(command)
        query = "detailed" if any("JobID" in str(x) for x in command) else "state_only"
        answer = self.answers[query]
        if answer is None:
            pytest.fail(f"unexpected {query} sacct query: {command}")
        if isinstance(answer, BaseException):
            raise answer
        returncode, stdout, stderr = answer
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)


@pytest.fixture
def fake_run(monkeypatch):
    def install(**answers):
        fake = FakeSacct(**answers)
        monkeypatch.setattr(sc.subprocess, "run", fake)
        return fake

    return install


class TestCheckSlurmJob:
    def test_out_of_memory_reports_memory_and_node(self, fake_run):
        fake_run(detailed=(0, OOM_JOB, ""))

        status, message = sc.check_slurm_job("u19prod", "spock", "1415")

        assert status == ERROR_STATUS
        assert message.startswith("slurm 1415 OUT_OF_MEMORY")
        assert "MaxRSS 30.0G" in message
        assert "spock-g3" in message

    def test_failed_reports_exit_code(self, fake_run):
        fake_run(detailed=(0, FAILED_JOB, ""))

        status, message = sc.check_slurm_job("u19prod", "spock", "200")

        assert status == ERROR_STATUS
        assert message.startswith("slurm 200 FAILED")
        assert "ExitCode 1:0" in message

    def test_cancelled_by_user_is_an_error(self, fake_run):
        # "CANCELLED by <uid>" used to be cropped to "CANCELLED+" by sacct's column width
        fake_run(detailed=(0, CANCELLED_JOB, ""))

        status, message = sc.check_slurm_job("u19prod", "spock", "300")

        assert status == ERROR_STATUS
        assert message.startswith("slurm 300 CANCELLED by uid 12345")

    @pytest.mark.parametrize(
        "state",
        ["OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE", "PREEMPTED", "TIMEOUT"],
    )
    def test_states_slurm_kills_jobs_with_are_errors(self, fake_run, state):
        fake_run(
            detailed=(
                0,
                sacct(f"9|{state}|0:0|0:0|None|00:00:01|02:00:00||spock-c1"),
                "",
            )
        )

        status, message = sc.check_slurm_job("u19prod", "spock", "9")

        assert status == ERROR_STATUS
        assert message.startswith(f"slurm 9 {state}")

    @pytest.mark.parametrize(
        "state",
        ["PENDING", "RUNNING", "COMPLETING", "CONFIGURING", "REQUEUED", "SUSPENDED"],
    )
    def test_unfinished_states_do_not_change_status(self, fake_run, state):
        fake_run(
            detailed=(
                0,
                sacct(f"9|{state}|0:0|0:0|None|00:00:01|02:00:00||spock-c1"),
                "",
            )
        )

        assert sc.check_slurm_job("u19prod", "spock", "9") == (NO_CHANGE, "")

    def test_completed_has_no_message(self, fake_run):
        fake_run(
            detailed=(
                0,
                sacct("9|COMPLETED|0:0|0:0|None|00:00:01|02:00:00||spock-c1"),
                "",
            )
        )

        assert sc.check_slurm_job("u19prod", "spock", "9") == (NEXT_STATUS, "")

    def test_slurm_id_kept_with_the_longest_description(self, fake_run):
        node_list = "spock-g[" + ",".join(str(x) for x in range(500)) + "]"
        fake_run(
            detailed=(
                0,
                sacct(
                    f"6519400|OUT_OF_MEMORY|0:125|0:0|None|01:02:03|1-00:00:00||{node_list}"
                ),
                "",
            )
        )

        _, message = sc.check_slurm_job("u19prod", "spock", "6519400")

        assert message.startswith("slurm 6519400 OUT_OF_MEMORY")
        assert len(message) <= 255

    def test_job_1415_completed_near_its_memory_request_is_not_an_error(self, fake_run):
        # sacct of the slurm job behind recording process job 1415 on spock: it completed
        fake_run(
            detailed=(
                0,
                sacct(
                    "6519400|COMPLETED|0:0|0:0|None|02:33:04|1-00:00:00||spockmk2-23-06",
                    "6519400.batch|COMPLETED|0:0|||02:33:04||52427304K|spockmk2-23-06",
                ),
                "",
            )
        )

        assert sc.check_slurm_job("u19prod", "spock", "6519400") == (NEXT_STATUS, "")

    def test_unknown_state_is_reported_not_raised(self, fake_run):
        # This used to raise a KeyError out of the status check
        fake_run(
            detailed=(
                0,
                sacct("9|SOMETHING_NEW|0:0|0:0|None|00:00:01|02:00:00||spock-c1"),
                "",
            )
        )

        status, message = sc.check_slurm_job("u19prod", "spock", "9")

        assert status == ERROR_STATUS
        assert message.startswith("slurm 9 unexpected state SOMETHING_NEW")

    def test_command_runs_through_ssh_unless_local(self, fake_run):
        fake = fake_run(detailed=(0, RUNNING_JOB, ""))

        sc.check_slurm_job("u19prod", "spock", "500")
        sc.check_slurm_job("u19prod", "spock", "500", local_user=True)

        assert fake.commands[0][:2] == ["ssh", "u19prod@spock"]
        assert fake.commands[1][0] == "sacct"
        assert "500" in fake.commands[1]
        assert "-P" in fake.commands[1]

    def test_detailed_query_rejected_falls_back_to_state_only(self, fake_run):
        # e.g. an older sacct that does not know one of the requested fields
        fake = fake_run(
            detailed=(1, "", 'sacct: error: Invalid field requested: "Reason"'),
            state_only=(0, "FAILED\nFAILED\n", ""),
        )

        status, message = sc.check_slurm_job("u19prod", "spock", "200")

        assert status == ERROR_STATUS
        assert message == "slurm 200 FAILED"
        assert len(fake.commands) == 2

    def test_sacct_unreachable_is_retried_not_a_job_failure(self, fake_run):
        # Job 1415 had COMPLETED, but one failed ssh/sacct call during polling
        # marked it as failed with an empty error log
        err = "ssh: connect to host spock port 22: Connection timed out"
        fake_run(detailed=(255, "", err), state_only=(255, "", err))

        status, message = sc.check_slurm_job("u19prod", "spock", "200")

        assert status == NO_CHANGE
        assert message.startswith("Failed to retrieve status of slurm job 200: ")
        assert "Connection timed out" in message
        assert len(message) <= 255

    def test_sacct_hanging_is_retried_not_a_hang(self, fake_run):
        timeout = subprocess.TimeoutExpired(cmd="ssh", timeout=1)
        fake_run(detailed=timeout, state_only=timeout)

        status, message = sc.check_slurm_job("u19prod", "spock", "200")

        assert status == NO_CHANGE
        assert message.startswith("Failed to retrieve status of slurm job 200: ")

    def test_ssh_missing_is_retried(self, fake_run):
        error = FileNotFoundError("ssh")
        fake_run(detailed=error, state_only=error)

        assert sc.check_slurm_job("u19prod", "spock", "200")[0] == NO_CHANGE

    def test_job_missing_from_sacct(self, fake_run):
        fake_run(detailed=(0, "", ""), state_only=(0, "", ""))

        status, message = sc.check_slurm_job("u19prod", "spock", "123")

        assert status == ERROR_STATUS
        assert message == "slurm 123 not found in sacct"


# --------------------------------------------------------------------------------------
# get_job_error_info: which log the error is taken from
# --------------------------------------------------------------------------------------

KILOSORT_STDOUT = """Time   0s. Loading raw data...
Time  35s. Computing whitening matrix..
Error using gpuArray
Out of memory on device.
"""

TRACEBACK = """Traceback (most recent call last):
  File "run.py", line 1, in <module>
MemoryError: Unable to allocate 30.0 GiB for an array
"""


@pytest.fixture
def cluster_logs(monkeypatch, tmp_path):
    """Fake cluster with an error and an output log for job 1415, copied over with a fake scp."""
    local_error_dir = tmp_path / "local_errors"
    local_output_dir = tmp_path / "local_output"
    local_error_dir.mkdir()
    local_output_dir.mkdir()

    saved_custom = dj.config.get("custom")
    dj.config["custom"] = {
        **(saved_custom or {}),
        "error_logs_dir": str(local_error_dir),
        "output_logs_dir": str(local_output_dir),
    }
    monkeypatch.setattr(
        ft,
        "get_cluster_vars",
        lambda cluster: {
            "user": "u19prod",
            "hostname": "spock",
            "error_files_dir": "/cluster/ErrorLog",
            "log_files_dir": "/cluster/OutputLog",
        },
    )

    # Contents on the cluster, None for a log that cannot be copied
    remote = {"ERROR": "", "OUTPUT": ""}

    def fake_scp(source, dest):
        log_type = "ERROR" if "/cluster/ErrorLog/" in source else "OUTPUT"
        if remote[log_type] is None:
            return 1
        pathlib.Path(dest).write_text(remote[log_type])
        return SUCCESS

    monkeypatch.setattr(ft, "scp_file_transfer", fake_scp)
    yield types.SimpleNamespace(
        remote=remote, error_dir=local_error_dir, output_dir=local_output_dir
    )
    dj.config["custom"] = saved_custom


PARAMS = {"process_cluster": "spock"}


class TestGetJobErrorInfo:
    def test_error_from_stderr(self, cluster_logs):
        cluster_logs.remote["ERROR"] = TRACEBACK

        message, exception = ft.get_job_error_info(
            1415, PARAMS, "FAILED (ExitCode 1:0)"
        )

        assert message.startswith("FAILED (ExitCode 1:0) - MemoryError")
        assert message.endswith(str(cluster_logs.error_dir / "job_id_1415.log") + ")")
        assert "MemoryError" in exception
        assert exception.rstrip().endswith("FAILED (ExitCode 1:0)")

    def test_empty_stderr_falls_back_to_stdout(self, cluster_logs):
        # Job 1415: Kilosort/MATLAB write their errors to stdout, stderr was empty
        cluster_logs.remote["OUTPUT"] = KILOSORT_STDOUT

        message, exception = ft.get_job_error_info(
            1415, PARAMS, "FAILED (ExitCode 1:0)"
        )

        assert "stdout: Out of memory on device." in message
        assert message.endswith(str(cluster_logs.output_dir / "job_id_1415.log") + ")")
        assert "error log is empty" in exception
        assert "Computing whitening matrix" in exception

    def test_untransferred_stderr_is_reported_and_stale_copy_ignored(
        self, cluster_logs
    ):
        # A stale copy from an earlier run of the same job must not be reported
        (cluster_logs.error_dir / "job_id_1415.log").write_text(
            "OldError: from a previous run\n"
        )
        cluster_logs.remote["ERROR"] = None
        cluster_logs.remote["OUTPUT"] = ""

        message, exception = ft.get_job_error_info(
            1415, PARAMS, "OUT_OF_MEMORY (MaxRSS 30.0G)"
        )

        assert "OldError" not in message
        assert "OldError" not in exception
        assert "error log could not be copied" in message
        assert message.endswith(
            "(LOG: u19prod@spock:/cluster/ErrorLog/job_id_1415.log)"
        )
        assert "OUT_OF_MEMORY (MaxRSS 30.0G)" in message

    def test_no_log_text_at_all_still_says_what_slurm_knows(self, cluster_logs):
        message, exception = ft.get_job_error_info(
            1415, PARAMS, "OUT_OF_MEMORY (MaxRSS 30.0G, node spock-g3)"
        )

        assert message.startswith("OUT_OF_MEMORY (MaxRSS 30.0G, node spock-g3) - ")
        assert "error log is empty" in message
        assert "output log is empty" in message
        assert exception
        assert "OUT_OF_MEMORY" in exception

    @pytest.mark.parametrize("slurm_message", ["", None])
    def test_without_slurm_message(self, cluster_logs, slurm_message):
        cluster_logs.remote["ERROR"] = TRACEBACK

        message, _ = ft.get_job_error_info(1415, PARAMS, slurm_message)

        assert message.startswith("MemoryError")
