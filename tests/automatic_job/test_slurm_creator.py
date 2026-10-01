"""Tests for the ephys (uv) SLURM path in ``u19_pipeline.automatic_job.slurm_creator``.

``slurm_creator`` imports ``params_config`` (which queries ``lab.SlackWebhooks`` at
import time) and ``clusters_paths_and_transfers`` (which needs ``element_interface``
and ``dj.config['custom']['root_data_dir']``). None of that is exercised here, so the
module is imported once with those dependencies stubbed, and ``sys.modules`` /
``dj.config`` are restored afterwards so no stub leaks into other test modules.
"""

import subprocess
import sys
import threading
import types
from unittest import mock

import datajoint as dj
import pytest


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
    try:
        with mock.patch.dict(sys.modules, stubs):
            from u19_pipeline.automatic_job import slurm_creator

            return slurm_creator
    finally:
        dj.config["custom"] = saved_custom


sc = _import_slurm_creator()
ft = sc.ft
SUCCESS = sc.config.system_process["SUCCESS"]

EPHYS_REPO = "BrainCogsEphysSorters"
IMAGING_REPO = "element-calcium-imaging"


def selection(cluster="spock", repository=EPHYS_REPO):
    return {"process_cluster": cluster, "process_repository": repository}


def slurm_dict(job_id=7):
    return {
        "job-name": f"job_id_{job_id}",
        "output": f"/logs/job_id_{job_id}.log",
        "error": f"/errors/job_id_{job_id}.log",
        # Anything else the caller passes in should not override the ephys defaults.
        "time": "01:00:00",
        "cpus-per-task": 1,
    }


def sbatch_lines(slurm_text):
    return [line for line in slurm_text.splitlines() if "#SBATCH" in line]


# --------------------------------------------------------------------------------------
# generate_slurm_file: template dispatch
# --------------------------------------------------------------------------------------


class TestGenerateSlurmFileDispatch:
    @pytest.fixture
    def written(self, monkeypatch, tmp_path):
        written = {}
        monkeypatch.setattr(sc, "slurms_filepath", str(tmp_path))
        monkeypatch.setattr(sc, "write_file", lambda path, text: written.update(text=text))
        monkeypatch.setattr(sc, "transfer_slurm_file", lambda *a: SUCCESS)
        monkeypatch.setattr(sc, "is_this_spock", lambda: False)
        return written

    def test_spock_ephys_uses_uv_template(self, written):
        status, _ = sc.generate_slurm_file(3, selection("spock", EPHYS_REPO))

        assert status == SUCCESS
        assert "uv run --frozen --offline python -u ${process_script_path}" in written["text"]
        assert "conda activate" not in written["text"]

    def test_spock_imaging_keeps_conda_template(self, written):
        sc.generate_slurm_file(3, selection("spock", IMAGING_REPO))

        assert "conda activate" in written["text"]
        assert "uv " not in written["text"]

    def test_tiger_ephys_keeps_conda_template(self, written):
        sc.generate_slurm_file(3, selection("tiger", EPHYS_REPO))

        assert "conda activate BrainCogsEphysSorters_env" in written["text"]
        assert "uv " not in written["text"]


# --------------------------------------------------------------------------------------
# generate_slurm_spockmk2_ephys: rendered script
# --------------------------------------------------------------------------------------


class TestGenerateSlurmSpockmk2Ephys:
    def test_sbatch_directives_immediately_follow_shebang(self):
        lines = sc.generate_slurm_spockmk2_ephys(slurm_dict()).splitlines()

        assert lines[0] == "#!/bin/bash"
        n_sbatch = len(sbatch_lines("\n".join(lines)))
        assert n_sbatch > 0
        # Every #SBATCH line must start the line and come before any command;
        # SLURM stops reading directives at the first non-comment line.
        assert all(line.startswith("#SBATCH --") for line in lines[1 : 1 + n_sbatch])

    def test_source_bashrc_is_its_own_line_in_the_body(self):
        text = sc.generate_slurm_spockmk2_ephys(slurm_dict())

        assert "source ~/.bashrc#SBATCH" not in text
        assert any(line.strip() == "source ~/.bashrc" for line in text.splitlines())
        assert text.index("source ~/.bashrc") > text.rindex("#SBATCH")

    def test_resources_come_from_ephys_defaults(self):
        text = sc.generate_slurm_spockmk2_ephys(slurm_dict())
        lines = sbatch_lines(text)

        assert "#SBATCH --cpus-per-task=8" in lines
        assert "#SBATCH --time=30:00:00" in lines
        assert "#SBATCH --gres=gpu:2" in lines
        assert "#SBATCH --time=01:00:00" not in lines
        assert "#SBATCH --cpus-per-task=1" not in lines

    def test_per_job_fields_come_from_caller(self):
        lines = sbatch_lines(sc.generate_slurm_spockmk2_ephys(slurm_dict(job_id=42)))

        assert "#SBATCH --job-name=job_id_42" in lines
        assert "#SBATCH --output=/logs/job_id_42.log" in lines
        assert "#SBATCH --error=/errors/job_id_42.log" in lines

    def test_does_not_mutate_module_defaults(self):
        before = dict(ft.slurm_dict_spockmk2_ephys)

        sc.generate_slurm_spockmk2_ephys(slurm_dict(job_id=99))

        assert ft.slurm_dict_spockmk2_ephys == before

    def test_mail_type_list_renders_one_directive_per_value(self):
        lines = sbatch_lines(sc.generate_slurm_spockmk2_ephys(slurm_dict()))

        assert lines.count("#SBATCH --mail-type=END") == 1

    def test_offline_sync_fails_the_job_before_uv_run(self):
        text = sc.generate_slurm_spockmk2_ephys(slurm_dict())

        sync = text.index("uv sync --frozen --offline ||")
        run = text.index("uv run --frozen --offline")
        assert sync < run
        assert "exit 1" in text[sync:run]

    def test_offline_sync_message_does_not_claim_a_lock_mismatch(self):
        # `--frozen` installs from uv.lock without checking it against pyproject.toml,
        # so an offline failure means packages are missing from the uv cache.
        text = sc.generate_slurm_spockmk2_ephys(slurm_dict())
        sync_line = next(line for line in text.splitlines() if "uv sync --frozen --offline" in line)

        assert "does not match" not in sync_line
        assert "cache" in sync_line
        assert "prefetch" in sync_line


# --------------------------------------------------------------------------------------
# prefetch_uv_env
# --------------------------------------------------------------------------------------


class FakePopen:
    """Stand-in for subprocess.Popen that records the command it was given."""

    calls = []

    def __init__(self, command, stdout=None, stderr=None, returncode=0, out=b"", err=b""):
        FakePopen.calls.append(command)
        self.returncode = returncode
        self._out, self._err = out, err

    def communicate(self, timeout=None):
        return self._out, self._err

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        pass


@pytest.fixture
def fake_popen(monkeypatch):
    FakePopen.calls = []

    def install(**result):
        monkeypatch.setattr(sc.subprocess, "Popen", lambda cmd, **kw: FakePopen(cmd, **result))
        return FakePopen.calls

    return install


@pytest.fixture
def off_spock(monkeypatch):
    monkeypatch.setattr(sc, "is_this_spock", lambda: False)


@pytest.fixture
def on_spock(monkeypatch):
    monkeypatch.setattr(sc, "is_this_spock", lambda: True)


class TestPrefetchUvEnvGating:
    @pytest.mark.parametrize("cluster", ["spock", "tiger"])
    def test_non_ephys_repository_is_a_noop(self, fake_popen, off_spock, cluster):
        calls = fake_popen()

        assert sc.prefetch_uv_env(selection(cluster, IMAGING_REPO), "imaging") == (SUCCESS, "")
        assert calls == []

    def test_tiger_ephys_is_a_noop(self, fake_popen, off_spock):
        # generate_slurm_tiger still uses conda, so a uv sync on della is pointless and
        # would fail the job wherever uv or the uv checkout is missing.
        calls = fake_popen(returncode=127, err=b"bash: uv: command not found")

        assert sc.prefetch_uv_env(selection("tiger", EPHYS_REPO), "electrophysiology") == (SUCCESS, "")
        assert calls == []


class TestPrefetchUvEnvCommand:
    def test_off_spock_runs_over_ssh_to_head_node(self, fake_popen, off_spock):
        calls = fake_popen()

        sc.prefetch_uv_env(selection(), "electrophysiology")

        spock = ft.cluster_vars["spock"]
        repo_dir = spock["electrophysiology_process_dir"] + "/" + EPHYS_REPO
        assert calls == [
            ["ssh", f"{spock['user']}@{spock['hostname']}", "bash", "-lc", f"cd {repo_dir} && uv sync --locked"]
        ]

    def test_on_spock_runs_locally(self, fake_popen, on_spock):
        calls = fake_popen()

        sc.prefetch_uv_env(selection(), "electrophysiology")

        assert len(calls) == 1
        assert calls[0][:2] == ["bash", "-lc"]
        assert calls[0][2].endswith("&& uv sync --locked")

    def test_uses_modality_process_dir(self, fake_popen, off_spock):
        calls = fake_popen()

        sc.prefetch_uv_env(selection(), "imaging")

        assert ft.cluster_vars["spock"]["imaging_process_dir"] + "/" + EPHYS_REPO in calls[0][-1]


class TestPrefetchUvEnvResult:
    def test_success_returns_empty_error_even_with_stderr(self, fake_popen, off_spock):
        # uv writes progress ("Resolved 120 packages ...") to stderr on success.
        fake_popen(returncode=0, err=b"Resolved 120 packages in 3ms")

        assert sc.prefetch_uv_env(selection(), "electrophysiology") == (SUCCESS, "")

    def test_failure_returns_returncode_and_stderr(self, fake_popen, off_spock):
        fake_popen(returncode=2, err=b"error: The lockfile needs to be updated")

        status, message = sc.prefetch_uv_env(selection(), "electrophysiology")

        assert status == 2
        assert status != SUCCESS
        assert message == "error: The lockfile needs to be updated"

    def test_failure_with_empty_stderr_still_reports_something(self, fake_popen, off_spock):
        fake_popen(returncode=255, err=b"")

        status, message = sc.prefetch_uv_env(selection(), "electrophysiology")

        assert status == 255
        assert message != ""

    def test_non_utf8_stderr_does_not_raise(self, fake_popen, off_spock):
        fake_popen(returncode=1, err=b"bad byte \xff here")

        status, message = sc.prefetch_uv_env(selection(), "electrophysiology")

        assert status == 1
        assert "bad byte" in message


class TestPrefetchUvEnvRealProcess:
    """Run a real child process in place of ssh/uv to exercise pipe and timeout handling."""

    @pytest.fixture
    def run_instead(self, monkeypatch):
        real_popen = subprocess.Popen

        def install(command):
            monkeypatch.setattr(sc.subprocess, "Popen", lambda _cmd, **kw: real_popen(command, **kw))

        return install

    @staticmethod
    def call_with_deadline(seconds):
        result = {}
        thread = threading.Thread(
            target=lambda: result.update(value=sc.prefetch_uv_env(selection(), "electrophysiology")),
            daemon=True,
        )
        thread.start()
        thread.join(seconds)
        assert not thread.is_alive(), f"prefetch_uv_env did not return within {seconds}s"
        return result["value"]

    def test_large_output_does_not_deadlock(self, run_instead, off_spock):
        # More than a pipe buffer (64 KiB on Linux) on both streams, like a cold-cache
        # uv sync listing every installed package.
        run_instead(
            [
                sys.executable,
                "-c",
                "import sys; sys.stdout.write('o' * 300000); sys.stderr.write('e' * 300000); sys.exit(1)",
            ]
        )

        status, message = self.call_with_deadline(20)

        assert status == 1
        assert len(message) == 300000

    def test_hung_command_times_out_as_error(self, run_instead, off_spock, monkeypatch):
        monkeypatch.setattr(sc, "prefetch_uv_env_timeout_s", 0.5)
        run_instead([sys.executable, "-c", "import time; time.sleep(60)"])

        status, message = self.call_with_deadline(20)

        assert status != SUCCESS
        assert "timed out" in message
