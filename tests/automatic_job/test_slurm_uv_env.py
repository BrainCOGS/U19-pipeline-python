"""Tests for the python environment of ephys jobs on spock (conda or uv).

``slurm_creator`` imports ``params_config`` (which declares DataJoint schemas) and
``clusters_paths_and_transfers`` (which reads ``dj.config["custom"]["root_data_dir"]``).
Both are stubbed, so the tests run without a database, ssh or a cluster.
"""

import importlib
import shlex
import subprocess
import sys
import types

import pytest

pytest.importorskip("element_interface")  # comes from the "pipeline" extra

MODULES = (
    "u19_pipeline.automatic_job.slurm_creator",
    "u19_pipeline.automatic_job.clusters_paths_and_transfers",
)

REPO_DIR = "/mnt/cup/braininit/Shared/repos/AutomaticPipelineProcessing/electrophysiology_processing/BrainCogsEphysSorters"

EPHYS_SPOCK = {
    "recording_modality": "electrophysiology",
    "process_repository": "BrainCogsEphysSorters",
    "process_cluster": "spock",
    "process_script": "main_script.py",
}
IMAGING_SPOCK = {
    "recording_modality": "imaging",
    "process_repository": "U19-pipeline-python",
    "process_cluster": "spock",
    "process_script": "u19_pipeline/automatic_job/suite2p_slurm_job.py",
}
EPHYS_TIGER = dict(EPHYS_SPOCK, process_cluster="tiger")

SLURM_DICT = {
    "job-name": "job_id_7",
    "nodes": 1,
    "mail-type": ["END"],
    "output": "/logs/job_id_7.log",
    "error": "/errors/job_id_7.log",
}

# generate_slurm_spock's output on master before the uv environment existed
CONDA_SLURM_TEXT = """#!/bin/bash
#SBATCH --job-name=job_id_7
#SBATCH --nodes=1
#SBATCH --mail-type=END
#SBATCH --output=/logs/job_id_7.log
#SBATCH --error=/errors/job_id_7.log

    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    source ~/.bashrc
    module load anacondapy/2023.07-cuda -s
    module load matlab/R2024a -s

    conda activate u19_pipeline_python_env3

    cd ${repository_dir}
    python -u ${process_script_path}
    #python ${process_script_path} ${recording_process_id}
    """


@pytest.fixture
def custom(monkeypatch):
    """dj.config["custom"] for the test; set custom["spock_ephys_env"] to pick the env"""
    dj = pytest.importorskip("datajoint")
    custom = dict(dj.config.get("custom") or {}, root_data_dir="/tmp/root")
    custom.pop("spock_ephys_env", None)
    monkeypatch.setitem(dj.config, "custom", custom)
    return custom


@pytest.fixture
def sc(monkeypatch, custom):
    params_config = types.ModuleType("params_config")
    params_config.system_process = {"COMPLETED": 1, "SUCCESS": 0, "ERROR": -1}
    monkeypatch.setitem(
        sys.modules, "u19_pipeline.automatic_job.params_config", params_config
    )
    for module in MODULES:
        monkeypatch.delitem(sys.modules, module, raising=False)
    module = importlib.import_module(MODULES[0])
    monkeypatch.setattr(module, "is_this_spock", lambda: False)
    yield module
    for name in MODULES:
        sys.modules.pop(name, None)


def completed(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


# --- which environment -----------------------------------------------------------------


def test_env_is_conda_unless_configured(sc):
    assert sc.spock_ephys_env() == "conda"


@pytest.mark.parametrize(
    "value, env", [("uv", "uv"), (" UV\n", "uv"), ("Conda", "conda")]
)
def test_env_read_from_dj_config(sc, custom, value, env):
    custom["spock_ephys_env"] = value
    assert sc.spock_ephys_env() == env


@pytest.mark.parametrize("value", ["venv", "", None, 1])
def test_unknown_env_is_an_error(sc, custom, value):
    custom["spock_ephys_env"] = value
    with pytest.raises(ValueError, match="spock_ephys_env"):
        sc.spock_ephys_env()


def test_env_without_custom_section(sc, monkeypatch):
    import datajoint as dj

    monkeypatch.setitem(dj.config, "custom", None)
    assert sc.spock_ephys_env() == "conda"


@pytest.mark.parametrize(
    "params, env, uses_uv",
    [
        (EPHYS_SPOCK, "uv", True),
        (EPHYS_SPOCK, "conda", False),
        (IMAGING_SPOCK, "uv", False),
        (EPHYS_TIGER, "uv", False),
    ],
)
def test_only_spock_ephys_jobs_use_uv(sc, custom, params, env, uses_uv):
    custom["spock_ephys_env"] = env
    assert sc.ephys_job_uses_uv(params) is uses_uv


# --- the slurm script ------------------------------------------------------------------


def test_conda_script_is_unchanged(sc):
    assert sc.generate_slurm_spock(SLURM_DICT) == CONDA_SLURM_TEXT


def test_conda_script_is_unchanged_when_selected(sc, custom):
    custom["spock_ephys_env"] = "conda"
    assert sc.generate_slurm_spock(SLURM_DICT) == CONDA_SLURM_TEXT


def body_commands(slurm_text):
    """The commands of a slurm script: no #SBATCH header, comments or blank lines"""
    lines = [line.strip() for line in slurm_text.splitlines()]
    return [line for line in lines if line and not line.startswith("#")]


def test_uv_script_keeps_the_slurm_header(sc, custom):
    conda_text = sc.generate_slurm_spock(SLURM_DICT)
    custom["spock_ephys_env"] = "uv"
    uv_text = sc.generate_slurm_spock(SLURM_DICT)

    def header(text):
        return [line for line in text.splitlines() if line.startswith("#")]

    assert header(uv_text) == header(conda_text)


def test_uv_script_runs_from_the_venv_without_conda(sc, custom):
    custom["spock_ephys_env"] = "uv"
    commands = body_commands(sc.generate_slurm_spock(SLURM_DICT))

    assert not any("conda" in command for command in commands)
    # matlab stays for the Kilosort 2 / 3 sorters
    assert "module load matlab/R2024a -s" in commands
    # umask and PATH (uv) of the account come from its .bashrc
    assert "source ~/.bashrc" in commands

    cd = next(i for i, c in enumerate(commands) if c.startswith("cd ${repository_dir}"))
    check = next(i for i, c in enumerate(commands) if c.startswith("uv sync "))
    activate = commands.index("source .venv/bin/activate")
    run = commands.index("python -u ${process_script_path}")
    assert cd < check < activate < run
    assert run == len(commands) - 1  # the job's exit code is main_script's


def test_uv_script_never_installs_on_the_node(sc, custom):
    custom["spock_ephys_env"] = "uv"
    commands = body_commands(sc.generate_slurm_spock(SLURM_DICT))

    (check,) = [c for c in commands if c.startswith("uv ")]
    assert check.startswith("uv sync --frozen --offline --check || {")
    assert check.endswith("exit 1; }")
    assert "uv run" not in " ".join(commands)


def test_uv_script_stops_without_the_checkout(sc, custom):
    custom["spock_ephys_env"] = "uv"
    commands = body_commands(sc.generate_slurm_spock(SLURM_DICT))

    (cd,) = [c for c in commands if c.startswith("cd ")]
    assert cd.endswith("exit 1; }")


def test_python_env_argument_overrides_the_config(sc, custom):
    custom["spock_ephys_env"] = "uv"
    assert sc.generate_slurm_spock(SLURM_DICT, python_env="conda") == CONDA_SLURM_TEXT


def test_imaging_script_ignores_the_switch(sc, custom):
    conda_text = sc.generate_slurm_spock_imaging(SLURM_DICT, "/params/job.json")
    custom["spock_ephys_env"] = "uv"
    assert sc.generate_slurm_spock_imaging(SLURM_DICT, "/params/job.json") == conda_text
    assert "conda activate u19_suite2p_env" in conda_text


def test_tiger_script_ignores_the_switch(sc, custom):
    conda_text = sc.generate_slurm_tiger(SLURM_DICT)
    custom["spock_ephys_env"] = "uv"
    assert sc.generate_slurm_tiger(SLURM_DICT) == conda_text


# --- uv sync on the login node ---------------------------------------------------------


@pytest.fixture
def runs(sc, monkeypatch):
    """Record subprocess.run calls of slurm_creator; runs.result is what they return"""
    calls = types.SimpleNamespace(commands=[], kwargs=[], result=completed())

    def run(command, **kwargs):
        calls.commands.append(command)
        calls.kwargs.append(kwargs)
        if isinstance(calls.result, Exception):
            raise calls.result
        return calls.result

    monkeypatch.setattr(sc.subprocess, "run", run)
    return calls


CLUSTER_VARS = {"user": "u19prod", "hostname": "spockmk2-loginvm.pni.princeton.edu"}


def test_sync_runs_uv_over_ssh(sc, runs):
    assert sc.sync_uv_env(CLUSTER_VARS, REPO_DIR) == (0, "")

    (command,) = runs.commands
    assert command[:2] == ["ssh", "u19prod@spockmk2-loginvm.pni.princeton.edu"]
    # ssh hands the remote shell its arguments joined by spaces: the whole script has to
    # arrive as the one argument of bash -lc
    assert shlex.split(" ".join(command[2:])) == [
        "bash",
        "-lc",
        "cd " + REPO_DIR + " && uv sync --locked",
    ]
    assert runs.kwargs[0]["timeout"] == sc.uv_sync_timeout_s


def test_sync_quotes_the_checkout_path(sc, runs):
    sc.sync_uv_env(CLUSTER_VARS, "/data/my repos/Sorters")

    (script,) = shlex.split(" ".join(runs.commands[0][2:]))[2:]
    assert shlex.split(script) == [
        "cd",
        "/data/my repos/Sorters",
        "&&",
        "uv",
        "sync",
        "--locked",
    ]


def test_sync_runs_locally_on_spock(sc, runs, monkeypatch):
    monkeypatch.setattr(sc, "is_this_spock", lambda: True)
    sc.sync_uv_env(CLUSTER_VARS, REPO_DIR)

    assert runs.commands == [["bash", "-lc", "cd " + REPO_DIR + " && uv sync --locked"]]


def test_sync_failure_reports_what_uv_said(sc, runs):
    runs.result = completed(
        2,
        stderr="error: The lockfile at `uv.lock` needs to be updated,\n but `--locked`",
    )
    status, message = sc.sync_uv_env(CLUSTER_VARS, REPO_DIR)

    assert status == 2
    assert message == (
        "uv sync --locked failed in " + REPO_DIR + ": "
        "error: The lockfile at `uv.lock` needs to be updated, but `--locked`"
    )


def test_sync_failure_without_stderr_reports_the_exit_code(sc, runs):
    runs.result = completed(255)
    status, message = sc.sync_uv_env(CLUSTER_VARS, REPO_DIR)

    assert status == 255
    assert message.endswith(": exit code 255")


@pytest.mark.parametrize(
    "error, reason",
    [
        (subprocess.TimeoutExpired("ssh", 1800), "no answer in 1800 s"),
        (FileNotFoundError("ssh"), "ssh"),
    ],
)
def test_sync_that_cannot_run_is_an_error(sc, runs, error, reason):
    runs.result = error
    status, message = sc.sync_uv_env(CLUSTER_VARS, REPO_DIR)

    assert status == -1
    assert message.startswith("uv sync --locked failed in " + REPO_DIR)
    assert reason in message


# --- sbatch ----------------------------------------------------------------------------


@pytest.fixture
def sbatch(sc, monkeypatch):
    """Record the sbatch commands queue_slurm_file starts; they answer with a job id"""
    commands = []

    class Popen:
        returncode = 0

        def __init__(self, command, **kwargs):
            commands.append(command)

        def wait(self):
            return 0

        def communicate(self):
            return b"Submitted batch job 123\n", b""

    monkeypatch.setattr(sc.subprocess, "Popen", Popen)
    return commands


def queue(sc, params):
    return sc.queue_slurm_file(
        7, params, "raw/dir", "proc/dir", params["recording_modality"], "/slurm/f.slurm"
    )


def test_queue_syncs_the_uv_env_before_sbatch(sc, custom, runs, sbatch):
    custom["spock_ephys_env"] = "uv"
    assert queue(sc, EPHYS_SPOCK) == (0, "123", "")

    (sync,) = runs.commands
    assert "cd " + REPO_DIR + " && uv sync --locked" in sync[2]
    assert len(sbatch) == 1 and "sbatch" in sbatch[0]


def test_queue_does_not_submit_when_the_sync_fails(sc, custom, runs, sbatch):
    custom["spock_ephys_env"] = "uv"
    runs.result = completed(127, stderr="bash: uv: command not found")
    status, slurm_jobid, message = queue(sc, EPHYS_SPOCK)

    assert status == 127
    assert slurm_jobid == -1
    assert "uv: command not found" in message
    assert sbatch == []


@pytest.mark.parametrize(
    "params, env",
    [(EPHYS_SPOCK, "conda"), (EPHYS_SPOCK, None), (IMAGING_SPOCK, "uv")],
)
def test_queue_does_not_sync_for_conda_jobs(sc, custom, runs, sbatch, params, env):
    if env:
        custom["spock_ephys_env"] = env
    assert queue(sc, params) == (0, "123", "")

    assert runs.commands == []
    assert len(sbatch) == 1
