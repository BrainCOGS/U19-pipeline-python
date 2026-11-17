"""Tests for the python environment of ephys and imaging jobs on spock (conda or uv).

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
    custom.pop("spock_imaging_env", None)
    custom.pop("spock_uv_cache_dir", None)
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
    # the only module is matlab, for the Kilosort 2 / 3 sorters that run in MATLAB on the
    # node: python comes from .venv, not from anacondapy
    assert [c for c in commands if c.startswith("module ")] == [
        "module load matlab/R2024a -s"
    ]
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


def test_uv_script_says_when_uv_is_not_on_path(sc, custom):
    custom["spock_ephys_env"] = "uv"
    commands = body_commands(sc.generate_slurm_spock(SLURM_DICT))

    (which,) = [c for c in commands if c.startswith("command -v uv ")]
    assert "uv is not on PATH" in which
    assert which.endswith("exit 1; }")
    assert commands.index(which) < commands.index(
        next(c for c in commands if c.startswith("uv sync "))
    )


def test_uv_script_uses_the_default_cache_unless_configured(sc, custom):
    custom["spock_ephys_env"] = "uv"
    assert "UV_CACHE_DIR" not in sc.generate_slurm_spock(SLURM_DICT)


SHARED_CACHE = "/mnt/bucket/scratch/u19prod/uv_cache"


def test_uv_script_checks_against_the_shared_cache(sc, custom):
    custom["spock_ephys_env"] = "uv"
    custom["spock_uv_cache_dir"] = SHARED_CACHE
    commands = body_commands(sc.generate_slurm_spock(SLURM_DICT))

    export = commands.index("export UV_CACHE_DIR=" + SHARED_CACHE)
    check = commands.index(next(c for c in commands if c.startswith("uv sync ")))
    assert export < check


def test_uv_script_quotes_the_cache_dir(sc, custom):
    custom["spock_ephys_env"] = "uv"
    custom["spock_uv_cache_dir"] = "/scratch/my cache"
    commands = body_commands(sc.generate_slurm_spock(SLURM_DICT))

    (export,) = [c for c in commands if "UV_CACHE_DIR=" in c]
    assert shlex.split(export) == ["export", "UV_CACHE_DIR=/scratch/my cache"]


def test_conda_script_ignores_the_cache_dir(sc, custom):
    custom["spock_uv_cache_dir"] = SHARED_CACHE
    assert sc.generate_slurm_spock(SLURM_DICT) == CONDA_SLURM_TEXT


@pytest.mark.parametrize("value", [None, "", "  "])
def test_cache_dir_is_unset_by_default(sc, custom, value):
    if value is not None:
        custom["spock_uv_cache_dir"] = value
    assert sc.spock_uv_cache_dir() is None


def test_cache_dir_read_from_dj_config(sc, custom):
    custom["spock_uv_cache_dir"] = " " + SHARED_CACHE + "/\n"
    assert sc.spock_uv_cache_dir() == SHARED_CACHE


@pytest.mark.parametrize("value", ["uv_cache", "~/uv_cache", 3])
def test_cache_dir_must_be_an_absolute_path(sc, custom, value):
    custom["spock_uv_cache_dir"] = value
    with pytest.raises(ValueError, match="spock_uv_cache_dir"):
        sc.spock_uv_cache_dir()


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


def test_sync_uses_the_shared_cache(sc, custom, runs):
    custom["spock_uv_cache_dir"] = "/scratch/my cache"
    sc.sync_uv_env(CLUSTER_VARS, REPO_DIR)

    (script,) = shlex.split(" ".join(runs.commands[0][2:]))[2:]
    # group-writable files in the shared cache; copies in .venv, no links into the cache
    assert shlex.split(script) == [
        "umask",
        "002",
        "&&",
        "export",
        "UV_CACHE_DIR=/scratch/my cache",
        "UV_LINK_MODE=copy",
        "&&",
        "cd",
        REPO_DIR,
        "&&",
        "uv",
        "sync",
        "--locked",
    ]


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


# --- imaging (suite2p) jobs: spock_imaging_env ------------------------------------------

IMAGING_REPO_DIR = "/mnt/cup/braininit/Shared/repos/AutomaticPipelineProcessing/imaging_processing/U19-pipeline-python"
JOB_FILE = "/params/suite2p_job_7.json"

# generate_slurm_spock_imaging's output on master before spock_imaging_env existed, but for
# the mail-user line, which comes from clusters_paths_and_transfers.slurm_dict_spock_imaging
CONDA_IMAGING_SLURM_TEXT = """#!/bin/bash
#SBATCH --job-name=job_id_7
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=12:00:00
#SBATCH --mem=64G
#SBATCH --gres=gpu:1
#SBATCH --mail-user={mail_user}
#SBATCH --mail-type=END
#SBATCH --output=/logs/job_id_7.log
#SBATCH --error=/errors/job_id_7.log

    echo "SLURM_JOB_ID: ${{SLURM_JOB_ID}}"
    echo "SLURM_SUBMIT_DIR: ${{SLURM_SUBMIT_DIR}}"
    echo "RECORDING_PROCESS_ID: ${{recording_process_id}}"
    echo "RAW_DATA_DIRECTORY: ${{raw_data_directory}}"
    echo "PROCESSED_DATA_DIRECTORY: ${{processed_data_directory}}"
    echo "REPOSITORY_DIR: ${{repository_dir}}"
    echo "PROCESS_SCRIPT_PATH: ${{process_script_path}}"
    echo "CUDA_VISIBLE_DEVICES: ${{CUDA_VISIBLE_DEVICES}}"
    nvidia-smi --query-gpu=name,driver_version,memory.used,memory.total --format=csv

    source ~/.bashrc
    module load anacondapy/2023.07-cuda -s

    conda activate u19_suite2p_env

    export suite2p_job_file='/params/suite2p_job_7.json'
    # CPU-side work (tiff reading, parts of detection) uses the cores slurm gave the job
    export OMP_NUM_THREADS=${{SLURM_CPUS_PER_TASK:-1}}
    export MKL_NUM_THREADS=${{SLURM_CPUS_PER_TASK:-1}}

    # Without the checkout the script path would resolve against the submit directory
    cd ${{repository_dir}} || {{ echo "repository_dir not found: ${{repository_dir}}" >&2; exit 1; }}
    python -u ${{process_script_path}}
    """


def conda_imaging_text(sc):
    return CONDA_IMAGING_SLURM_TEXT.format(
        mail_user=sc.ft.slurm_dict_spock_imaging["mail-user"]
    )


def imaging_text(sc, **kwargs):
    return sc.generate_slurm_spock_imaging(SLURM_DICT, JOB_FILE, **kwargs)


def test_imaging_env_is_conda_unless_configured(sc):
    assert sc.spock_imaging_env() == "conda"


@pytest.mark.parametrize(
    "value, env", [("uv", "uv"), (" UV\n", "uv"), ("Conda", "conda")]
)
def test_imaging_env_read_from_dj_config(sc, custom, value, env):
    custom["spock_imaging_env"] = value
    assert sc.spock_imaging_env() == env


@pytest.mark.parametrize("value", ["venv", "", None, 1])
def test_unknown_imaging_env_is_an_error(sc, custom, value):
    custom["spock_imaging_env"] = value
    with pytest.raises(ValueError, match="spock_imaging_env"):
        sc.spock_imaging_env()


def test_imaging_env_without_custom_section(sc, monkeypatch):
    import datajoint as dj

    monkeypatch.setitem(dj.config, "custom", None)
    assert sc.spock_imaging_env() == "conda"


def test_ephys_and_imaging_switches_are_independent(sc, custom):
    custom["spock_imaging_env"] = "uv"
    assert sc.spock_ephys_env() == "conda"
    custom["spock_imaging_env"] = "conda"
    custom["spock_ephys_env"] = "uv"
    assert sc.spock_imaging_env() == "conda"


@pytest.mark.parametrize(
    "params, ephys_env, imaging_env, uses_uv",
    [
        (IMAGING_SPOCK, "conda", "uv", True),
        (IMAGING_SPOCK, "uv", "conda", False),
        (IMAGING_SPOCK, "uv", None, False),
        (EPHYS_SPOCK, "conda", "uv", False),
        (dict(IMAGING_SPOCK, process_cluster="tiger"), "uv", "uv", False),
    ],
)
def test_only_spock_imaging_jobs_follow_the_imaging_switch(
    sc, custom, params, ephys_env, imaging_env, uses_uv
):
    custom["spock_ephys_env"] = ephys_env
    if imaging_env:
        custom["spock_imaging_env"] = imaging_env
    assert sc.imaging_job_uses_uv(params) is uses_uv
    assert sc.ephys_job_uses_uv(params) is (params is EPHYS_SPOCK and ephys_env == "uv")


# the conda script, byte for byte


def test_conda_imaging_script_is_unchanged(sc):
    assert imaging_text(sc) == conda_imaging_text(sc)


@pytest.mark.parametrize(
    "setting",
    [
        {"spock_imaging_env": "conda"},
        {"spock_ephys_env": "uv"},
        {"spock_uv_cache_dir": "/mnt/bucket/scratch/u19prod/uv_cache"},
    ],
)
def test_conda_imaging_script_is_unchanged_with(sc, custom, setting):
    custom.update(setting)
    assert imaging_text(sc) == conda_imaging_text(sc)


def test_imaging_python_env_argument_overrides_the_config(sc, custom):
    custom["spock_imaging_env"] = "uv"
    assert imaging_text(sc, python_env="conda") == conda_imaging_text(sc)


# the uv script


@pytest.fixture
def uv_imaging(sc, custom):
    custom["spock_imaging_env"] = "uv"
    return imaging_text(sc)


def test_uv_imaging_script_keeps_the_slurm_header(sc, uv_imaging):
    def header(text):
        return [line for line in text.splitlines() if line.startswith("#")]

    assert header(uv_imaging) == header(conda_imaging_text(sc))


def test_uv_imaging_script_runs_from_the_venv_without_conda(uv_imaging):
    commands = body_commands(uv_imaging)

    assert not any("conda" in command for command in commands)
    # torch brings its own CUDA libraries: no module at all
    assert not any(command.startswith("module ") for command in commands)
    assert "source ~/.bashrc" in commands

    cd = next(i for i, c in enumerate(commands) if c.startswith("cd ${repository_dir}"))
    which = next(i for i, c in enumerate(commands) if c.startswith("command -v uv "))
    check = next(i for i, c in enumerate(commands) if c.startswith("uv sync "))
    activate = commands.index("source .venv/bin/activate")
    run = commands.index("python -u ${process_script_path}")
    assert commands.index("source ~/.bashrc") < cd < which < check < activate < run
    assert run == len(commands) - 1  # the job's exit code is suite2p_slurm_job's


def test_uv_imaging_script_keeps_the_job_settings(sc, uv_imaging):
    """Everything but how python is found is as in the conda script"""
    commands = body_commands(uv_imaging)
    conda_commands = body_commands(conda_imaging_text(sc))

    for command in conda_commands:
        if command.startswith(("echo ", "nvidia-smi ", "export ")):
            assert command in commands
    assert "export suite2p_job_file='" + JOB_FILE + "'" in commands


def test_uv_imaging_script_checks_the_pipeline_extra_offline(uv_imaging):
    commands = body_commands(uv_imaging)

    (check,) = [c for c in commands if c.startswith("uv ")]
    # the same extra the login-node sync installs, or --check would want to remove suite2p
    assert check.startswith("uv sync --frozen --offline --check --extra pipeline || {")
    assert "uv sync --locked --extra pipeline" in check  # what to run, in the message
    assert check.endswith("exit 1; }")
    assert "uv run" not in " ".join(commands)


def test_uv_imaging_script_stops_without_the_checkout_or_uv(uv_imaging):
    commands = body_commands(uv_imaging)

    (cd,) = [c for c in commands if c.startswith("cd ")]
    assert cd.endswith("exit 1; }")
    (which,) = [c for c in commands if c.startswith("command -v uv ")]
    assert "uv is not on PATH" in which and which.endswith("exit 1; }")


def test_uv_imaging_script_uses_the_default_cache_unless_configured(uv_imaging):
    assert "UV_CACHE_DIR" not in uv_imaging


def test_uv_imaging_script_checks_against_the_shared_cache(sc, custom):
    custom["spock_imaging_env"] = "uv"
    custom["spock_uv_cache_dir"] = SHARED_CACHE
    commands = body_commands(imaging_text(sc))

    export = commands.index("export UV_CACHE_DIR=" + SHARED_CACHE)
    check = commands.index(next(c for c in commands if c.startswith("uv sync ")))
    assert commands.index("umask 002") < export < check


def test_ephys_uv_script_checks_without_extras(sc, custom):
    custom["spock_ephys_env"] = "uv"
    commands = body_commands(sc.generate_slurm_spock(SLURM_DICT))

    (check,) = [c for c in commands if c.startswith("uv ")]
    assert "--extra" not in check


# generate_slurm_file, as the handler calls it


@pytest.fixture
def written(sc, monkeypatch):
    """Slurm files generate_slurm_file writes; nothing is transferred"""
    files = {}
    monkeypatch.setattr(
        sc.config, "default_suite2p_job_filename", "suite2p_job_%s.json", raising=False
    )
    monkeypatch.setattr(sc, "write_file", lambda path, text: files.update({path: text}))
    monkeypatch.setattr(sc, "transfer_slurm_file", lambda *args: 0)
    return files


@pytest.mark.parametrize("env", [None, "conda", "uv"])
def test_generate_slurm_file_follows_the_imaging_switch(sc, custom, written, env):
    if env:
        custom["spock_imaging_env"] = env
    status, _ = sc.generate_slurm_file(7, IMAGING_SPOCK)

    assert status == 0
    (text,) = written.values()
    assert ("conda activate u19_suite2p_env" in text) is (env != "uv")
    assert ("uv sync --frozen --offline --check --extra pipeline" in text) is (
        env == "uv"
    )


# the login-node sync and sbatch


def test_sync_installs_the_extras(sc, runs):
    assert sc.sync_uv_env(CLUSTER_VARS, IMAGING_REPO_DIR, extras=("pipeline",)) == (
        0,
        "",
    )

    (script,) = shlex.split(" ".join(runs.commands[0][2:]))[2:]
    assert script == "cd " + IMAGING_REPO_DIR + " && uv sync --locked --extra pipeline"


def test_sync_failure_names_the_command_with_extras(sc, runs):
    runs.result = completed(1, stderr="error: boom")
    status, message = sc.sync_uv_env(CLUSTER_VARS, IMAGING_REPO_DIR, ("pipeline",))

    assert status == 1
    assert message == (
        "uv sync --locked --extra pipeline failed in "
        + IMAGING_REPO_DIR
        + ": error: boom"
    )


def test_queue_syncs_the_imaging_env_before_sbatch(sc, custom, runs, sbatch):
    custom["spock_imaging_env"] = "uv"
    assert queue(sc, IMAGING_SPOCK) == (0, "123", "")

    (sync,) = runs.commands
    assert "cd " + IMAGING_REPO_DIR + " && uv sync --locked --extra pipeline" in sync[2]
    assert len(sbatch) == 1 and "sbatch" in sbatch[0]


def test_queue_does_not_submit_imaging_when_the_sync_fails(sc, custom, runs, sbatch):
    custom["spock_imaging_env"] = "uv"
    runs.result = completed(2, stderr="error: Failed to download `torch==2.14.0`")
    status, slurm_jobid, message = queue(sc, IMAGING_SPOCK)

    assert status == 2
    assert slurm_jobid == -1
    assert "Failed to download" in message
    assert sbatch == []


@pytest.mark.parametrize(
    "params, setting",
    [
        (IMAGING_SPOCK, {}),
        (IMAGING_SPOCK, {"spock_imaging_env": "conda", "spock_ephys_env": "uv"}),
        (EPHYS_SPOCK, {"spock_imaging_env": "uv"}),
    ],
)
def test_queue_does_not_sync_for_conda_imaging_jobs(
    sc, custom, runs, sbatch, params, setting
):
    custom.update(setting)
    assert queue(sc, params) == (0, "123", "")

    assert runs.commands == []
    assert len(sbatch) == 1
