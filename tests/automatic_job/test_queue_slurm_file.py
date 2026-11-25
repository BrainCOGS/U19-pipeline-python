"""Tests for the sbatch command queue_slurm_file runs (#203).

From u19proc the command goes through ssh, and the remote shell parses it. On spock
(``is_this_spock()``) sbatch runs directly, with no shell: what the job gets in its
environment has to be the same both ways, with no quote characters.

``params_config`` (which declares DataJoint schemas) is stubbed, so the tests run
without a database, ssh or a cluster.
"""

import importlib
import shlex
import sys
import types

import pytest

pytest.importorskip("element_interface")  # comes from the "pipeline" extra

MODULES = (
    "u19_pipeline.automatic_job.slurm_creator",
    "u19_pipeline.automatic_job.clusters_paths_and_transfers",
)

SPOCK = "u19prod@spockmk2-loginvm.pni.princeton.edu"
REPO_DIR = "/mnt/cup/braininit/Shared/repos/AutomaticPipelineProcessing/electrophysiology_processing/BrainCogsEphysSorters"

EPHYS_SPOCK = {
    "recording_modality": "electrophysiology",
    "process_repository": "BrainCogsEphysSorters",
    "process_cluster": "spock",
    "process_script": "main_script.py",
}


@pytest.fixture
def sc(monkeypatch):
    dj = pytest.importorskip("datajoint")
    custom = dict(dj.config.get("custom") or {}, root_data_dir="/tmp/root")
    custom.pop("spock_ephys_env", None)
    custom.pop("spock_imaging_env", None)
    monkeypatch.setitem(dj.config, "custom", custom)
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


@pytest.fixture
def sbatch(sc, monkeypatch):
    """Record the commands queue_slurm_file starts; they answer with a job id"""
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


def queue(sc, raw="raw/dir", proc="proc/dir"):
    return sc.queue_slurm_file(
        7, EPHYS_SPOCK, raw, proc, "electrophysiology", "/slurm/f.slurm"
    )


def sbatch_words(command):
    """The words sbatch is started with: after the remote shell parses them over ssh"""
    if command[0] == "ssh":
        assert command[1] == SPOCK
        # ssh joins its arguments with spaces for the remote shell
        return shlex.split(" ".join(command[2:]))
    return command


def expected_words(raw="raw/dir", proc="proc/dir"):
    export = (
        "--export=recording_process_id=7"
        f",raw_data_directory={raw}"
        f",processed_data_directory={proc}"
        f",repository_dir={REPO_DIR}"
        ",process_script_path=main_script.py"
    )
    return ["sbatch", export, "/slurm/f.slurm"]


def test_sbatch_through_ssh(sc, sbatch):
    assert queue(sc) == (0, "123", "")

    (command,) = sbatch
    assert command[:2] == ["ssh", SPOCK]
    assert sbatch_words(command) == expected_words()


def test_sbatch_on_spock_gets_no_quotes(sc, sbatch, monkeypatch):
    monkeypatch.setattr(sc, "is_this_spock", lambda: True)

    assert queue(sc) == (0, "123", "")

    (command,) = sbatch
    assert command == expected_words()
    assert not any("'" in word or '"' in word for word in command)


@pytest.mark.parametrize("on_spock", [False, True])
def test_paths_with_spaces_stay_one_value(sc, sbatch, monkeypatch, on_spock):
    monkeypatch.setattr(sc, "is_this_spock", lambda: on_spock)

    queue(sc, raw="raw dir/a b", proc="proc dir/c")

    (command,) = sbatch
    assert sbatch_words(command) == expected_words("raw dir/a b", "proc dir/c")
