"""Tests for the suite2p slurm job's handling of output a previous run left behind.

Job 1405: a second suite2p run started over complete output, deleted every plane's
stat/F/iscell (suite2p 1.1.0 does that first when it finds existing binaries) and did
not finish. The stale ``combined`` folder was left, and Segmentation then failed on
``plane0/iscell.npy``. No suite2p run happens here: ``run_suite2p`` is faked and the
output folders are written by hand.

suite2p itself is not imported (it needs torch, which the CI pipeline leg leaves out):
``build_suite2p_inputs`` is replaced by a stand-in that returns the ``db`` keys
``verify_outputs`` reads and the stored params as settings.
"""

import json

import numpy as np
import pytest

pytest.importorskip("element_calcium_imaging.suite2p_settings")

from element_calcium_imaging.suite2p_settings import (
    expected_plane_folders,
    suite2p_save_dir,
)

from u19_pipeline.automatic_job import suite2p_slurm_job as job_script

PLANE_FILES = ("ops.npy", "iscell.npy", "F.npy", "settings.npy")


@pytest.fixture(autouse=True)
def build_calls(monkeypatch):
    """Stand-in for build_suite2p_inputs; records the arguments it was called with."""
    calls = []

    def fake_build_suite2p_inputs(params, *, save_path0, scan_info, **kwargs):
        calls.append(
            dict(params=params, save_path0=save_path0, scan_info=scan_info, **kwargs)
        )
        db = dict(
            save_path0=save_path0, save_folder="suite2p", nplanes=scan_info["nplanes"]
        )
        settings = {**params, "torch_device": "cpu"}
        return db, settings, []

    monkeypatch.setattr(job_script, "build_suite2p_inputs", fake_build_suite2p_inputs)
    return calls


@pytest.fixture
def job(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    image_files = [raw / f"scan_{i:05d}.tif" for i in (1, 2)]
    for f in image_files:
        f.touch()
    return dict(
        job_id=1405,
        params={"tau": 1.0},
        image_files=[f.as_posix() for f in image_files],
        scan_info=dict(fs=15.0, nplanes=3, nchannels=1),
        output_dir=(tmp_path / "job_id_1405" / "suite2p_output").as_posix(),
        torch_device="cuda",
    )


def write_complete_output(job):
    """Plane folders as a finished run of ``job`` leaves them."""
    db, settings = job_script.suite2p_inputs(job)
    save_dir = suite2p_save_dir(db)
    planes = [save_dir / name for name in expected_plane_folders(db)]
    for plane in planes:
        plane.mkdir(parents=True)
        for f in PLANE_FILES:
            np.save(plane / f, settings if f == "settings.npy" else np.zeros(1))
    return planes


def remove_detection_outputs(planes):
    """What suite2p's rerun cleanup leaves before it gets to detection (job 1405)."""
    for plane in planes:
        for f in ("iscell.npy", "F.npy"):
            (plane / f).unlink()


def test_suite2p_inputs_apply_job_torch_device(job):
    _, settings = job_script.suite2p_inputs(job)
    assert settings["torch_device"] == "cuda"


def test_suite2p_inputs_match_run_suite2p(job, tmp_path, build_calls):
    """Same build_suite2p_inputs arguments as run_suite2p, so the check sees the same run."""
    job_script.suite2p_inputs(job)
    assert build_calls == [
        dict(
            params=job["params"],
            data_path=(tmp_path / "raw").as_posix(),
            file_list=job["image_files"],
            save_path0=job["output_dir"],
            input_format="tif",
            scan_info=job["scan_info"],
        )
    ]


def test_suite2p_inputs_without_image_files(job):
    job["image_files"] = []
    with pytest.raises(FileNotFoundError):
        job_script.suite2p_inputs(job)


def test_output_problem_none_for_complete_output(job):
    write_complete_output(job)
    assert job_script.output_problem(job) is None


def test_output_problem_without_output(job):
    problem = job_script.output_problem(job)
    assert problem is not None
    assert "plane0: missing" in problem


def test_output_problem_after_unfinished_rerun(job):
    remove_detection_outputs(write_complete_output(job))
    problem = job_script.output_problem(job)
    assert "iscell.npy" in problem
    assert "plane2" in problem


def test_output_problem_for_other_settings(job):
    write_complete_output(job)
    job["params"] = {"tau": 0.7}
    problem = job_script.output_problem(job)
    assert "tau" in problem


def test_require_complete_output_passes(job):
    write_complete_output(job)
    job_script.require_complete_output(job)


def test_require_complete_output_explains_unfinished_rerun(job):
    remove_detection_outputs(write_complete_output(job))
    with pytest.raises(RuntimeError, match="iscell.npy") as e:
        job_script.require_complete_output(job)
    assert job["output_dir"] in str(e.value)


@pytest.fixture
def run_main(job, tmp_path, monkeypatch):
    job_file = tmp_path / "suite2p_job_1405.json"
    monkeypatch.setenv("suite2p_job_file", job_file.as_posix())
    monkeypatch.delenv("recording_process_id", raising=False)
    calls = []
    monkeypatch.setattr(
        job_script, "run_suite2p", lambda *args, **kwargs: calls.append(kwargs)
    )

    def run():
        job_file.write_text(json.dumps(job))
        job_script.main()
        return calls

    return run


def test_main_skips_complete_output(job, run_main):
    planes = write_complete_output(job)
    assert run_main() == []
    assert all((p / "iscell.npy").exists() for p in planes)


def test_main_runs_without_output(run_main):
    assert len(run_main()) == 1


def test_main_reruns_incomplete_output(job, run_main):
    remove_detection_outputs(write_complete_output(job))
    assert len(run_main()) == 1


def test_main_reruns_for_other_settings(job, run_main):
    write_complete_output(job)
    job["params"] = {"tau": 0.7}
    assert len(run_main()) == 1
