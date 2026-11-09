"""Tests of the suite2p slurm job's output check against the real suite2p 1.x.

``test_suite2p_slurm_job.py`` replaces ``build_suite2p_inputs`` with a stand-in so it runs
without suite2p. Here nothing is replaced: the settings ``verify_outputs`` compares are
the ones suite2p 1.1.0 builds, and the end-to-end tests run suite2p on the CPU over a tiny
synthetic recording, then run ``main()`` again over its output (job 1405).

suite2p needs torch, so these skip where the ``pipeline`` extra (and torch) is missing.
"""

import json
import pathlib

import numpy as np
import pytest

pytest.importorskip("suite2p")
tifffile = pytest.importorskip("tifffile")
pytest.importorskip("element_calcium_imaging.suite2p_settings")

from element_calcium_imaging.suite2p_settings import (
    expected_plane_folders,
    suite2p_save_dir,
)

from u19_pipeline.automatic_job import suite2p_slurm_job as job_script

# torch warns about its own sparse tensors when suite2p detects ROIs; pytest turns warnings
# into errors (pyproject filterwarnings).
pytestmark = pytest.mark.filterwarnings(
    "ignore:Sparse (invariant checks|CSR tensor support):UserWarning"
)

PLANE_FILES = ("ops.npy", "iscell.npy", "F.npy", "settings.npy")
NPLANES = 2


def synthetic_movie(*, blobs=True, frames=440, size=64, seed=0):
    """
    A multi-page TIFF array, planes interleaved frame by frame (plane = frame % NPLANES).
    suite2p warns below 200 frames per plane (an error under our pytest filters).
    Each plane has three gaussian blobs that flicker; without blobs it is noise only.
    """
    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[:size, :size]
    centers = [
        [(16, 16), (44, 40), (20, 48)],
        [(32, 12), (12, 40), (48, 48)],
    ]
    movie = np.empty((frames, size, size), np.uint16)
    for t in range(frames):
        plane, step = t % NPLANES, t // NPLANES
        img = rng.normal(200, 10, (size, size))
        for k, (cy, cx) in enumerate(centers[plane]):
            on = np.sin(step * (0.5 + 0.3 * k) + k) > 0.3
            if blobs and on:
                img += 400 * np.exp(-((yy - cy) ** 2 + (xx - cx) ** 2) / (2 * 2.5**2))
        movie[t] = np.clip(img, 0, 65535)
    return movie


def make_job(tmp_path, *, nplanes=NPLANES, **params):
    """A job dict as build_suite2p_job writes it; one (empty) input file, not yet run."""
    raw = tmp_path / "raw"
    raw.mkdir()
    image_file = raw / "scan_00001.tif"
    image_file.touch()
    return dict(
        job_id=1405,
        params={"nplanes": nplanes, "fs": 10.0, "tau": 1.0, **params},
        image_files=[image_file.as_posix()],
        scan_info=dict(fs=10.0, nplanes=nplanes, nchannels=1),
        output_dir=(tmp_path / "job_id_1405" / "suite2p_output").as_posix(),
        torch_device="cpu",
    )


@pytest.fixture
def job(tmp_path):
    return make_job(tmp_path)


def write_complete_output(job):
    """Plane folders as a finished run of ``job`` leaves them (real settings.npy)."""
    db, settings = job_script.suite2p_inputs(job)
    planes = [suite2p_save_dir(db) / name for name in expected_plane_folders(db)]
    for plane in planes:
        plane.mkdir(parents=True)
        for f in PLANE_FILES:
            np.save(plane / f, settings if f == "settings.npy" else np.zeros(1))
    return planes


def test_real_settings_apply_job_torch_device(job):
    job["torch_device"] = "cuda"
    _, settings = job_script.suite2p_inputs(job)
    assert settings["torch_device"] == "cuda"


def test_real_inputs_have_one_plane_folder_per_plane(tmp_path):
    db, _ = job_script.suite2p_inputs(make_job(tmp_path, nplanes=3))
    assert expected_plane_folders(db) == ["plane0", "plane1", "plane2"]


def test_real_complete_output_has_no_problem(job):
    write_complete_output(job)
    assert job_script.output_problem(job) is None


def test_real_output_problem_without_output(job):
    assert "plane0: missing" in job_script.output_problem(job)


def test_real_output_problem_after_unfinished_rerun(job):
    planes = write_complete_output(job)
    for plane in planes:
        (plane / "iscell.npy").unlink()
        (plane / "F.npy").unlink()
    problem = job_script.output_problem(job)
    assert "iscell.npy" in problem
    assert "plane1" in problem


def test_real_output_problem_for_other_tau(job):
    write_complete_output(job)
    job["params"]["tau"] = 0.7
    assert "tau" in job_script.output_problem(job)


def test_real_output_problem_for_other_torch_device(job):
    write_complete_output(job)
    job["torch_device"] = "cuda"
    assert "torch_device" in job_script.output_problem(job)


def test_real_inputs_without_image_files(job):
    job["image_files"] = []
    with pytest.raises(FileNotFoundError):
        job_script.suite2p_inputs(job)


# End to end: real suite2p on the CPU over a synthetic recording.


@pytest.fixture
def e2e(tmp_path, monkeypatch):
    """
    A job over a synthetic movie, and ``run()`` -> number of times suite2p ran in main().
    Registration is off to keep the run short and the spatial scale is given, as suite2p
    warns when it fails to estimate it on a tiny movie.
    """
    job = make_job(tmp_path, diameter=6, spatial_scale=1, do_registration=False)
    job_file = tmp_path / "suite2p_job_1405.json"
    monkeypatch.setenv("suite2p_job_file", job_file.as_posix())
    monkeypatch.delenv("recording_process_id", raising=False)

    runs = []
    real_run_suite2p = job_script.run_suite2p

    def counting_run_suite2p(*args, **kwargs):
        runs.append(kwargs["output_dir"])
        return real_run_suite2p(*args, **kwargs)

    monkeypatch.setattr(job_script, "run_suite2p", counting_run_suite2p)

    def write_movie(movie):
        tifffile.imwrite(job["image_files"][0], movie)

    def run():
        """Write the job file from ``job`` as it is now, then run main()."""
        job_file.write_text(json.dumps(job))
        before = len(runs)
        job_script.main()
        return len(runs) - before

    return job, write_movie, run


def plane_folders(job):
    save_dir = pathlib.Path(job["output_dir"]) / "suite2p"
    return [save_dir / f"plane{i}" for i in range(job["scan_info"]["nplanes"])]


def snapshot(planes, names=("iscell.npy", "F.npy")):
    """(path, mtime_ns, size) of the given result files in every plane."""
    return {
        p / n: ((p / n).stat().st_mtime_ns, (p / n).stat().st_size)
        for p in planes
        for n in names
    }


def test_main_second_run_keeps_suite2p_output(e2e):
    """Job 1405: suite2p runs once; the same job again leaves every plane's results alone."""
    job, write_movie, run = e2e
    write_movie(synthetic_movie())

    assert run() == 1
    planes = plane_folders(job)
    assert all((p / f).exists() for p in planes for f in PLANE_FILES)
    assert job_script.output_problem(job) is None
    results = snapshot(planes)
    stat = {p: np.load(p / "stat.npy", allow_pickle=True) for p in planes}
    assert all(len(s) > 0 for s in stat.values())

    assert run() == 0
    assert snapshot(planes) == results
    job_script.require_complete_output(job)


def test_main_reruns_suite2p_when_a_plane_lost_its_results(e2e):
    job, write_movie, run = e2e
    write_movie(synthetic_movie())
    assert run() == 1
    planes = plane_folders(job)
    (planes[1] / "iscell.npy").unlink()
    assert "iscell.npy" in job_script.output_problem(job)

    assert run() == 1
    assert (planes[1] / "iscell.npy").exists()
    assert job_script.output_problem(job) is None
    assert run() == 0


def test_main_reruns_suite2p_for_other_settings(e2e):
    job, write_movie, run = e2e
    write_movie(synthetic_movie())
    assert run() == 1

    job["params"]["tau"] = 0.7
    assert "tau" in job_script.output_problem(job)
    assert run() == 1
    assert job_script.output_problem(job) is None


def test_main_without_rois_fails_and_is_run_again(e2e):
    """
    A recording with nothing to detect: suite2p raises (no iscell.npy is written), so the
    job fails and the output does not count as complete.
    """
    job, write_movie, run = e2e
    write_movie(synthetic_movie(blobs=False))

    with pytest.raises(ValueError, match="no ROIs"):
        run()
    assert "iscell.npy" in job_script.output_problem(job)
    with pytest.raises(ValueError, match="no ROIs"):
        run()


def test_main_without_image_files(e2e):
    job, _, run = e2e
    job["image_files"] = []
    with pytest.raises(FileNotFoundError):
        run()
