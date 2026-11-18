"""Tests for the suite2p slurm job's handling of output a previous run left behind.

Job 1405: a second suite2p run started over complete output, deleted every plane's
stat/F/iscell (suite2p 1.1.0 does that first when it finds existing binaries) and did
not finish. The stale ``combined`` folder was left, and Segmentation then failed on
``plane0/iscell.npy``. No suite2p run happens here: ``run_suite2p`` is faked and the
output folders are written by hand.

suite2p itself is not imported (it needs torch, which the CI pipeline leg leaves out):
``build_suite2p_inputs`` is replaced by a stand-in that returns the ``db`` keys
``verify_outputs`` reads and the stored params as settings.
``test_suite2p_slurm_job_real.py`` runs the same checks against the real suite2p.

Job 1480: the last tiff of a recording was cut off inside its last frame, and suite2p
failed on it after converting all the others ("Got unrecognized element type in a tiff
tag."). The input tiffs are now checked before suite2p runs.
"""

import json
import pathlib
import struct

import numpy as np
import pytest

tifffile = pytest.importorskip("tifffile")
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


def write_tiff(path, frames=3, size=8, **kwargs):
    """A multi-page int16 BigTIFF like the ScanImage ones (one IFD per frame)."""
    movie = np.arange(frames * size * size, dtype=np.int16).reshape(frames, size, size)
    tifffile.imwrite(
        path, movie, **{"bigtiff": True, "photometric": "minisblack", **kwargs}
    )
    return path


def write_scanimage_tiff(path, frames=3, size=8, closed=True):
    """
    A BigTIFF laid out as ScanImage writes it: each frame's header (IFD) and then its
    pixels, the header pointing to the next frame's header right after them.
    tifffile writes the pixels first, so cutting it off never ends inside a frame.
    Until ScanImage closes the file, the last header also points past its pixels, to a
    frame not written yet (``closed=False``).
    """
    nbytes = size * size * 2
    tags = [  # (tag, type, value); type 3 = SHORT, 16 = LONG8
        (256, 3, size),  # ImageWidth
        (257, 3, size),  # ImageLength
        (258, 3, 16),  # BitsPerSample
        (259, 3, 1),  # Compression: none
        (262, 3, 1),  # PhotometricInterpretation: min is black
        (273, 16, None),  # StripOffsets: filled in per frame
        (277, 3, 1),  # SamplesPerPixel
        (278, 3, size),  # RowsPerStrip
        (279, 16, nbytes),  # StripByteCounts
        (339, 3, 2),  # SampleFormat: signed int
    ]
    ifd_size = 8 + 20 * len(tags) + 8
    out = bytearray(b"II" + struct.pack("<HHHQ", 43, 8, 0, 16))
    for i in range(frames):
        ifd = len(out)
        data = ifd + ifd_size
        next_ifd = data + nbytes if i < frames - 1 or not closed else 0
        out += struct.pack("<Q", len(tags))
        for tag, typ, value in tags:
            value = data if value is None else value
            fmt = "<HHQQ" if typ == 16 else "<HHQH6x"
            out += struct.pack(fmt, tag, typ, 1, value)
        out += struct.pack("<Q", next_ifd)
        out += np.full((size, size), i, "<i2").tobytes()
    path.write_bytes(bytes(out))
    return path


def truncate(path, nbytes):
    with open(path, "r+b") as f:
        f.truncate(nbytes)


def frame_layout(path):
    """[(ifd_offset, data_end)] per frame"""
    with tifffile.TiffFile(path) as tif:
        return [
            (
                p.offset,
                max(
                    o + c for o, c in zip(p.dataoffsets, p.databytecounts, strict=True)
                ),
            )
            for p in tif.pages
        ]


@pytest.fixture
def job(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    image_files = [raw / f"scan_{i:05d}.tif" for i in (1, 2)]
    for f in image_files:
        write_tiff(f)
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


# Job 1480: input tiffs damaged before suite2p runs


def test_tiff_problem_none_for_complete_tiff(tmp_path):
    assert job_script.tiff_problem(write_tiff(tmp_path / "a.tif")) is None


def test_scanimage_layout_reads_back(tmp_path):
    """The hand-written ScanImage-like tiff is valid: one frame per page, in order."""
    path = write_scanimage_tiff(tmp_path / "a.tif", frames=3)
    assert job_script.tiff_problem(path) is None
    assert tifffile.imread(path)[:, 0, 0].tolist() == [0, 1, 2]


@pytest.mark.parametrize(
    "kwargs",
    [dict(frames=1), dict(frames=2000, size=4), dict(bigtiff=False)],
    ids=["one frame", "many frames", "classic tiff"],
)
def test_tiff_problem_none_for_complete_tiffs(tmp_path, kwargs):
    assert job_script.tiff_problem(write_tiff(tmp_path / "a.tif", **kwargs)) is None


def test_tiff_problem_for_last_frame_cut_off(tmp_path):
    """
    Job 1480: the file ends inside its last frame's pixels; the frame before points to its
    header, so it is still listed (ef505_act133_11182026_mainrecording_00001_00035.tif:
    1415 frames, frame 1415 ends at byte 745260708 of a 744762740-byte file).
    """
    path = write_scanimage_tiff(tmp_path / "scan_00035.tif", frames=3, closed=False)
    _, data_end = frame_layout(path)[-1]
    truncate(path, data_end - 10)
    problem = job_script.tiff_problem(path)
    assert problem is not None
    assert "truncated: frame 3 ends" in problem
    assert "2 complete frames" in problem


def test_tiff_problem_for_next_frame_past_end(tmp_path):
    """The file ends right before a frame header that the previous one points to."""
    path = write_tiff(tmp_path / "scan_00035.tif", frames=3)
    ifd_offset, _ = frame_layout(path)[2]
    truncate(path, ifd_offset)
    problem = job_script.tiff_problem(path)
    assert problem is not None
    assert "frame 3 starts" in problem
    assert "2 complete frames" in problem


@pytest.mark.parametrize("content", [b"", b"not a tiff"], ids=["empty", "not a tiff"])
def test_tiff_problem_for_unreadable_file(tmp_path, content):
    path = tmp_path / "scan_00001.tif"
    path.write_bytes(content)
    assert "cannot be read" in job_script.tiff_problem(path)


def test_damaged_tiffs_lists_only_damaged_tiffs(tmp_path):
    good = write_tiff(tmp_path / "scan_00001.tif")
    bad = write_tiff(tmp_path / "scan_00002.TIFF")
    truncate(bad, frame_layout(bad)[-1][1] - 1)
    other = tmp_path / "scan.h5"
    other.write_bytes(b"")
    damaged = job_script.damaged_tiffs(
        [good.as_posix(), bad.as_posix(), other.as_posix()]
    )
    assert list(damaged) == [bad.as_posix()]


def test_damaged_tiffs_without_files():
    assert job_script.damaged_tiffs([]) == {}


def test_main_fails_on_damaged_tiff_before_suite2p(job, run_main):
    """Job 1480: a clear error naming the file, before suite2p reads any of them."""
    last = write_scanimage_tiff(
        pathlib.Path(job["image_files"][-1]), frames=3, closed=False
    )
    truncate(last, frame_layout(last)[-1][1] - 10)
    with pytest.raises(RuntimeError) as e:
        run_main()
    message = str(e.value)
    assert "1 of 2 input tiffs damaged" in message
    # the pipeline keeps the last 255 characters
    assert "scan_00002.tif" in message[-255:]
    assert "complete frames" in message[-255:]
    assert not (pathlib.Path(job["output_dir"])).exists()


def test_main_runs_with_complete_tiffs(run_main):
    assert len(run_main()) == 1


def test_main_skips_damaged_tiff_check_for_complete_output(job, run_main):
    """Output already complete: nothing to read, so a damaged input does not matter."""
    write_complete_output(job)
    for f in job["image_files"]:
        truncate(f, 0)
    assert run_main() == []
