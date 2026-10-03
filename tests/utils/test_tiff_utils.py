"""Tests for ``u19_pipeline.utils.tiff_utils.get_recording_info``.

``BehavFrames`` holds one entry per imaging frame, parsed from the ScanImage
``I2CData`` field: ``[]`` when a frame carries no behaviour sync, otherwise
whatever the parser produced (currently ``np.nan``, or a parsed value).
"""

import numpy as np
import pytest

pytest.importorskip("tifffile")  # tiff_utils imports come from the "pipeline" extra
pytest.importorskip("sklearn")

from u19_pipeline.utils import tiff_utils as tu  # noqa: E402

FRAME_RATE = 10.0


def make_file(behav_frames, ts_start=0.0):
    """Return (imheader, parsed_info) for one tiff with one entry per frame."""
    n = len(behav_frames)
    parsed_info = {
        "frameRate": FRAME_RATE,
        "Timing": {
            "Frame_ts_sec": ts_start + np.arange(n) / FRAME_RATE,
            "BehavFrames": list(behav_frames),
        },
    }
    return [None] * n, parsed_info


def run(files):
    imheader = [f[0] for f in files]
    parsed_info = [f[1] for f in files]
    fl = [f"file_{i:05d}.tif" for i in range(len(files))]
    return tu.get_recording_info(fl, imheader, parsed_info)


NO_SYNC = []
SYNC = np.nan  # what the parser currently stores for frames with I2C data


def assert_one_entry_per_frame(rec_info):
    behav = rec_info["Timing"]["BehavFrames"]
    assert behav.ndim == 1
    assert behav.dtype == object
    assert len(behav) == rec_info["nFrames"]


class TestBehavFramesConcatenation:
    def test_first_file_without_sync_then_file_with_sync(self):
        # Regression: recording 856 (ef932, 2026-09-25). The first file had no I2C
        # sync on any frame, so np.array([[], ...], dtype=object) became 2-D (n, 0)
        # and concatenating the next (1-D) file raised ValueError.
        files = [
            make_file([NO_SYNC] * 4),
            make_file([NO_SYNC, SYNC, SYNC], ts_start=0.4),
        ]
        rec_info, frames_per_file = run(files)
        assert_one_entry_per_frame(rec_info)
        assert rec_info["nFrames"] == 7
        assert list(frames_per_file) == [4, 3]

    def test_later_file_without_sync_keeps_one_entry_per_frame(self):
        files = [
            make_file([SYNC, NO_SYNC, SYNC]),
            make_file([NO_SYNC] * 5, ts_start=0.3),
        ]
        rec_info, _ = run(files)
        assert_one_entry_per_frame(rec_info)
        assert rec_info["nFrames"] == 8

    def test_no_sync_in_any_file(self):
        files = [make_file([NO_SYNC] * 3), make_file([NO_SYNC] * 2, ts_start=0.3)]
        rec_info, _ = run(files)
        assert_one_entry_per_frame(rec_info)
        assert all(
            isinstance(x, list) and not x for x in rec_info["Timing"]["BehavFrames"]
        )

    def test_equal_length_parsed_entries_are_not_turned_into_2d(self):
        # If every frame of a file parses to a list of the same length, np.array
        # would build a 2-D array; each frame must stay a single entry.
        files = [
            make_file([[1.5, [1, 0, 1]], [1.6, [1, 0, 2]]]),
            make_file([[1.7, [1, 0, 3]]], ts_start=0.2),
        ]
        rec_info, _ = run(files)
        assert_one_entry_per_frame(rec_info)
        assert rec_info["Timing"]["BehavFrames"][2] == [1.7, [1, 0, 3]]

    def test_single_frame_later_file(self):
        # np.squeeze on a 1-frame file gives a 0-d array, which cannot be concatenated.
        files = [make_file([SYNC, SYNC]), make_file([SYNC], ts_start=0.2)]
        rec_info, _ = run(files)
        assert_one_entry_per_frame(rec_info)
        assert rec_info["nFrames"] == 3

    def test_single_file(self):
        rec_info, frames_per_file = run([make_file([NO_SYNC] * 3)])
        assert_one_entry_per_frame(rec_info)
        assert list(frames_per_file) == [3]

    def test_none_and_nan_entries_are_preserved(self):
        files = [make_file([None, SYNC]), make_file([NO_SYNC], ts_start=0.2)]
        rec_info, _ = run(files)
        behav = rec_info["Timing"]["BehavFrames"]
        assert behav[0] is None
        assert np.isnan(behav[1])
        assert behav[2] == []


class TestZeroFrameFiles:
    def test_later_file_with_zero_frames(self):
        files = [make_file([SYNC, SYNC]), make_file([], ts_start=0.2)]
        rec_info, frames_per_file = run(files)
        assert_one_entry_per_frame(rec_info)
        assert rec_info["nFrames"] == 2
        assert list(frames_per_file) == [2, 0]


class TestFrameTimestamps:
    def test_file_restarting_at_zero_is_offset_after_previous_file(self):
        files = [make_file([SYNC] * 3), make_file([SYNC] * 2, ts_start=0.0)]
        rec_info, _ = run(files)
        ts = rec_info["Timing"]["Frame_ts_sec"]
        np.testing.assert_allclose(ts, [0.0, 0.1, 0.2, 0.3, 0.4])

    def test_file_with_continuing_timestamps_is_unchanged(self):
        files = [make_file([SYNC] * 3), make_file([SYNC] * 2, ts_start=0.3)]
        rec_info, _ = run(files)
        np.testing.assert_allclose(
            rec_info["Timing"]["Frame_ts_sec"], [0.0, 0.1, 0.2, 0.3, 0.4]
        )
