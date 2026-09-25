"""
Tests for frame_times_on_behavior_clock, the clock fit that turns I2C packets
into a behavior-clock timestamp for every imaging frame.

The fixtures build a synthetic session whose true imaging -> behavior mapping
is known, so the fit can be checked exactly. Late packets are modelled on what
the sample session shows: once per trial, the iteration that ends the trial does
its end-of-trial work *after* vr.timeElapsed is stamped and *before* the I2C
packet goes out, so that one packet reaches ScanImage 200-450 ms after the time
the behavior log records for it.
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from u19_pipeline.utils.imaging_behavior_sync import (
    behavior_page_window,
    frame_times_on_behavior_clock,
)

TRUE_SLOPE = 1.000005  # +5 ppm, the drift measured on the sample session
TRUE_OFFSET = -11.03  # imaging started ~11 s before behavior


def _session(
    n_trials=6,
    iters_per_trial=120,
    dt=0.012,
    iti=1.0,
    late_packets=(),
    lead_frames=0,
    n_blocks=1,
    trail_frames=0,
):
    """
    Synthetic sync dict + behavior log with one imaging frame per iteration.

    late_packets: iterable of (trial_index_0based, iteration_1based, delay_s).
    lead_frames: frames acquired before behavior starts (no packet), as in the
        real session's ~11 s of pre-behavior imaging.
    """
    blocks = [SimpleNamespace(trial=[]) for _ in range(n_blocks)]
    frame_time, sync_time, blk, tri, itr = [], [], [], [], []

    # Imaging frames before behavior: on the imaging clock, no packet.
    for k in range(lead_frames):
        frame_time.append(k * dt)
        sync_time.append(np.nan)
        blk.append(0)
        tri.append(0)
        itr.append(0)
    t0_img = lead_frames * dt

    t_behav = (t0_img * TRUE_SLOPE) + TRUE_OFFSET  # behavior time of first iteration
    late = {(t, i): d for t, i, d in late_packets}
    trial_global = 0
    for b in range(n_blocks):
        per_block = n_trials // n_blocks
        for t_in_block in range(per_block):
            times = np.arange(iters_per_trial) * dt
            trial = SimpleNamespace(start=t_behav, time=times)
            blocks[b].trial.append(trial)
            for i, tt in enumerate(times, start=1):
                behav = t_behav + tt
                img = (behav - TRUE_OFFSET) / TRUE_SLOPE
                frame_time.append(img)
                sync_time.append(img + late.get((trial_global, i), 0.0))
                blk.append(b + 1)
                tri.append(t_in_block + 1)
                itr.append(i)
            t_behav += times[-1] + iti
            trial_global += 1

    # Imaging frames after behavior ended: no packet.
    last = frame_time[-1]
    for k in range(1, trail_frames + 1):
        frame_time.append(last + k * dt)
        sync_time.append(np.nan)
        blk.append(0)
        tri.append(0)
        itr.append(0)

    f = SimpleNamespace(
        frame_time=np.asarray(frame_time),
        sync_time=np.asarray(sync_time),
    )
    sync = {
        "files": [f],
        "sync_behav_block_by_im_frame": np.asarray(blk),
        "sync_behav_trial_by_im_frame": np.asarray(tri),
        "sync_behav_iter_by_im_frame": np.asarray(itr),
    }
    log = SimpleNamespace(block=blocks if n_blocks > 1 else blocks[0])
    return sync, log


class TestCleanData:
    def test_recovers_the_true_mapping(self):
        sync, log = _session()
        ts, slope, offset, residual = frame_times_on_behavior_clock(sync, log)

        assert slope == pytest.approx(TRUE_SLOPE, abs=1e-9)
        assert offset == pytest.approx(TRUE_OFFSET, abs=1e-6)
        assert residual == pytest.approx(0.0, abs=1e-6)

    def test_zero_scatter_does_not_reject_everything(self):
        """Perfect data has zero MAD; an outlier threshold of k * 0 would
        reject every point. The fit must still use all of them."""
        sync, log = _session()
        ts, slope, offset, _ = frame_times_on_behavior_clock(sync, log)
        expected = TRUE_SLOPE * sync["files"][0].frame_time + TRUE_OFFSET
        np.testing.assert_allclose(ts, expected, atol=1e-6)


class TestLatePackets:
    """Regression: one late packet per trial must not bias the clock fit."""

    LATE = [(0, 90, 0.57), (1, 88, 0.23), (2, 95, 0.22), (3, 80, 0.45), (4, 91, 0.44)]

    def test_late_packets_do_not_bias_slope_or_offset(self):
        sync, log = _session(late_packets=self.LATE)
        _, slope, offset, _ = frame_times_on_behavior_clock(sync, log)

        # A 0.2-0.6 s outlier in a plain least-squares fit shifts these far
        # more than this; the robust fit should be essentially exact.
        assert (slope - TRUE_SLOPE) * 1e6 == pytest.approx(0.0, abs=0.5)  # ppm
        assert offset == pytest.approx(TRUE_OFFSET, abs=1e-3)

    def test_residual_reports_scatter_of_good_packets(self):
        sync, log = _session(late_packets=self.LATE)
        _, _, _, residual = frame_times_on_behavior_clock(sync, log)
        assert residual < 1e-3

    def test_timestamps_of_frames_with_late_packets_are_not_shifted(self):
        """Timestamps come from the fit, not from the late packet itself."""
        sync, log = _session(late_packets=self.LATE)
        ts, *_ = frame_times_on_behavior_clock(sync, log)
        expected = TRUE_SLOPE * sync["files"][0].frame_time + TRUE_OFFSET
        np.testing.assert_allclose(ts, expected, atol=1e-3)

    def test_single_late_packet_on_a_short_baseline(self):
        """File 1 of the sample session: ~3 trials and one 573 ms stall gave
        a +28 ppm slope with a plain fit."""
        sync, log = _session(n_trials=3, late_packets=[(0, 90, 0.573)])
        _, slope, _, _ = frame_times_on_behavior_clock(sync, log)
        assert abs(slope - TRUE_SLOPE) * 1e6 < 1.0


class TestCoverage:
    def test_frames_without_packets_still_get_timestamps(self):
        """Pre-behavior frames carry no packet; they are placed by the fit
        and come out negative on the behavior clock."""
        sync, log = _session(lead_frames=50)
        ts, *_ = frame_times_on_behavior_clock(sync, log)

        assert ts.size == sync["files"][0].frame_time.size
        assert np.all(np.isfinite(ts))
        assert ts[0] < 0
        assert np.all(np.diff(ts) > 0)

    def test_zero_and_out_of_range_indices_are_ignored(self):
        sync, log = _session()
        n = sync["sync_behav_iter_by_im_frame"].size
        # a frame whose packet decoded to zeros, and one past the trial's end
        sync["sync_behav_block_by_im_frame"][5] = 0
        sync["sync_behav_iter_by_im_frame"][7] = 10_000
        ts, slope, offset, _ = frame_times_on_behavior_clock(sync, log)

        assert ts.size == n
        assert slope == pytest.approx(TRUE_SLOPE, abs=1e-9)
        assert offset == pytest.approx(TRUE_OFFSET, abs=1e-6)

    def test_multiple_blocks_use_their_own_trials(self):
        sync, log = _session(n_trials=6, n_blocks=2)
        _, slope, offset, residual = frame_times_on_behavior_clock(sync, log)
        assert slope == pytest.approx(TRUE_SLOPE, abs=1e-9)
        assert offset == pytest.approx(TRUE_OFFSET, abs=1e-6)
        assert residual == pytest.approx(0.0, abs=1e-6)


class TestDegenerateInput:
    def test_fewer_than_two_synced_frames_raises(self):
        sync, log = _session()
        st = sync["files"][0].sync_time
        st[1:] = np.nan
        with pytest.raises(ValueError, match="Not enough synchronized frames"):
            frame_times_on_behavior_clock(sync, log)

    def test_no_synced_frames_raises(self):
        sync, log = _session()
        sync["files"][0].sync_time[:] = np.nan
        with pytest.raises(ValueError, match="Not enough synchronized frames"):
            frame_times_on_behavior_clock(sync, log)

    def test_exactly_two_synced_frames_fit_exactly(self):
        sync, log = _session()
        st = sync["files"][0].sync_time
        keep = [3, 200]
        mask = np.ones(st.size, bool)
        mask[keep] = False
        st[mask] = np.nan
        _, slope, offset, residual = frame_times_on_behavior_clock(sync, log)
        assert slope == pytest.approx(TRUE_SLOPE, abs=1e-8)
        assert offset == pytest.approx(TRUE_OFFSET, abs=1e-5)
        assert residual == pytest.approx(0.0, abs=1e-6)

    def test_three_points_with_one_outlier_does_not_crash(self):
        """Rejection must never leave fewer than two points to fit."""
        sync, log = _session(late_packets=[(0, 50, 0.4)])
        st = sync["files"][0].sync_time
        keep = [10, 49, 300]  # frame 49 is iteration 50 of trial 0: the late one
        mask = np.ones(st.size, bool)
        mask[keep] = False
        st[mask] = np.nan
        ts, slope, offset, residual = frame_times_on_behavior_clock(sync, log)
        assert np.all(np.isfinite(ts))
        assert np.isfinite(slope) and np.isfinite(offset) and np.isfinite(residual)


class TestBehaviorPageWindow:
    """
    Frames outside the recorded behavior cannot be tied to the experiment, so
    the export keeps only the pages from the one containing the first logged
    iteration to the one containing the last. The window is found by time on
    the fitted clock, not by where packets landed: the first packet of a trial
    is routinely late (trial setup runs between stamping the time and sending
    it), which would otherwise push the start a frame or two too late.
    """

    @staticmethod
    def _drop_leading(sync, n):
        f = sync["files"][0]
        f.frame_time, f.sync_time = f.frame_time[n:], f.sync_time[n:]
        for k in ("block", "trial", "iter"):
            key = f"sync_behav_{k}_by_im_frame"
            sync[key] = sync[key][n:]

    def test_leading_and_trailing_frames_are_outside(self):
        sync, log = _session(lead_frames=50, trail_frames=30)
        n = sync["files"][0].frame_time.size
        assert behavior_page_window(sync, log) == (50, n - 31)

    def test_no_padding_means_every_page(self):
        sync, log = _session()
        n = sync["files"][0].frame_time.size
        assert behavior_page_window(sync, log) == (0, n - 1)

    def test_late_first_packet_does_not_move_the_start(self):
        """Regression: the window started where trial 1's late packet landed."""
        sync, log = _session(lead_frames=50)
        sync["files"][0].sync_time[50:52] = np.nan  # first packet two frames late
        assert behavior_page_window(sync, log)[0] == 50

    def test_packets_for_trials_missing_from_the_log_are_outside(self):
        """An aborted final trial is stamped in the TIFF but absent from the log."""
        sync, log = _session(trail_frames=30)
        n = sync["files"][0].frame_time.size
        f = sync["files"][0]
        f.sync_time[-30:] = f.frame_time[-30:]
        sync["sync_behav_block_by_im_frame"][-30:] = 1
        sync["sync_behav_trial_by_im_frame"][-30:] = 99
        sync["sync_behav_iter_by_im_frame"][-30:] = np.arange(1, 31)
        assert behavior_page_window(sync, log) == (0, n - 31)

    def test_packetless_gaps_inside_the_window_are_kept(self):
        """End-of-trial stalls leave frames without packets mid-session."""
        sync, log = _session()
        sync["files"][0].sync_time[100:120] = np.nan
        n = sync["files"][0].frame_time.size
        assert behavior_page_window(sync, log) == (0, n - 1)

    def test_imaging_starting_after_behavior_starts_at_page_zero(self):
        sync, log = _session()
        self._drop_leading(sync, 10)
        assert behavior_page_window(sync, log)[0] == 0

    def test_no_synchronized_frames_raises(self):
        sync, log = _session()
        sync["files"][0].sync_time[:] = np.nan
        with pytest.raises(ValueError, match="synchroniz"):
            behavior_page_window(sync, log)
