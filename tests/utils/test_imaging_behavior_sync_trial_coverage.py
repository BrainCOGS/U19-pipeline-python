"""
Trials only partly covered by imaging must not get a frame span.

A researcher stopped imaging before the behavior ended. The trial running when
imaging stopped got a normal-looking frame span (on the sample session, 51 of
its 676 iterations were imaged), so it would be analysed as an imaged trial.
The same happens when imaging starts mid-trial or a trial straddles the gap
between two acquisitions. Those trials, like trials with no imaging at all, get
an empty span and zero per-frame block/trial/iteration labels.

The synthetic session has one frame per behavior iteration, with the imaging
and behavior clocks equal, so frame ``k`` carries the packet of iteration ``k``.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import numpy as np
import pytest

from u19_pipeline.utils import imaging_behavior_sync as ibs

DT = 0.02
EPOCH = datetime(2026, 8, 7, 10, 0, 0)
EMPTY = (0, 2)


def _log(n_trials=(5,), iters=50):
    """Behavior log: blocks of back-to-back trials, ``iters`` iterations each."""
    blocks, t0 = [], 0.0
    for n in n_trials:
        trials = []
        for _ in range(n):
            trials.append(SimpleNamespace(start=t0, time=np.arange(iters) * DT))
            t0 += iters * DT
        blocks.append(
            SimpleNamespace(trial=trials, start=[2026, 8, 7, 10, 0, len(blocks)])
        )
    return SimpleNamespace(block=blocks if len(blocks) > 1 else blocks[0])


def _frames(log, lead=0, trail=0):
    """Per-frame (time, sync_time, block, trial, iteration) for the whole log."""
    rows = [(-(lead - k) * DT, np.nan, 0, 0, 0) for k in range(lead)]
    for b, blk in enumerate(ibs._as_list(log.block), start=1):
        for t, trial in enumerate(ibs._as_list(blk.trial), start=1):
            for i, ti in enumerate(trial.time, start=1):
                rows.append((trial.start + ti, trial.start + ti, b, t, i))
    end = rows[-1][0]
    rows += [(end + k * DT, np.nan, 0, 0, 0) for k in range(1, trail + 1)]
    return np.array(rows, dtype=float)


def _file(rows, acquisition=1):
    return ibs.FileSyncInfo(
        movie_file=f"scan_{acquisition:05d}_00001.tif",
        acquisition=acquisition,
        epoch=EPOCH,
        frame_time=rows[:, 0].copy(),
        sync_time=rows[:, 1].copy(),
        block=rows[:, 2].astype(np.int64),
        trial=rows[:, 3].astype(np.int64),
        iteration=rows[:, 4].astype(np.int64),
    )


@pytest.fixture
def run_sync(monkeypatch):
    """Run sync_imaging_behavior on in-memory files instead of TIFFs."""

    def run(files, log):
        by_name = {f.movie_file: f for f in files}
        monkeypatch.setattr(ibs, "parse_scanimage_sync", lambda name: by_name[name])
        return ibs.sync_imaging_behavior(list(by_name), log)

    return run


def _spans(sync):
    return [s.shape for s in sync["sync_im_frame_span_by_behav_trial"]]


def _imaged(sync):
    return [s.size > 0 for s in sync["sync_im_frame_span_by_behav_trial"]]


def test_imaging_covering_the_whole_session_keeps_every_trial(run_sync):
    log = _log()
    sync = run_sync([_file(_frames(log, lead=30, trail=30))], log)
    assert _imaged(sync) == [True] * 5
    assert all(s.size for s in sync["sync_im_frame_span_by_behav_iter"])


def test_imaging_stopped_mid_trial_drops_that_trial_and_later_ones(run_sync):
    """The reported case: imaging ended 10 iterations into trial 3."""
    log = _log()
    rows = _frames(log, lead=30)[: 30 + 2 * 50 + 10]
    sync = run_sync([_file(rows)], log)
    assert _imaged(sync) == [True, True, False, False, False]
    assert _spans(sync)[2] == EMPTY
    assert sync["sync_im_frame_span_by_behav_iter"][2].shape == EMPTY
    assert np.count_nonzero(sync["sync_behav_trial_by_im_frame"] == 3) == 0


def test_imaging_started_mid_trial_drops_that_trial(run_sync):
    log = _log()
    rows = _frames(log, trail=30)[20:]
    sync = run_sync([_file(rows)], log)
    assert _imaged(sync) == [False, True, True, True, True]


def test_imaging_stopped_on_a_trial_boundary_keeps_the_last_trial(run_sync):
    log = _log()
    rows = _frames(log, lead=30)[: 30 + 2 * 50]
    sync = run_sync([_file(rows)], log)
    assert _imaged(sync) == [True, True, False, False, False]


def test_one_missing_frame_at_the_end_is_tolerated(run_sync):
    """Imaging stopped one iteration before trial 2 ended: within a frame."""
    log = _log()
    rows = _frames(log, lead=30)[: 30 + 2 * 50 - 1]
    sync = run_sync([_file(rows)], log)
    assert _imaged(sync)[:2] == [True, True]


def test_late_packets_do_not_drop_a_trial(run_sync):
    """End-of-trial packets reach ScanImage 200-450 ms late."""
    log = _log()
    rows = _frames(log, lead=30, trail=30)
    trial_ends = np.flatnonzero(rows[:, 4] == 50)
    rows[trial_ends, 1] += 0.4
    sync = run_sync([_file(rows)], log)
    assert _imaged(sync) == [True] * 5


def test_trial_with_a_single_imaged_frame_is_dropped(run_sync):
    log = _log()
    rows = _frames(log, lead=30)[: 30 + 50 + 1]
    sync = run_sync([_file(rows)], log)
    assert _imaged(sync) == [True, False, False, False, False]


def test_trial_straddling_two_acquisitions_is_dropped(run_sync):
    """Imaging was stopped and restarted during trial 3."""
    log = _log()
    rows = _frames(log, lead=30, trail=30)
    a = 30 + 2 * 50 + 10
    b = 30 + 2 * 50 + 40
    sync = run_sync([_file(rows[:a], 1), _file(rows[b:], 2)], log)
    assert _imaged(sync) == [True, True, False, True, True]


def test_imaging_ending_in_the_first_block_drops_the_rest(run_sync):
    log = _log(n_trials=(3, 2))
    rows = _frames(log, lead=30)[: 30 + 50 + 25]
    sync = run_sync([_file(rows)], log)
    assert _imaged(sync) == [True, False, False]
    assert sync["sync_im_frame_span_by_behav_block"][1].shape == EMPTY


def test_without_a_log_spans_are_kept(run_sync):
    """No behavior log: no iteration times to judge coverage with."""
    log = _log()
    rows = _frames(log, lead=30)[: 30 + 2 * 50 + 10]
    sync = run_sync([_file(rows)], None)
    assert _imaged(sync) == [True, True, True]


def test_trial_without_iteration_times_is_kept(run_sync):
    log = _log()
    rows = _frames(log, lead=30)[: 30 + 2 * 50 + 10]
    log.block.trial[2].time = np.array([])
    sync = run_sync([_file(rows)], log)
    assert _imaged(sync)[:3] == [True, True, True]


@pytest.mark.parametrize("case", ["stop", "start", "restart", "only_partial"])
def test_partial_trial_labels_and_block_spans_are_consistent(run_sync, case):
    log = _log()
    rows = _frames(log, lead=30, trail=30)
    if case == "stop":
        rows = rows[:140]
        files = [_file(rows)]
        dropped = [3]
    elif case == "start":
        rows = rows[50:]
        files = [_file(rows)]
        dropped = [1]
    elif case == "restart":
        files = [_file(rows[:140], 1), _file(rows[170:], 2)]
        rows = np.concatenate((rows[:140], rows[170:]))
        dropped = [3]
    else:
        rows = rows[50:60]
        files = [_file(rows)]
        dropped = [1]
    sync = run_sync(files, log)
    rejected = np.isin(rows[:, 3], dropped)
    for column, field in [(2, "block"), (3, "trial"), (4, "iter")]:
        expected = rows[:, column].astype(np.int64)
        expected[rejected] = 0
        np.testing.assert_array_equal(sync[f"sync_behav_{field}_by_im_frame"], expected)
    for trial in dropped:
        assert sync["sync_im_frame_span_by_behav_trial"][trial - 1].size == 0
        assert sync["sync_im_frame_span_by_behav_iter"][trial - 1].size == 0
    retained = np.flatnonzero(sync["sync_behav_block_by_im_frame"] == 1)
    span = sync["sync_im_frame_span_by_behav_block"][0]
    if retained.size:
        np.testing.assert_array_equal(span, [retained[0] + 1, retained[-1] + 1])
    else:
        assert span.size == 0
