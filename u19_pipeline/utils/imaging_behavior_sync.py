"""Python port of the MATLAB imaging<->behavior synchronization.

This reimplements, with tifffile instead of a libtiff MEX, the pipeline that
has been used since ~2018 to attach ViRMEn behavior coordinates to every
two-photon / mesoscope frame:

* ``getSyncInfo`` (U19-pipeline-matlab/utils/imagingSync/getSyncInfo.cpp):
  parses each ScanImage TIFF frame's ImageDescription tag and extracts
  ``acquisitionNumbers``, ``epoch``, ``frameTimestamps_sec`` and the first
  ``I2CData`` packet -> ported here as :func:`parse_scanimage_sync`.

* ``imaging_pipeline.SyncImagingBehavior.makeTuples``
  (U19-pipeline-matlab/schemas/+imaging_pipeline/SyncImagingBehavior.m):
  stitches the per-file sync streams together, forward-fills frames that
  received no I2C packet, sanity-checks against the behavior log and builds
  the per-frame block/trial/iteration vectors and the frame-span tables
  stored in ``u19_imaging_pipeline.SyncImagingBehavior``
  -> ported here as :func:`sync_imaging_behavior`.

The I2C payload is produced on the ViRMEn side by
``updateDAQSyncSignals([block, trial, iteration])`` (ViRMEn/experiments/
common/updateDAQSyncSignals.m), which bit-bangs the three values over two
NI-DAQ digital lines (ViRMEn/experiments/daq/nidaqI2C.cpp).  ScanImage
receives them on its I2C input and stamps each packet into the header of the
frame being acquired when it arrived.  Each value is a little-endian uint16,
so a packet is 6 bytes: ``[block, trial, iteration]``.

Output field names and index conventions (1-based frame indices, and the
iteration-span offset quirk) intentionally match the MATLAB code so results
are directly comparable with existing database entries.
"""

import re
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np

I2C_DTYPE = np.dtype('<u2')     # matches getSyncInfo(..., 'uint16') in MATLAB
I2C_VALUES_PER_PACKET = 3       # [block, trial, iteration]

# for assuming that frames with no sync data are actually within some
# abnormally long behavioral iteration (cfg.minBehaviorSecs in MATLAB)
MIN_BEHAVIOR_SECS = 2.0

_RE_ACQ = re.compile(r'acquisitionNumbers = (\S+)')
_RE_FRAME_TS = re.compile(r'frameTimestamps_sec = (\S+)')
_RE_EPOCH = re.compile(r'epoch = \[([^\]]*)\]')
_RE_I2C = re.compile(r'I2CData = (\{.*)')
_RE_I2C_PACKET = re.compile(r'\{([0-9.eE+-]+)\s*,\s*\[([^\]]*)\]\}')
_RE_FILE_ACQ = re.compile(r'_([0-9]+)_[0-9]+\.[^.]+$')


@dataclass
class FileSyncInfo:
    """Per-TIFF-file synchronization info (output of ``getSyncInfo``)."""

    movie_file: str
    acquisition: int
    epoch: datetime                 # wall clock time of frame-clock zero
    frame_time: np.ndarray          # frameTimestamps_sec, one per frame
    sync_time: np.ndarray           # I2C packet timestamp per frame, NaN if none
    block: np.ndarray               # per-frame block number, 0 if no packet
    trial: np.ndarray               # per-frame trial number, 0 if no packet
    iteration: np.ndarray           # per-frame iteration number, 0 if no packet
    num_frames: int = 0
    clock_time: np.ndarray = field(default=None)   # absolute frame times (datetime64)

    def __post_init__(self):
        self.num_frames = len(self.frame_time)


def _decode_i2c_bytes(byte_str):
    """Decode one I2C packet's byte list into (block, trial, iteration).

    ScanImage prints the raw bytes, e.g. ``[1,0,1,0,83,2]`` (older versions
    use spaces as separators).  The values are little-endian uint16.
    """
    raw = np.array([int(b) for b in re.split(r'[\s,]+', byte_str.strip()) if b],
                   dtype=np.uint8)
    if raw.size % I2C_DTYPE.itemsize:
        raise ValueError(f'I2C packet has {raw.size} bytes, '
                         f'not a multiple of {I2C_DTYPE.itemsize}')
    return raw.view(I2C_DTYPE).astype(np.int64)


def parse_scanimage_sync(tif_path):
    """Extract per-frame sync metadata from a ScanImage TIFF.

    Port of getSyncInfo.cpp: for every frame (TIFF page) read the
    ImageDescription tag and pull out the frame timestamp and the *first*
    I2C packet, if any.  Frames without a packet get sync_time = NaN and
    block/trial/iteration = 0.
    """
    import tifffile

    tif_path = str(tif_path)
    n_values = None
    frame_time, sync_time, data_rows = [], [], []
    acquisition, epoch = None, None

    with tifffile.TiffFile(tif_path) as tif:
        for i_page, page in enumerate(tif.pages):
            desc = page.description

            if i_page == 0:
                m = _RE_ACQ.search(desc)
                acquisition = int(float(m.group(1))) if m else 0
                m = _RE_EPOCH.search(desc)
                if not m:
                    raise ValueError(f'Failed to parse epoch in frame 1 of {tif_path}')
                y, mo, d, h, mi, s = [float(x) for x in m.group(1).split()]
                epoch = datetime(int(y), int(mo), int(d), int(h), int(mi),
                                 int(s), int(round((s % 1) * 1e6)))

            m = _RE_FRAME_TS.search(desc)
            if not m:
                raise ValueError(f'Failed to parse frameTimestamps_sec '
                                 f'in frame {i_page + 1} of {tif_path}')
            frame_time.append(float(m.group(1)))

            m = _RE_I2C.search(desc)
            packets = _RE_I2C_PACKET.findall(m.group(1)) if m else []
            if packets:
                # MATLAB getSyncInfo retains only the first packet of a frame
                ts, byte_str = packets[0]
                values = _decode_i2c_bytes(byte_str)
                if n_values is None:
                    n_values = len(values)
                elif len(values) != n_values:
                    raise ValueError(
                        f'Inconsistent number of I2CData entries {len(values)} '
                        f'(expected {n_values}) in frame {i_page + 1} of {tif_path}')
                sync_time.append(float(ts))
                data_rows.append(values)
            else:
                sync_time.append(np.nan)
                data_rows.append(None)

    n_frames = len(frame_time)
    if n_values is None:
        # no I2C packet in the whole file
        block = trial = iteration = np.zeros(n_frames, dtype=np.int64)
    else:
        if n_values < I2C_VALUES_PER_PACKET:
            raise ValueError(f'I2C packets in {tif_path} carry {n_values} values; '
                             f'expected at least {I2C_VALUES_PER_PACKET} '
                             f'(block, trial, iteration)')
        data = np.zeros((n_frames, n_values), dtype=np.int64)
        for i, row in enumerate(data_rows):
            if row is not None:
                data[i] = row
        block, trial, iteration = data[:, 0], data[:, 1], data[:, 2]

    return FileSyncInfo(
        movie_file=tif_path,
        acquisition=acquisition,
        epoch=epoch,
        frame_time=np.asarray(frame_time),
        sync_time=np.asarray(sync_time),
        block=block.copy(),
        trial=trial.copy(),
        iteration=iteration.copy(),
    )


def split_vec(x):
    """Runs of equal consecutive values, like SplitVec(x,'equal','firstval','bracket').

    Returns (first_values, brackets) where brackets is (n_runs, 2) with
    0-based [first, last] index of each run.
    """
    x = np.asarray(x)
    if x.size == 0:
        return np.array([]), np.zeros((0, 2), dtype=np.int64)
    change = np.flatnonzero(np.diff(x) != 0)
    first = np.concatenate(([0], change + 1))
    last = np.concatenate((change, [x.size - 1]))
    return x[first], np.column_stack((first, last))


def load_behavior_log(mat_path):
    """Load a ViRMEn behavior .mat file and return the ``log`` struct."""
    from scipy.io import loadmat
    mat = loadmat(str(mat_path), squeeze_me=True, struct_as_record=False)
    return mat['log']


def _as_list(x):
    """scipy squeeze_me collapses length-1 struct arrays; always get a list."""
    return [x] if np.ndim(x) == 0 else list(x)


def sync_imaging_behavior(tif_files, log=None, min_behavior_secs=MIN_BEHAVIOR_SECS):
    """Synchronize a sequence of ScanImage TIFFs with a ViRMEn behavior log.

    Port of imaging_pipeline.SyncImagingBehavior.makeTuples.

    Parameters
    ----------
    tif_files : sequence of str/Path, in acquisition order (file_number order)
    log : the ``log`` struct from the behavior .mat file (see
        :func:`load_behavior_log`), or None to skip the behavior-based
        consistency checks (per-frame vectors and spans are still produced).

    Returns
    -------
    dict with the same fields (and 1-based index conventions) as the
    ``u19_imaging_pipeline.SyncImagingBehavior`` DataJoint table, plus
    ``files``: the list of :class:`FileSyncInfo` used.
    """
    blocks = _as_list(log.block) if log is not None else None

    files = []
    sync_frame, sync_global = [], []
    ref_epoch = None
    current_acquis, total_frames = 0, 0

    for i_file, tif_file in enumerate(tif_files):
        info = parse_scanimage_sync(tif_file)

        if i_file == 0 and np.all(np.isnan(info.sync_time)):
            warnings.warn('Sync data was not found')
            return None

        # trust the acquisition number embedded in the file name over the header
        m = _RE_FILE_ACQ.search(str(tif_file))
        if m:
            file_acquis = int(m.group(1))
            if file_acquis != info.acquisition:
                warnings.warn(
                    f'Acquisition number according to file name ({file_acquis}) '
                    f'not equal to that stored ({info.acquisition}) in file {tif_file}.')
                info.acquisition = file_acquis

        # express all times relative to the first file's epoch
        if i_file == 0:
            ref_epoch = info.epoch
        else:
            offset = (info.epoch - ref_epoch).total_seconds()
            info.frame_time = info.frame_time + offset
            info.sync_time = info.sync_time + offset

        info.clock_time = (np.datetime64(info.epoch, 'ms')
                           + np.round(info.frame_time).astype('timedelta64[s]'))

        if info.acquisition > current_acquis:
            current_acquis = info.acquisition
            total_frames = 0
        elif info.acquisition < current_acquis:
            raise ValueError('Encountered decreasing acquisition number while '
                             'processing supposedly sorted files.')

        frames = np.arange(1, info.num_frames + 1)
        sync_frame.append(frames)
        sync_global.append(total_frames + frames)
        total_frames += info.num_frames

        files.append(info)

    sync_frame = np.concatenate(sync_frame)
    sync_global = np.concatenate(sync_global)
    frame_time = np.concatenate([f.frame_time for f in files])
    sync_time = np.concatenate([f.sync_time for f in files])
    sync_block = np.concatenate([f.block for f in files])
    sync_trial = np.concatenate([f.trial for f in files])
    sync_iter = np.concatenate([f.iteration for f in files])

    if np.any(np.diff(frame_time) <= 0):
        raise ValueError('Frame times are not in strictly ascending order. '
                         'Are the file timestamps correct?')

    # ---- Patch frames with no sync info (forward-fill), omitting the
    # ambiguous no-data stretches at the very beginning and end of the session
    no_sync_val, brackets = split_vec(np.isnan(sync_time).astype(int))
    interior = ((no_sync_val == 1)
                & (brackets[:, 0] > 0)
                & (brackets[:, 1] < frame_time.size - 1))
    i1, i2 = brackets[interior, 0], brackets[interior, 1]

    delta_time = frame_time[i2 + 1] - frame_time[i1]
    poor = delta_time > min_behavior_secs
    if np.any(poor):
        warnings.warn('long lags encountered between synching timestamps: '
                      f'{delta_time[poor]}')

    for a, b in zip(i1, i2):
        sync_block[a:b + 1] = sync_block[a - 1]
        sync_trial[a:b + 1] = sync_trial[a - 1]
        sync_iter[a:b + 1] = sync_iter[a - 1]

    if np.any((sync_block == 0) != (sync_trial == 0)):
        raise ValueError('Incompatible presence of block/trial synchronization info.')

    # ---- HACK (from MATLAB): forcibly terminated trials appear in the sync
    # stream but not in the behavior log; erase them
    if blocks is not None:
        img_block, blk_brackets = split_vec(sync_block)
        for j_block, (a, b) in zip(img_block, blk_brackets):
            if j_block < 1:
                continue
            n_trials = np.size(blocks[j_block - 1].trial)
            max_trial = sync_trial[a:b + 1].max()
            if max_trial == n_trials + 1:
                warnings.warn(
                    f'Nonexistent trial {max_trial} recorded in imaging data for '
                    f'behavioral block {j_block} ({n_trials} trials); '
                    'will assume that it was aborted.')
                erase = np.flatnonzero(sync_trial[a:b + 1] == max_trial) + a
                sync_block[erase] = 0
                sync_trial[erase] = 0
                sync_iter[erase] = 0
            elif max_trial > n_trials:
                raise ValueError(
                    f'Trial {max_trial} recorded in imaging data for behavioral '
                    f'block {j_block} which only has {n_trials} trials.')

        # ---- Wall-clock sanity check: behavior block start times should line
        # up (in order) with the imaging wall-clock time of each block's first
        # frame.  (The MATLAB code additionally re-derives block indices with a
        # tolerance-based binary search to absorb clock drift between the two
        # computers; content-based matching makes that redundant here, so we
        # only verify ordering.)
        img_block, blk_brackets = split_vec(sync_block)
        has_img = img_block > 0
        if np.any(has_img):
            img_first_time = frame_time[blk_brackets[has_img, 0]]
            if np.any(np.diff(img_first_time) <= 0):
                raise ValueError('Expected blocks recorded during imaging '
                                 'to be non-decreasing.')
            block_starts = []
            for blk in blocks:
                y, mo, d, h, mi, s = np.asarray(blk.start, dtype=float)
                block_starts.append(datetime(int(y), int(mo), int(d), int(h),
                                             int(mi), int(s),
                                             int(round((s % 1) * 1e6))))
            if sorted(block_starts) != block_starts:
                raise ValueError('Behavioral block start times are not sorted.')

    # ---- Spans: first/last frame (1-based, over the concatenated frame axis)
    # for each behavior block, trial and iteration
    img_block, blk_brackets = split_vec(sync_block)

    n_blocks = len(blocks) if blocks is not None else int(sync_block.max())
    span_by_block = [np.zeros((0, 2), dtype=np.int64)] * n_blocks
    span_by_trial = []
    span_by_iter = []

    for i_block in range(1, n_blocks + 1):
        run = np.flatnonzero(img_block == i_block)
        if run.size == 0:
            continue
        a, b = blk_brackets[run[0]]
        span_by_block[i_block - 1] = np.array([a + 1, b + 1])  # 1-based

        # trial runs within this block
        trial_vals, trial_brackets = split_vec(sync_trial[a:b + 1])
        trial_brackets = trial_brackets + a            # absolute 0-based
        n_trials = (np.size(blocks[i_block - 1].trial)
                    if blocks is not None else int(trial_vals.max()))
        trial_span = [np.zeros((0, 2), dtype=np.int64)] * n_trials
        for t_val, (ta, tb) in zip(trial_vals, trial_brackets):
            if t_val > 0:
                trial_span[t_val - 1] = np.array([ta + 1, tb + 1])  # 1-based

        for i_trial in range(n_trials):
            tspan = trial_span[i_trial]
            if tspan.size == 0:
                span_by_iter.append(np.zeros((0, 2), dtype=np.int64))
                continue
            ta, tb = tspan[0] - 1, tspan[1] - 1        # back to 0-based
            iter_vals, iter_brackets = split_vec(sync_iter[ta:tb + 1])
            n_iter = int(iter_vals.max())
            iteration = np.zeros((n_iter, 2), dtype=np.int64)
            sel = iter_vals > 0
            iteration[iter_vals[sel] - 1] = iter_brackets[sel] + 1  # 1-based, rel.

            # iterations without info fall in the same frame as the previous one
            no_info, nb = split_vec((iteration[:, 0] < 1).astype(int))
            for v, (na, nbend) in zip(no_info, nb):
                if v == 1 and na > 0:
                    iteration[na:nbend + 1] = iteration[na - 1]

            # NOTE: MATLAB stores iteration spans as (run index within the
            # trial) + (trial span start), i.e. one greater than the absolute
            # 1-based frame index used by the trial/block spans.  Kept as-is
            # for compatibility with existing database entries.
            span_by_iter.append(iteration + tspan[0])

        span_by_trial.extend(trial_span)

    return {
        'sync_im_frame': sync_frame,
        'sync_im_frame_global': sync_global,
        'sync_behav_block_by_im_frame': sync_block,
        'sync_behav_trial_by_im_frame': sync_trial,
        'sync_behav_iter_by_im_frame': sync_iter,
        'sync_im_frame_span_by_behav_block': span_by_block,
        'sync_im_frame_span_by_behav_trial': span_by_trial,
        'sync_im_frame_span_by_behav_iter': span_by_iter,
        'files': files,
    }


def frame_times_on_behavior_clock(sync, log):
    """Per-frame timestamps on the ViRMEn behavior clock, for NWB alignment.

    For every imaging frame with sync info, the behavior time of its
    (block, trial, iteration) is ``trial.start + trial.time[iteration - 1]``
    (seconds since ViRMEn session start).  A least-squares linear fit of
    behavior time against the imaging frame clock then yields timestamps for
    *all* frames — including the leading/trailing stretches without I2C
    packets — expressed on the behavior clock.

    Returns (timestamps, slope, offset, residual_std).
    """
    blocks = _as_list(log.block)
    frame_time = np.concatenate([f.frame_time for f in sync['files']])
    sync_time = np.concatenate([f.sync_time for f in sync['files']])

    has_sync = ~np.isnan(sync_time)
    behav_t = np.full(frame_time.size, np.nan)
    for i in np.flatnonzero(has_sync):
        blk = sync['sync_behav_block_by_im_frame'][i]
        tri = sync['sync_behav_trial_by_im_frame'][i]
        itr = sync['sync_behav_iter_by_im_frame'][i]
        if blk < 1 or tri < 1 or itr < 1:
            continue
        trial = _as_list(blocks[blk - 1].trial)[tri - 1]
        t = np.atleast_1d(trial.time)
        if itr <= t.size:
            behav_t[i] = trial.start + t[itr - 1]

    valid = ~np.isnan(behav_t)
    if valid.sum() < 2:
        raise ValueError('Not enough synchronized frames to fit a clock mapping.')
    # the I2C packet timestamp marks when the iteration happened on the
    # imaging clock; frames between packets interpolate linearly
    slope, offset = np.polyfit(sync_time[valid], behav_t[valid], 1)
    residuals = behav_t[valid] - (slope * sync_time[valid] + offset)
    return slope * frame_time + offset, slope, offset, float(np.std(residuals))


def _main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(
        description='Synchronize ScanImage TIFF(s) with a ViRMEn behavior log.')
    parser.add_argument('tif_files', nargs='+', help='TIFF files in acquisition order')
    parser.add_argument('--behavior-mat', help='ViRMEn behavior .mat file')
    args = parser.parse_args(argv)

    log = load_behavior_log(args.behavior_mat) if args.behavior_mat else None
    sync = sync_imaging_behavior(args.tif_files, log)
    if sync is None:
        return

    n = sync['sync_im_frame'].size
    blk = sync['sync_behav_block_by_im_frame']
    tri = sync['sync_behav_trial_by_im_frame']
    itr = sync['sync_behav_iter_by_im_frame']
    print(f'{len(sync["files"])} file(s), {n} frames')
    print(f'frames with behavior info: {np.count_nonzero(blk > 0)} '
          f'({100 * np.count_nonzero(blk > 0) / n:.1f}%)')
    for b in np.unique(blk[blk > 0]):
        t_in_b = tri[blk == b]
        print(f'  block {b}: trials {t_in_b.min()}-{t_in_b.max()}, '
              f'frames {np.flatnonzero(blk == b)[0] + 1}-{np.flatnonzero(blk == b)[-1] + 1}')
    for span, label in [(sync['sync_im_frame_span_by_behav_trial'][:5], 'trial')]:
        for i, s in enumerate(span):
            print(f'  {label} {i + 1} frame span: {s.tolist() if s.size else "(not imaged)"}')
    if log is not None:
        ts, slope, offset, res = frame_times_on_behavior_clock(sync, log)
        print(f'behavior-clock fit: slope={slope:.9f}, offset={offset:.3f}s, '
              f'residual std={res * 1000:.1f} ms')
        print(f'frame 1 behavior time: {ts[0]:.3f}s, last: {ts[-1]:.3f}s')
        print(f'iteration {itr[np.flatnonzero(blk > 0)[0]]} of trial '
              f'{tri[np.flatnonzero(blk > 0)[0]]} is the first synced frame')


if __name__ == '__main__':
    _main()
