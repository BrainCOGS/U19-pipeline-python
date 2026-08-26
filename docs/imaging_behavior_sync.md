# How two-photon imaging is synchronized with ViRMEn behavior

*Verified 2026-08-26 against `U19-pipeline-python`, `U19-pipeline-matlab`, and `ViRMEn`
source, and against a real recording (`ef932_act131_08072026_00001_00001.tif` +
its behavior log).*

## TL;DR

There is **no shared hardware clock and no TTL-edge alignment** for 2p imaging.
Instead, ViRMEn sends the current `[block, trial, iteration]` numbers to the
ScanImage computer **on every ViRMEn display iteration**, over an I2C-style
serial link bit-banged on two NI-DAQ digital lines. ScanImage stamps each
received packet into the TIFF header of the microscope frame being acquired at
that moment. Synchronization is therefore **content-based**: every imaging
frame carries the behavioral coordinate that was active when it was acquired,
and the offline sync step just decodes those headers, cleans them up, and
stores per-frame vectors in the `u19_imaging_pipeline.SyncImagingBehavior`
DataJoint table.

```
ViRMEn PC (behavior rig)                          ScanImage PC (microscope)
────────────────────────                          ─────────────────────────
runtimeCodeFun (every iteration)
  logger.logTick() → [block, trial, iter]
  updateDAQSyncSignals(data)
    └─ nidaqI2C('send', data)   ── 2 DO lines ──▶ ScanImage I2C input
       (CLK + DTA, I2C protocol,                    └─ appends packet to the
        6 bytes = 3 × uint16 LE)                       ImageDescription tag of
                                                       the current TIFF frame
                                                       "I2CData = {{t,[b0..b5]}}"

                       OFFLINE (nightly populate_tables.m, MATLAB)
                       ────────────────────────────────────────────
     imaging_pipeline.SyncImagingBehavior.makeTuples
       ├─ getSyncInfo MEX (C++/libtiff): parse every frame's header
       ├─ + behavior .mat log ("log.block")
       └─ → per-frame block/trial/iteration vectors + frame-span tables
            stored in u19_imaging_pipeline.SyncImagingBehavior
```

## 1. The sending side (ViRMEn repo)

Every ViRMEn experiment's runtime function ends each iteration with
(e.g. `ViRMEn/experiments/LSTT_Stationary_TrialStructure_EF.m:738-743`):

```matlab
loggingIndices = vr.logger.logTick(vr, vr.sensorData, vr.isLick, true);
if RigParameters.hasDAQ
    updateDAQSyncSignals(vr.iterFcn(loggingIndices));
end
```

- `ExperimentLog.logTick()` (`ViRMEn/experiments/classes/ExperimentLog.m:513-581`)
  logs position/velocity/etc. for the current iteration and returns exactly
  `[numel(obj.block), obj.writeIndex, obj.currentIt]` = **[block #, trial #,
  iteration #]**, all 1-based. `[0 0 0]` before the first trial starts.
- `vr.iterFcn` comes from `smallestUIntStorage(maxTrialDuration/minIterationDT)`
  — in practice a cast to **uint16** (that is why the reader decodes uint16).
- `updateDAQSyncSignals` (`ViRMEn/experiments/common/updateDAQSyncSignals.m`)
  sends only when trial ≠ 0 and iteration ≠ 0. For imaging rigs
  (`RigParameters.hasSyncComm == true`) it calls
  `nidaqI2C('send', data, true, false)` — the other branches (`nidaqSync`,
  `SyncPulses`, …) are the parallel-port / TTL schemes used by ephys and
  widefield rigs, not by 2p.
- `nidaqI2C` (`ViRMEn/experiments/daq/nidaqI2C.cpp`, a MEX compiled per rig;
  initialized in `initializeDAQ.m` with
  `nidaqI2C('init', nidaqDevice, nidaqPort, syncClockChannel, syncDataChannel)`)
  bit-bangs a full I2C write transaction — start condition, 7-bit slave
  address 0, write bit, ACK slots, then the payload bytes MSB-first — on two
  digital output lines clocked at 1 MHz from a hardware counter. The payload
  is the 3 uint16 values = **6 bytes, little-endian**. Transmission runs in a
  background thread so it never blocks the render loop.

ScanImage's standard **I2C sync feature** receives this on its dedicated
input and appends each packet, with its own frame-clock timestamp, to the
`ImageDescription` TIFF tag of the frame being scanned when the packet
arrived.

## 2. What lands in the TIFF header (verified on real data)

Each TIFF page (= microscope frame) description contains, among others:

```
frameNumbers = 1001
acquisitionNumbers = 1
frameTimestamps_sec = 19.918480740          ← imaging frame clock (s)
epoch = [2026  8  7 12  3 13.109]           ← wall-clock datevec of clock zero
I2CData = {{19.925094135, [1,0,1,0,83,2]} } ← {i2c_timestamp, [6 bytes]}
```

`[1,0,1,0,83,2]` decodes (3 × uint16 LE) to **block 1, trial 1, iteration 595**.

Real-file facts that the sync logic must (and does) handle:

- Frames acquired before the first trial have `I2CData = {}` (in the sample
  file: the first 643 of 2000 frames, ≈12.8 s of pre-behavior imaging).
- ViRMEn iterations (~50–120 Hz) and frames (50.2 Hz here) are
  incommensurate: most frames carry one packet, some carry **two**, some
  carry **none** even mid-session.
- Iteration is non-decreasing within a trial and resets to 1 at each trial
  transition; trial transitions in the header match the behavior log.

## 3. The reading side (U19-pipeline-matlab repo)

`imaging_pipeline.SyncImagingBehavior` (a `dj.Computed`,
`schemas/+imaging_pipeline/SyncImagingBehavior.m`; near-identical legacy
variants exist in `+imaging/` and `+meso/`) does, per FOV (`TiffSplit`):

1. **Load behavior**: the session's `.mat` log (path from
   `acquisition.SessionStarted.new_remote_path_behavior_file`), take
   `log.block`, run `fixLogs` to backfill missing trialType/choice.
2. **Parse every TIFF** of the FOV in `file_number` order with
   `getSyncInfo(file, 'uint16')` — a **compiled C++ MEX**
   (`utils/imagingSync/getSyncInfo.cpp`, ~270 lines, libtiff). Its `.m` file
   is only a doc stub; the runtime uses `getSyncInfo.mexa64/w64/maci64`. It
   returns per file: acquisition number, `epoch`, `frameTimestamps_sec` per
   frame, the **first** I2C packet's timestamp per frame (NaN if none), and
   the decoded `[block; trial; iteration]` matrix (zeros where no packet).
3. **Stitch files**: express all times relative to the first file's `epoch`
   (`etime` offsets); build per-file frame numbers (`sync_im_frame`) and a
   global frame counter that resets when the acquisition number increments
   (`sync_im_frame_global`).
4. **Forward-fill gaps**: interior runs of frames with no packet inherit the
   values of the last frame that had one (leading/trailing runs stay 0 —
   they are genuinely outside behavior). Warn if a gap exceeds 2 s
   (`cfg.minBehaviorSecs`).
5. **Sanity checks / hacks**: `block==0 ⇔ trial==0`; a trial index equal to
   `numTrials+1` for a block is a forcibly-terminated trial absent from the
   log → its frames are zeroed; larger overshoots are errors. Behavior block
   wall-clock start times (`block.start`) are cross-checked against the
   imaging wall clock of each block's first frame (`binarySearch` with
   tolerance, plus a relative-positioning step to absorb clock drift between
   the two computers — the wall clocks are *only* used for this sanity check,
   never for the frame assignment itself).
6. **Spans**: with `SplitVec` (runs of equal values), compute first/last
   frame per behavior block, per trial, and per iteration (iterations that
   fell between frames inherit the previous iteration's span).
7. **Insert** into `u19_imaging_pipeline.SyncImagingBehavior`:

| field | content |
|---|---|
| `sync_im_frame` | frame # within its TIFF file (1-based) |
| `sync_im_frame_global` | global frame # in the scan |
| `sync_behav_block_by_im_frame` | behavior block per frame (0 = none) |
| `sync_behav_trial_by_im_frame` | behavior trial per frame |
| `sync_behav_iter_by_im_frame` | behavior iteration per frame |
| `sync_im_frame_span_by_behav_block` | [first, last] frame per block |
| `sync_im_frame_span_by_behav_trial` | [first, last] frame per trial |
| `sync_im_frame_span_by_behav_iter` | per-trial (nIter × 2) frame spans |

*(Quirk, kept for compatibility: the stored iteration spans are offset by
+1 relative to the trial/block span convention — MATLAB adds `span(1)`
instead of `span(1)-1` when flattening.)*

### Who runs it

- The Python automation (`u19_pipeline/automatic_job/recording_handler.py`)
  populates `ImagingPipelineSession` / `AcquiredTiff`; the legacy path shells
  out to MATLAB via `automatic_job/ingest_scaninfo_shell.sh` →
  `scripts/populate_Imaging_AcquiredTiff.m` (TIFF splitting / ScanInfo).
- `SyncImagingBehavior` itself is populated by the **nightly MATLAB cron**
  `scripts/populate_tables.m` (same script that populates the pupillometry
  and posture sync tables): `populate(imaging_pipeline.SyncImagingBehavior)`.
- The Python repo declares the table (`u19_pipeline/meso.py:85`,
  `imaging_pipeline.py`) but its `make` lives only in MATLAB.

### Readable vs. compiled ("encoded") code — verified inventory

| piece | runtime form | source available? |
|---|---|---|
| `SyncImagingBehavior.m` (×3 variants) | plain MATLAB | yes — fully readable |
| `getSyncInfo` | compiled MEX (ELF, links libtiff) | yes — `getSyncInfo.cpp`; the `.m` is a doc-only stub |
| `binarySearch` | compiled MEX | yes — `binarySearch.c` (3rd-party, Avi Ziskind); `.m` is a doc stub |
| `SplitVec.m` | plain MATLAB | yes (copies in both repos) |
| `nidaqI2C` | MEX built on the rig (no binary in repo) | yes — `nidaqI2C.cpp` |
| true p-code | only `connect_tech.p`, `cprintf.p` etc. | unrelated to sync |

So nothing in the sync path is irrecoverably encoded; the "different beast"
parts are ordinary C/C++ MEX files whose sources are checked in.
`~/neuro-data` contains no `.m` files — just the TIFF and the behavior log.

## 4. Python port (new in this repo)

[`u19_pipeline/utils/imaging_behavior_sync.py`](../u19_pipeline/utils/imaging_behavior_sync.py)
reimplements the whole offline chain without MATLAB:

- `parse_scanimage_sync(tif)` — port of `getSyncInfo.cpp` using **tifffile**
  (no byte-level TIFF parsing; per-page `description` + regex, first packet
  per frame, uint16-LE decode).
- `sync_imaging_behavior(tif_files, log)` — port of
  `SyncImagingBehavior.makeTuples` (stitching, forward-fill, aborted-trial
  hack, spans). Returns a dict with the exact DataJoint field names and
  1-based conventions. The wall-clock drift-correction step is simplified to
  an ordering check (it was only ever a sanity check).
- `frame_times_on_behavior_clock(sync, log)` — **new**: maps every frame's
  I2C-tagged iteration to its behavior time (`trial.start +
  trial.time[iter-1]`) and fits a linear clock mapping, yielding a timestamp
  for *all* frames (including un-synced lead-in) on the ViRMEn clock — this
  is the "clock vector" needed for NWB.

Verified on the sample session (2000 frames, 3 trials imaged):

```
$ uv run python -m u19_pipeline.utils.imaging_behavior_sync \
      ~/neuro-data/ef932_act131_08072026_00001_00001.tif \
      --behavior-mat ~/neuro-data/Session_..._ef932_act131_20260807_1.mat
1 file(s), 2000 frames
frames with behavior info: 1357 (67.8%)
  block 1: trials 1-3, frames 644-2000
  trial 1 frame span: [644, 1176]
  trial 2 frame span: [1177, 1690]
  trial 3 frame span: [1691, 2000]
behavior-clock fit: slope=1.000027891, offset=-11.028s, residual std=10.4 ms
```

Frame spans match an independent raw-header decode exactly; the fitted slope
(≈28 ppm) is the real clock drift between the two computers, and the 10 ms
residual is the expected sub-frame jitter (iterations arrive faster than
frames and only the first packet per frame is kept).

## 5. Building an NWB file from this

**Ecosystem note**: *spikeinterface* is for extracellular ephys and has no
role here. For imaging the relevant NWB stack is **roiextractors** (raw
imaging extractors) + **neuroconv** (interfaces, metadata, temporal
alignment) + **pynwb** (trials, behavior). Checked against neuroconv 0.10.0 /
roiextractors 0.9.0 / pynwb 4.1.0:

- `neuroconv.datainterfaces.ScanImageImagingInterface` reads modern ScanImage
  TIFFs (BigTIFF, multi-file, volumetric — `tif.is_scanimage` is True for our
  files) into a `TwoPhotonSeries`.
- Every neuroconv interface inherits the temporal-alignment API:
  `get_original_timestamps()`, `set_aligned_timestamps()`,
  `set_aligned_starting_time()`, `align_by_interpolation()`.
- `Suite2pSegmentationInterface` covers the processed side (ROIs, dF/F) if
  suite2p output is added later.

### Recipe

The header gives us everything needed to put imaging and behavior on one
clock — that is exactly what `frame_times_on_behavior_clock` computes:

```python
from u19_pipeline.utils.imaging_behavior_sync import (
    sync_imaging_behavior, load_behavior_log, frame_times_on_behavior_clock)
from neuroconv.datainterfaces import ScanImageImagingInterface
from pynwb import TimeSeries

log  = load_behavior_log(behavior_mat)
sync = sync_imaging_behavior(tif_files, log)
timestamps, slope, offset, res = frame_times_on_behavior_clock(sync, log)

interface = ScanImageImagingInterface(file_path=tif_files[0])
interface.set_aligned_timestamps(timestamps)     # imaging now on ViRMEn clock
nwbfile = interface.create_nwbfile(metadata=...)  # TwoPhotonSeries w/ timestamps

# trials table straight from the spans / behavior log
for i, tr in enumerate(log.block.trial):
    nwbfile.add_trial(start_time=tr.start, stop_time=tr.start + tr.duration)
    # + columns: trialType, choice, cuePos, ... and the imaging frame span
    #   from sync['sync_im_frame_span_by_behav_trial'][i]

# per-iteration behavior (position, velocity) as TimeSeries on the same clock
# t = tr.start + tr.time;  data = tr.position / tr.velocity

# per-frame sync vectors as auxiliary TimeSeries so the mapping survives:
# sync['sync_behav_{block,trial,iter}_by_im_frame'] with the frame timestamps
```

Choices worth noting:

- **Clock choice**: use the ViRMEn session clock as the NWB timebase (trials
  and behavior arrays are already in it; `session_start_time` =
  `log.initialTimestamp`). Imaging gets explicit per-frame `timestamps`
  instead of a start+rate pair — this also absorbs the measured 28 ppm drift.
- **Sub-frame accuracy**: the linear fit is good to ~10 ms (≈ half a frame at
  50 Hz). If per-iteration precision is ever needed, the I2C packet
  timestamps (`sync_time`) pin individual iterations to the imaging clock at
  millisecond level.
- **Multi-file / volumetric sessions**: pass all split TIFFs in order;
  `ScanImageImagingInterface` accepts `file_paths=[...]` and `plane_index`
  for the 5-slice fastZ stacks, and the sync vectors are per *frame* (page),
  so slice handling only affects how timestamps are subset.
- The `u19_pipeline/utils/matlab_utils.py` helpers (by Ben Dichter/Cody
  Baker) already convert the loaded `log` structs to dicts, so the behavior
  side of a full `NWBConverter` can reuse them.

### Smoke test (ran on the sample session)

The recipe above was executed for real against the sample data (neuroconv
0.10.0): `ScanImageImagingInterface` opened the 620 MB BigTIFF and, because
of the 5-slice fastZ stack, exposed it as a **volumetric** series of **400
volumes** (2000 pages / 5 slices), with `get_original_timestamps()` returning
400 volume timestamps. Aligning with `set_aligned_timestamps(ts[::5][:400])`
(one behavior-clock timestamp per volume, from
`frame_times_on_behavior_clock`) and building the file produced:

```
nwbfile: 2026-08-07 12:03:17.783000-04:00 | trials: 179 | acq: ['TwoPhotonSeries']
TwoPhotonSeries n timestamps: 400 | t[0]=-11.028 t[-1]=28.710 (behavior clock s)
trial 1 row: {'start_time': 1.757, 'stop_time': 12.400,
              'first_im_frame': 644, 'last_im_frame': 1176}
```

Cross-check: trial 1 starts at 1.757 s on the behavior clock, and its first
imaging frame (page 644) maps to ≈1.77 s — the two data streams agree to
within the expected sub-frame jitter. Negative timestamps are the ~11 s of
imaging acquired before ViRMEn behavior started (frames with `I2CData = {}`).

### Relationship to the existing NWB export branches

This repo already has an NWB export backend on the remote branches
`nwb-export-backend`, `fix/nwb-export-handler-schema` and
`feat/nwb-export-handler-completion` (~8.4k lines: `u19_pipeline/nwb_export/`
state machine, readiness checks, output validation, DANDI upload client, and
the `automatic_job/nwb_export_handler.py` cronjob). Its modality registry
declares `imaging-raw` / `imaging-processed`, **but imaging conversion is not
wired yet** — the handler raises
*"could not resolve imaging Scan … imaging export not yet wired"* (TODO at
`nwb_export_handler.py:247-254` on that branch). The sync module and recipe
in this document are the missing ingredient for that TODO: resolve the scan's
TIFFs, run `sync_imaging_behavior` + `frame_times_on_behavior_clock` (or
fetch the stored vectors from `u19_imaging_pipeline.SyncImagingBehavior`),
and hand the aligned timestamps to `ScanImageImagingInterface`.

## 6. Which clock is `trial.start` on? (and the ~27 ms NWB offset)

Short answer: `trial.start` is on ViRMEn's `vr.timeElapsed` clock, which is
zeroed at **block start**, not at `log.session.start`. NWB zeroes its timeline
at `session.start`. The gap between the two is the offset, and it must be read
out of each session's log rather than assumed.

### Where the numbers come from

`ExperimentLog` writes trial times straight off the engine clock, with no
block-relative correction anywhere:

```matlab
% ExperimentLog.m:501
obj.currentTrial.start = vr.timeElapsed;
% ExperimentLog.m:~540 (logTick)
obj.currentTrial.time(obj.currentIt,1) = vr.timeElapsed - obj.currentTrial.start;
```

`obj.blockStart` exists but is only ever read to compute `block.duration`
(`ExperimentLog.m:381,848`) — it is never subtracted from `trial.start`.

So the question is what zeroes `vr.timeElapsed`. The engine:

```matlab
% virmenEngine.m
101  vr.initialTimestamp = clock;              % logged as log.initialTimestamp
102  vr.preTic = tic;
105  vr = vr.code.initialization(vr);          % <- ExperimentLog built in here
123  vr.timeElapsedFirstTrial = toc(vr.preTic);
124  firstTic = tic;                           % <- vr.timeElapsed zero
367  timeElapsed = toc(firstTic);
```

`vr.code.initialization` is where the `ExperimentLog` is constructed, and that
constructor stamps `session.start = clock` (`ExperimentLog.m:228`) and then, via
`newBlock()`, `block.start = clock` (`ExperimentLog.m:864`). Both land *before*
`firstTic`. So the ordering on the wall clock is:

```
initialTimestamp ... session.start ... block.start ... firstTic (timeElapsed = 0)
   12:03:17.783      12:03:23.152     12:03:23.179     ~12:03:23.179 + eps
```

### What the 27 ms actually is

It is the wall-clock time between two `clock` calls during ViRMEn startup —
the tail of the `ExperimentLog` constructor plus the start of `newBlock()`:

- version/bookkeeping struct assembly,
- `obj.makeOrContinueLog(cfg.logFile)` — **file I/O on the behavior log**,
- `repmat(obj.trialInfo, 1, totalTrials)` — allocating the entire trial struct
  array up front (`ExperimentLog.m:858`).

It is not clock drift, not a physical delay in the data, and not a sync
artifact. It is allocation and file-I/O cost. It therefore **varies per
session** — it scales with `totalTrials` and with whatever else the machine was
doing — so it must be computed from each log as
`block[0].start - session.start` and never hardcoded to 27 ms.

### Consequence for NWB export

Everything measured in `timeElapsed` units shares the block-start zero: trial
starts, per-iteration `trial.time`, and — because
`frame_times_on_behavior_clock` fits against `trial.start + trial.time` — our
per-frame imaging timestamps. NWB zeroes at `session.start`. So all of them take
the same shift:

```python
epoch_offset = (block[0].start - session.start).total_seconds()
timestamps = frame_times_on_behavior_clock(sync, log)[0] + epoch_offset
```

`VirmenDataInterface` already applies this shift to its trials table
(`epoch_start_nwb`). Imaging must match it. Skipping it puts trial 1's first
imaging frame 4.7 ms *before* trial 1 starts instead of 22.3 ms after — wrong
by one frame period, and small enough to pass for ordinary jitter.

### Why wall clocks can't do this job

The imaging TIFF header carries an absolute `epoch` (acquisition start on the
ScanImage PC). Reconstructing the `timeElapsed` zero through it — imaging
`epoch` + `frameTimestamps_sec` for a frame, minus that frame's behavior time —
gives an instant **958 ms** after `block.start`. That residual is inter-machine
wall-clock skew between the ViRMEn and ScanImage PCs, and it is three orders of
magnitude larger than the alignment we need.

It does cleanly rule out `initialTimestamp` as the zero (that candidate misses
by 6.35 s, far outside any plausible skew), which is what it was used for here.
But it is also the whole reason the sync is content-based: the I2C
`[block, trial, iteration]` packets tie the two streams together by *what* was
happening, not by *when* two unsynchronized clocks each thought it was. Never
align these streams through `epoch`.
