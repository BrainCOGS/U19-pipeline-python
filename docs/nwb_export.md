# Running and extending the NWB export pipeline

*Verified 2026-08-26 against `u19_pipeline/automatic_job/nwb_export_handler.py`,
`u19_pipeline/nwb_export/`, `u19_pipeline/nwb_production.py`,
`u19_pipeline/nwb_production_utils.py` and `scripts/run_nwb_export.py`. Several
of these files are under active development on other branches of the same
effort (issue #111) as this document was written — see the note at the end of
§2 for what was still unwired at read time.*

This document has two audiences: someone who needs to run (or debug) an
export job, and someone who needs to add a new data modality to the pipeline.
For the imaging-specific sync mechanics, this document defers to
`docs/imaging_behavior_sync.md` rather than repeating it.

## 1. Pipeline stages

An export job is one row in `nwb_production.NwbExportJob`, keyed by
`nwb_job_id` and pointing at one `acquisition.Session`. Its `status_id`
(`u19_pipeline/nwb_export_enums.py`, `NwbExportStatusEnum`) walks a fixed
state machine, enforced declaratively in
`u19_pipeline/nwb_export/state_machine.py:38-49`:

```
QUEUED → DATA_VALIDATION → PROCESSING → VALIDATION → COMPLETED
                                             ↘ UPLOAD → UPLOADED ↗
Any of the above → FAILED (terminal)
```

`UPLOAD`/`UPLOADED` is only entered if the job requested a DANDI upload;
otherwise `VALIDATION` goes straight to `COMPLETED`. `COMPLETED` and `FAILED`
are terminal — nothing transitions out of them. `state_machine.py` is
currently descriptive (`is_valid_transition` / `assert_valid_transition`) but
not yet called from the handler below; the handler enforces the same shape by
construction (each stage method only ever proposes one of two next states:
success or `FAILED`).

### The handler loop

`NwbExportHandler.pipeline_handler_main()`
(`u19_pipeline/automatic_job/nwb_export_handler.py:69-188`) is the driver. It
fetches every job whose `status_id` is non-terminal —

```python
restriction = f"status_id >= 0 AND status_id < {completed} AND status_id != {failed}"
```

— and, per job, dispatches on `current_status` to exactly one stage method,
then advances `status_id` (or drops it to `FAILED`) and writes a row to
`nwb_production.NwbExportLogStatus` via `update_status_pipeline` (`:540-579`).
A Slack notification fires on both `COMPLETED` and `FAILED`
(`config.slack_webhooks_dict["nwb_export_notification"]`); a failure to send
the Slack message itself is caught and logged, not fatal. Any exception
*outside* the stage methods (a bug in the dispatch loop itself) is also caught
per-job so one bad job can't wedge the whole batch; it is recorded as a
`FAILED` transition with the traceback truncated to 4095 characters (the
`error_exception` column's width). The loop sleeps 1 second between jobs.

Stage methods share a return contract: `(success: bool, error_info: dict)`
where `error_info` has `error_message` (truncated to 255 chars — the column
width) and `error_exception` (4095 chars). `process_upload` additionally
smuggles the next status through `error_info["_next_status"]`, since it must
choose between `UPLOAD`→`UPLOADED` and `UPLOAD`/`VALIDATION`→`COMPLETED`
depending on whether a DANDI upload was requested; the dispatch loop pops
that key back out (`:114-123`).

| Stage | Method | What it does | Where it can fail |
|---|---|---|---|
| QUEUED → DATA_VALIDATION | `process_data_validation` (`:191-264`) | For each row in `NwbExportModality` for this job, branch on `modality_name` and call that modality's `validate_*_data_exists` function. | Any modality's validator returns `(False, msg)`, or raises — becomes `ValueError`, caught, job → `FAILED`. |
| DATA_VALIDATION → PROCESSING | `process_nwb_conversion` (`:266-324`) | Parses `export_parameters`, resolves input paths (`resolve_input_paths`), calls `run_conversion_to_file` (shared with the CLI — see §1.3), writes `actual_file_size_gb`. | Missing/unresolvable source files, converter exceptions (including cross-repo `tank_lab_to_nwb` errors — see §5), disk write failures. |
| PROCESSING → VALIDATION | `process_validation` (`:326-457`) | Opens the written NWB file with `h5py` and checks it has top-level keys; if `nwbinspector` is importable, runs `inspect_nwbfile` and counts errors/warnings. Inserts (or replaces) one `NwbExportValidation` row. | HDF5 won't open, or NWB Inspector reports `ERROR`-importance messages. `nwbinspector` not being installed is *not* a failure — it's skipped and `nwb_inspector_passed` is left `True`. |
| VALIDATION → UPLOAD/COMPLETED | `process_upload` (`:459-538`) | If the job has no `NwbExportJobDandi` row, finalizes straight to `COMPLETED`. Otherwise checks `can_upload_to_dandi(user_id)`, fetches the decrypted API key, and uploads via `DandiUploadClient`. | No DANDI credentials configured for a job that requested upload (a requested upload is never silently skipped — this is a hard failure, not a fallback to `COMPLETED`), or the DANDI client raising. |
| UPLOAD → UPLOADED/COMPLETED | `process_upload` again | Same method handles both `VALIDATION` and `UPLOAD` entry points. | Same as above. |
| UPLOADED → COMPLETED | inline in `pipeline_handler_main` (`:125-131`) | No-op finalize; upload already succeeded. | Not expected to fail. |

Note on `process_upload`'s recorded asset id: the neuroconv/DANDI upload
client returns organized file paths, not a DANDI asset ID, so
`dandi_asset_id` is left `NULL` on success today (`:523-525`, marked `TODO`
in the code).

### Entry points

- **Cronjob** — `u19_pipeline/automatic_job/cronjob_nwb_export.py` loads the
  DataJoint conf, then loops `NwbExportHandler.pipeline_handler_main()` every
  5 seconds forever, catching and logging any exception that escapes the
  handler entirely so the process itself never dies. This is the production
  entry point; wire it into `automatic_job/crontab_example` /
  `call_cronjob_automatic_job.sh`-style supervision the same way the other
  `cronjob_*.py` scripts in that directory are run. (`cronjob_nwb_export_enhanced.py`
  also exists in the same directory — check which one is actually deployed
  before assuming `cronjob_nwb_export.py` is current.)
- **CLI** — `scripts/run_nwb_export.py` drives a *single* job (or, with no
  `--job-id`, every non-terminal job in submission order) through the same
  stages synchronously and prints progress instead of going through the
  cron loop. It shares `run_conversion_to_file` with the handler
  (imported from `u19_pipeline.nwb_export.conversion`) so behavior matches
  production, but it does its own thinner DATA_VALIDATION (only
  `validate_behavior_data_exists`, `:181-192`) and VALIDATION (HDF5-only,
  `:237-246`) — it does not run the ephys/imaging validators or NWB
  Inspector that the handler runs. Useful for a manual/debug run of one job;
  do not treat its validation as equivalent to the cronjob's. It refuses to
  run a job that isn't `QUEUED` or `FAILED` (`:139-144`), and `--dry-run`
  prints the intended transition sequence without touching the DB or disk.

## 2. How a modality gets wired in

Every modality follows the same recipe. Behavior and ephys are the two
already fully wired; the general shape, using them as examples:

1. **Registry row** — `nwb_production.NwbExportModality`
   (`u19_pipeline/nwb_production.py:91-106`) has one row per
   `(nwb_job_id, modality_name)`, with `modality_name` a free-text column
   (`'behavior'`, `'ephys'`, `'imaging'` by convention — there is no lookup
   table constraining it) plus `modality_type` and a JSON-array-as-string
   column for sub-selection (`probe_numbers` for ephys, `fov_numbers` for
   imaging). Jobs are created through `submit_nwb_export_job` (`:222-290`),
   which takes a list of `(modality_name, modality_type, numbers)` tuples.
2. **Data-existence validator** — a `validate_<modality>_data_exists(key,
   ...)` function in `u19_pipeline/nwb_production_utils.py` returning
   `(bool, error_message)`. `validate_behavior_data_exists` (`:241-265`)
   checks `acquisition.Session` and `behavior.TowersBlock.Trial`.
   `validate_ephys_data_exists` (`:268-297`) checks `recording.Recording`
   and, per requested probe, `ephys_element.ProbeInsertion`.
   `validate_imaging_data_exists` (`:300-355`) is the imaging analogue —
   see §3. Because `NwbExportJob` only carries the `acquisition.Session`
   primary key (not `recording_id`), any modality that needs a recording
   (ephys, imaging) must first resolve it via `recording_ids_for_session`
   (`:12-34`, which reads `recording.Recording.BehaviorSession`) and loop
   over the result — a session can, in principle, map to more than one
   recording.
3. **Size estimator** — an `estimate_<modality>_size_gb(...)` function in
   the same file, summed by `estimate_total_size` (`:179-238`), which is
   what informs `NwbExportJob.estimated_file_size_gb` at submission time.
   Being wrong here doesn't fail an export; it just mis-sizes disk/quota
   planning.
4. **Path resolution + `source_data`** — `resolve_input_paths` and
   `build_source_data` in `u19_pipeline/nwb_export/conversion.py`
   (`:29-87`, `:112-165`) turn the job record and `export_parameters` into
   a `source_data` dict keyed by neuroconv interface name
   (`"VirmenData"`, `"KilosortProbe0"`, ...) with the arguments that
   interface's `__init__` expects (typically `file_path` or `folder_path`).
   `resolve_input_paths` currently only resolves the single behavior
   `.mat` file and a Kilosort base directory; a new modality that needs
   its own path convention (imaging's TIFF splits, for instance) needs a
   third resolution branch here, not just in `build_source_data`.
5. **Neuroconv interface on `TowersNWBConverter`** — the actual read/write
   code lives in the external `tank-lab-to-nwb` repository (see §5), in
   `tank_lab_to_nwb/convert_towers_task/towersnwbconverter.py`, whose
   `data_interface_classes` dict maps `source_data` keys to neuroconv
   `DataInterface` subclasses (e.g. `VirmenDataInterface`,
   `SpikeGLXRecordingInterface`, per-probe `KiloSortWithProbeInterface`
   registered dynamically for any `source_data` key starting with
   `"Kilosort"`, `:69-70`). Adding a modality here is a change in that
   repo, not this one.
6. **Handler branch** — `NwbExportHandler.process_data_validation`
   (`nwb_export_handler.py:218-255`) has one `if modality_name == ...`
   branch per modality that calls step 2's validator (resolving
   `recording_ids_for_session` first if needed, mirroring the ephys
   branch at `:226-245`).

### State at the time of this read

Reading the branch live during the imaging work (2026-08-26), the pieces
above are wired for imaging **unevenly** — worth flagging explicitly since
these files are being edited concurrently and may look different by the time
you read this:

- `validate_imaging_data_exists` and `estimate_imaging_size_gb` in
  `nwb_production_utils.py` are fully implemented against the real imaging
  tables (`imaging_pipeline.TiffSplit` / `TiffSplit.File`), not the stale
  `imaging_element.Scan`/`FieldOfView` references an earlier version had.
- `estimate_total_size`'s imaging branch (`nwb_production_utils.py:222-236`)
  correctly resolves `recording_ids_for_session` and calls the estimator.
- **But** `NwbExportHandler.process_data_validation`'s imaging branch
  (`nwb_export_handler.py:247-255`) still unconditionally raises
  `"could not resolve imaging Scan ... imaging export not yet wired"` — it
  does not call `validate_imaging_data_exists` at all yet. Any imaging job
  fails DATA_VALIDATION today regardless of whether the data actually
  exists.
- `conversion.py`'s `build_source_data` (`:112-165`) has no imaging branch —
  only `VirmenData` and per-probe `Kilosort*` entries — and
  `resolve_input_paths` has no notion of a TIFF/FOV path.
- `towersnwbconverter.py` on `tank-lab-to-nwb`'s
  `feat/scanimage-per-interface-alignment` branch (the branch with the most
  recent imaging-related work as of this read) still maps `"TiffImagaging"`
  to the generic `TiffImagingInterface`, not `ScanImageImagingInterface`, and
  `temporally_align_data_interfaces` still applies one `sync_timestamps`
  array to every interface rather than a per-interface array.

In other words: the validation/estimation half of imaging wiring (step 2/3
above) is done; the path-resolution, source-data, and converter-registration
halves (steps 4/5/6) were not yet landed on this branch as of this read. If
you're picking this up, check `git log` / the other in-flight branches before
assuming either state — this section describes a snapshot, not a guarantee.

## 3. Imaging specifics

The full mechanism — how ScanImage frames get I2C-stamped with
`[block, trial, iteration]`, how the Python port
(`u19_pipeline/utils/imaging_behavior_sync.py`) decodes and aligns them, and
the verified numbers from the sample session — is documented in
`docs/imaging_behavior_sync.md`. This section only summarizes the parts
relevant to wiring imaging into this pipeline; **read
`docs/imaging_behavior_sync.md` §5 (the NWB recipe) and §6 (the clock
convention) for the actual mechanics and derivations** rather than relying on
the summary below.

In brief: imaging data arrives as ScanImage BigTIFFs, read by
`neuroconv.datainterfaces.ScanImageImagingInterface` into a
`TwoPhotonSeries`. Per-frame timestamps on the behavior clock come from
`u19_pipeline/utils/imaging_behavior_sync.py`
(`sync_imaging_behavior` + `frame_times_on_behavior_clock`), not from the
TIFF's own clock (see §4 below for why). One subtlety worth restating because
it's easy to get backwards: a volumetric (fastZ) acquisition has more TIFF
*pages* than the interface exposes as *volumes* — the sample session's
5-slice fastZ file has 2000 pages but `ScanImageImagingInterface` reports 400
volumes, so the per-frame timestamp array must be subset `[::5][:n_volumes]`
(one timestamp per volume, taking every 5th frame time) before being handed
to `set_aligned_timestamps`. Passing all 2000 per-frame timestamps to a
400-volume series is a length mismatch that neuroconv will reject.

## 4. The clock convention

This was investigated (`docs/imaging_behavior_sync.md` §6) and is settled
policy — record it here as the rule, not as an open design question:

- **Imaging-only sessions use the ViRMEn behavior clock.** The NWB file's
  `session_start_time` is `log.session.start`.
- **All `vr.timeElapsed`-based quantities are zeroed at block start, not
  session start.** This includes trial start times, per-iteration times
  within a trial, and — because `frame_times_on_behavior_clock` fits against
  `trial.start + trial.time` — the imaging frame timestamps too. All of them
  must be shifted onto the NWB timeline by
  `epoch_offset = (block[0].start - session.start).total_seconds()`
  before being written. On the sample session, `epoch_offset` is +27.0 ms.
- **That offset is not a constant.** It is the wall-clock cost of MATLAB
  struct allocation plus behavior-log file I/O between two `clock()` calls
  during ViRMEn startup, so it scales with trial count and machine load. It
  must be computed from each session's own log, never hardcoded to 27 ms or
  any other value.
- **Never align imaging to behavior through wall clocks.** The TIFF header's
  absolute `epoch` timestamp (ScanImage PC clock) and the ViRMEn PC's clock
  disagree by roughly 958 ms on the sample session — three orders of
  magnitude larger than the alignment precision needed. Alignment must stay
  content-based, through the I2C `[block, trial, iteration]` packets, exactly
  as `imaging_behavior_sync.py` does it.
- **Mixed ephys+imaging sessions are an open question, not a decided one.**
  Ephys exports today align behavior onto the ephys clock (via the
  `nwb_production.BehaviorSync` table consumed in
  `conversion.py:query_metadata`, `:223-236`). That directly conflicts with
  the ViRMEn-clock rule above for imaging. No resolution is written down for
  a session that has both modalities in one export job. **This needs
  explicit sign-off before the first DANDI upload of a mixed-modality
  session** — once timestamps are published to DANDI they are effectively
  immutable, so this is not a decision to make casually or silently default.
  Do not invent an answer here; flag it on the tracking issue if you hit it.

## 5. The `tank-lab-to-nwb` dependency

The actual NWB-writing code — `TowersNWBConverter` and all its per-modality
`DataInterface`s — lives in a separate repository, `tank-lab-to-nwb`, not in
this one. This repo only builds `source_data` dicts and calls into it
(`u19_pipeline/nwb_export/conversion.py:264-276`).

This is an operational hazard worth taking seriously:

- **The live branch is `building-nwb-converter`**, not `main`. `main` is
  stale and known broken (its `__init__` references `se.` without importing
  the module it comes from). Whatever branch you install must match the
  `TowersNWBConverter(source_data, sync_timestamps=...)` signature the
  handler calls — check the branch's `towersnwbconverter.py` signature
  against `conversion.py:273-276` if conversion starts failing with a
  `TypeError` on construction.
- **It is path-installed and version-unpinned.** Nothing in this repo
  records a commit SHA, tag, or lockfile entry for `tank-lab-to-nwb`; it is
  installed as a local editable package
  (`pip install -e /path/to/tank-lab-to-nwb-clean`, per the docstrings in
  `scripts/run_nwb_export.py:26-28` and the CLI's own `ImportError` message
  at `:202-206`). Two clones of this repo pointed at different
  `tank-lab-to-nwb` checkouts can silently behave differently.
- **Installing it needs `--no-deps`.** `tank-lab-to-nwb`'s own
  `pyproject.toml` declares
  `[tool.uv.sources]` entries pointing at sibling directories —
  `../ndx-tank-metadata-clean` and `../U19-pipeline_python` — that assume a
  specific multi-repo checkout layout next to it. In a worktree (or any
  layout that doesn't have those exact sibling paths), a plain
  `uv pip install -e <path>` fails trying to resolve those sources. Install
  it with `uv pip install --no-deps -e <path>` and separately bring in the
  NWB/neuroconv stack it needs
  (`neuroconv[kilosort,openephys,spikeglx]`, `pynwb>=3`, `hdmf>=4`,
  `spikeinterface`, `ndx-tank-metadata`, ...) — see
  `docs/HANDOFF_nwb_imaging_export.md` for a working incantation.
- **The two repos must move in lockstep.** Any change to the `source_data`
  keys or converter signature on either side breaks the other silently at
  runtime (an unexpected key, or a missing one, doesn't raise until
  conversion actually runs). There is no CI cross-check between them today.
- **It isn't pinned today because the converter is still under active,
  fast-moving development** for this integration (imaging support, the
  per-interface-timestamps fix, the MATLAB-hard-dependency fix below are
  all landing on it concurrently as of this writing). Pin it — to a specific
  commit SHA via `uv`'s git source syntax, or vendor a lockfile entry — once
  the imaging work stabilizes; until then a moving pin would just have to be
  bumped constantly and would give false confidence.

**Known constraint to work around until fixed upstream:**
`VirmenDataInterface` (in `tank-lab-to-nwb`) calls
`convert_function_handle_to_str` (ported into this repo too, at
`u19_pipeline/utils/matlab_utils.py:145`) to shell out to MATLAB and convert
a few metadata fields (`experiment_name`, `protocol_name`, `trial_choice`,
`trial_type`) that were stored as MATLAB function handles. That helper
**raises if MATLAB is not on `PATH`**, which takes down the *entire*
conversion — even though all four values it produces are already treated as
optional by their only caller. A fix (making the helper warn and return `{}`
instead of raising) is planned for `tank-lab-to-nwb`, but until it lands, any
export host without a MATLAB installation cannot run a conversion at all,
regardless of whether the session actually needs those four fields.

## Reference: relevant files

| Purpose | File |
|---|---|
| Status enum | `u19_pipeline/nwb_export_enums.py` |
| Transition table | `u19_pipeline/nwb_export/state_machine.py` |
| Handler / stage logic | `u19_pipeline/automatic_job/nwb_export_handler.py` |
| Cronjob entry point | `u19_pipeline/automatic_job/cronjob_nwb_export.py` |
| CLI entry point | `scripts/run_nwb_export.py` |
| Shared conversion logic | `u19_pipeline/nwb_export/conversion.py` |
| Validators / size estimators | `u19_pipeline/nwb_production_utils.py` |
| Schema / job submission API | `u19_pipeline/nwb_production.py` |
| DANDI upload client | `u19_pipeline/nwb_export/dandi/upload_client.py` |
| Imaging-behavior sync mechanism | `docs/imaging_behavior_sync.md` |
| Imaging wiring handoff notes | `docs/HANDOFF_nwb_imaging_export.md`, `docs/nwb_imaging_export_plan.md` |
