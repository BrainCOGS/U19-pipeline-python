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

### Imaging is wired differently

Imaging doesn't go through `build_source_data` and `TowersNWBConverter`. The
converter, `tank-lab-to-nwb`, has its own imaging conversion, which reads the
TIFFs and the behavior log, aligns them itself and checks the file it writes
against the sources. This repo only finds the inputs and calls it:

```text
process_data_validation          validate_imaging_data_exists (TiffSplit rows)
process_nwb_conversion
  with_imaging_modality          IMAGING_RAW -> "raw", IMAGING_PROCESSED -> "suite2p"
  run_conversion_to_file
    run_imaging_conversion
      resolve_imaging_tiffs      TiffSplit.File -> paths; originalStacks for per-ROI splits
      resolve_suite2p_dirs       ProcessingTask.processing_output_dir, finished runs only
      converter_metadata         experimenter, subject sex and date of birth from the DB
      tank_lab_to_nwb.imaging.convert.convert_session      raw TIFFs + behavior
      tank_lab_to_nwb.imaging.suite2p.convert_suite2p      Suite2p + behavior
```

## 3. Imaging specifics

**Alignment is the converter's.** It matches each I2C `[block, trial, iteration]`
packet in the TIFF headers to its logged iteration, fits a line from ViRMEn's
clock to ScanImage's, and puts every frame on the file's clock. Its method,
limits and the checks against the production MATLAB sync are in the
converter's `docs/imaging-timing.md`. Nothing here computes frame times.
`u19_pipeline/utils/imaging_behavior_sync.py` stays for the DataJoint sync
table; the export doesn't use it.

**What a file holds.** Behavior plus one acquisition: every field of view and
plane, each its own `TwoPhotonSeries` on its own frame times, and every frame,
including those before the first trial and after the last. The
`scanimage_frames` table labels each frame with its block, trial and iteration
as `SyncImagingBehavior` does, 0 for none. That includes trials imaging
started or stopped partway through, which the alignment table lists as
`labels.partly_imaged_trials`.

**Raw or Suite2p.** The job's imaging modality picks one, and both carry the
same frame times. Raw and processed together are two files, so they are two
jobs. The Suite2p conversion also needs the TIFFs: it proves that Suite2p's
registered frames are the raw pages it claims before writing any trace.

**Inputs.** `resolve_imaging_tiffs` takes the files `TiffSplit.File` lists. For
a mesoscope recording those are the lab's per-ROI copies (`ROI<r>_z<d>`
folders); when the recording folder still has `originalStacks`, the original
stack is used instead, because the copies lost the ScanImage settings and
clipped negative pixels. The converter refuses inputs it can't show are the
whole acquisition unless `export_parameters["allow_incomplete_imaging"]` is
set, and the file then says so. `fov_numbers` can't pick a subset of fields:
the converter writes them all.

**Overrides in `export_parameters`:** `tiff_paths`, `suite2p_dirs`,
`recording_ids` (exactly one), `paramset_idx` (when a split has Suite2p runs
under several parameter sets), `imaging_data` (`"raw"` or `"suite2p"`) and
`allow_incomplete_imaging`.

## 4. The clock convention

- **Imaging sessions use the ViRMEn behavior clock.** The converter starts the
  file at `log.session.start`, moved earlier by whole microseconds if imaging
  began before it, so that no frame has a negative time. The ViRMEn clock
  restarting within a session is refused.
- **Never align through wall clocks.** The ScanImage and ViRMEn PCs disagreed by
  0.15 to 0.96 s across the sessions checked. Alignment is content-based,
  through the I2C packets.
- **Ephys exports align behavior onto the ephys clock** (`BehaviorSync`, read
  in `query_metadata`). A job with both imaging and ephys is refused with
  `NotImplementedError`: one clock for both hasn't been decided. Decide and
  write that rule down before the first DANDI upload of such a session, since
  published timestamps are effectively permanent.

## 5. The `tank-lab-to-nwb` dependency

The NWB-writing code lives in `tank-lab-to-nwb`. It is the optional `nwb`
extra, pinned to a commit in `[tool.uv.sources]`:

```bash
uv sync --extra nwb
```

- **Bump the pin deliberately.** The export calls
  `tank_lab_to_nwb.imaging.convert.convert_session` and
  `tank_lab_to_nwb.imaging.suite2p.convert_suite2p` with
  `(log, tiffs, [suite2p,] output, metadata=..., allow_incomplete=...)`. Check
  those signatures when moving the pin, and rerun
  `tests/nwb_export/test_imaging_conversion.py`.
- **The converter supports Python 3.12 and 3.13 only**, so the extra is skipped
  on 3.14.
- **uv doesn't inherit a dependency's sources or constraints.** The pin
  therefore repeats the converter's `ndx-tank-metadata` git tag and its
  `zarr<3` constraint. Drop each when the converter drops it.
- **Behavior and ephys still go through `TowersNWBConverter`**, whose
  multi-modal version is being reworked in the converter. Its signature must
  match `run_conversion_to_file`.

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
| Imaging-behavior sync for the DataJoint table | `docs/imaging_behavior_sync.md` |
| Imaging alignment used by the export | `docs/imaging-timing.md` in `tank-lab-to-nwb` |
