# Scheduling source selections from Streamlit

The Streamlit NWB Export page writes to the 001 worker's existing `NwbExportJob`,
`NwbExportModality`, and `NwbExportLogStatus` tables in one transaction. A unique
request identifier makes retries of the same submission idempotent. The worker
uses the same status transitions, validation table and polling service as legacy
exports. The GUI shows the signed-in user's recent jobs, output paths and errors.

## Queue setup

1. Stop existing NWB workers before changing an existing schema. With DataJoint
   configured for the intended database, preview the in-place upgrade.

   ```sh
   uv run --python 3.13 --extra nwb python -m scripts.upgrade_nwb_source_queue --schema u19_nwb_production
   ```

   Repeat with `--apply` to make the change. It preserves job IDs, session values,
   modality rows, history and validation records, removes only the session foreign
   key, and adds nullable request fields. For a fresh schema, import
   `u19_pipeline.nwb_production` to create the tables instead.

2. Start the updated worker with `uv run --python 3.13 --extra nwb python -m
   u19_pipeline.automatic_job.cronjob_nwb_export_enhanced`. It must be able to
   read `/mnt/cup/braininit/Data` and write its `Processed/NWB` subdirectory.
3. Deploy the scheduler with the same DataJoint database and a read-only mount.
   Deploy the updated worker before accepting source selections. Older workers
   do not understand the source snapshot.

## Source snapshot

`request_id` is a UUID without separators and determines the output filename under
`Processed/NWB`. `source_request` holds a version-1 dictionary with `source`,
`behavior_path`, and optional `recording` (`modality`, `path`, optional
`recording_id`). Registered jobs store the session key in the request and job
columns. Manual jobs leave all three session columns null. The worker rechecks
the registered session and the exact recording link, and uses the selected
paths rather than searching for another recording.

The behavior log is required for these Towers conversions. Imaging uses the
selected ScanImage TIFF or the TIFFs at the top level of the folder, preferring
`originalStacks` when present. Electrophysiology uses the selected SpikeGLX run
with synchronized raw AP and available LF data. The worker checks input size
against available space and publishes the file after the converter completes.
Existing queue clients can continue submitting session-based jobs. The source
picker schedules local NWB outputs. DANDI settings and processed-job selection
remain in the legacy workflow.

Manual sources are available to signed-in lab users under the shared Data root.
The root check is a filesystem boundary. DataJoint ownership checks apply to
registered selections. Manual sources may contain work without discoverable
ownership, and lab storage permissions should reflect who may export that work.
The worker checks alignment and rechecks paths when it runs, so missing or
incompatible inputs result in a failed job. It does not make immutable copies
of the source data.
