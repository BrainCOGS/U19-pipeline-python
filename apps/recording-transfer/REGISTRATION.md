# Optional direct registration

Copy-only remains the default: queue a source folder, copy into incoming storage,
verify SHA-256 hashes and retain the original path and preferences in a manifest.
No database credentials or connection are required for that workflow.

The optional direct workflow combines the researcher's NPX script with the desktop
application's persistent queue, verified copies and deployment tools. It supports
ephys and imaging, explicit session/parameter selection and recovery after failures.
It uses the existing pipeline states and tables; no schema migration is needed.

## Enable on a pilot rig

Keep credentials outside the application, release assets, queue and catalog. Give
the acquisition account access to the raw-data share and only the necessary DB
privileges: reads of referenced lab, subject, action, acquisition and parameter
tables, plus inserts into recording.Recording, its BehaviorSession part and
recording.DefaultParams. Use a pilot/test database before enabling production writes.

Add this section to the installed machine.json, using real system/rig identifiers:

```json
"registration": {
  "enabled": true,
  "dj_config": "C:\\BrainCOGS\\private\\dj_local_conf.json",
  "system": "182-recording",
  "behavior_rigs": ["182-Imaging-Rig1"]
}
```

Use a recent read-only catalog snapshot to populate the GUI. Live preview validates
the selected session and effective parameter IDs against the database. A stale
catalog cannot bypass those checks. Imaging defaults prefer the named profile
when available in the catalog; explicit selections are validated and frozen when
approved. Changes to modality defaults after approval do not silently change a job.

## Researcher workflow

1. Select a source folder and an ingested behavior session. Subject/date matching
   narrows the session list. SpikeGLX metadata can suggest sessions by recording
   time, but never selects one automatically. Interval matches rank before start
   proximity; ambiguous or incompatible timestamps produce warnings.
2. Review processing parameters, then preprocessing parameters. Existing surgery
   information is required; this workflow does not create surgery/insertion rows.
   Leave Add surgery unchecked.
3. Click **Copy + register…**. A background read-only preview shows source,
   destination, subject, session and the actual parameter IDs. Confirmation defaults
   to No. Confirming authorizes registration at status 2, which allows the existing
   pipeline to begin ingestion and subsequent migration/processing.
4. The worker revalidates the plan, copies and verifies every source file, then
   inserts Recording, BehaviorSession and fragment-0 DefaultParams in one database
   transaction. The queue records the resulting recording_id and Registered state.

The final relative directory follows the existing GUI convention:

```text
<subject owner>/<subject fullname>/<YYYYMMDD>_g<behavior session number>/<source folder>
```

The source folder's SpikeGLX g-number is retained in its leaf; it is not used as
the behavior session number. The modality root comes from lab.DjCustomVariables
and boundary-aware Windows lab.Path mapping. The original local_directory and rig
location are preserved in Recording, as well as in the copy receipt.

An already completed copy-only queue entry can be explicitly upgraded. This
copies the original source again to the final path and retains the incoming copy;
it does not rename staging or delete either copy. If the local original has already
been deleted, this workflow cannot finalize that incoming copy. A website endpoint
for registering/finalizing staged data remains separate work.

## Command line

From this project directory, preview without copying or inserting:

```powershell
uv run --extra registration u19-recording-register --machine-config C:\BrainCOGS\machine.json --source D:\NPX_DATA\jyanar\ya070_20260901_g0 --subject jyanar_ya070 --date 2026-09-01 --session-number 1
```

Add `--yes` to copy and register that selected job. `--processing-id` and
`--preprocessing-id` override modality defaults; run `--help` for available options.
Duplicate behavior-session association is rejected unless explicitly approved with
`--allow-duplicate-session`. The CLI never processes unrelated queued jobs.

## Recovery and cleanup

Unowned existing final directories are rejected. Destination ownership receipts
and a shared filesystem lock protect concurrent application copies. Database named
locks serialize registrations for the same source, destination or session among
these clients. Legacy tools that ignore those locks remain outside this guarantee;
avoid registering the same data concurrently through MATLAB or other scripts.

Failed inserts roll back all three rows and retain verified files for retry. If a
commit succeeds but the local receipt fails, retry recognizes the exact existing
registration and re-verifies without overwriting registered files. Changed source,
destination, associations or parameters require operator review. Do not remove
ownership/completion sidecars during recovery.

Cleanup stays disabled by default. Registered jobs require the same inventory,
hash, age, machine identity and source-root checks as copy-only jobs. To permit
cleanup of originals registered directly, explicitly add the corresponding final
raw-data root to `cleanup.destination_roots`. The default incoming-only cleanup
scope cannot delete those originals. Preview before enabling deletion.

## Validation boundary

Automated tests exercise the actual registration adapter with in-memory table and
transaction doubles, real temporary file copies, GUI confirmation and failure
recovery. They do not prove production privileges or downstream ingestion behavior.
No production registration is part of the test suite. Pilot one representative
recording per modality against a test database/share before wider rollout.
