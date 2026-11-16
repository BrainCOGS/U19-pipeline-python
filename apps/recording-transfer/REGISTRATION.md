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

Add this section to the installed machine.json, using real system/rig identifiers.
The referenced DataJoint JSON holds the host, username, TLS and schema-prefix
settings; passwords are obtained from Windows Credential Manager:

```json
"registration": {
  "enabled": true,
  "dj_config": "C:\\BrainCOGS\\private\\dj_local_conf.json",
  "system": "182-recording",
  "behavior_rigs": ["182-Imaging-Rig1"]
}
```

Open **Logins…**, select Database, enter the researcher's database username and
password, then **Test and save login**. A failed test leaves the previous saved
login untouched. A successful save removes `database.password` from that local
JSON file and retains the other settings. Existing copies/backups of an old
configuration must be handled separately. Plaintext passwords in config files
are no longer used by registration or catalog export. TLS is required by default;
an existing explicit `database.use_tls` setting is honored.

The same dialog supports **Storage share** for a configured UNC incoming path,
such as `\\cup.pni.princeton.edu\braininit\Data\Incoming`. Use the share account
(for example `DOMAIN\username`), which can differ from the database account.
Saved storage credentials are reused for incoming and final directories on that
same UNC share. Other shares need their own login; mapped drive letters continue
to use the Windows session's existing access. If no storage credential is saved,
the app uses the current Windows session normally. It never disconnects existing
share connections to replace another account; resolve Windows error 1219 in Windows.

The account bar below the BrainCOGS header shows one active researcher. On rigs
with registration enabled, the saved database login identifies that researcher;
copy-only UNC rigs use the saved storage login. Database and storage credentials
can differ because they authenticate different services, not two app sessions.
Local-only demonstrations do not need an account.

Use **Change user** to sign out before another researcher signs in. Saving a
different username over an existing login is rejected. **Change password** keeps
the username fixed and tests/replaces its saved password; it does not change the
password on the database or storage server. Account changes hold the same rig
transfer lock as copying and are disabled while a transfer is running.

**Log out** removes the configured database and storage vault entries, leaves
queued recordings in place, and blocks manual transfers, scheduled transfers,
registration and cleanup until sign-in. New jobs record their owner and can only
run or be cleaned up while that owner is signed in. Older jobs without an owner
remain compatible but still require a signed-in user. Nightly work is deliberately
paused after logout; leave the intended researcher signed in for unattended work.

**Forget saved login** for the primary account also signs out. Logout attempts to
close the app's saved storage connection without forcing other applications to
close files. If Windows reports open files or another problem, the app remains
signed out and reports how to disconnect it in Windows. Connections for which the
app has no saved credential are not disconnected. No legacy config backups are
erased. This uses the [native non-forced disconnect API](https://learn.microsoft.com/en-us/windows/win32/api/winnetwk/nf-winnetwk-wnetcancelconnection2w).
Password fields are
masked and cleared after submission/cancel; login failures report safe messages.
There is no plaintext fallback store. Passwords must exist in process memory while
authenticating; the app does not persist them in its queue, manifests or logs.

The native implementation calls Windows `CredWriteW`, `CredReadW`, `CredDeleteW`
and `WNetAddConnection2W` directly through Python's standard-library ctypes.
Entries persist for the current Windows user on this computer. They are not
application-wide secrets shared across Windows accounts, and software running as
that same user can access them. Run scheduled tasks under the same Windows account
that saved the login. Missing/expired credentials stop unattended work with an
actionable message; the worker never opens a terminal password prompt.

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
uv run --extra registration u19-recording-login --machine-config C:\BrainCOGS\machine.json --kind database --username researcher
uv run --extra registration u19-recording-login --machine-config C:\BrainCOGS\machine.json --kind storage --username DOMAIN\researcher
uv run --extra registration u19-recording-register --machine-config C:\BrainCOGS\machine.json --source D:\NPX_DATA\jyanar\ya070_20260901_g0 --subject jyanar_ya070 --date 2026-09-01 --session-number 1
```

The login command requires an interactive terminal and uses a hidden password
prompt. It never accepts a password argument. Add `--forget` to remove an entry
without prompting for a password. The GUI's Logins dialog provides the same setup.
Catalog export uses the same saved database login for its configured host.

For CLI account changes, add `--change-user` to sign out before entering the new
login, or use `u19-recording-login --machine-config <path> --logout` to sign out
without opening the GUI. Primary-account `--forget` also signs out.

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
