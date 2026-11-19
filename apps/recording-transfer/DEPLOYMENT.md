# Rig installation, GitHub updates and releases

The maintained application is `BrainCOGS/U19-pipeline-python/apps/recording-transfer`.
It has an independent package, frozen dependency lockfile, tests and release tools;
installing it does not install the cluster pipeline. The earlier MATLAB-repo demo
is an experimental snapshot. Make future application changes in this project.

## Distribution model

All application distribution and update discovery use **public GitHub Releases**.
Rigs need internet and uv, but no Git checkout, GitHub login, token, or software
release share on cup. Recording data still goes to the configured braininit share.
Never publish machine settings, catalogs containing private data, or DB credentials
as release assets.

Each release uses a tag such as `recording-transfer/v0.1.0`. The updater lists
GitHub releases and filters this exact application namespace; it never uses the
repository-wide latest release. Drafts and malformed version tags are ignored.
Stable rigs accept published full releases; pilot rigs accept both full releases
and prereleases. The highest eligible numeric version wins. Optional
`github.pinned_version` fixes a rig to one eligible version. A prerelease can be
promoted to stable by changing its GitHub prerelease flag; the same tag, ZIP,
checksums and installed version are retained.

The permanent desktop shortcut launches `launch.ps1`, which reads `active.json`
and starts an already-installed version. Researchers never edit shortcuts or run
Git commands to receive features. Normal launches do not require internet access.

```text
%LOCALAPPDATA%\BrainCOGS\U19RecordingTransfer\
  launch.ps1                  stable shortcut/task entry point
  bootstrap.py                standard-library launcher/updater, API 1
  bootstrap-python.txt        absolute uv-managed Python 3.12 path
  tasks.ps1                   preview/install scheduled tasks
  machine.json                local settings, preserved by updates
  active.json                 current and previous successful version
  runtime.lock                excludes concurrent GUI/copy/cleanup/update
  versions\0.1.0\.venv\       permanent, non-editable release environment
  versions\0.1.1\.venv\       next environment; old versions are preserved
  state\queue.json            shared persistent transfer queue
  logs\launcher.log           bounded launcher/update logs
  logs\install-<version>.log  dependency installation output
  logs\application.log        GUI/copy/cleanup output, rotated between runs
  last_error.txt              most recent launcher failure
  failed\                     preserved incomplete installation attempts
```

## One-time pilot installation

1. Install uv through the lab's approved software process. Choose the Windows
   account used for acquisition, copying and scheduled tasks. Use the same account
   throughout and confirm its braininit UNC access.
2. Download the setup ZIP and its SHA-256 file from the selected application's
   GitHub Release, and verify the checksum with `Get-FileHash`. Extract the ZIP to
   a setup directory. Do not use GitHub's automatically generated source archives
   as the application artifact; the release assets include the required lockfile
   and bootstrap bundle.
3. Copy `machine.example.json` to a local file and enter the real `source_roots`,
   `incoming_root`, `modality`, and optional `catalog_path`. Set `github.channel`
   to `pilot` initially. Keep cleanup disabled and automatic update-on-launch off.
4. Run setup as that normal Windows user using absolute paths:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File C:\Setup\U19Transfer\setup.ps1 -MachineConfig C:\Setup\U19Transfer\182-recording.json -Initialize -DesktopShortcuts
```

Setup prepares Python 3.12, copies the stable launcher, discovers the eligible
GitHub release, installs it and creates application/update/rollback shortcuts.
It previews scheduled tasks without installing them. No transfer or deletion is
started. `-ExecutionPolicy Bypass` applies to this PowerShell process only.
The first install requires a published application release; the repository's
other releases cannot satisfy it.

Setup refuses to overwrite an existing `machine.json`. After a failed initial
install, correct configuration/connectivity and use `launch.ps1 -Mode update`.
Do not reinstall over the state directory. Python and uv maintenance are separate
operator tasks; do not remove the managed interpreter referenced by the launcher.
The project supports Python 3.12 through 3.14; installed rigs default to 3.12.

Each rig has its own local configuration; example UNC paths are not claims about
actual storage layout. The GUI constrains source folders and incoming storage to
those configured roots. Its title displays the installed application version.

## Researcher workflow and current boundaries

Select a recording folder, optionally choose its session and parameters, then
Copy now or Queue for later. Keep the GUI open while copying. Normal transfer
preserves originals. Surgery starts unchecked; processing appears before
preprocessing; the named imaging default is selected only when present in the
loaded catalog. Subject/date matches restrict sessions; no match shows all loaded
sessions. Choices travel in the completion manifest and still require website
confirmation. This app does not write registrations to DataJoint or move staged
files into final registered paths.

The read-only catalog exporter is optional and separate from the deployed GUI.
Credentials remain outside releases. Refresh the catalog to see recent sessions.
The nighttime copier handles queued folders only, not automatic discovery of all
recordings. Quiet time is a heuristic, not an acquisition-completion signal.
Interrupted large files restart from the beginning; verified complete files are
reused. Scans fail on unreadable folders rather than silently omitting them.
Existing linked source/destination paths are rejected before writes, and temporary
files are exclusively created with unpredictable names.

## Scheduling

After a pilot copy succeeds:

```powershell
$app = "$env:LOCALAPPDATA\BrainCOGS\U19RecordingTransfer"
powershell -NoProfile -ExecutionPolicy Bypass -File "$app\tasks.ps1" -Install
```

The tasks call the same stable launcher after every update:

| Time | Operation | Default behavior |
| --- | --- | --- |
| 22:30 | Update | Discover an eligible GitHub Release and install if newer |
| 23:00 | Copy | Process queued recordings |
| 06:00 | Cleanup preview | Verify retained copies and report candidates; no deletion |

Tasks run as the current user with an interactive token. The account must remain
logged in; a locked desktop is fine. StartWhenAvailable handles missed triggers.
Logged-out operation requires an IT-configured account/task with network access;
this installer does not collect passwords. An open GUI holds the runtime lock,
so updates skip rather than switching beneath it. Close the GUI during the
maintenance window or use the Update shortcut after closing it. Busy or quiet
folders wait for a later invocation. No scheduled task is installed by default.

Manual operations:

```powershell
& "$app\launch.ps1" -Mode status
& "$app\launch.ps1" -Mode update
& "$app\launch.ps1" -Mode copy
& "$app\launch.ps1" -Mode cleanup-preview
& "$app\launch.ps1" -Mode rollback
```

Optional `auto_update_on_launch=true` checks GitHub before opening the GUI; API,
network or install failures fall back to the installed version. It defaults off
so launch does not wait for a network request. API failures/rate limits leave the
current version active and are logged. Release pagination is bounded at 1,000
repository releases and fails closed rather than making an incomplete selection.

## Update and rollback guarantees

Discovery validates tag/version, manifest identity, asset URLs and checksums.
Assets must come from that release in the configured GitHub repository. The
updater verifies the ZIP SHA-256, rejects unsafe/duplicate archive paths, links
and oversized archives, and installs into a permanent version directory using
`uv sync --frozen --no-dev --no-editable`. It checks installed version and runs an
actual offscreen Qt/asset health check before atomically switching `active.json`.
Failed installs do not change the active pointer; retries preserve incomplete
attempts in `failed/`. Environments never move after successful installation.

Rollback checks the previous installed environment and swaps the pointer without
a network request. To prevent reinstallation of a bad version, pin
`github.pinned_version` to the previous approved version (and retain the appropriate
channel), then roll back. Downgrades through ordinary update checks are refused.
`bootstrap.py --root <install> update --allow-downgrade` is an explicit operator
option for a pinned older version. No update reverses copied/deleted data.

Configuration/queue schema is 1. The updater never resets or migrates shared
state, and requires a matching manifest schema. Future incompatible migrations
need explicit backup/migration design and launcher compatibility; changing a
number in the manifest is insufficient. Launcher API changes require an operator
maintenance step preserving machine settings, queue and installed versions.
App ZIPs do not silently overwrite the stable bootstrap.

## Maintainer release process

Work in `apps/recording-transfer`. Bump `pyproject.toml` and
`src/u19_recording_transfer/__init__.py`, refresh `uv.lock`, and run:

```powershell
uv sync --locked
uv run pytest
uv run u19-recording-transfer --self-test
uv run python tools/smoke_deployment.py --python 3.12
uv run python tools/build_release.py --out dist
```

The application ZIP is deterministic and includes source, assets and the lockfile.
The setup ZIP contains the stable scripts, example config and this guide. CI tests
Windows Python 3.12 and 3.14, including real side-by-side install/update/rollback.
The dedicated release workflow runs only when manually dispatched on master and
requires these checks before a separate contents-write publication job.

After merging the PR and version change:

```powershell
gh workflow run recording-transfer-release.yml -R BrainCOGS/U19-pipeline-python -f version=0.1.0 -f channel=pilot
```

The workflow validates version identity, builds assets, creates the namespaced
release as a draft, uploads all assets, then publishes it as a prerelease. It does
not change the repository-wide latest release marker. The maintainer can also
choose `channel=stable` for a directly approved full release. Enable GitHub's
immutable-release protection in repository settings where available.

After testing the pilot, promote the SAME release without rebuilding:

```powershell
gh release edit recording-transfer/v0.1.0 -R BrainCOGS/U19-pipeline-python --prerelease=false --latest=false
```

Stable rigs then discover it automatically. An existing published version is never
replaced. If publication fails while a draft exists, inspect and remove only that
failed draft before retrying; do not overwrite a published release. Do not dispatch
this workflow from an unmerged feature branch. A release's source commit and
assets remain linked for audit and rollback.

## Migration and cleanup

To import the earlier demo queue: close its GUI, stop its tasks, back up
`%LOCALAPPDATA%\U19\RecordingTransfer\queue.json`, then copy queue.json only into
the new empty state directory with the installed app closed. Never overwrite a
nonempty queue or copy lock files. Inspect all source/destination paths against
the machine settings before resuming. Preserve the original queue for recovery.

The 06:00 task is always preview-only. Actual deletion requires deliberate
`cleanup.enabled=true` AND a separately configured task calling `-Mode cleanup`.
It checks approved roots, exact source/copy file sets, sizes, fresh SHA-256 hashes,
PC/transfer identity and completion manifests after a default 14-day retention.
Missing/changed copies, unreadable folders or links prevent deletion. Only this
app's completed transfers are eligible; legacy MATLAB recordings are not scanned.
Moved incoming folders need reconciliation. Verification proves a copy, not website
registration. Run outside acquisition: concurrent mutation cannot be transactional,
and interruption mid-deletion can leave a partial local folder for inspection.

## Rollout checklist

1. Merge the application/CI PR; publish a pilot release through GitHub Actions.
2. Install one pilot rig with real storage/catalog settings and deletion disabled.
3. Validate several recording days, offline launch, updates, rollback and previews.
4. Enable update/copy/cleanup-preview tasks under the rig's actual Windows account.
5. Promote the exact tested release, then install other rigs on the stable channel.
6. Consider actual deletion only after retention and previews are approved.
