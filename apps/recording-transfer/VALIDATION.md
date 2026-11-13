# Validation record

Pre-PR validation used isolated temporary directories and synthetic recording
files. No deployment has occurred on a recording rig and no real release was
published. CI will repeat the application checks on Windows.

The application suite contains 64 tests covering Windows Python 3.12 and 3.14.
Repository lint passes, including explicit checks of the newly added files.
Actual release environments were exercised on Python 3.12:
the transfer/GUI/cleanup suite, update locking in a separate process, checksums,
archive path checks, failed-install retry, offline launch fallback, rollback, and
preservation of machine configuration and queue state.

The real installation smoke test simulates GitHub API/asset transport, installs
0.1.0, builds a temporary 0.1.1 fixture,
updates, rolls back, rejects altered release metadata, and runs a verified GUI
copy from the installed package. These are temporary test releases, not published
lab releases. The actual public GitHub Releases API was read successfully without
authentication. PowerShell setup was exercised in local scratch directories;
task commands were previewed, never installed. Publication uses a fake gh CLI to
test tag verification and draft/pilot/stable behavior without write operations.

## Broader pipeline baseline

`uv run --no-sync pytest -rs -m 'not db'` on this Windows host produced **621 passed,
6 skipped, 3 failed**. All three failures were reproduced from a clean `git archive`
snapshot of untouched master `372a626d` using the same environment:

- `TestGetJobErrorInfo::test_error_from_stderr`: Windows backslashes versus the
  implementation's POSIX log path in the assertion.
- `TestGetJobErrorInfo::test_empty_stderr_falls_back_to_stdout`: same path assertion.
- `TestDescribeSlurmJob::test_signal_is_named`: Windows has no POSIX SIGKILL enum.

These existing cluster-test portability failures are recorded rather than folded
into this application PR. Root pipeline implementation/tests were not modified.
Skipped tests require a configured database or optional scientific-processing
dependencies absent from this host. GitHub's existing pipeline workflow runs its
normal Linux matrix; the new app workflow explicitly tests Windows.

## Local integration review

Release-workflow review identified that `gh release create --target` does not
validate an already-existing tag. Publication now resolves an existing tag to its
commit and rejects a mismatch before creating/uploading a release. Missing tags
are explicitly created at the validated build commit, and release creation uses
`--verify-tag`. Mock-GitHub CLI tests exercise missing/matching/mismatching tags
and pilot/stable flags without publishing anything.

Pre-PR review found three additional transfer-integrity defects in the uncommitted
application. GitHub issue creation was attempted again and rejected with
`Resource not accessible by personal access token (createIssue)`; the user's
existing authorization to log locally is used here.

**Reported errors (agent reproductions, no user-pasted error)**
```
Unreadable nested folder: run_pending returned 0 errors, Complete, and a manifest omitting unreadable data.
Existing .transfer-partial hardlink: the original source was truncated to zero bytes before verification failed.
Destination junction: transfer failed after overwriting an unrelated file outside the approved destination.
```
**Regression tests:** `tests/test_transfer_integrity.py` exercises all three using
synthetic files and an NTFS junction on Windows. **Root cause:** suppressed scan
errors in `Path.rglob`, a predictable partial path opened with `wb`, and destination
link validation occurring only after copy writes. **Linked PR:** the recording
transfer mega PR; no issue number is invented.

The user authorized local issue logging in this work because GitHub issue creation
was rejected by the configured token (HTTP 403). While integrating multiple source
roots, review identified that cleanup's single-root checker would report queue
entries belonging to another approved root as failures. The installed application
must scope cleanup to the selected approved root while retaining strict validation
for candidate recordings. A regression check covers mixed-root queues. Standalone
cleanup retains its strict root-mismatch reporting by default.

**Reported error (agent regression test, no user-pasted error)**
```
TypeError: cleanup() got an unexpected keyword argument 'skip_outside_root'
```
**Regression test:** `tests/test_recording_options.py::test_installed_cleanup_scopes_a_mixed_root_queue`
failed before the scoped cleanup implementation. **Root cause:** the single-root
cleanup API had no way for the installed multi-root launcher to omit other roots'
entries. **Linked PR:** none; local issue logging was authorized and this work has
not been committed or published.

The first test invocation also selected the old MATLAB working directory because
`uv run --project` does not change the shell's working directory. Use the documented
`uv --directory apps/recording-transfer run pytest` command. No unrelated MATLAB
tests or code were changed. An archive-test fixture was corrected for Python's
Windows ZIP filename normalization; archive validation itself was unchanged.
