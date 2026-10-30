# Quickstart: Test Planning and Minimum DB Checks

## Prerequisites

- Python 3.12+
- DataJoint configuration available for integration checks
- Access to U19 pipeline modules

## 1) Run fast local tests (no DB)

```bash
pytest -m no_db
```

## 2) Run DB-backed tests

```bash
pytest -m with_db
```

## 3) Minimum ephys readiness check (target subject)

Run a DB-backed check that verifies:
- subject `jyanar_ya014` exists,
- at least 2 ephys sessions exist,
- one ephys session is on `2024-07-22`.

Pseudo-query pattern:

```python
subject_ok = bool(subject.Subject & "subject_fullname='jyanar_ya014'")
ephys_rows = (
    acquisition.Session * recording.Recording.EphysSession
    & "subject_fullname='jyanar_ya014'"
).fetch("session_date")

count_ok = len(ephys_rows) >= 2
date_ok = any(str(d) == "2024-07-22" for d in ephys_rows)
passed = subject_ok and count_ok and date_ok
```

## 4) Scope note

- Imaging-session checks are intentionally excluded from the minimum gate in this phase.
- Imaging validation will be added in a later iteration.

## 5) Expected pass output format

- `✓ Subject found: jyanar_ya014`
- `✓ Ephys sessions found: N`
- `✓ Required ephys date found: 2024-07-22`
- `✓ Minimum DB ephys readiness: PASS`
