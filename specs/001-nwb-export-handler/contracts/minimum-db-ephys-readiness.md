# Contract: Minimum DB Ephys Readiness Check

## Purpose

Define the minimum DB-backed check required before implementing/running full NWB export workflow.

## Input Contract

```yaml
subject_fullname: string   # required, default: jyanar_ya014
required_session_date: string  # required, ISO date, default: 2024-07-22
min_ephys_sessions: integer # required, default: 2
```

## Query Contract

1. Subject existence query:
   - `subject.Subject & subject_fullname`
2. Ephys session query:
   - `acquisition.Session * recording.Recording.EphysSession & subject_fullname`
3. Date inclusion check:
   - Required date exists in fetched ephys-session dates.

## Output Contract

```yaml
subject_exists: boolean
ephys_session_count: integer
required_date_present: boolean
required_session_date: string
ephys_session_dates: [string]
imaging_checked: boolean   # always false in this phase
passed: boolean
messages: [string]
```

## Pass/Fail Rules

- PASS when all are true:
  - `subject_exists == true`
  - `ephys_session_count >= min_ephys_sessions`
  - `required_date_present == true`
- FAIL otherwise, with clear failure messages.

## Non-Goals (Current Phase)

- No imaging-session assertions.
- No behavior-session assertions.

