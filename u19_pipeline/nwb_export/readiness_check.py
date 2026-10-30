"""
Minimum DB ephys readiness check (T031 / US2).

Implements the output structure from:
  specs/001-nwb-export-handler/contracts/minimum-db-ephys-readiness.md

Checks that a subject has a minimum number of ephys sessions and that a
required session date is among them.  Used before triggering full NWB export
to ensure source data is available.

Usage (with_db)::

    from u19_pipeline.nwb_export.readiness_check import check_ephys_readiness

    result = check_ephys_readiness(
        subject_fullname="jyanar_ya014",
        required_session_date="2024-07-22",
        min_ephys_sessions=2,
    )
    if result.passed:
        print("Ready to export")
    else:
        for msg in result.messages:
            print(msg)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

# ---------------------------------------------------------------------------
# Output contract type
# ---------------------------------------------------------------------------


@dataclass
class EphysReadinessResult:
    """Structured result matching the minimum-db-ephys-readiness.md output contract.

    Attributes:
        subject_exists:        True if subject found in database.
        ephys_session_count:   Number of ephys sessions found.
        required_date_present: True if *required_session_date* is in the sessions.
        required_session_date: The ISO date string that was checked.
        ephys_session_dates:   All ephys session dates found for the subject.
        imaging_checked:       Always False in this phase (non-goal).
        passed:                True when all pass rules are satisfied.
        messages:              Human-readable list of pass/fail messages.
    """

    subject_exists: bool
    ephys_session_count: int
    required_date_present: bool
    required_session_date: str
    ephys_session_dates: List[str]
    imaging_checked: bool = False
    passed: bool = False
    messages: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def check_ephys_readiness(
    subject_fullname: str,
    required_session_date: str,
    min_ephys_sessions: int = 2,
) -> EphysReadinessResult:
    """Run the minimum DB ephys readiness check for a subject.

    Performs three queries against the DataJoint pipeline:
    1. ``subject.Subject`` — verifies subject exists.
    2. ``acquisition.Session * recording.Recording.EphysSession`` — lists
       all ephys sessions for the subject.
    3. Date inclusion check — verifies *required_session_date* is present.

    Args:
        subject_fullname:     Subject identifier (e.g. ``'jyanar_ya014'``).
        required_session_date: ISO-8601 date string (e.g. ``'2024-07-22'``).
        min_ephys_sessions:   Minimum number of ephys sessions required.

    Returns:
        :class:`EphysReadinessResult` with all output contract fields populated.
    """
    from u19_pipeline import acquisition, recording
    from u19_pipeline import subject as subj_module  # type: ignore

    messages: List[str] = []

    # ------------------------------------------------------------------
    # 1. Subject existence
    # ------------------------------------------------------------------
    subject_exists = bool(subj_module.Subject & {"subject_fullname": subject_fullname})
    if subject_exists:
        messages.append(f"PASS — subject '{subject_fullname}' found in database")
    else:
        messages.append(f"FAIL — subject '{subject_fullname}' not found in subject.Subject")

    # ------------------------------------------------------------------
    # 2. Ephys session count
    # ------------------------------------------------------------------
    ephys_rows = (
        acquisition.Session * recording.Recording.EphysSession & {"subject_fullname": subject_fullname}
    ).fetch("session_date", as_dict=False)

    # session_date may arrive as datetime.date objects; normalise to ISO string
    ephys_session_dates = sorted(str(d) for d in ephys_rows)
    ephys_session_count = len(ephys_session_dates)

    if ephys_session_count >= min_ephys_sessions:
        messages.append(f"PASS — {ephys_session_count} ephys session(s) found (required ≥ {min_ephys_sessions})")
    else:
        messages.append(f"FAIL — only {ephys_session_count} ephys session(s) found (required ≥ {min_ephys_sessions})")

    # ------------------------------------------------------------------
    # 3. Required date inclusion
    # ------------------------------------------------------------------
    required_date_str = str(required_session_date)
    required_date_present = required_date_str in ephys_session_dates

    if required_date_present:
        messages.append(f"PASS — required session date {required_date_str} is present")
    else:
        messages.append(
            f"FAIL — required session date {required_date_str} not found in ephys sessions: {ephys_session_dates}"
        )

    # ------------------------------------------------------------------
    # Overall pass/fail
    # ------------------------------------------------------------------
    passed = subject_exists and ephys_session_count >= min_ephys_sessions and required_date_present

    return EphysReadinessResult(
        subject_exists=subject_exists,
        ephys_session_count=ephys_session_count,
        required_date_present=required_date_present,
        required_session_date=required_date_str,
        ephys_session_dates=ephys_session_dates,
        imaging_checked=False,
        passed=passed,
        messages=messages,
    )
