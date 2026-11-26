"""
NWB export status transition state machine.

Encodes the valid state transitions from data-model.md:

    QUEUED          → DATA_VALIDATION
    DATA_VALIDATION → PROCESSING | FAILED
    PROCESSING      → VALIDATION | FAILED
    VALIDATION      → COMPLETED | UPLOAD | FAILED
    UPLOAD          → UPLOADED  | FAILED
    UPLOADED        → COMPLETED

Terminal states (no outgoing transitions):
    COMPLETED, FAILED

Usage::

    from u19_pipeline.nwb_export.state_machine import (
        ALLOWED_TRANSITIONS,
        is_valid_transition,
        assert_valid_transition,
    )

    assert_valid_transition(NwbExportStatusEnum.QUEUED, NwbExportStatusEnum.DATA_VALIDATION)
"""

from __future__ import annotations


from u19_pipeline.nwb_export.errors import NwbStatusTransitionError
from u19_pipeline.nwb_export_enums import NwbExportStatusEnum as Status

# ---------------------------------------------------------------------------
# Canonical transition table
# ---------------------------------------------------------------------------

ALLOWED_TRANSITIONS: dict[Status, set[Status]] = {
    Status.QUEUED: {Status.DATA_VALIDATION},
    Status.DATA_VALIDATION: {Status.PROCESSING, Status.FAILED},
    Status.PROCESSING: {Status.VALIDATION, Status.FAILED},
    Status.VALIDATION: {Status.COMPLETED, Status.UPLOAD, Status.FAILED},
    Status.UPLOAD: {Status.UPLOADED, Status.FAILED},
    Status.UPLOADED: {Status.COMPLETED},
    # Terminal states — explicitly present with empty sets so they are not
    # accidentally omitted from the map.
    Status.COMPLETED: set(),
    Status.FAILED: set(),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def is_valid_transition(from_state: Status, to_state: Status) -> bool:
    """Return True if transitioning from *from_state* to *to_state* is allowed."""
    return to_state in ALLOWED_TRANSITIONS.get(from_state, set())


def assert_valid_transition(from_state: Status, to_state: Status) -> None:
    """Raise :exc:`NwbStatusTransitionError` if the transition is not allowed.

    Args:
        from_state: Current export job status.
        to_state:   Desired next status.

    Raises:
        NwbStatusTransitionError: When the transition is illegal.
    """
    if not is_valid_transition(from_state, to_state):
        raise NwbStatusTransitionError(
            from_state=from_state.name,
            to_state=to_state.name,
        )
