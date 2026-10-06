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

from typing import Set

from u19_pipeline.nwb_export.errors import NwbStatusTransitionError
from u19_pipeline.nwb_export_enums import NwbExportStatusEnum as _S

# ---------------------------------------------------------------------------
# Canonical transition table
# ---------------------------------------------------------------------------

ALLOWED_TRANSITIONS: dict[_S, Set[_S]] = {
    _S.QUEUED: {_S.DATA_VALIDATION},
    _S.DATA_VALIDATION: {_S.PROCESSING, _S.FAILED},
    _S.PROCESSING: {_S.VALIDATION, _S.FAILED},
    _S.VALIDATION: {_S.COMPLETED, _S.UPLOAD, _S.FAILED},
    _S.UPLOAD: {_S.UPLOADED, _S.FAILED},
    _S.UPLOADED: {_S.COMPLETED},
    # Terminal states — explicitly present with empty sets so they are not
    # accidentally omitted from the map.
    _S.COMPLETED: set(),
    _S.FAILED: set(),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def is_valid_transition(from_state: _S, to_state: _S) -> bool:
    """Return True if transitioning from *from_state* to *to_state* is allowed."""
    return to_state in ALLOWED_TRANSITIONS.get(from_state, set())


def assert_valid_transition(from_state: _S, to_state: _S) -> None:
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
