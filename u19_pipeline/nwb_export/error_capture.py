"""
Standardized failure capture with traceback persistence (T047 / US6).

Converts Python exceptions into the error-info dictionaries used by
``update_job_status`` in ``nwb_production.py``:

    {
        "error_message": str,       # Short summary for display
        "error_exception": str,     # Full traceback for debugging
    }

Usage::

    from u19_pipeline.nwb_export.error_capture import capture_exception

    try:
        do_something_risky()
    except Exception as exc:
        error_info = capture_exception(exc)
        update_job_status(job_key, NwbExportStatusEnum.FAILED, **error_info)
"""

from __future__ import annotations

import traceback
from typing import Dict, Optional

_MAX_TRACEBACK_CHARS = 4096  # matches NwbExportLogStatus.error_exception column width
_MAX_MESSAGE_CHARS = 512  # matches NwbExportLogStatus.error_message column width


def capture_exception(exc: BaseException) -> Dict[str, Optional[str]]:
    """Convert an exception into a storable error-info dict.

    Args:
        exc: The exception to capture.

    Returns:
        Dict with keys ``error_message`` and ``error_exception``, both
        truncated to fit the corresponding DataJoint column widths.
    """
    message = _truncate(str(exc), _MAX_MESSAGE_CHARS)
    tb = _truncate(
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        _MAX_TRACEBACK_CHARS,
    )
    return {
        "error_message": message,
        "error_exception": tb,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    suffix = "... [truncated]"
    return text[: max_chars - len(suffix)] + suffix
