"""
NWB output metadata completeness validator (T054 / US4 / FR-022).

Checks that all fields listed in ``REQUIRED_NWB_METADATA_FIELDS`` are present
and non-empty in the session metadata dict extracted from a generated NWB file.

Usage::

    from u19_pipeline.nwb_export.output_validation.metadata_validator import (
        validate_metadata_completeness,
    )

    result = validate_metadata_completeness(metadata_dict)
    if not result.passed:
        print("Missing:", result.missing_fields)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from u19_pipeline.nwb_export.config import REQUIRED_NWB_METADATA_FIELDS

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class MetadataValidationResult:
    """Result of a metadata completeness check.

    Attributes:
        passed:         True when all required fields are non-empty.
        missing_fields: List of field names that are absent or empty.
        messages:       Human-readable list of pass/fail messages.
    """

    passed: bool
    missing_fields: List[str]
    messages: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_metadata_completeness(
    metadata: Dict[str, Any],
) -> MetadataValidationResult:
    """Check that all required NWB metadata fields are present and non-empty.

    Args:
        metadata: Dict of NWB session metadata (e.g. from ``nwbfile.__dict__``
                  or a pre-extracted mapping).

    Returns:
        :class:`MetadataValidationResult` with ``passed``, ``missing_fields``,
        and ``messages``.
    """
    missing: List[str] = []
    messages: List[str] = []

    for field_name in REQUIRED_NWB_METADATA_FIELDS:
        value = metadata.get(field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(field_name)
            messages.append(f"FAIL — required field '{field_name}' is absent or empty")
        else:
            messages.append(f"PASS — '{field_name}' present")

    passed = len(missing) == 0
    if passed:
        messages.append("PASS — all required metadata fields are present")

    return MetadataValidationResult(
        passed=passed,
        missing_fields=missing,
        messages=messages,
    )
