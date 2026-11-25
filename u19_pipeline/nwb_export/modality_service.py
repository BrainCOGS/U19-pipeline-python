"""
Modality parsing and normalization service for NWB export jobs (US1 / T019).

Converts user-supplied modality strings (e.g. "ephys-raw") into structured
:class:`ParsedModality` objects that the submission pipeline can consume.

Valid modality strings (case-insensitive):
    - ``behavior``          → towers-task behavioral data
    - ``ephys-raw``         → raw SpikeGLX ephys recordings
    - ``ephys-processed``   → Kilosort-processed ephys units
    - ``imaging-raw``       → raw calcium imaging stacks (ScanImage / TIFF)
    - ``imaging-processed`` → DF/F and ROI data

Usage::

    from u19_pipeline.nwb_export.modality_service import parse_modalities

    parsed = parse_modalities(["behavior", "ephys-raw"])
    for mod in parsed:
        print(mod.name, mod.modality_type, mod.numbers)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Set

from u19_pipeline.nwb_export.errors import NwbExportError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_MODALITIES: Set[str] = {
    "behavior",
    "ephys-raw",
    "ephys-processed",
    "imaging-raw",
    "imaging-processed",
}

# Canonical mapping: modality_string → (name, modality_type)
_MODALITY_MAP: dict[str, tuple[str, str]] = {
    "behavior": ("behavior", "towers_task"),
    "ephys-raw": ("ephys", "raw"),
    "ephys-processed": ("ephys", "processed"),
    "imaging-raw": ("imaging", "raw"),
    "imaging-processed": ("imaging", "processed"),
}


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedModality:
    """Structured representation of a single parsed modality.

    Attributes:
        name:          Canonical modality name: ``'behavior'``, ``'ephys'``, or ``'imaging'``.
        modality_type: Canonical sub-type: ``'towers_task'``, ``'raw'``, or ``'processed'``.
        numbers:       Optional list of probe/FOV indices.  ``None`` when not provided.
    """

    name: str
    modality_type: str
    numbers: Optional[List[int]] = field(default=None)


# ---------------------------------------------------------------------------
# Error
# ---------------------------------------------------------------------------


class ModalityParseError(NwbExportError):
    """Raised when one or more modality strings cannot be parsed.

    Args:
        invalid_value: The unrecognised modality string that caused the error.
    """

    def __init__(self, message: str, *, invalid_value: str = "") -> None:
        super().__init__(message)
        self.invalid_value = invalid_value

    def __str__(self) -> str:  # pragma: no cover
        return self.args[0]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_modalities(modalities: List[str]) -> List[ParsedModality]:
    """Parse and normalise a list of modality strings.

    Strings are lower-cased and stripped before lookup.  Duplicates are
    silently removed (first occurrence wins among the deduplicated set).

    Args:
        modalities: Raw user-supplied modality strings.

    Returns:
        Ordered list of :class:`ParsedModality` objects (one per unique modality).

    Raises:
        ModalityParseError: If *modalities* is empty, or contains any
                            unrecognised string.
    """
    if not modalities:
        raise ModalityParseError(
            f"no modalities provided — at least one of {sorted(VALID_MODALITIES)} is required.",
            invalid_value="",
        )

    # Normalise and deduplicate while preserving first-seen order
    seen: Set[str] = set()
    normalised: List[str] = []
    for raw in modalities:
        key = raw.strip().lower()
        if key not in seen:
            seen.add(key)
            normalised.append(key)

    # Validate
    for key in normalised:
        if key not in _MODALITY_MAP:
            raise ModalityParseError(
                f"unknown modality '{key}'. valid values: {sorted(VALID_MODALITIES)}",
                invalid_value=key,
            )

    return [ParsedModality(name=_MODALITY_MAP[k][0], modality_type=_MODALITY_MAP[k][1]) for k in normalised]
