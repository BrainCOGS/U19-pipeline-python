"""
Custom exception hierarchy for the NWB export pipeline.

Per Constitution Principle IV: explicit, typed errors are part of the domain
model — they make failure modes self-documenting and enable targeted catch blocks.

Hierarchy
---------
NwbExportError                  (base — catch-all for the whole pipeline)
├── NwbValidationError          (data-source validation failed for a modality)
├── NwbConversionError          (NWB file conversion/write failed)
├── NwbStatusTransitionError    (illegal state-machine transition attempted)
└── NwbDandiUploadError         (DANDI upload failed; carries attempt count)
"""

from __future__ import annotations


class NwbExportError(Exception):
    """Base class for all NWB export pipeline errors."""


class NwbValidationError(NwbExportError):
    """
    Raised when source data validation fails for a specific modality.

    Attributes
    ----------
    modality : str
        The modality that failed validation (e.g. ``"behavior"``, ``"ephys"``,
        ``"imaging"``).
    """

    def __init__(self, message: str, *, modality: str) -> None:
        super().__init__(message)
        self.modality = modality

    def __str__(self) -> str:
        return f"[{self.modality}] {super().__str__()}"


class NwbConversionError(NwbExportError):
    """Raised when NWB file conversion or HDF5 write fails."""


class NwbStatusTransitionError(NwbExportError):
    """
    Raised when an illegal status transition is attempted.

    Attributes
    ----------
    from_state : str
        The current (source) status name.
    to_state : str
        The attempted (target) status name.
    """

    def __init__(self, *, from_state: str, to_state: str) -> None:
        super().__init__(f"Illegal status transition: {from_state!r} → {to_state!r}")
        self.from_state = from_state
        self.to_state = to_state


class NwbDandiUploadError(NwbExportError):
    """
    Raised when a DANDI upload fails after all automatic retries are exhausted.

    Attributes
    ----------
    attempt : int
        The 1-based attempt number on which this error occurred (max = MAX_DANDI_RETRIES).
    """

    def __init__(self, message: str, *, attempt: int) -> None:
        super().__init__(message)
        self.attempt = attempt

    def __str__(self) -> str:
        return f"[attempt {self.attempt}] {super().__str__()}"
