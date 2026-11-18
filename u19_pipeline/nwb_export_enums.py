"""
Enum definitions for NWB export pipeline states.

Per Constitution Principle IV: Use Python Enum classes to model all domain states
for type-safe, self-documenting state management.
"""

from enum import IntEnum


class NwbExportStatusEnum(IntEnum):
    """
    States in the NWB export pipeline.

    Pipeline flow:
    QUEUED → DATA_VALIDATION → PROCESSING → VALIDATION → (UPLOAD) → COMPLETED
                                                ↓
                                             FAILED (terminal)

    Upload is optional; if skipped, transitions directly VALIDATION → COMPLETED
    """

    QUEUED = 0
    """Job submitted, waiting for processing to begin."""

    DATA_VALIDATION = 1
    """Validating that source data exists for all requested modalities."""

    PROCESSING = 2
    """Converting data from native formats to NWB 2.0."""

    VALIDATION = 3
    """Validating generated NWB file (NWB Inspector, HDF5, metadata checks)."""

    UPLOAD = 4
    """Uploading NWB file to DANDI (optional stage, skipped if credentials incomplete)."""

    UPLOADED = 5
    """NWB file uploaded to DANDI; asset ID persisted — transitions to COMPLETED."""

    COMPLETED = 6
    """Export successful and complete (terminal state)."""

    FAILED = -1
    """Export failed at some stage; check NwbExportLogStatus for error details (terminal state)."""

    @property
    def is_terminal(self) -> bool:
        """Return True if this is a terminal state (job cannot progress further)."""
        return self in (NwbExportStatusEnum.COMPLETED, NwbExportStatusEnum.FAILED)

    @property
    def is_active(self) -> bool:
        """Return True if job is in progress (not terminal)."""
        return not self.is_terminal

    def __str__(self) -> str:
        """Return human-readable status name."""
        return f"{self.name}"


class DataModalityTypeEnum(IntEnum):
    """Data types that can be included in export."""

    BEHAVIOR = 1
    """Behavior data from Towers task (position, velocity, trials)."""

    EPHYS_RAW = 2
    """Raw ephys data from probes (continuous recordings)."""

    EPHYS_PROCESSED = 3
    """Spike-sorted ephys data (Kilosort spike times, amplitudes, quality metrics)."""

    IMAGING_RAW = 4
    """Raw imaging data (full imaging stacks)."""

    IMAGING_PROCESSED = 5
    """Processed imaging data (ROI masks and Ca2+ traces)."""


class DandiUploadStatusEnum(IntEnum):
    """Status of DANDI upload for a completed export."""

    NOT_APPLICABLE = 0
    """Job does not have DANDI upload enabled (missing credentials)."""

    PENDING = 1
    """Upload to DANDI pending (credentials provided but not yet uploaded)."""

    IN_PROGRESS = 2
    """Upload to DANDI in progress."""

    COMPLETED = 3
    """Upload to DANDI completed successfully."""

    FAILED = -1
    """Upload to DANDI failed (check error log)."""
