"""
Pipeline-level constants for the NWB export system.

All tuneable limits are defined here so they can be changed in a single place
and are easy to test (Constitution Principle II — explicit, typed public API).

DANDI retry policy (FR-034 / research Decision 1)
--------------------------------------------------
Automatic bounded retries for transient upload failures:
  * MAX_DANDI_RETRIES       = 3 attempts
  * DANDI_RETRY_BASE_DELAY  = 2 s (doubles each attempt)
  * DANDI_RETRY_JITTER      = 0.3  (±30 % random jitter on each delay)
After exhausting retries, raise NwbDandiUploadError and surface for manual retry.

Log retention (SC-009)
-----------------------
NwbExportLogStatus records older than LOG_RETENTION_DAYS days may be purged.
Minimum: 30 days.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# DANDI upload retry policy  (FR-034)
# ---------------------------------------------------------------------------

MAX_DANDI_RETRIES: int = 3
"""Maximum number of automatic upload attempts before surfacing failure."""

DANDI_RETRY_BASE_DELAY_SECONDS: float = 2.0
"""Base delay (seconds) for exponential back-off between DANDI upload retries."""

DANDI_RETRY_JITTER_FRACTION: float = 0.3
"""
Fraction of the current delay added as random jitter.

``actual_delay = delay * (1 + uniform(-jitter, +jitter))``
Must be in the range (0, 1].
"""

# ---------------------------------------------------------------------------
# Log retention  (SC-009)
# ---------------------------------------------------------------------------

LOG_RETENTION_DAYS: int = 30
"""NwbExportLogStatus records older than this many days may be purged (SC-009)."""

# ---------------------------------------------------------------------------
# Required NWB metadata fields  (FR-019)
# ---------------------------------------------------------------------------

REQUIRED_NWB_METADATA_FIELDS: tuple[str, ...] = (
    "session_start_time",
    "institution",
    "experimenter",
    "session_description",
    "identifier",
    "lab",
)
"""
Fields that MUST be present in the NWBFile before validation is considered
complete.  Absence of any field causes metadata_complete_passed=False (FR-019).
"""
