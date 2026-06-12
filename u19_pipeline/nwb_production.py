"""
Enhanced DataJoint schema for NWB export job management.

Manages the full pipeline for exporting behavioral, electrophysiological, and
imaging data to NWB 2.0 format with optional DANDI upload integration.

Per Constitution Principles:
- Principle I: DataJoint-First (all DB operations via DataJoint)
- Principle IV: Enum-based state modeling (NwbExportStatusEnum)
- Principle II: Type hints on all public functions
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import datajoint as dj

from u19_pipeline.nwb_export.credentials_crypto import decrypt_api_key, encrypt_api_key
from u19_pipeline.nwb_export.dandi.eligibility import is_eligible_for_upload
from u19_pipeline.nwb_export_enums import NwbExportStatusEnum
from u19_pipeline import acquisition  # needed so DataJoint can resolve -> acquisition.Session FK

schema = dj.schema(dj.config["custom"]["database.prefix"] + "nwb_production")


@schema
class NwbExportStatus(dj.Lookup):
    """Lookup table for NWB export job status values."""

    definition = """
    status_id: TINYINT          # Status ID (maps to NwbExportStatusEnum)
    ---
    status_name: VARCHAR(32)    # Status name (QUEUED, PROCESSING, etc.)
    status_description: VARCHAR(256)  # Human-readable description
    is_terminal: BOOLEAN        # True if job cannot progress further
    """

    contents = [
        (int(NwbExportStatusEnum.QUEUED), "QUEUED", "Job submitted, waiting for processing", False),
        (
            int(NwbExportStatusEnum.DATA_VALIDATION),
            "DATA_VALIDATION",
            "Validating source data exists for all modalities",
            False,
        ),
        (int(NwbExportStatusEnum.PROCESSING), "PROCESSING", "Converting data to NWB format", False),
        (
            int(NwbExportStatusEnum.VALIDATION),
            "VALIDATION",
            "Validating NWB output (NWB Inspector, HDF5, metadata)",
            False,
        ),
        (int(NwbExportStatusEnum.UPLOAD), "UPLOAD", "Uploading NWB file to DANDI", False),
        (
            int(NwbExportStatusEnum.UPLOADED),
            "UPLOADED",
            "NWB file uploaded; asset ID persisted, transitioning to COMPLETED",
            False,
        ),
        (int(NwbExportStatusEnum.COMPLETED), "COMPLETED", "Export successful and complete", True),
        (int(NwbExportStatusEnum.FAILED), "FAILED", "Export failed; see error log for details", True),
    ]


@schema
class NwbExportJob(dj.Manual):
    """
    Main table tracking NWB export jobs.

    One job = one session exported to one NWB file with one or more modalities.
    """

    definition = """
    nwb_job_id: INT AUTO_INCREMENT   # Unique job identifier
    ---
    -> acquisition.Session           # Reference to behavior session
    job_name: VARCHAR(128)           # User-provided job name
    user_id: VARCHAR(64)             # User who submitted job
    -> NwbExportStatus               # Current job status
    submission_timestamp: DATETIME   # When job was submitted
    completion_timestamp=NULL: DATETIME   # When job completed (if applicable)
    output_filepath: VARCHAR(512)    # Path to output NWB file
    estimated_file_size_gb: FLOAT    # Estimated output file size (GB)
    actual_file_size_gb=NULL: FLOAT  # Actual file size after conversion (GB)
    export_parameters=NULL: VARCHAR(5000)  # JSON blob with conversion parameters
    nwb_file_hash=NULL: VARCHAR(64)  # SHA256 hash of NWB file for integrity
    """


@schema
class NwbExportModality(dj.Manual):
    """
    Associate modalities with export jobs.

    Tracks which modalities (behavior, ephys, imaging) are included in each job,
    with their sub-types (raw vs processed).
    """

    definition = """
    -> NwbExportJob
    modality_name: VARCHAR(32)       # 'behavior', 'ephys', or 'imaging'
    ---
    modality_type: VARCHAR(32)       # 'towers_task', 'raw', or 'processed'
    probe_numbers=NULL: VARCHAR(256) # JSON array of probe IDs (for ephys)
    fov_numbers=NULL: VARCHAR(256)   # JSON array of FOV IDs (for imaging)
    """


@schema
class DandiCredentials(dj.Manual):
    """
    Store user DANDI credentials for optional upload functionality.

    API key is stored encrypted. Both API key AND dandiset ID must be present
    to enable DANDI upload; either can be NULL to skip the upload stage.

    Security note: In production, dandi_api_key should be encrypted via:
    - Application-level encryption (e.g., AES-256 with per-tenant keys)
    - Database-level encryption (e.g., TDE, column encryption)
    Never log or print API keys.
    """

    definition = """
    user_id: VARCHAR(64)             # User identifier
    ---
    dandi_api_key=NULL: VARCHAR(256) # DANDI API key (AES-256-GCM encrypted)
    created_timestamp: DATETIME      # When credentials were created
    updated_timestamp=NULL: DATETIME # When credentials were last updated
    """


@schema
class DandiRegisteredDandiset(dj.Manual):
    """
    Per-user registry of DANDI dandisets.

    A user may register multiple dandisets (e.g. one per project).  Exactly one
    can be flagged as the default, which is pre-selected in the job-submission
    form.  The dandiset_id chosen at submission time is stored on NwbExportJobDandi.
    """

    definition = """
    user_id: VARCHAR(64)              # FK → DandiCredentials
    dandiset_id: VARCHAR(32)          # DANDI dandiset ID, e.g. '000123'
    ---
    description=NULL: VARCHAR(256)    # Human-readable description
    is_default=0: BOOLEAN             # Whether this is the user's default dandiset
    created_timestamp: DATETIME       # When this entry was registered
    """


@schema
class NwbExportJobDandi(dj.Manual):
    """
    Link NWB export job to DANDI dandiset and asset.

    Allows per-job override of default dandiset (user can choose different
    dandisets for different exports). Tracks upload status and DANDI asset ID.
    """

    definition = """
    -> NwbExportJob
    ---
    dandiset_id: VARCHAR(32)         # DANDI dandiset ID for this job
    upload_status: TINYINT           # Status of DANDI upload
    dandi_asset_id=NULL: VARCHAR(64) # DANDI asset ID on upload success
    upload_timestamp=NULL: DATETIME   # When upload completed
    upload_error_message=NULL: VARCHAR(512)  # Error message if upload failed
    """


@schema
class NwbExportLogStatus(dj.Manual):
    """
    Audit trail of status transitions for each job.

    Enables tracking of when status changed, from what to what, and any
    error context. Can rebuild job history by querying this table.
    """

    definition = """
    log_id: INT AUTO_INCREMENT       # Log entry ID
    ---
    -> NwbExportJob
    status_old: TINYINT              # Previous status ID
    status_new: TINYINT              # New status ID
    status_timestamp: DATETIME       # When status changed
    error_message=NULL: VARCHAR(512) # Error message if failed
    error_exception=NULL: VARCHAR(4096)  # Exception traceback if failed
    """


@schema
class NwbExportValidation(dj.Manual):
    """
    Validation results for completed NWB files.

    Stores NWB Inspector output, HDF5 integrity checks, metadata validation,
    and warning/error counts. Enables post-export quality assurance.
    """

    definition = """
    -> NwbExportJob
    ---
    validation_timestamp: DATETIME   # When validation ran
    validation_passed: BOOLEAN       # Overall validation pass/fail
    validation_report_json: LONGBLOB # Full NWB Inspector report (JSON)
    file_size_gb: FLOAT              # Actual file size in GB
    nwb_inspector_passed: BOOLEAN    # NWB Inspector check passed
    hdf5_integrity_passed: BOOLEAN   # HDF5 structure valid
    metadata_complete_passed: BOOLEAN  # Required metadata present
    validation_warnings_count: INT   # Number of warnings
    validation_errors_count: INT     # Number of errors
    """


# ============================================================================
# Public API Functions for Job Management
# ============================================================================


def submit_nwb_export_job(
    session_key: Dict[str, Any],
    job_name: str,
    user_id: str,
    modalities: List[Tuple[str, str, Optional[List[int]]]],
    output_filepath: str,
    estimated_size_gb: float,
    export_params: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Submit a new NWB export job.

    Args:
        session_key: Dictionary with keys for acquisition.Session (subject_id, session_date, session_number)
        job_name: User-friendly job name
        user_id: User identifier
        modalities: List of (modality_name, modality_type, probe_or_fov_numbers).
                   Example: [('behavior', 'towers_task', None),
                            ('ephys', 'processed', [0, 1, 2])]
        output_filepath: Where to write NWB file
        estimated_size_gb: Estimated output file size
        export_params: Optional JSON-serializable dict with conversion parameters

    Returns:
        nwb_job_id of newly created job

    Raises:
        ValueError: If session_key invalid
        Exception: If database operation fails
    """
    from u19_pipeline import acquisition  # Avoid circular imports

    # Validate session exists
    if not (acquisition.Session & session_key).fetch():
        raise ValueError(f"Session not found: {session_key}")

    # Create main job record
    job_record = {
        **session_key,
        "job_name": job_name,
        "user_id": user_id,
        "status_id": int(NwbExportStatusEnum.QUEUED),
        "submission_timestamp": datetime.now(),
        "output_filepath": output_filepath,
        "estimated_file_size_gb": estimated_size_gb,
        "export_parameters": str(export_params) if export_params else None,
    }

    NwbExportJob.insert1(job_record)

    # Get auto-generated job ID
    job_id = (NwbExportJob & session_key).fetch1("nwb_job_id")

    # Add modality associations
    for modality_name, modality_type, numbers in modalities:
        mod_record = {
            "nwb_job_id": job_id,
            "modality_name": modality_name,
            "modality_type": modality_type,
            "probe_numbers": str(numbers) if modality_name == "ephys" and numbers else None,
            "fov_numbers": str(numbers) if modality_name == "imaging" and numbers else None,
        }
        NwbExportModality.insert1(mod_record)

    return job_id


def update_job_status(
    job_key: Dict[str, Any],
    new_status: NwbExportStatusEnum,
    error_message: Optional[str] = None,
    error_exception: Optional[str] = None,
) -> None:
    """
    Update job status and log the transition.

    Args:
        job_key: Dictionary with nwb_job_id
        new_status: New status (NwbExportStatusEnum value)
        error_message: Optional error message (if status == FAILED)
        error_exception: Optional exception traceback (if status == FAILED)

    Raises:
        KeyError: If job_key does not exist
    """
    # Get current status
    old_status = (NwbExportJob & job_key).fetch1("status_id")

    # Update job record
    update_dict = {**job_key, "status_id": int(new_status)}

    # Set completion timestamp if transitioning to terminal state
    if new_status.is_terminal:
        update_dict["completion_timestamp"] = datetime.now()

    NwbExportJob.update1(update_dict)

    # Log transition
    log_entry = {
        **job_key,
        "status_old": old_status,
        "status_new": int(new_status),
        "status_timestamp": datetime.now(),
        "error_message": error_message,
        "error_exception": error_exception,
    }

    NwbExportLogStatus.insert1(log_entry)


def get_job_status(job_key: Dict[str, Any]) -> Tuple[NwbExportStatusEnum, str]:
    """
    Get current status of a job.

    Args:
        job_key: Dictionary with nwb_job_id

    Returns:
        Tuple of (status: NwbExportStatusEnum, status_name: str)
    """
    record = (NwbExportJob & job_key).fetch1()
    status = NwbExportStatusEnum(record["status_id"])
    status_name = (NwbExportStatus & {"status_id": int(status)}).fetch1("status_name")
    return status, status_name


def get_job_history(job_key: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get full status history for a job.

    Args:
        job_key: Dictionary with nwb_job_id

    Returns:
        List of log entries in chronological order
    """
    history = (NwbExportLogStatus & job_key).fetch(as_dict=True)
    return sorted(history, key=lambda x: x["status_timestamp"])


def save_dandi_api_key(user_id: str, api_key: str) -> None:
    """
    Persist (or update) a user's encrypted DANDI API key.

    Args:
        user_id: User identifier.
        api_key: Plaintext API key; encrypted before storage.
    """
    now = datetime.now()
    exists = bool(DandiCredentials & {"user_id": user_id})
    if exists:
        DandiCredentials.update1(
            {"user_id": user_id, "dandi_api_key": encrypt_api_key(api_key), "updated_timestamp": now}
        )
    else:
        DandiCredentials.insert1(
            {"user_id": user_id, "dandi_api_key": encrypt_api_key(api_key), "created_timestamp": now}
        )


def get_dandi_api_key_masked(user_id: str) -> Optional[str]:
    """
    Return a masked representation of the stored API key for safe UI display.

    Returns ``None`` if no key is stored, otherwise ``"dandi-..." + last-4``.

    Args:
        user_id: User identifier.
    """
    try:
        record = (DandiCredentials & {"user_id": user_id}).fetch1()
        raw = decrypt_api_key(record.get("dandi_api_key"))
        if not raw:
            return None
        # Show first 6 + masked middle + last 4
        if len(raw) <= 10:
            return "****"
        return raw[:6] + "…" + raw[-4:]
    except Exception:
        return None


def list_user_dandisets(user_id: str) -> List[Dict[str, Any]]:
    """
    Return all dandisets registered by *user_id*, default first.

    Args:
        user_id: User identifier.

    Returns:
        List of dicts with keys: dandiset_id, description, is_default, created_timestamp.
    """
    rows = (DandiRegisteredDandiset & {"user_id": user_id}).fetch(as_dict=True)
    return sorted(rows, key=lambda r: (not r.get("is_default", False), r["dandiset_id"]))


def add_user_dandiset(
    user_id: str,
    dandiset_id: str,
    description: Optional[str] = None,
    is_default: bool = False,
) -> None:
    """
    Register a dandiset for *user_id*.  If *is_default* is True, any existing
    default is cleared first (only one default allowed per user).

    Args:
        user_id: User identifier.
        dandiset_id: DANDI dandiset ID, e.g. ``'000123'``.
        description: Optional human-readable label.
        is_default: Whether to make this the default dandiset.
    """
    if is_default:
        # Clear any existing default for this user
        existing_defaults = (DandiRegisteredDandiset & {"user_id": user_id, "is_default": 1}).fetch(as_dict=True)
        for row in existing_defaults:
            DandiRegisteredDandiset.update1({**row, "is_default": 0})

    DandiRegisteredDandiset.insert1(
        {
            "user_id": user_id,
            "dandiset_id": dandiset_id,
            "description": description or "",
            "is_default": int(is_default),
            "created_timestamp": datetime.now(),
        },
        skip_duplicates=True,
    )


def remove_user_dandiset(user_id: str, dandiset_id: str) -> None:
    """
    Remove a registered dandiset for *user_id*.

    Args:
        user_id: User identifier.
        dandiset_id: Dandiset ID to remove.
    """
    (DandiRegisteredDandiset & {"user_id": user_id, "dandiset_id": dandiset_id}).delete_quick()


def set_default_dandiset(user_id: str, dandiset_id: str) -> None:
    """
    Mark *dandiset_id* as the default for *user_id*, clearing any prior default.

    Args:
        user_id: User identifier.
        dandiset_id: Dandiset ID to set as default.
    """
    # Clear existing default
    existing = (DandiRegisteredDandiset & {"user_id": user_id, "is_default": 1}).fetch(as_dict=True)
    for row in existing:
        DandiRegisteredDandiset.update1({**row, "is_default": 0})
    # Set new default
    target = (DandiRegisteredDandiset & {"user_id": user_id, "dandiset_id": dandiset_id}).fetch1()
    DandiRegisteredDandiset.update1({**target, "is_default": 1})


# ---------------------------------------------------------------------------
# Legacy / compatibility wrappers
# ---------------------------------------------------------------------------


def set_dandi_credentials(
    user_id: str,
    api_key: Optional[str] = None,
    dandiset_id: Optional[str] = None,
) -> None:
    """
    Convenience wrapper: save an API key and optionally register a dandiset.

    Args:
        user_id: User identifier.
        api_key: Plaintext DANDI API key.
        dandiset_id: Optional dandiset ID to register as default.
    """
    if api_key:
        save_dandi_api_key(user_id, api_key)
    if dandiset_id:
        add_user_dandiset(user_id, dandiset_id, is_default=True)


def get_dandi_credentials(user_id: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (plaintext_api_key, default_dandiset_id) for *user_id*.

    Returns (None, None) when no credentials are configured.
    """
    try:
        record = (DandiCredentials & {"user_id": user_id}).fetch1()
        api_key = decrypt_api_key(record.get("dandi_api_key"))
    except Exception:
        api_key = None

    dandisets = list_user_dandisets(user_id)
    default_ds = next((d["dandiset_id"] for d in dandisets if d.get("is_default")), None)
    if default_ds is None and dandisets:
        default_ds = dandisets[0]["dandiset_id"]

    return api_key, default_ds


def can_upload_to_dandi(user_id: str) -> bool:
    """
    Return True if both an API key and at least one dandiset are configured.

    Args:
        user_id: User identifier.
    """
    api_key, dandiset_id = get_dandi_credentials(user_id)
    return is_eligible_for_upload(api_key, dandiset_id)
