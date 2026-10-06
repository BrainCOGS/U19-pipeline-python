# Data Model: NWB Export Handler

## Entity: NwbExportJob

- Fields:
  - `nwb_job_id` (PK, auto-increment)
  - `session_key` (subject/session identity)
  - `user_id`
  - `status` (Enum: `NwbExportStatus`)
  - `submission_timestamp`
  - `completion_timestamp` (nullable)
  - `output_filepath`
  - `estimated_file_size_gb`
  - `actual_file_size_gb` (nullable)
  - `nwb_file_hash` (nullable)
- Relationships:
  - one-to-many with `NwbExportLogStatus`
  - one-to-one with `NwbExportValidation`
  - one-to-many with `NwbExportModality`
- Validation rules:
  - session must exist
  - at least one modality required (behavior mandatory in this feature)
  - status transitions must follow state machine

## Entity: NwbExportModality

- Fields:
  - `nwb_job_id` (FK)
  - `modality_name` (`behavior`, `ephys`, `imaging`)
  - `modality_type` (`raw`, `processed`, task-specific type)
  - `probe_numbers` (optional collection)
  - `fov_numbers` (optional collection)
- Validation rules:
  - ephys modalities must reference existing probe/session resources
  - imaging validations deferred in minimum DB gate tests

## Entity: NwbExportValidation

- Fields:
  - `nwb_job_id` (FK)
  - `validation_timestamp`
  - `validation_passed` (bool)
  - `validation_report_json`
  - `nwb_inspector_passed` (bool)
  - `hdf5_integrity_passed` (bool)
  - `metadata_complete_passed` (bool)
  - `validation_warnings_count`
  - `validation_errors_count`

## Entity: NwbExportLogStatus

- Fields:
  - `log_id` (PK)
  - `nwb_job_id` (FK)
  - `status_old` (Enum)
  - `status_new` (Enum)
  - `status_timestamp`
  - `error_message` (nullable)
  - `error_exception` (nullable)

## Entity: DandiCredentials

- Fields:
  - `user_id` (PK/FK)
  - `dandi_api_key_encrypted` (nullable)
  - `default_dandiset_id` (nullable)
  - `created_timestamp`
  - `updated_timestamp`
- Validation rules:
  - upload eligibility requires both API key and dandiset ID

## Entity: NwbExportJobDandi

- Fields:
  - `nwb_job_id` (FK)
  - `dandiset_id`
  - `upload_status` (Enum)
  - `dandi_asset_id` (nullable)
  - `upload_error_message` (nullable)

## Entity: MinimumDbEphysReadinessCheck

- Purpose: Pre-flight DB test to validate minimum ephys readiness for upcoming technical implementation.
- Inputs:
  - `subject_fullname` (default `jyanar_ya014`)
  - `required_session_date` (default `2024-07-22`)
  - `min_ephys_sessions` (default `2`)
- Output fields:
  - `subject_exists` (bool)
  - `ephys_session_count` (int)
  - `has_required_date` (bool)
  - `matching_session_dates` (list)
  - `passed` (bool)
  - `message` (string)
- Validation rules:
  - pass iff `subject_exists=true`, `ephys_session_count >= min_ephys_sessions`, and `has_required_date=true`
  - imaging presence is not validated in this phase

## State Transitions

### NwbExportStatus

- `QUEUED -> DATA_VALIDATION`
- `DATA_VALIDATION -> PROCESSING | FAILED`
- `PROCESSING -> VALIDATION | FAILED`
- `VALIDATION -> COMPLETED | UPLOAD | FAILED`
- `UPLOAD -> UPLOADED | FAILED`
- `UPLOADED -> COMPLETED`

### DandiUploadStatus

- `NOT_APPLICABLE -> PENDING -> IN_PROGRESS -> COMPLETED | FAILED`

