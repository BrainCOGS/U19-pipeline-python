# Feature Specification: NWB Export Handler & DANDI Upload Pipeline

**Feature Branch**: `001-nwb-export-handler`
**Created**: 2026-02-24
**Status**: Draft
**Input**: Comprehensive NWB export system for behavioral, electrophysiological, and imaging data

## User Scenarios & Testing

### User Story 1 - Submit NWB Export Job (Priority: P1)

User submits a request to export behavior data with optional ephys (raw or processed) and imaging (raw or processed) into a single NWB file. System validates input, estimates file size, and queues the job for processing.

**Why this priority**: Core MVP—enables users to initiate export workflow

**Independent Test**: Job submission creates record with QUEUED status and can be queried; preconditions validated (modalities specified, session exists)

**Acceptance Scenarios**:

1. **Given** user specifies behavior data only, **When** submitting job, **Then** job created with status=QUEUED, estimated_file_size calculated
2. **Given** user specifies behavior + ephys data, **When** validating, **Then** system verifies probe data exists
3. **Given** user specifies behavior + imaging, **When** validating, **Then** system confirms raw imaging files exist
4. **Given** invalid session key, **When** submitting, **Then** job rejected with validation error

---

### User Story 2 - Validate Data Exists (Priority: P1)

System validates source data (behavior trials, ephys Units/spike data, imaging ROIs/raw stacks) exists before conversion. Provides clear error messages for missing data.

**Why this priority**: Prevents waste of compute resources; provides early feedback

**Independent Test**: Data validation step can run independently; returns (bool, error_message) tuple; detects missing behavior trials, missing probes, missing imaging FOVs

**Acceptance Scenarios**:

1. **Given** session with no trials, **When** validating behavior, **Then** validation fails with "No trials" message
2. **Given** probe inserted but no spike sorts, **When** validating ephys, **Then** validation fails with clear error
3. **Given** FOV in database but no Ca2+ data, **When** validating imaging, **Then** validation fails with "No ROI data" message

---

### User Story 3 - Convert Data to NWB Format (Priority: P1)

System converts validated data to NWB 2.0 format across multiple NWB modules:
- **Behavior**: Position/velocity timeseries via VirmenDataInterface
- **Ephys**: spike times, spike amplitudes, unit quality metrics via KilosortInterface (or SpikeGLX if unprocessed)
- **Imaging**: ROI masks, calcium traces via ImagingInterface (or Tiff, ScanImage, or "ScanImage Legacy" if unprocessed, (have 3 possible options for th einput))

Metadata populated from U19-pipeline and ndx-tank-metadata extensions.

**Why this priority**: Core conversion logic—enables standardized data format

**Independent Test**: NWB file created with all modalities; can be read by PyNWB; passes HDF5 integrity check

**Acceptance Scenarios**:

1. **Given** behavior data + ephys probes, **When** converting, **Then** NWB contains ProcessingModule 'behavior' + 'ecephys'
2. **Given** behavior + imaging FOVs, **When** converting, **Then** NWB contains 'behavior' + 'ophys' with ImageSegmentation
3. **Given** all three modalities, **When** converting, **Then** all modules present with correct metadata

---

### User Story 4 - Validate NWB Output (Priority: P2)

System validates generated NWB file using NWB Inspector, checks HDF5 integrity, confirms required metadata present, counts warnings/errors.

**Why this priority**: Quality assurance before upload; prevents corrupted uploads

**Independent Test**: NWB file validated independently; inspection passes/fails; warnings/errors counted

**Acceptance Scenarios**:

1. **Given** valid NWB file, **When** validating, **Then** validation_passed=true, validation_report populated
2. **Given** missing required metadata, **When** validating, **Then** metadata_complete_passed=false, errors counted
3. **Given** corrupted HDF5, **When** validating, **Then** hdf5_integrity_passed=false

---

### User Story 5 - Manage DANDI Credentials & Dandiset (Priority: P2)

User provides optional DANDI API key and associates job with optional dandiset ID. System stores encrypted credentials per user and validates dandiset existence before upload. If either credential is missing, upload stage is skipped with no error state.

**Why this priority**: Enables optional DANDI integration; enforces all-or-nothing credential requirement

**Independent Test**: Credentials stored/retrieved; validation checks both API key + dandiset; upload skipped if incomplete

**Acceptance Scenarios**:

1. **Given** user with DANDI API key + dandiset ID, **When** submitting job, **Then** upload stage enabled
2. **Given** user with no DANDI API key, **When** job completes, **Then** upload stage skipped, no error
3. **Given** incomplete credentials (only API key, no dandiset), **When** attempting upload, **Then** blocked until both provided
4. **Given** invalid dandiset ID, **When** attempting upload, **Then** validation fails with clear error

---

### User Story 6 - Track Job Status Through Pipeline (Priority: P1)

System transitions job through states: QUEUED → DATA_VALIDATION → PROCESSING → VALIDATION → COMPLETED (or FAILED). Each transition logged with timestamp and errors. User can query job status at any time.

**Why this priority**: Provides observability; enables debugging; user needs real-time feedback

**Independent Test**: Status transitions logged; query returns current status + full history; error messages captured

**Acceptance Scenarios**:

1. **Given** job in QUEUED, **When** validation succeeds, **Then** status → DATA_VALIDATION, log created
2. **Given** conversion fails, **When** status updates to FAILED, **Then** log includes error_message + exception traceback
3. **Given** completed job, **When** querying history, **Then** all transitions visible with timestamps

---

### User Story 7 - Upload NWB to DANDI (Priority: P2)

System uploads completed NWB file to DANDI using provided credentials and dandiset ID. Validates upload success, stores DANDI asset ID with job record.

**Why this priority**: Enables final data sharing step; downstream systems depend on DANDI URL

**Independent Test**: Upload succeeds; asset ID stored; can retrieve from DANDI API; failed uploads logged

**Acceptance Scenarios**:

1. **Given** validated NWB + valid DANDI credentials, **When** uploading, **Then** file appears in DANDI dandiset
2. **Given** valid DANDI credentials + invalid dandiset, **When** uploading, **Then** upload fails with clear error
3. **Given** network failure during upload, **When** retried, **Then** upload succeeds on retry

---

### Edge Cases

- What happens when session data changes during conversion (e.g., new trials added)? → Conversion continues with snapshot; warn user
- How system handles very large files (>1TB)? → Streaming write to HDF5; no in-memory copy
- User deletes local behavior file after job submitted? → Data validation fails; user notified
- Ephys processing crashes mid-conversion? → Status → FAILED, exception logged, can retry
- DANDI API returns rate limit? → Queue for retry; exponential backoff
- User provides dandiset ID but wrong API key? → Validation fails, clear error message

## Requirements

### Functional Requirements

**Job Management**:
- **FR-001**: System MUST accept job submission with session key + list of modality strings (`list[str]`; valid values: `behavior`, `ephys-raw`, `ephys-processed`, `imaging-raw`, `imaging-processed`)
- **FR-002**: System MUST calculate estimated_file_size_gb based on behavior trial count, ephys probe count, imaging FOV count
- **FR-003**: System MUST create NwbExportJob record with status=QUEUED, submission_timestamp, output_filepath
- **FR-004**: System MUST assign unique auto-increment nwb_job_id per job

**Data Validation**:
- **FR-005**: System MUST validate behavior data: session exists, trials > 0
- **FR-006**: System MUST validate ephys data: probes exist, spike data present (for processed) or raw probe files exist (for raw)
- **FR-007**: System MUST validate imaging data: FOVs exist, ROI data present (for processed) or raw imaging stacks exist (for raw)
- **FR-008**: System MUST return (is_valid: bool, error_message: str) tuple for each modality
- **FR-009**: System MUST transition status: QUEUED → DATA_VALIDATION → (PROCESSING or FAILED)

**NWB Conversion**:
- **FR-010**: System MUST create NWBFile with session metadata (identifier, session_start_time, institution, lab, experimenter, etc.)
- **FR-011**: System MUST add behavior module with VirmenDataInterface (position, velocity, trial structure)
- **FR-012**: System MUST add ecephys module with Units table (spike times, amplitudes, quality metrics) for processed ephys OR raw probe data via SpikeGLX interface for raw ephys
- **FR-013**: System MUST add ophys module with ImageSegmentation (ROI masks) + Fluorescence (Ca2+ traces) for processed imaging OR raw imaging data for raw imaging (interface selected by file-extension/header detection in priority order: ScanImage → ScanImage Legacy → Tiff as fallback)
- **FR-014**: System MUST populate ndx-tank-metadata extension with rig configuration, maze parameters
- **FR-015**: System MUST write NWB to HDF5 file at output_filepath
- **FR-016**: System MUST transition status: DATA_VALIDATION → PROCESSING → (VALIDATION or FAILED)

**Output Validation**:
- **FR-017**: System MUST run NWB Inspector on completed file; capture report_json, warnings_count, errors_count
- **FR-018**: System MUST validate HDF5 integrity via h5py
- **FR-019**: System MUST confirm required metadata fields present (session_start_time, institution, experimenter, etc.)
- **FR-020**: System MUST transition status: PROCESSING → VALIDATION → (COMPLETED or FAILED)

**DANDI Integration** (retry policy resolved in FR-034; see research Decision 1):
- **FR-021**: System MUST accept optional DANDI API key and dandiset ID per user (stored in DandiCredentials table using AES-256-GCM encryption; encryption key material stored outside the database)
- **FR-022**: System MUST validate dandiset exists before upload (via DANDI API)
- **FR-023**: System MUST require BOTH API key AND dandiset ID to proceed to upload stage (zero exceptions)
- **FR-024**: System MUST skip upload and leave status at COMPLETED if either credential missing (no error state, no message)
- **FR-025**: System MUST upload NWB file to DANDI dandiset using Python SDK
- **FR-026**: System MUST store DANDI asset ID in NwbExportJob record upon successful upload
- **FR-027**: System MUST transition status: VALIDATION → UPLOAD → UPLOADED → COMPLETED (after DANDI asset ID is persisted successfully) or FAILED if upload fails at any stage
- **FR-028**: *(Consolidated into FR-024)* Upload stage is optional; job can reach COMPLETED without upload when credentials are absent. See FR-024 for authoritative wording.

**Status Tracking & Logging**:
- **FR-029**: System MUST define NwbExportStatus enum: QUEUED, DATA_VALIDATION, PROCESSING, VALIDATION, UPLOAD, UPLOADED, COMPLETED, FAILED
- **FR-030**: System MUST log all status transitions to NwbExportLogStatus with (timestamp, old_status, new_status, error_message, error_exception)
- **FR-031**: System MUST set completion_timestamp when entering COMPLETED or FAILED states
- **FR-032**: System MUST provide query interface to fetch job status, history, and validation results
- **FR-033**: System MUST support retry of failed jobs from last failed stage
- **FR-034**: System MUST automatically retry failed DANDI uploads up to 3 times with exponential backoff and jitter before surfacing the failure for manual user retry (per research Decision 1)

### Key Entities

**NwbExportJob**: Main job record with session reference, modality list, status, timestamps, file paths, size estimates
**NwbExportStatus**: Enum values (QUEUED, DATA_VALIDATION, PROCESSING, VALIDATION, UPLOAD, UPLOADED, COMPLETED, FAILED)
**NwbExportLogStatus**: Audit trail of status changes with error context
**NwbExportValidation**: Output validation results (NWB Inspector report, integrity checks, metadata check, warning/error counts)
**DandiCredentials** [NEW]: Per-user DANDI API key + preferred dandiset ID (encrypted storage)
**NwbExportModality** [NEW]: Association table linking job to modalities (Behavior, Ephys, Imaging) with sub-type (raw, processed)

## Success Criteria

**Measurable Outcomes**:

- **SC-001**: Users can submit NWB export job in <1 minute (job queued within 30 seconds)
- **SC-002**: Data validation completes within 5 seconds for standard session (behavior + 4 probes + 3 FOVs)
- **SC-003**: Behavior + ephys conversion completes within 5 minutes for standard session
- **SC-004**: Behavior + imaging conversion completes within 5 minutes for standard session
- **SC-005**: Full pipeline (all modalities) completes within 15 minutes for standard session
- **SC-006**: NWB output file passes NWB Inspector with zero critical errors
- **SC-007**: HDF5 integrity validated successfully (no file corruption)
- **SC-008**: DANDI upload succeeds within 5 minutes for 1GB file (success rate ≥95% measured over a 30-day rolling window in production; not enforced in automated tests)
- **SC-009**: Job status queryable 24/7; historical log retained for ≥30 days
- **SC-010**: Failed jobs can be retried with automatic recovery of last successful stage
- **SC-011**: 100% of required metadata fields populated in NWB file (zero missing required fields)
- **SC-012**: DANDI credentials properly encrypted; no plaintext API keys in database
- **SC-013**: Cronjob processes ≥10 concurrent jobs without performance degradation

## Testing Requirements

### Minimal Database Connectivity Test

Before running the full NWB export pipeline, verify database connection and required ephys data structure using subject `jyanar_ya014`:

**Test 1**: Query subject exists
- **Action**: Query `subject.Subject` for `subject_fullname='jyanar_ya014'`
- **Expected**: Subject found (non-empty result)

**Test 2**: Verify multiple ephys sessions exist
- **Action**: Query `acquisition.Session * recording.Recording.EphysSession` for `subject_fullname='jyanar_ya014'`
- **Expected**: Returns multiple ephys sessions (count ≥2)

**Test 3**: Verify at least one ephys session occurs on 2024-07-22
- **Action**: From the subject's ephys-session results, verify one entry has `session_date='2024-07-22'`
- **Expected**: At least one matching ephys session on `2024-07-22`

**Test 4**: Do not enforce imaging-session checks in this phase
- **Action**: Skip `recording.Recording.ImagingSession` assertions for this minimum test set
- **Expected**: No pass/fail criteria tied to imaging presence yet

**Success Criteria**: All 4 tests pass, confirming minimum DB connectivity + ephys-session readiness for subject `jyanar_ya014`

**Output Format**: Clear pass/fail indicators with result counts (e.g., "✓ Ephys sessions found (N sessions)")

**Future Scope Note**: Imaging-session validation will be added in a later test phase and is intentionally excluded from current minimum checks.

---

## Assumptions

1. **DataJoint Environment**: U19-pipeline DataJoint config already initialized and accessible; acquisition.Session, behavior.TowersBlock.Trial exist
2. **Ephys Data Structure**: Kilosort spike-sorted units available in recording_process.Processing; raw probe files in known locations
3. **Imaging Data Structure**: ROI data in imaging_element tables; raw imaging stacks in known filesystem locations
4. **NWB Dependencies**: PyNWB 3.0+, NWB extensions (neurodata_types) available; NWB Inspector CLI available
5. **DANDI Credentials**: Stored in separate encrypted table using AES-256-GCM; encryption key material stored outside the database (not in DataJoint tables)
6. **File System**: Output NWB files written to persistent shared storage (s3/local based on config)
7. **Cronjob**: Existing infrastructure available; can modify polling interval, error handling
8. **Python Version**: 3.12+ enforced per constitution
9. **No Breaking Changes**: Existing U19-pipeline tables remain unchanged; NwbExportJob is additive
10. **Retry Logic**: Failed jobs can be retried manually; automatic retry on transient network errors

