# NWB Export Handler Implementation Guide

**Status**: Feature Specification & TDD Complete  
**Constitution Compliance**: Enum-based states, DataJoint-first, Python 3.12+, TDD tests first

## Architecture Overview

```
User Submission
    ↓
[NwbExportJob.insert] → Status: QUEUED
    ↓
Cronjob (every 5 seconds) calls NwbExportHandler.pipeline_handler_main()
    ↓
For each job in QUEUED status:
    ├─ Phase 1: DATA_VALIDATION (test for data existence)
    │  └─ Validate behavior trials exist
    │  └─ Validate ephys (raw files OR spike sorts) exist
    │  └─ Validate imaging (raw stacks OR ROI data) exist
    │  └─ On success → PROCESSING
    │  └─ On failure → FAILED (log error)
    ├─ Phase 2: PROCESSING (convert to NWB)
    │  └─ Initialize NWBFile with metadata
    │  └─ Add Behavior module (VirmenDataInterface or raw data)
    │  └─ Add Ecephys module (Kilosort data or raw probes)
    │  └─ Add Ophys module (ROI data or raw imaging)
    │  └─ Write to HDF5 at output_filepath
    │  └─ On success → VALIDATION
    │  └─ On failure → FAILED (log error)
    ├─ Phase 3: VALIDATION (NWB Inspector, HDF5, metadata)
    │  └─ Run NWB Inspector
    │  └─ Check HDF5 integrity
    │  └─ Verify required metadata
    │  └─ Insert NwbExportValidation record
    │  └─ On success → (check DANDI credentials)
    │  └─ On failure → FAILED (log error)
    └─ Phase 4: UPLOAD (optional, only if both API key + dandiset provided)
       └─ Check can_upload_to_dandi(user_id)
       │  ├─ If False: skip to COMPLETED (no error)
       │  └─ If True: proceed to upload
       └─ Upload NWB to DANDI
       └─ Store DANDI asset ID
       └─ On success → COMPLETED
       └─ On failure → FAILED (log error)
```

## Key Design Decisions

### 1. Status Enum (Principle IV)
- All states defined as `NwbExportStatusEnum(IntEnum)` in `nwb_export_enums.py`
- States stored as integers in DB (backward compatible)
- Type-safe throughout pipeline (Python 3.12+ enforcement)

### 2. Modality Flexibility
- `NwbExportModality` table allows any combination:
  - Behavior alone
  - Ephys (raw or processed)
  - Imaging (raw or processed)
  - Any combination thereof
- Single job can have multiple modalities

### 3. DataJoint-First (Principle I)
- All DB operations use DataJoint query API
- Public API functions: `submit_nwb_export_job()`, `update_job_status()`, `get_dandi_credentials()`
- No raw SQL queries

### 4. DANDI Credentials (All-Or-Nothing)
- Both API key AND dandiset ID must exist to upload
- If either missing → skip upload stage, job marked COMPLETED
- No error state for incomplete credentials
- Credentials encrypted in production (AES-256 TBD)

### 5. Validation Throughout
- Phase 1: Data existence validation
- Phase 3: NWB output validation (NWB Inspector + HDF5 + metadata)
- `NwbExportValidation` table captures full report

### 6. Error Tracking
- Every failure logged to `NwbExportLogStatus` with timestamp + traceback
- Job history queryable via `get_job_history(job_key)`

## File Structure

```
u19_pipeline/
├── nwb_export_enums.py                    # Enum definitions (CREATED)
├── nwb_production.py                      # DataJoint schema + API (UPDATED)
├── nwb_production_utils.py                # Helper functions (EXISTS)
├── automatic_job/
│   ├── nwb_export_handler.py             # Main processor (UPDATE IN PROGRESS)
│   ├── cronjob_nwb_export.py             # Cronjob entry point (UPDATE NEEDED)
│   └── params_config.py                  # Config (EXISTS)

specs/001-nwb-export-handler/
├── spec.md                                # Feature specification (CREATED)
└── test_nwb_export_handler.py            # Test suite (CREATED)
```

## Implementation Checklist

- [x] **Feature Specification**: Comprehensive spec with 7 user stories
- [x] **Test Suite**: TDD tests defining all required functionality  
- [x] **Enum Definitions**: Status, modality, upload status enums
- [x] **DataJoint Schema**: Enhanced schema with DANDI support
- [x] **Public API**: Job submission, status updates, credential management
- [ ] **NwbExportHandler Implementation**: Core processing logic
- [ ] **Cronjob Updates**: Enhanced monitor script
- [ ] **Integration Tests**: End-to-end behavior+ephys+imaging export
- [ ] **DANDI Upload Implementation**: Deploy to dandiset with retries
- [ ] **Documentation**: Usage guide for end users

## Next Steps

1. Implement `NwbExportHandler.process_data_validation()` - validate all modalities exist
2. Implement `NwbExportHandler.process_nwb_conversion()` - convert to NWB
3. Implement `NwbExportHandler.process_validation()` - validate output
4. Implement `NwbExportHandler.process_upload_to_dandi()` - upload with retry logic
5. Update cronjob to use enhanced handler
6. Run full test suite

## Testing Strategy (Per Constitution)

**TDD Process**:
1. Tests written and approved locally ✅ (spec.md + test_nwb_export_handler.py)
2. Implementation code written to pass tests (IN PROGRESS)
3. All tests pass before PR (GATE)

**Test Coverage Required**:
- Unit tests for each pipeline phase
- Integration tests for full pipeline (behavior + ephys + imaging)
- DANDI credential validation tests
- Error recovery tests (failed jobs can be retried)

## Data Flow Example

```python
# User submits job
job_id = submit_nwb_export_job(
    session_key={'subject_id': 'mouse001', 'session_date': '2026-02-24', 'session_number': 1},
    job_name='export_towers_with_ephys_imaging',
    user_id='user123',
    modalities=[
        ('behavior', 'towers_task', None),
        ('ephys', 'processed', [0, 1, 2]),  # 3 probes
        ('imaging', 'processed', [0, 1])     # 2 FOVs
    ],
    output_filepath='/data/nwb/mouse001_2026-02-24_export.nwb',
    estimated_size_gb=2.5
)

# Cronjob picks it up and processes through phases
# Status transitions: QUEUED → DATA_VALIDATION → PROCESSING → VALIDATION → 
#                    [check DANDI creds] →
#                    (if both provided): UPLOAD → COMPLETED
#                    (if missing): COMPLETED

# User can query status
status, status_name = get_job_status({'nwb_job_id': job_id})
history = get_job_history({'nwb_job_id': job_id})

# If job fails, error logged with traceback
# User can examine errors and retry after fixing data
```

## Constitution Alignment Checklist

- [x] **Principle I (DataJoint-First)**: All DB ops via DataJoint schema + API functions
- [x] **Principle II (Modern Python 3.12+)**: Type hints on all public functions; enums used
- [x] **Principle III (Structural Reuse)**: Reuses existing `acquisition.Session`, `behavior.TowersBlock.Trial`, etc.
- [x] **Principle IV (Enum-Based States)**: `NwbExportStatusEnum`, `DataModalityTypeEnum`, `DandiUploadStatusEnum`
- [x] **Principle V (Test-First TDD)**: Feature spec + comprehensive test suite created before implementation
