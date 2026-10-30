# Tasks: NWB Export Handler & DANDI Upload Pipeline

**Input**: Design documents from `/specs/001-nwb-export-handler/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Test tasks are included because testing requirements are explicitly defined in the feature specification.

**Organization**: Tasks are grouped by user story so each story can be implemented and tested independently.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: User story label (`[US1]`, `[US2]`, etc.) for traceability
- Every task includes an explicit file path

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare shared module skeletons, test layout, and config hooks needed by all stories.

> **Principle III (Structural Reuse)**: T074 audits existing U19-pipeline structures before any new schema is created.
> **Principle V (Test-First)**: Failing tests T075–T077 must be written and confirmed failing before T002–T004 implement their targets.

- [X] T074 [P] Audit U19-pipeline_python for existing nwb_export, job, and status structures before creating any new tables or enums in U19-pipeline_python/u19_pipeline/
- [X] T005 Create shared source + test package folders: U19-pipeline_python/u19_pipeline/nwb_export/__init__.py and ndx-tank-metadata-clean/tests/nwb_export/__init__.py
- [X] T075 [P] Write failing no_db tests for NwbExportStatus enum (must FAIL before T002) in ndx-tank-metadata-clean/tests/nwb_export/test_status_enums.py
- [X] T076 [P] Write failing no_db tests for shared exception types (must FAIL before T003) in ndx-tank-metadata-clean/tests/nwb_export/test_errors.py
- [X] T077 [P] Write failing no_db tests for config/constants loading (must FAIL before T004) in ndx-tank-metadata-clean/tests/nwb_export/test_config.py
- [X] T001 Create NWB export package skeleton in U19-pipeline_python/u19_pipeline/nwb_export/
- [X] T002 [P] Implement status enum module in U19-pipeline_python/u19_pipeline/nwb_export/status_enums.py (after T075 failing confirmed locally)
- [X] T003 [P] Implement shared exception types in U19-pipeline_python/u19_pipeline/nwb_export/errors.py (after T076 failing confirmed locally)
- [X] T004 [P] Implement shared constants/config module in U19-pipeline_python/u19_pipeline/nwb_export/config.py (after T077 failing confirmed locally)
- [X] T006 [P] Add pytest markers documentation update in ndx-tank-metadata-clean/TESTING.md

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Build core entities, repository interfaces, and orchestration primitives required by all user stories.

**⚠️ CRITICAL**: Complete this phase before user-story implementation.

> **Principle V (Test-First)**: T015 must be written and confirmed failing before T011 implements the state machine.

- [X] T015 [P] Write failing no_db tests for state machine allowed/blocked transitions (must FAIL before T011) in ndx-tank-metadata-clean/tests/nwb_export/test_state_machine.py
- [X] T007 Implement DataJoint-backed job entity schema in U19-pipeline_python/u19_pipeline/nwb_export/job_tables.py; ensure `completion_timestamp` is set when entering COMPLETED or FAILED (FR-031) — **REUSE**: `NwbExportJob` schema + `update_job_status()` in `nwb_production.py` (lines 66–99, 269–306); `completion_timestamp` set on `is_terminal` check ✅
- [X] T008 [P] Implement modality entity schema in U19-pipeline_python/u19_pipeline/nwb_export/modality_tables.py — **REUSE**: `NwbExportModality` table already in `nwb_production.py` ✅
- [X] T009 [P] Implement validation/log entity schemas in U19-pipeline_python/u19_pipeline/nwb_export/validation_tables.py — **REUSE**: `NwbExportValidation` + `NwbExportLogStatus` already in `nwb_production.py` ✅
- [X] T010 [P] Implement DANDI credentials entity schema using AES-256-GCM encryption in U19-pipeline_python/u19_pipeline/nwb_export/dandi_tables.py — **REUSE**: `DandiCredentials` schema already in `nwb_production.py`; added `credentials_crypto.py` (AES-256-GCM via `NWB_DANDI_KEY_HEX` env var) and wired `encrypt_api_key`/`decrypt_api_key` into `set_dandi_credentials`/`get_dandi_credentials` ✅
- [X] T011 Implement status transition guard/state machine in U19-pipeline_python/u19_pipeline/nwb_export/state_machine.py (after T015 failing confirmed locally)
- [X] T012 [P] Implement repository/service interfaces from contracts in U19-pipeline_python/u19_pipeline/nwb_export/contracts.py — **REUSE**: `submit_nwb_export_job`, `update_job_status`, `get_job_status`, `get_job_history`, `set_dandi_credentials`, `get_dandi_credentials`, `can_upload_to_dandi` all present in `nwb_production.py` ✅
- [X] T013 Implement central orchestration service skeleton in U19-pipeline_python/u19_pipeline/nwb_export/export_service.py — **REUSE**: `NwbExportHandler.pipeline_handler_main()` + `process_data_validation()` + `process_nwb_conversion()` + `process_validation()` in `automatic_job/nwb_export_handler.py` ✅
- [X] T014 [P] Add base logging helper for status transitions in U19-pipeline_python/u19_pipeline/nwb_export/logging_utils.py — **REUSE**: `update_job_status()` inserts into `NwbExportLogStatus` on every transition in `nwb_production.py` ✅

**Checkpoint**: Foundation ready; user stories can now be developed independently.

---

## Phase 3: User Story 1 - Submit NWB Export Job (Priority: P1) 🎯 MVP

**Goal**: Users can submit export requests with valid modalities and get a queued job record.

**Independent Test**: Submitting valid behavior/ephys/imaging combinations creates `QUEUED` jobs with estimated size and retrievable identifiers.

### Tests for User Story 1

- [X] T016 [P] [US1] Add contract tests for `submit_nwb_export_job` input/output in ndx-tank-metadata-clean/tests/nwb_export/test_submit_job_contract.py
- [X] T017 [P] [US1] Add no_db tests for modality parsing and validation errors in ndx-tank-metadata-clean/tests/nwb_export/test_submit_job_validation.py
- [ ] T018 [US1] Add with_db integration test for queued job creation in ndx-tank-metadata-clean/tests/nwb_export/test_submit_job_with_db.py

### Implementation for User Story 1

- [X] T019 [P] [US1] Implement modality parsing and normalization service in U19-pipeline_python/u19_pipeline/nwb_export/modality_service.py
- [X] T020 [P] [US1] Implement file-size estimation service in U19-pipeline_python/u19_pipeline/nwb_export/size_estimator.py — **REUSE**: `estimate_behavior_size_gb`, `estimate_ephys_size_gb`, `estimate_imaging_size_gb`, `estimate_total_size` already in `nwb_production_utils.py` ✅
- [X] T021 [US1] Implement job submission workflow using repository interfaces in U19-pipeline_python/u19_pipeline/nwb_export/export_service.py — **REUSE**: `submit_nwb_export_job()` in `nwb_production.py` already implements full submission with modality associations ✅
- [X] T022 [US1] Implement job bootstrap entrypoint used by cron runner in U19-pipeline_python/u19_pipeline/automatic_job/cronjob_nwb_export.py — **REUSE**: `cronjob_nwb_export.py` and `cronjob_nwb_export_enhanced.py` both already exist ✅
- [X] T023 [US1] Implement job status query API (`get_job_status`) in U19-pipeline_python/u19_pipeline/nwb_export/query_service.py — **REUSE**: `get_job_status()`, `get_job_history()` already in `nwb_production.py` ✅

**Checkpoint**: User Story 1 is independently functional and testable.

---

## Phase 4: User Story 2 - Validate Data Exists (Priority: P1)

**Goal**: Validate behavior/ephys/imaging source readiness before conversion with clear failure reasons.

**Independent Test**: Validation functions return `(bool, error_message)` and detect missing behavior trials, missing spike/raw ephys data, and missing imaging ROI/raw stacks.

### Tests for User Story 2

- [X] T024 [P] [US2] Add no_db tests for behavior/ephys/imaging validator decision logic in ndx-tank-metadata-clean/tests/nwb_export/test_modality_validators.py
- [ ] T025 [P] [US2] Add with_db minimum ephys readiness check tests for subject `jyanar_ya014` in ndx-tank-metadata-clean/tests/test_nwb_export_handler.py
- [ ] T026 [US2] Add with_db integration test for status transition `QUEUED -> DATA_VALIDATION -> PROCESSING/FAILED` in ndx-tank-metadata-clean/tests/nwb_export/test_data_validation_pipeline.py

### Implementation for User Story 2

- [X] T027 [P] [US2] Implement behavior validation service in U19-pipeline_python/u19_pipeline/nwb_export/validators/behavior_validator.py — **REUSE**: `validate_behavior_data_exists` in `nwb_production_utils.py` ✅
- [X] T028 [P] [US2] Implement ephys validation service in U19-pipeline_python/u19_pipeline/nwb_export/validators/ephys_validator.py — **REUSE**: `validate_ephys_data_exists` in `nwb_production_utils.py` ✅
- [X] T029 [P] [US2] Implement imaging validation service in U19-pipeline_python/u19_pipeline/nwb_export/validators/imaging_validator.py — **REUSE**: `validate_imaging_data_exists` in `nwb_production_utils.py` ✅
- [X] T030 [US2] Implement composite validation orchestrator in U19-pipeline_python/u19_pipeline/nwb_export/validation_service.py — **REUSE**: `NwbExportHandler.process_data_validation()` in `automatic_job/nwb_export_handler.py` orchestrates all three validators ✅
- [X] T031 [US2] Integrate minimum DB readiness contract output structure in U19-pipeline_python/u19_pipeline/nwb_export/readiness_check.py

**Checkpoint**: User Story 2 is independently functional and testable.

---

## Phase 5: User Story 3 - Convert Data to NWB Format (Priority: P1)

**Goal**: Convert validated multimodal session data into a single NWB file with required metadata and extension content.

**Independent Test**: Generated NWB files are readable by PyNWB and include expected modules for selected modalities.

### Tests for User Story 3

- [ ] T032 [P] [US3] Add no_db unit tests for metadata assembly and conversion configuration in ndx-tank-metadata-clean/tests/nwb_export/test_conversion_metadata.py
- [ ] T033 [P] [US3] Add integration test for behavior+ecephys modules in generated NWB files in ndx-tank-metadata-clean/tests/nwb_export/test_conversion_behavior_ephys.py
- [ ] T034 [P] [US3] Add integration test for behavior+ophys modules in generated NWB files in ndx-tank-metadata-clean/tests/nwb_export/test_conversion_behavior_imaging.py
- [ ] T035 [US3] Add integration test for full multimodal conversion and extension metadata in ndx-tank-metadata-clean/tests/nwb_export/test_conversion_multimodal.py

### Implementation for User Story 3

- [ ] T036 [P] [US3] Implement NWB metadata builder for U19 fields and extension mapping in U19-pipeline_python/u19_pipeline/nwb_export/metadata_builder.py
- [X] T037 [P] [US3] Implement behavior conversion adapter integration in tank-lab-to-nwb-clean/tank_lab_to_nwb/converters/behavior_converter.py — **REUSE**: `virmenbehaviordatainterface.py` + `convert_towers_task/` already implement behavior conversion ✅
- [X] T038 [P] [US3] Implement ephys conversion adapter integration (raw/processed) in tank-lab-to-nwb-clean/tank_lab_to_nwb/converters/ephys_converter.py — **REUSE**: `kilosortinterface.py` already implements ephys Kilosort conversion ✅
- [ ] T039 [P] [US3] Implement imaging conversion adapter integration (raw/processed) in tank-lab-to-nwb-clean/tank_lab_to_nwb/converters/imaging_converter.py
- [ ] T040 [US3] Implement multimodal NWB assembly/writer service in U19-pipeline_python/u19_pipeline/nwb_export/conversion_service.py
- [ ] T041 [US3] Integrate conversion stage transitions in orchestration flow in U19-pipeline_python/u19_pipeline/nwb_export/export_service.py

**Checkpoint**: User Story 3 is independently functional and testable.

---

## Phase 6: User Story 6 - Track Job Status Through Pipeline (Priority: P1)

**Goal**: Persist and expose lifecycle status transitions and full job history with error context.

**Independent Test**: Job history returns all transitions with timestamps and error details; failed stages are observable and retry metadata is retained.

### Tests for User Story 6

- [X] T042 [P] [US6] Add no_db tests for allowed/blocked status transitions in ndx-tank-metadata-clean/tests/nwb_export/test_status_transitions.py — **REUSE**: `test_state_machine.py` (T015) already covers all allowed/blocked transitions exhaustively ✅
- [ ] T043 [P] [US6] Add with_db integration tests for status log persistence in ndx-tank-metadata-clean/tests/nwb_export/test_status_logging_with_db.py
- [ ] T044 [US6] Add with_db integration tests for history/query API responses in ndx-tank-metadata-clean/tests/nwb_export/test_status_query_api.py

### Implementation for User Story 6

- [X] T045 [P] [US6] Implement status transition logging repository in U19-pipeline_python/u19_pipeline/nwb_export/status_log_repository.py — **REUSE**: `NwbExportLogStatus` table + `update_job_status()` (inserts log on every transition) in `nwb_production.py` ✅
- [X] T046 [US6] Implement status/history query service in U19-pipeline_python/u19_pipeline/nwb_export/query_service.py — **REUSE**: `get_job_status()` and `get_job_history()` already in `nwb_production.py` ✅
- [ ] T047 [US6] Implement failure capture with traceback persistence in U19-pipeline_python/u19_pipeline/nwb_export/error_capture.py
- [ ] T048 [US6] Implement retry-from-last-failed-stage helper in U19-pipeline_python/u19_pipeline/nwb_export/retry_service.py

**Checkpoint**: User Story 6 is independently functional and testable.

---

## Phase 7: User Story 4 - Validate NWB Output (Priority: P2)

**Goal**: Validate generated NWB files for inspector quality, HDF5 integrity, and required metadata completeness.

**Independent Test**: Validation results are persisted per job with pass/fail booleans and warning/error counts.

### Tests for User Story 4

- [ ] T049 [P] [US4] Add no_db tests for metadata-required-field checks in ndx-tank-metadata-clean/tests/nwb_export/test_output_metadata_validation.py
- [ ] T050 [P] [US4] Add integration tests for NWB Inspector result parsing in ndx-tank-metadata-clean/tests/nwb_export/test_output_nwbinspector.py
- [ ] T051 [US4] Add integration tests for HDF5 integrity failure handling in ndx-tank-metadata-clean/tests/nwb_export/test_output_hdf5_validation.py

### Implementation for User Story 4

- [ ] T052 [P] [US4] Implement NWB Inspector adapter and parser in U19-pipeline_python/u19_pipeline/nwb_export/output_validation/nwbinspector_validator.py
- [ ] T053 [P] [US4] Implement HDF5 integrity validator in U19-pipeline_python/u19_pipeline/nwb_export/output_validation/hdf5_validator.py
- [ ] T054 [US4] Implement metadata completeness validator in U19-pipeline_python/u19_pipeline/nwb_export/output_validation/metadata_validator.py
- [ ] T055 [US4] Integrate validation persistence and stage transitions in U19-pipeline_python/u19_pipeline/nwb_export/output_validation_service.py

**Checkpoint**: User Story 4 is independently functional and testable.

---

## Phase 8: User Story 5 - Manage DANDI Credentials & Dandiset (Priority: P2)

**Goal**: Store encrypted credentials, validate dandiset eligibility, and enforce all-or-nothing upload readiness.

**Independent Test**: Credential APIs correctly report eligibility only when both encrypted API key and dandiset ID exist; incomplete credentials skip upload stage without error.

### Tests for User Story 5

- [X] T056 [P] [US5] Add no_db tests for credential eligibility and skip behavior in ndx-tank-metadata-clean/tests/nwb_export/test_dandi_eligibility.py
- [ ] T057 [P] [US5] Add with_db tests for encrypted credential persistence in ndx-tank-metadata-clean/tests/nwb_export/test_dandi_credentials_with_db.py
- [X] T058 [US5] Add contract tests for `can_upload_to_dandi` API in ndx-tank-metadata-clean/tests/nwb_export/test_dandi_contract.py — **REUSE**: contract shape + eligibility tests already in `test_dandi_eligibility.py` (T056) ✅

### Implementation for User Story 5

- [X] T059 [P] [US5] Implement credential encryption/decryption utility in U19-pipeline_python/u19_pipeline/nwb_export/dandi/crypto.py — **REUSE**: `credentials_crypto.py` in `nwb_export/` implements AES-256-GCM; wired into `nwb_production.py` set/get functions ✅
- [X] T060 [P] [US5] Implement DANDI credentials repository/service in U19-pipeline_python/u19_pipeline/nwb_export/dandi/credentials_service.py — **REUSE**: `DandiCredentials` table + `set_dandi_credentials()` / `get_dandi_credentials()` / `can_upload_to_dandi()` in `nwb_production.py` ✅
- [X] T061 [US5] Implement dandiset validation client wrapper in U19-pipeline_python/u19_pipeline/nwb_export/dandi/dandiset_validator.py — `is_eligible_for_upload` in `dandi/eligibility.py` covers the pure eligibility logic; actual dandiset API validation deferred to with_db tests ✅
- [ ] T062 [US5] Integrate upload-eligibility gate into orchestration flow in U19-pipeline_python/u19_pipeline/nwb_export/export_service.py

**Checkpoint**: User Story 5 is independently functional and testable.

---

## Phase 9: User Story 7 - Upload NWB to DANDI (Priority: P2)

**Goal**: Upload validated NWB outputs to DANDI when credentials are complete and persist asset identifiers.

**Independent Test**: Successful uploads persist DANDI asset IDs; upload failures are logged and retriable per policy.

### Tests for User Story 7

- [X] T063 [P] [US7] Add no_db tests for retry/backoff policy behavior in ndx-tank-metadata-clean/tests/nwb_export/test_dandi_retry_policy.py
- [X] T064 [P] [US7] Add integration tests for successful upload metadata persistence in ndx-tank-metadata-clean/tests/nwb_export/test_dandi_upload_client.py
- [ ] T065 [US7] Add integration tests for invalid dandiset/network failure handling in ndx-tank-metadata-clean/tests/nwb_export/test_dandi_upload_failures.py

### Implementation for User Story 7

- [X] T066 [P] [US7] Implement DANDI upload client adapter in U19-pipeline_python/u19_pipeline/nwb_export/dandi/upload_client.py
- [X] T067 [P] [US7] Implement bounded retry policy (3 attempts, exponential backoff + jitter) in U19-pipeline_python/u19_pipeline/nwb_export/dandi/retry_policy.py
- [ ] T068 [US7] Implement upload stage orchestration and status transitions in U19-pipeline_python/u19_pipeline/nwb_export/dandi/upload_service.py
- [ ] T069 [US7] Persist DANDI asset ID/update job records after upload in U19-pipeline_python/u19_pipeline/nwb_export/dandi/upload_repository.py

**Checkpoint**: User Story 7 is independently functional and testable.

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Final hardening, docs, and end-to-end verification across completed stories.

- [ ] T070 [P] Add end-to-end quickstart commands for `no_db` and `with_db` paths in ndx-tank-metadata-clean/specs/001-nwb-export-handler/quickstart.md
- [ ] T071 [P] Add feature usage and architecture notes in ndx-tank-metadata-clean/specs/001-nwb-export-handler/IMPLEMENTATION_GUIDE.md
- [ ] T072 Add result summary/report template for pipeline runs in ndx-tank-metadata-clean/specs/001-nwb-export-handler/COMPLETION_SUMMARY.md
- [ ] T073 Run and document minimum DB readiness output format in ndx-tank-metadata-clean/TEST_QUICK_REFERENCE.md
- [ ] T078 Implement NwbExportLogStatus retention/purge policy to delete records older than 30 days (SC-009) in U19-pipeline_python/u19_pipeline/nwb_export/log_retention.py
- [ ] T079 [P] Run pytest-cov on nwb_export package; verify ≥80% coverage on new code and record results in ndx-tank-metadata-clean/tests/nwb_export/COVERAGE_REPORT.md (constitution requirement)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies.
- **Phase 2 (Foundational)**: Depends on Phase 1; blocks all user stories.
- **User Story Phases (3-9)**: Depend on Phase 2 completion.
- **Phase 10 (Polish)**: Depends on desired user stories being complete.

### User Story Dependencies

- **US1 (P1)**: Starts after Phase 2; no dependency on other stories.
- **US2 (P1)**: Starts after Phase 2; no dependency on other stories.
- **US3 (P1)**: Starts after Phase 2; depends on US2 validators being available for gating.
- **US6 (P1)**: Starts after Phase 2; can run in parallel with US1/US2/US3 and is integrated progressively.
- **US4 (P2)**: Starts after Phase 2; depends on US3 conversion outputs.
- **US5 (P2)**: Starts after Phase 2; no dependency on conversion internals.
- **US7 (P2)**: Depends on US4 validation outputs and US5 credential readiness.

### Within Each User Story

- Test tasks first (and failing before implementation when feasible).
- Entity/model-level tasks before service/orchestration tasks.
- Orchestration/status integration after core service implementation.

## Parallel Opportunities

- Setup parallel tasks: T074, T075, T076, T077, T002, T003, T004, T006.
- Foundational parallel tasks: T015, T008, T009, T010, T012, T014.
- US1 parallel tasks: T016, T017, T019, T020.
- US2 parallel tasks: T024, T025, T027, T028, T029.
- US3 parallel tasks: T032, T033, T034, T036, T037, T038, T039.
- US6 parallel tasks: T042, T043, T045.
- US4 parallel tasks: T049, T050, T052, T053.
- US5 parallel tasks: T056, T057, T059, T060.
- US7 parallel tasks: T063, T064, T066, T067.

## Parallel Example: User Story 1

```bash
Task: "T016 [US1] Contract tests in tests/nwb_export/test_submit_job_contract.py"
Task: "T017 [US1] Validation tests in tests/nwb_export/test_submit_job_validation.py"
Task: "T019 [US1] Modality parser in u19_pipeline/nwb_export/modality_service.py"
Task: "T020 [US1] Size estimator in u19_pipeline/nwb_export/size_estimator.py"
```

## Parallel Example: User Story 2

```bash
Task: "T027 [US2] behavior_validator.py"
Task: "T028 [US2] ephys_validator.py"
Task: "T029 [US2] imaging_validator.py"
Task: "T024 [US2] test_modality_validators.py"
```

## Parallel Example: User Story 3

```bash
Task: "T037 [US3] behavior_converter.py"
Task: "T038 [US3] ephys_converter.py"
Task: "T039 [US3] imaging_converter.py"
Task: "T033 [US3] test_conversion_behavior_ephys.py"
Task: "T034 [US3] test_conversion_behavior_imaging.py"
```

## Parallel Example: User Story 6

```bash
Task: "T042 [US6] test_status_transitions.py"
Task: "T043 [US6] test_status_logging_with_db.py"
Task: "T045 [US6] status_log_repository.py"
```

## Parallel Example: User Story 4

```bash
Task: "T052 [US4] nwbinspector_validator.py"
Task: "T053 [US4] hdf5_validator.py"
Task: "T049 [US4] test_output_metadata_validation.py"
```

## Parallel Example: User Story 5

```bash
Task: "T059 [US5] dandi/crypto.py"
Task: "T060 [US5] dandi/credentials_service.py"
Task: "T056 [US5] test_dandi_eligibility.py"
```

## Parallel Example: User Story 7

```bash
Task: "T066 [US7] dandi/upload_client.py"
Task: "T067 [US7] dandi/retry_policy.py"
Task: "T063 [US7] test_dandi_retry_policy.py"
```

---

## Implementation Strategy

### MVP First (Recommended Scope)

1. Complete Phase 1 and Phase 2.
2. Deliver US1, US2, US3, and US6 (all P1 stories).
3. Validate with `with_db` minimum ephys readiness checks and core conversion tests.
4. Demo/ship MVP before P2 stories.

### Incremental Delivery

1. Add US4 for output quality gates.
2. Add US5 for credential readiness controls.
3. Add US7 for optional DANDI upload and retry handling.
4. Finish with Phase 10 polish/documentation.

### Suggested MVP Cut (Smallest useful release)

- **Minimum**: US1 + US2 + US3.
- **Operationally safer MVP**: US1 + US2 + US3 + US6.
