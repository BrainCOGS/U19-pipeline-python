# Implementation Plan: NWB Export Handler & DANDI Upload Pipeline

**Branch**: `001-nwb-export-handler` | **Date**: 2026-02-26 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-nwb-export-handler/spec.md`

## Summary

Implement an NWB export pipeline with DataJoint-backed job orchestration, modality validation (behavior/ephys/imaging), NWB conversion, output validation, optional DANDI upload, and status tracking. Current minimum database test scope is ephys-focused: verify subject `jyanar_ya014` exists, verify multiple ephys sessions exist, and verify one ephys session occurs on `2024-07-22`; imaging assertions are deferred.

## Technical Context

**Language/Version**: Python 3.12+  
**Primary Dependencies**: datajoint, pynwb>=3.0.0, neuroconv, nwbinspector, h5py, pytest  
**Storage**: DataJoint-managed MySQL/MariaDB + NWB files on filesystem/shared storage  
**Testing**: pytest with markers (`no_db`, `with_db`)  
**Target Platform**: Linux/macOS research compute environments  
**Project Type**: Python data pipeline + library integration  
**Performance Goals**: Full pipeline completion within 15 minutes for standard sessions; DB precheck in seconds  
**Constraints**: DataJoint-first access, Enum state modeling, no plaintext DANDI secrets, optional DANDI stage  
**Scale/Scope**: Concurrent cron processing (≥10 jobs), multi-modality sessions, long-running conversion jobs

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **I. DataJoint-First Database Access**: PASS — all DB interactions planned via DataJoint tables/modules only.
- **II. Modern Python Practices (>=3.12)**: PASS — Python 3.12+ target and typed public APIs retained.
- **III. Structural Reuse Before Creation**: PASS — extends existing U19 pipeline schemas/flows instead of replacing core tables.
- **IV. Explicit State Modeling via Enums**: PASS — uses dedicated export status/modality/upload enums.
- **V. Test-First Development**: PASS — minimum DB precheck tests specified before implementation tasks.

Post-Design Re-check: PASS (no constitution violations introduced by design artifacts).

## Project Structure

### Documentation (this feature)

```text
specs/001-nwb-export-handler/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
└── tasks.md
```

### Source Code (workspace)

```text
ndx-tank-metadata-clean/
├── specs/001-nwb-export-handler/
└── tests/

U19-pipeline_python/
└── u19_pipeline/
    ├── nwb_production.py
    ├── nwb_export_enums.py
    └── automatic_job/
        └── cronjob_nwb_export*.py

tank-lab-to-nwb-clean/
└── notebooks/
    └── unified_virmen_kilosort_conversion_v2.ipynb
```

**Structure Decision**: Keep planning/design artifacts in `specs/001-nwb-export-handler/`, keep test definitions in `tests/`, and implement runtime/export logic in `U19-pipeline_python` and converter integration in `tank-lab-to-nwb-clean`.

## Phase 0 Research Focus

1. Resolve DANDI retry-policy ambiguity in spec.
2. Define robust DataJoint query pattern for ephys-only minimum DB test.
3. Confirm best-practice separation between `no_db` and `with_db` test levels.

## Phase 1 Design Focus

1. Formalize job/state/data entities and transitions.
2. Define contracts for job submission/status queries and minimum DB readiness check.
3. Provide quickstart commands for local verification (`no_db` vs `with_db`).

## Complexity Tracking

No constitution violations requiring exemption.
