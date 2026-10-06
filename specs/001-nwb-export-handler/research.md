# Phase 0 Research: NWB Export Handler & Minimum DB Tests

## Decision 1: DANDI retry policy

- Decision: Use automatic bounded retries for transient upload failures (3 attempts, exponential backoff with jitter), then require explicit user-triggered retry for persistent failures.
- Rationale: Prevents silent infinite retry loops while handling common transient network/API faults.
- Alternatives considered:
  - Manual retries only: simpler but higher operator burden and lower resilience.
  - Infinite automatic retries: higher chance of queue blockage and unclear terminal behavior.

## Decision 2: Minimum database readiness check scope

- Decision: Minimum DB checks are ephys-focused only for now:
  1) subject `jyanar_ya014` exists,
  2) multiple ephys sessions exist,
  3) at least one ephys session is on `2024-07-22`,
  4) imaging checks are explicitly out of scope in this phase.
- Rationale: Matches current operational requirement and unblocks technical plan while imaging ingestion paths are still evolving.
- Alternatives considered:
  - Include behavior + imaging in minimum check: broader but currently adds unstable dependency for early gating.
  - Session-only existence check: too weak to validate ephys pipeline readiness.

## Decision 3: DataJoint query strategy for ephys checks

- Decision: Use DataJoint joins based on `acquisition.Session * recording.Recording.EphysSession` filtered by `subject_fullname` and inspected for session dates.
- Rationale: DataJoint-first constitutional requirement and direct mapping to ephys recording presence.
- Alternatives considered:
  - Query only `acquisition.Session`: insufficient proof of ephys data.
  - Query lower-level processing tables only: too coupled to downstream processing state.

## Decision 4: Test levels and execution boundaries

- Decision: Keep two explicit pytest levels:
  - `no_db`: pure logic/interface tests,
  - `with_db`: DataJoint connectivity and integration tests including minimum ephys readiness checks.
- Rationale: Fast local feedback plus deterministic integration validation.
- Alternatives considered:
  - Single test level: either too slow (all with DB) or too weak (all mocked).

## Decision 5: Contract shape for upcoming implementation phase

- Decision: Define lightweight markdown contracts for:
  - job/status API expectations,
  - minimum DB readiness check input/output and pass criteria.
- Rationale: Keeps implementation language-native and easy to align with existing Python services.
- Alternatives considered:
  - OpenAPI-only contracts: overkill for internal Python service APIs not exposed as HTTP endpoints.
