# Plan: wire imaging into the NWB export handler

Goal: an `imaging-raw` NWB export job runs through `nwb_export_handler`'s
validation → conversion flow and produces an NWB file whose `TwoPhotonSeries`
is aligned to the ViRMEn behavior clock via
`u19_pipeline/utils/imaging_behavior_sync.py`, instead of raising
"imaging export not yet wired". Plus forward-looking documentation of the
export process.

## Phase A — Feasibility survey (read-only, in progress)

- [x] Understand sync mechanism end-to-end (see `docs/imaging_behavior_sync.md`)
- [x] Python port of MATLAB sync (`u19_pipeline/utils/imaging_behavior_sync.py`), verified on sample data
- [x] Locate NWB branches: `origin/nwb-export-backend`, `origin/fix/nwb-export-handler-schema`, `origin/feat/nwb-export-handler-completion` (3 commits ahead of an ancestor 7 behind master)
- [x] Read export handler stages (`automatic_job/nwb_export_handler.py`): QUEUED→DATA_VALIDATION→PROCESSING→VALIDATION→UPLOAD→COMPLETED; imaging fails loud in `process_data_validation`
- [x] Read shared conversion path (`u19_pipeline/nwb_export/conversion.py`): `build_source_data` (behavior+ephys only) → `TowersNWBConverter` from external `tank_lab_to_nwb`
- [x] Find imaging stubs: `nwb_production_utils.validate_imaging_data_exists` exists but references `imaging_element.Scan` / `FieldOfView` which don't exist under those names (imaging_element = element_calcium_imaging.imaging_preprocess)
- [x] Confirm `recording_ids_for_session` helper exists for session→recording resolution
- [x] Locate local checkouts: `~/code/tank-lab-to-nwb`, `~/code/ndx-tank-metadata`
- [x] Survey `tank-lab-to-nwb`: live work is on `origin/building-nwb-converter` (modern neuroconv, `sync_timestamps` param, KilosortWithProbeInterface, PyNWB 3/HDMF 4); local `main` is stale and its `__init__` is broken (`se` used but not imported). Converter already declares `Suite2pSegmentation` + `TiffImagaging` (sic, generic TiffImagingInterface — wrong one for ScanImage volumetric BigTIFF) and applies ONE sync_timestamps array to all interfaces
- [x] Survey `ndx-tank-metadata`: rig/task metadata extension; `origin/001-nwb-export-handler` branch = spec-kit/test tooling only, no blocker
- [x] Check all local/remote branches of both repos for newer work (`building-nwb-converter` and `001-nwb-export-handler` are the relevant ones)
- [x] Determine tiff path resolution route: session → `recording_ids_for_session` (recording.Recording.BehaviorSession) → `imaging_pipeline.TiffSplit`/`TiffSplitFile` under `ImagingRootDataDir`; existing `validate_imaging_data_exists` stub references nonexistent `imaging_element.Scan`/`FieldOfView` and must be fixed
- [x] Write feasibility report (`~/.claude/plans/feasibility-nwb-imaging-wiring-2026-08-26.md`): **Feasible with caveats, size M** — no hard blockers; caveats are two-repo lockstep (tank-lab-to-nwb is path-installed, unpinned) and the imaging clock convention, which becomes sticky at first DANDI upload

Tracking issue: https://github.com/BrainCOGS/U19-pipeline-python/issues/111

## Phase B — Implementation (after feasibility verdict + user go-ahead)

- [ ] Rebase/merge strategy: bring `feat/nwb-export-handler-completion` and our sync branch together
- [ ] `validate_imaging_data_exists`: fix table references; resolve session→recording→TiffSplit
- [ ] `resolve_input_paths` / `build_source_data`: add imaging tiff paths + behavior file
- [ ] Converter: add ScanImage imaging interface with behavior-clock timestamps (subclass or extend TowersNWBConverter)
- [ ] Handler `process_data_validation` imaging branch: replace fail-loud TODO
- [ ] Size estimation: `estimate_imaging_size_gb` with real numbers (raw frames ≫ 0.05 GB/FOV)
- [ ] Tests (mirror existing handler tests)
- [ ] End-to-end run on the `~/neuro-data` sample session

## Phase C — Documentation

- [ ] `docs/nwb_export.md`: how imaging export works and how to add future modalities
- [ ] Update `docs/imaging_behavior_sync.md` cross-references
