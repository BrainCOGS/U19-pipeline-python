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

### Spike result — **PASS** (2026-08-26)

`TowersNWBConverter` + `ScanImageImagingInterface` + our behavior-clock
timestamps coexist in one env (neuroconv 0.10.0, roiextractors 0.9.0, pynwb
4.1.0, hdmf 6.2.0, spikeinterface 0.104.8, datajoint 0.14.9 — no conflict).
A real conversion of the sample session wrote a 0.42 GB NWB with VirmenData
behavior (179 trials) + a `TwoPhotonSeries` of 400 volume timestamps, and
trial 1's first imaging frame lands **+22.3 ms** after trial 1 starts — one
frame period at 50.2 Hz, as expected.

Three defects surfaced that task D must fix in `tank-lab-to-nwb`:

1. **One `sync_timestamps` array for every interface.**
   `temporally_align_data_interfaces` applies the same array to all
   interfaces; imaging has a different sample count (400 volumes vs behavior's
   per-iteration frames) and needs its own. Alignment must become
   per-interface.
2. **`convert_function_handle_to_str` hard-requires MATLAB.** It raises when
   `which("matlab")` is None, which kills the whole conversion — even though
   all four values it produces (`experiment_name`, `protocol_name`,
   `trial_choice`, `trial_type`) are already treated as optional by the only
   caller. It should warn and return `{}`. No MATLAB on the export host means
   no export at all today.
3. **tz-aware/naive mismatch in `VirmenDataInterface.get_original_timestamps`.**
   `_get_session_start_time()` is tz-aware (`America/New_York`) but
   `array_to_dt(epoch["start"])` is naive, so the subtraction raises
   `TypeError`. It is masked whenever `sync_timestamps` is supplied — which an
   imaging-only session has no reason to supply, so this fires the moment
   imaging goes through.

**Clock zero-point (contract detail, easy to get wrong).**
`frame_times_on_behavior_clock` returns times on the *block-relative* ViRMEn
clock (`trial.start + trial.time`), but the converter zeroes the NWB timeline
at `log.session.start` and shifts its trials table by `epoch_start_nwb`. On
the sample session that offset is **+27.0 ms**. Imaging timestamps must take
the same shift; without it frame 644 lands 4.7 ms *before* trial 1 starts,
which is physically backwards.

- [x] Spike: converter + ScanImage interface + our timestamps in one env, real conversion, aligned TwoPhotonSeries
- [x] Rebase/merge strategy: bring `feat/nwb-export-handler-completion` and our sync branch together
- [x] `validate_imaging_data_exists`: fix table references; resolve session→recording→TiffSplit
- [x] `resolve_input_paths` / `build_source_data`: add imaging tiff paths + behavior file
- [x] Converter: add ScanImage imaging interface with behavior-clock timestamps (subclass or extend TowersNWBConverter)
- [x] Handler `process_data_validation` imaging branch: replace fail-loud TODO
- [x] Size estimation: `estimate_imaging_size_gb` with real numbers (raw frames ≫ 0.05 GB/FOV)
- [x] Tests (mirror existing handler tests)
- [x] End-to-end run on the `~/neuro-data` sample session

## Phase C — Documentation

- [x] `docs/nwb_export.md`: how imaging export works and how to add future modalities
- [x] Update `docs/imaging_behavior_sync.md` cross-references

## Phase D — Cross-repo contract (coordinator)

- [x] `tank-lab-to-nwb`: register `ScanImageImagingInterface`, fix the `TiffImagaging` typo key
- [x] `tank-lab-to-nwb`: per-interface `aligned_timestamps`, with a length guard on the shared array
- [x] `tank-lab-to-nwb`: stop `convert_function_handle_to_str` raising when MATLAB is absent
- [x] `tank-lab-to-nwb`: run its scratch files in a TemporaryDirectory instead of the cwd
- [x] `tank-lab-to-nwb`: fix the tz-aware/naive subtraction in `get_original_timestamps`
- [x] Clock convention decided and written down (`docs/imaging_behavior_sync.md` section 6, `docs/nwb_export.md`)
- [x] Mixed ephys+imaging clock rule — **not applicable**: the two modalities are not acquired in the same session (confirmed 2026-08-27)
- [ ] Pin the `tank-lab-to-nwb` dependency to a commit
- [ ] PRs: this repo + `tank-lab-to-nwb` (`building-nwb-converter`)

### End-to-end result (sample session, through the shipped code path)

```
source_data: ['ScanImageImaging', 'VirmenData']
diagnostics: slope 1.000027891, residual 10.4 ms, epoch_offset 27.0 ms,
             2000 frames -> 400 volumes
nwb:         TwoPhotonSeries + 179 trials, 0.420 GB
frame 644 (trial 1's first imaging frame) = +22.3 ms after trial 1 start
```

The per-frame check is the meaningful one; the written file stores volumes, so
its resolution is one volume period (99.6 ms).

### Known gaps

- `resolve_imaging_paths` and the handler's imaging branch are exercised only by
  mocked tests — no DataJoint instance was reachable here, so the
  session -> recording -> TiffSplit hop is unverified against a real database.
- The end-to-end run supplied TIFF paths directly, bypassing that same hop.
- `tests/nwb_export/test_nwb_export_handler.py` has 10 failures + 9 errors that
  predate this work: they fail at import on `dj.config["custom"]`, with no DB
  configured.
