# Handoff: implement imaging NWB export (issue #111)

Instructions for a fresh agent session — written to be self-contained. You are
the **coordinator** (Fable): do the cross-repo design work and merges yourself,
and delegate the well-scoped mechanical tasks to Sonnet subagents (Opus for
anything requiring deeper design judgment). Keep the checkbox files and the
GitHub issue updated as you go.

## Mission

Implement https://github.com/BrainCOGS/U19-pipeline-python/issues/111 —
wire imaging (with behavior-clock sync) into the NWB export handler, and write
`docs/nwb_export.md` documenting the export process. The feasibility verdict
is **Feasible with caveats, size M**; the analysis is done, don't redo it.

## Read these first, in this order

1. `docs/nwb_imaging_export_plan.md` — the checkbox tracker (Phase A done,
   you own Phases B and C). **Update it and issue #111's checkboxes as you
   complete tasks** (`gh issue edit 111 --body-file ...` or check boxes via
   the web body).
2. `docs/imaging_behavior_sync.md` — how the I2C sync works end-to-end, the
   verified NWB recipe, and the smoke-test numbers you must reproduce.
3. `~/.claude/plans/feasibility-nwb-imaging-wiring-2026-08-26.md` — every
   touchpoint with `file:line`, constraints, and the spike definition.
4. Issue #111 itself (`gh issue view 111`) — the task list of record.

## Where everything lives

| What | Where |
|---|---|
| This repo (worktree) | `/home/chris/code/U19-pipeline-python/.claude/worktrees/imaging-behavior-sync-fb610a`, branch **`feat/nwb-imaging-export`** (off master). Do NOT `cd` to the main checkout. |
| Work you inherit (already committed on that branch) | `u19_pipeline/utils/imaging_behavior_sync.py` (the sync port), `docs/` (this handoff, `imaging_behavior_sync.md`, `nwb_imaging_export_plan.md`), `uv.lock`. |
| NWB export backend | branches `origin/feat/nwb-export-handler-completion` (tip, use this), `origin/fix/nwb-export-handler-schema`, `origin/nwb-export-backend`. Only 3 commits ahead of an ancestor 7 behind master — merging with master/our branch is cheap. |
| Converter | `~/code/tank-lab-to-nwb`, branch **`origin/building-nwb-converter`** (the one whose `TowersNWBConverter(source_data, sync_timestamps=)` signature matches the handler; local `main` is stale and broken). Use `git -C ~/code/tank-lab-to-nwb worktree add <dir> origin/building-nwb-converter` — don't switch that repo's checked-out branch. |
| NWB extension | `~/code/ndx-tank-metadata` (not on the critical path). |
| Reference-only repos | `~/code/U19-pipeline-matlab` (SyncImagingBehavior.m, getSyncInfo.cpp), `~/code/ViRMEn` (nidaqI2C.cpp, updateDAQSyncSignals.m). |
| Sample data | `~/neuro-data/`: `ef932_act131_08072026_00001_00001.tif` (2000 pages, 5-slice fastZ → 400 volumes, 50.2 Hz) + the `Session_..._ef932_act131_20260807_1.mat` behavior log. |

## Environment

```bash
cd /home/chris/code/U19-pipeline-python/.claude/worktrees/imaging-behavior-sync-fb610a
uv sync        # venv with datajoint/scipy/tifffile; pyproject "exclude-newer" TOML warnings are benign — ignore
```

- NWB stack is NOT in the project deps. For scratch experiments:
  `uv run --no-project --with neuroconv --with roiextractors --with tifffile --with scipy python ...`
  (verified working: neuroconv 0.10.0, roiextractors 0.9.0, pynwb 4.1.0).
- For the integration itself, install the converter editable into the venv:
  `uv pip install -e <tank-lab-to-nwb worktree dir>` (its deps pull neuroconv).
- `import u19_pipeline` requires datajoint (package `__init__`); to use the
  sync module without the venv, load it by path with `importlib` (see the
  smoke-test snippet in `docs/imaging_behavior_sync.md`).
- Sanity command that must keep working:

```bash
uv run python -m u19_pipeline.utils.imaging_behavior_sync ~/neuro-data/ef932_act131_08072026_00001_00001.tif --behavior-mat ~/neuro-data/Session_z_LSTT_Active_TrialStructure_World_Recording_EF_182-Imaging-Rig1_efonseca_ef932_act131_20260807_1.mat
```

Expected: trials at frames 644–1176 / 1177–1690 / 1691–2000; fit slope
≈1.000027891, residual ≈10.4 ms.

## Step 0 — done for you

The sync port and docs are already committed on `feat/nwb-imaging-export`
(this worktree's checked-out branch). Verify with `git log --oneline -3`.

## Step 1 — merge in the export backend

```bash
git merge origin/feat/nwb-export-handler-completion   # resolve; divergence is small
```

## Step 2 — the spike (do this before any real wiring)

Goal: prove `TowersNWBConverter` (building-nwb-converter) + our timestamps +
`ScanImageImagingInterface` coexist in one env. Half a day, coordinator does
this personally (it's the decision point).

1. Worktree + install tank-lab-to-nwb as above.
2. Hack `u19_pipeline/nwb_export/conversion.py:build_source_data` to add a
   ScanImage entry for the sample TIFF; compute timestamps with
   `sync_imaging_behavior` + `frame_times_on_behavior_clock`; remember the
   file is volumetric — the interface exposes 400 volumes, so pass
   `timestamps[::5][:400]`.
3. Run the conversion (stub-sized write is fine).

**Pass:** NWB file has VirmenData behavior + TwoPhotonSeries, trial 1
(start 1.757 s) has its first imaging frame at ≈1.77 s.
**Fail:** version conflict or the converter can't take per-interface
timestamps → report findings on issue #111 before proceeding; the fix then
happens in tank-lab-to-nwb first.

## Step 3 — implementation (delegate the parallel pieces)

Suggested split — spawn Sonnet subagents for A/B/C (each is well-scoped and
independent after the spike); keep D and the merge/decision work yourself:

- **A (Sonnet):** `u19_pipeline/nwb_production_utils.py` —
  rewrite `validate_imaging_data_exists` (`:236-262`; it references
  `imaging_element.Scan`/`FieldOfView` which don't exist — `imaging_element`
  is `element_calcium_imaging.imaging_preprocess` per
  `u19_pipeline/imaging_pipeline.py:15-16`; validate via
  `imaging_pipeline.TiffSplit`/`TiffSplit.File` restricted through
  `recording_ids_for_session`, `:12-35`), and fix `estimate_imaging_size_gb`
  (`:82-98`, 0.05 GB/FOV is >10× off — the sample is 0.62 GB for 2000 frames).
- **B (Sonnet):** tests mirroring the branch's existing handler test suite
  for the new imaging paths (validation, path resolution, conversion source
  data).
- **C (Sonnet):** draft `docs/nwb_export.md` — pipeline stages, modality
  wiring recipe (use `docs/imaging_behavior_sync.md` §5 as the imaging
  source of truth), dependency/pinning story for tank-lab-to-nwb.
- **D (coordinator, or Opus):** the cross-repo contract —
  1. tank-lab-to-nwb `towersnwbconverter.py`: add `ScanImageImagingInterface`
     (fix the `"TiffImagaging"` typo key) and per-interface aligned
     timestamps in `temporally_align_data_interfaces` (currently applies ONE
     `sync_timestamps` array to every interface — wrong for imaging, whose
     timestamp count differs from behavior's).
  2. `u19_pipeline/nwb_export/conversion.py`: TIFF path resolution
     (`TiffSplit.tiff_split_directory` under `dj.config` ImagingRootDataDir)
     + imaging in `build_source_data` + imaging timestamps into the
     converter call.
  3. `nwb_export_handler.py:247-257`: replace the fail-loud imaging branch.
  4. **Decide and write down the clock convention** (imaging-only sessions →
     ViRMEn clock; rule for mixed ephys+imaging) — this hardens at first
     DANDI upload, so it goes in `docs/nwb_export.md` and on issue #111 for
     sign-off before any production upload.

## Step 4 — end-to-end + wrap-up

- Run the handler path (or `scripts/run_nwb_export.py`) against the sample
  session; verify the acceptance numbers.
- Tick boxes in `docs/nwb_imaging_export_plan.md` and issue #111.
- PRs: one against `U19-pipeline-python` (targeting the NWB branch line or
  master per the user's call — ask), one against `tank-lab-to-nwb`
  (`building-nwb-converter`). Confirm with the user before pushing/opening.

## Gotchas (learned the hard way — don't rediscover)

- `u19_pipeline/utils/tiff_matlab_imaging_utils.py` has an I2C parse using
  `ast.literal_eval` on MATLAB brace syntax — it silently always fails. Don't
  reuse it; the working parser is `imaging_behavior_sync.parse_scanimage_sync`.
- The sync port keeps MATLAB quirks on purpose (1-based frame indices,
  iteration spans offset by +1, first-I2C-packet-per-frame). They match the
  production DataJoint table — do not "fix" them.
- Behavior logs load with `scipy.io.loadmat(..., squeeze_me=True,
  struct_as_record=False)`; length-1 struct arrays collapse to scalars
  (helper `_as_list` in the sync module handles it). The raw log has no
  block-level `trialType` — count trials with `np.size(block.trial)`.
- ScanImage BigTIFF: use `tifffile` page `description` strings; never parse
  the file byte-wise. `I2CData` bytes are little-endian uint16 triples
  `[block, trial, iteration]`; frames can have 0, 1, or 2 packets.
- Shared git stash across worktrees: never bare `git stash` — use a WIP
  commit instead.
- A memory file for future sessions exists at
  `~/.claude/projects/-home-chris-code-U19-pipeline-python/memory/imaging-behavior-sync-mechanism.md`.
