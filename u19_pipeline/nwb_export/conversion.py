"""
Shared NWB conversion logic for the U19 export pipeline.

This module holds the importable functions that drive a real NWB conversion via
``TowersNWBConverter``. Both the CLI (``scripts/run_nwb_export.py``) and the
cronjob handler (``u19_pipeline/automatic_job/nwb_export_handler.py``) import
from here so there is a single, shared code path.

The logic was ported from ``scripts/run_nwb_export.py`` (``_build_source_data``,
``_query_metadata`` and the PROCESSING block) so it can be reused without
duplication.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Input-path resolution
# ──────────────────────────────────────────────────────────────────────────────


def resolve_input_paths(
    job: dict,
    export_params: dict,
) -> tuple[Path | None, Path | None]:
    """
    Resolve the behavioral Virmen file and the Kilosort base directory.

    Resolution order:
      1. Explicit paths from ``export_params`` (keys ``virmen_file`` /
         ``kilosort_dir``).
      2. A data-root env var (``NWB_EXPORT_DATA_ROOT``) joined with the session
         identifiers. This is intentionally a *simple* convention; we do not try
         to guess a specific lab directory layout we cannot verify.
      3. If nothing resolves, raise a clear, actionable error.

    Args:
        job: NwbExportJob record dict (carries subject_fullname/session_date/...).
        export_params: Parsed export_parameters dict for the job.

    Returns:
        ``(virmen_file, kilosort_dir)`` — ``kilosort_dir`` may be ``None`` when
        the job has no ephys component.

    Raises:
        FileNotFoundError: If the Virmen behavioral file cannot be resolved.
    """
    virmen_file: Path | None = None
    kilosort_dir: Path | None = None

    # 1. Explicit paths from export_params.
    if export_params.get("virmen_file"):
        virmen_file = Path(export_params["virmen_file"])
    if export_params.get("kilosort_dir"):
        kilosort_dir = Path(export_params["kilosort_dir"])

    # 2. Fall back to a data-root env var + session identifiers.
    data_root = os.environ.get("NWB_EXPORT_DATA_ROOT")
    if virmen_file is None and data_root:
        candidate = (
            Path(data_root)
            / str(job.get("subject_fullname", ""))
            / f"{job.get('session_date', '')}_{job.get('session_number', '')}.mat"
        )
        if candidate.exists():
            virmen_file = candidate

    # 3. Could not resolve the (required) behavioral file → fail loud.
    if virmen_file is None:
        raise FileNotFoundError(
            "Could not resolve the Virmen behavioral .mat file for this job. "
            "Provide it explicitly via export_parameters['virmen_file'] "
            "(and export_parameters['kilosort_dir'] for ephys), or set the "
            "NWB_EXPORT_DATA_ROOT environment variable to a data root containing "
            "<subject_fullname>/<session_date>_<session_number>.mat."
        )
    if not virmen_file.exists():
        raise FileNotFoundError(f"Virmen file not found: {virmen_file}")

    return virmen_file, kilosort_dir


# ──────────────────────────────────────────────────────────────────────────────
# source_data construction (ported from run_nwb_export.py)
# ──────────────────────────────────────────────────────────────────────────────


def _find_kilosort_output(probe_dir: Path) -> Path | None:
    """
    Return the highest-numbered job's Kilosort output directory under probe_dir.

    Expected layout:
        <kilosort_dir>/<probe>_imec<N>/job_id_<N>/kilosort<k>_output/
    """
    job_dirs = sorted(
        [p for p in probe_dir.glob("job_id_*") if p.name.split("_")[-1].isdigit()],
        key=lambda p: int(p.name.split("_")[-1]),
    )
    if not job_dirs:
        return None
    kilosort_outputs = list(job_dirs[-1].glob("kilosort*_output"))
    return kilosort_outputs[0] if kilosort_outputs else None


def resolve_imaging_paths(recording_key: dict, fov_numbers: list | None = None) -> list:
    """
    Resolve the split TIFF files for an imaging recording to absolute paths.

    Mirrors ``imaging_pipeline.get_scan_image_files`` (:277-290), joining
    ``TiffSplit.tiff_split_directory`` / ``TiffSplit.File.tiff_split_filename``
    onto the configured imaging root. Files come back in acquisition order
    (``tiff_split``, then ``file_number``), which is what the ScanImage
    interface needs — it treats a multi-file list as one continuous recording,
    so out-of-order paths silently produce out-of-order frames.

    Args:
        recording_key: Key resolvable to a ``recording.Recording`` row, e.g.
            ``{"recording_id": 3}``.
        fov_numbers: ``tiff_split`` numbers to include. Empty or ``None`` means
            every split belonging to the recording.

    Returns:
        List of absolute path strings; empty if nothing resolves.
    """
    import pathlib as _pathlib  # noqa: PLC0415

    import datajoint as dj  # noqa: PLC0415

    from u19_pipeline import imaging_pipeline  # noqa: PLC0415
    from u19_pipeline.nwb_production_utils import (
        recording_ids_for_session,  # noqa: PLC0415
    )

    restriction = dict(recording_key)
    if "recording_id" not in restriction:
        # A job carries the acquisition.Session key; the imaging tables hang off
        # recording.Recording, so take the same hop the validators do.
        recording_ids = recording_ids_for_session(restriction)
        if not recording_ids:
            return []
        restriction = [{"recording_id": rid} for rid in recording_ids]

    splits = imaging_pipeline.TiffSplit & restriction
    if fov_numbers:
        splits = splits & [{"tiff_split": int(n)} for n in fov_numbers]

    # tiff_split_directory lives on the master, tiff_split_filename on the Part,
    # so the join is required to get both -- same shape as
    # imaging_pipeline.get_scan_image_files.
    rows = (imaging_pipeline.TiffSplit.File * splits).fetch(
        "tiff_split_directory", "tiff_split_filename", as_dict=True
    )

    def _order(row):
        return (row.get("tiff_split", 0), row.get("file_number", 0))

    # Same roots imaging_pipeline.get_imaging_root_data_dir reads. Resolved
    # inline rather than through element_interface.find_full_path because
    # element-interface lives in the optional "pipeline" extra, and this helper
    # should stay importable without it.
    roots = dj.config.get("custom", {}).get("imaging_root_data_dir", None) or []
    if isinstance(roots, (str, _pathlib.Path)):
        roots = [roots]

    paths = []
    for row in sorted(rows, key=_order):
        relative = (
            _pathlib.Path(row["tiff_split_directory"]) / row["tiff_split_filename"]
        )
        for root in roots:
            candidate = _pathlib.Path(root) / relative
            if candidate.exists():
                paths.append(candidate.as_posix())
                break
        else:
            log.warning(
                "Imaging file listed in TiffSplit.File not found under any "
                "imaging_root_data_dir: %s",
                relative,
            )
    return paths


def resolve_imaging_paths_by_fov(
    recording_key: dict, fov_numbers: list | None = None
) -> dict:
    """
    Same resolution as :func:`resolve_imaging_paths`, grouped by ``tiff_split``.

    A ``tiff_split`` is one field of view of a mesoscope session. Fields of view
    are separate regions of tissue, not continuations of one another, so each
    needs its own imaging interface and its own ``TwoPhotonSeries``. Flattening
    them into a single file list would present unrelated fields of view as one
    continuous recording, and would misalign every one after the first.

    Returns:
        ``{tiff_split: [absolute path, ...]}``, each list in acquisition order.
        Splits whose files are all missing from disk are omitted.
    """
    import pathlib as _pathlib  # noqa: PLC0415

    import datajoint as dj  # noqa: PLC0415

    from u19_pipeline import imaging_pipeline  # noqa: PLC0415
    from u19_pipeline.nwb_production_utils import (  # noqa: PLC0415
        recording_ids_for_session,
    )

    restriction = dict(recording_key)
    if "recording_id" not in restriction:
        recording_ids = recording_ids_for_session(restriction)
        if not recording_ids:
            return {}
        restriction = [{"recording_id": rid} for rid in recording_ids]

    splits = imaging_pipeline.TiffSplit & restriction
    if fov_numbers:
        splits = splits & [{"tiff_split": int(n)} for n in fov_numbers]

    rows = (imaging_pipeline.TiffSplit.File * splits).fetch(
        "tiff_split",
        "file_number",
        "tiff_split_directory",
        "tiff_split_filename",
        as_dict=True,
    )

    roots = dj.config.get("custom", {}).get("imaging_root_data_dir", None) or []
    if isinstance(roots, (str, _pathlib.Path)):
        roots = [roots]

    by_fov: dict = {}
    for row in sorted(
        rows, key=lambda r: (r.get("tiff_split", 0), r.get("file_number", 0))
    ):
        relative = (
            _pathlib.Path(row["tiff_split_directory"]) / row["tiff_split_filename"]
        )
        for root in roots:
            candidate = _pathlib.Path(root) / relative
            if candidate.exists():
                by_fov.setdefault(int(row.get("tiff_split", 0)), []).append(
                    candidate.as_posix()
                )
                break
        else:
            log.warning(
                "Imaging file listed in TiffSplit.File not found under any "
                "imaging_root_data_dir: %s",
                relative,
            )
    return by_fov


def scanimage_plane_count(tiff_path) -> int:
    """
    Number of fastZ planes interleaved page by page in a ScanImage TIFF.

    Uses ``SI.hStackManager.actualNumSlices``, not ``numSlices``: the latter
    can hold a stale setting (the sample mesoscope file says 91 for a
    single-plane acquisition). Only the layouts the export handles are
    accepted: one saved channel, and either no stack or a fast (interleaved)
    stack.
    """
    import tifffile  # noqa: PLC0415

    with tifffile.TiffFile(str(tiff_path)) as tif:
        meta = tif.scanimage_metadata
    frame_data = (meta or {}).get("FrameData") or {}
    if not frame_data:
        raise ValueError(
            f"{tiff_path} has no ScanImage metadata, so its plane layout cannot be "
            f"determined. Per-ROI splits written by the legacy u19_meso pipeline "
            f"are missing it; export from the raw ScanImage files instead."
        )

    saved = frame_data.get("SI.hChannels.channelSave", 1)
    n_channels = len(saved) if isinstance(saved, (list, tuple)) else 1
    if n_channels != 1:
        raise NotImplementedError(
            f"{tiff_path} saves {n_channels} channels; pages then interleave "
            f"channels as well as planes, which the imaging export does not handle."
        )

    if not frame_data.get("SI.hStackManager.enable", False):
        return 1
    mode = frame_data.get("SI.hStackManager.stackMode", "fast")
    if mode != "fast":
        raise NotImplementedError(
            f"{tiff_path} is a '{mode}' z-stack; only fast (interleaved) stacks "
            f"are supported."
        )
    n = frame_data.get("SI.hStackManager.actualNumSlices")
    if n is None:
        n = frame_data.get("SI.hStackManager.numSlices", 1)
    return max(int(n), 1)


def page_timestamps_for_session(tiff_paths: list, virmen_file):
    """
    One NWB-timeline timestamp per TIFF page for ``tiff_paths``.

    Runs the I2C content-based sync (``u19_pipeline.utils.imaging_behavior_sync``)
    and then applies the block-vs-session shift, because the two clocks do not
    share a zero: ``trial.start`` rides ViRMEn's ``vr.timeElapsed``, zeroed at
    block start, while NWB zeroes at ``log.session.start``. That offset is
    allocation and file-I/O cost during ViRMEn startup, so it differs per
    session and is read from each log rather than assumed. See
    ``docs/imaging_behavior_sync.md`` section 6.

    Args:
        tiff_paths: The TIFFs of one field of view, in acquisition order.
        virmen_file: The session's ViRMEn behavior .mat file.

    Returns:
        ``(page_timestamps, diagnostics)`` -- diagnostics carries the fit slope,
        residual and the applied offset, for logging and validation.
    """
    import numpy as np  # noqa: PLC0415

    from u19_pipeline.utils.imaging_behavior_sync import (  # noqa: PLC0415
        _as_list,
        frame_times_on_behavior_clock,
        load_behavior_log,
        sync_imaging_behavior,
    )

    log_struct = load_behavior_log(str(virmen_file))
    sync = sync_imaging_behavior([str(p) for p in tiff_paths], log_struct)
    timestamps, slope, offset, residual = frame_times_on_behavior_clock(
        sync, log_struct
    )

    def _to_datetime(datevec):
        arr = np.asarray(datevec, dtype=float)
        return datetime(
            *[int(v) for v in arr[:5]],
            int(arr[5]),
            int(round(np.mod(arr[5], 1) * 1e6)),
        )

    block_start = _to_datetime(_as_list(log_struct.block)[0].start)
    session_start = _to_datetime(log_struct.session.start)
    epoch_offset = (block_start - session_start).total_seconds()
    timestamps = timestamps + epoch_offset

    diagnostics = {
        "slope": float(slope),
        "fit_offset": float(offset),
        "residual_std_s": float(residual),
        "epoch_offset_s": float(epoch_offset),
        "n_pages": int(np.size(timestamps)),
    }
    log.info(
        "  imaging sync: %d pages, clock slope %.9f, residual %.1f ms, "
        "block-vs-session offset %+.1f ms",
        diagnostics["n_pages"],
        slope,
        residual * 1000,
        epoch_offset * 1000,
    )
    return timestamps, diagnostics


def plane_timestamps(page_timestamps, plane_index: int, n_planes: int):
    """
    Timestamps for one fastZ plane: every ``n_planes``-th page from
    ``plane_index``, over complete volumes only.

    ScanImage writes page p to plane ``p % n_planes``. neuroconv's per-plane
    reader (``ScanImageImagingInterface(plane_index=k)``) keeps only complete
    volumes, so a recording that stops partway through its last volume gives
    every plane ``n_pages // n_planes`` samples; this matches that count. Each
    plane keeps its own page times rather than sharing one time per volume --
    within a 5-plane volume at 50 Hz the planes span 80 ms.
    """
    import numpy as np  # noqa: PLC0415

    if n_planes < 1:
        raise ValueError(f"n_planes must be at least 1, got {n_planes}.")
    if not 0 <= plane_index < n_planes:
        raise ValueError(
            f"plane_index {plane_index} is out of range for {n_planes} plane(s)."
        )
    page_timestamps = np.asarray(page_timestamps)
    n_volumes = page_timestamps.size // n_planes
    if n_volumes == 0:
        raise ValueError(
            f"{page_timestamps.size} page(s) do not make up one complete volume "
            f"of {n_planes} plane(s)."
        )
    return page_timestamps[plane_index::n_planes][:n_volumes]


def imaging_aligned_timestamps(source_data: dict, virmen_file) -> dict:
    """
    Per-interface NWB-timeline timestamps for every ScanImage interface in
    ``source_data``.

    The I2C sync runs once per field of view -- all its planes share the same
    files and page clock -- and each plane then takes its own pages
    (:func:`plane_timestamps`). Returns ``{interface_name: timestamps}``, ready
    for ``TowersNWBConverter(aligned_timestamps=...)``.
    """
    aligned: dict = {}
    by_files: dict = {}
    for name in source_data:
        if name.startswith("ScanImageImaging"):
            files = tuple(source_data[name]["file_paths"])
            by_files.setdefault(files, []).append(name)
    for files, names in by_files.items():
        page_ts, diagnostics = page_timestamps_for_session(list(files), virmen_file)
        n_planes = scanimage_plane_count(files[0])
        log.info(f"  {names} sync diagnostics: {diagnostics}")
        for name in names:
            aligned[name] = plane_timestamps(
                page_ts, source_data[name].get("plane_index", 0), n_planes
            )
    return aligned


def build_source_data(
    job: dict,
    export_params: dict,
    virmen_file: Path | None,
    kilosort_dir: Path | None,
) -> dict:
    """
    Translate the DataJoint job record + export_params into a ``source_data``
    dict accepted by ``TowersNWBConverter``.

    Raises:
        FileNotFoundError: If a required data file cannot be located.
    """
    source_data: dict = {}

    # ── Behavior (always required) ────────────────────────────────────────────
    if virmen_file is None:
        raise FileNotFoundError(
            "No Virmen .mat file provided. Cannot locate the behavioral data."
        )
    if not Path(virmen_file).exists():
        raise FileNotFoundError(f"Virmen file not found: {virmen_file}")
    source_data["VirmenData"] = {"file_path": str(virmen_file)}

    # ── Ephys ─────────────────────────────────────────────────────────────────
    if export_params.get("include_ephys") and kilosort_dir is not None:
        kilosort_dir = Path(kilosort_dir)
        probe_dirs = sorted(kilosort_dir.glob("*_imec*"))
        if not probe_dirs:
            log.warning(
                "kilosort_dir given but no *_imec* subdirectories found – skipping ephys."
            )
        else:
            for probe_dir in probe_dirs:
                probe_idx = "".join(
                    filter(str.isdigit, probe_dir.name.split("imec")[-1])
                )
                interface_name = (
                    f"KilosortProbe{probe_idx}" if probe_idx else "Kilosort"
                )
                ks_output = _find_kilosort_output(probe_dir)
                if ks_output is None:
                    log.warning(
                        f"No Kilosort output found under {probe_dir} – skipping."
                    )
                    continue
                source_data[interface_name] = {"folder_path": str(ks_output)}
                log.info(f"  {interface_name}: {ks_output}")
    elif export_params.get("include_ephys"):
        log.warning(
            "include_ephys=True but no kilosort_dir provided; ephys data will not be included."
        )

    # ── Imaging ───────────────────────────────────────────────────────────────
    if export_params.get("include_imaging"):
        explicit = export_params.get("tiff_paths")
        if explicit:
            # Manual override: exactly the files given, as field of view 0.
            by_fov = {0: [str(p) for p in explicit]}
        else:
            fov_numbers = export_params.get("fov_numbers") or []
            recording_ids = export_params.get("recording_ids") or []
            if recording_ids:
                by_fov = {}
                for rid in recording_ids:
                    for fov, paths in resolve_imaging_paths_by_fov(
                        {"recording_id": rid}, fov_numbers
                    ).items():
                        by_fov.setdefault(fov, []).extend(paths)
            else:
                session_key = {
                    k: job[k]
                    for k in ("subject_fullname", "session_date", "session_number")
                    if k in job
                }
                by_fov = resolve_imaging_paths_by_fov(session_key, fov_numbers)

        # One interface per field of view and plane. Fields of view are separate
        # regions and fastZ planes separate depths; each is its own
        # TwoPhotonSeries with its own page times. Every interface gets a unique
        # metadata_key -- sharing neuroconv's default made the second series
        # collide with the first.
        for fov in sorted(by_fov):
            paths = by_fov[fov]
            n_planes = scanimage_plane_count(paths[0])
            for k in range(n_planes):
                entry = {"file_paths": paths, "metadata_key": f"fov{fov}_plane{k}"}
                if n_planes > 1:
                    entry["plane_index"] = k
                source_data[f"ScanImageImagingFOV{fov}Plane{k}"] = entry
            log.info(
                f"  ScanImageImagingFOV{fov}: {len(paths)} tiff file(s), "
                f"{n_planes} plane(s)"
            )

        if not by_fov:
            log.warning(
                "include_imaging=True but no TIFF files resolved; "
                "imaging data will not be included."
            )

    return source_data


# ──────────────────────────────────────────────────────────────────────────────
# DB metadata query (ported from run_nwb_export.py)
# ──────────────────────────────────────────────────────────────────────────────


def query_metadata(session_key: dict) -> dict:
    """
    Pull experimenter, subject sex/DoB and sync timestamps from DataJoint.

    Returns a dict with keys: experimenter, subject_sex, subject_dob,
    sync_timestamps (may be None if not found).
    """
    import datajoint as dj

    result: dict = {
        "experimenter": [],
        "subject_sex": "U",
        "subject_dob": None,
        "sync_timestamps": None,
    }

    try:
        subject = dj.create_virtual_module(
            "subject", dj.config["custom"]["database.prefix"] + "subject"
        )
        lab = dj.create_virtual_module(
            "lab", dj.config["custom"]["database.prefix"] + "lab"
        )

        subject_fullname = session_key["subject_fullname"]
        sub_info = (
            subject.Subject() * lab.User() & f"subject_fullname = '{subject_fullname}'"
        ).fetch1()

        owner_full = sub_info.get("full_name", sub_info.get("user_id", ""))
        if " " in owner_full:
            parts = owner_full.rsplit(" ", 1)
            result["experimenter"].append(f"{parts[-1]}, {parts[0]}")
        else:
            result["experimenter"].append(owner_full)

        sex_map = {"Male": "M", "Female": "F", "Unknown": "U", "m": "M", "f": "F"}
        result["subject_sex"] = sex_map.get(str(sub_info.get("sex", "U")), "U")

        dob = sub_info.get("dob")
        if dob is not None:
            result["subject_dob"] = (
                datetime.combine(dob, datetime.min.time())
                if hasattr(dob, "year")
                else dob
            )

    except Exception as exc:
        log.warning(f"Could not query all metadata from DB: {exc}")

    # Sync timestamps (optional BehaviorSync table)
    try:
        nwb_prod = dj.create_virtual_module(
            "nwb_production", dj.config["custom"]["database.prefix"] + "nwb_production"
        )
        sync_rows = (nwb_prod.BehaviorSync & session_key).fetch(
            "sync_timestamps", as_dict=True
        )
        if sync_rows:
            import numpy as np

            result["sync_timestamps"] = np.array(sync_rows[0]["sync_timestamps"])
    except Exception:
        pass  # BehaviorSync is optional

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Conversion driver (ported from run_nwb_export.py PROCESSING block)
# ──────────────────────────────────────────────────────────────────────────────


def run_conversion_to_file(
    job: dict,
    export_params: dict,
    session_key: dict,
    virmen_file: Path | None,
    kilosort_dir: Path | None,
    output_path: str,
) -> float:
    """
    Run the full NWB conversion to ``output_path`` and return the size in GB.

    Builds source_data, queries DB metadata, runs ``TowersNWBConverter`` and
    writes the NWB file (overwriting any existing file at the path).

    Raises:
        ImportError: If ``tank_lab_to_nwb`` is not installed.
        Exception: Propagated from the converter on conversion failure.
    """
    from tank_lab_to_nwb.convert_towers_task.towersnwbconverter import (
        TowersNWBConverter,
    )

    source_data = build_source_data(job, export_params, virmen_file, kilosort_dir)
    log.info(f"  source_data keys: {list(source_data.keys())}")

    metadata = query_metadata(session_key)

    aligned_timestamps = imaging_aligned_timestamps(source_data, virmen_file)

    converter = TowersNWBConverter(
        source_data=source_data,
        sync_timestamps=metadata["sync_timestamps"],
        aligned_timestamps=aligned_timestamps or None,
    )

    raw_metadata = converter.get_metadata()

    raw_metadata["NWBFile"]["session_description"] = (
        f"U19 pipeline export – {session_key['subject_fullname']} "
        f"{session_key['session_date']}"
    )
    if metadata["experimenter"]:
        raw_metadata["NWBFile"]["experimenter"] = metadata["experimenter"]

    if "Subject" not in raw_metadata:
        raw_metadata["Subject"] = {}
    if metadata["subject_sex"]:
        raw_metadata["Subject"]["sex"] = metadata["subject_sex"]
    if metadata["subject_dob"] is not None:
        raw_metadata["Subject"]["date_of_birth"] = metadata["subject_dob"]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    log.info(f"  Writing NWB to: {output_path}")
    converter.run_conversion(
        nwbfile_path=output_path,
        metadata=raw_metadata,
        overwrite=True,
    )
    log.info("  ✓ Conversion complete")

    size_gb = Path(output_path).stat().st_size / (1024**3)
    log.info(f"  ✓ File size: {size_gb:.3f} GB")
    return size_gb
