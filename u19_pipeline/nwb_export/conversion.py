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


def imaging_timestamps_for_session(
    tiff_paths: list,
    virmen_file,
    n_samples: int | None = None,
):
    """
    Per-frame imaging timestamps on the NWB timeline for ``tiff_paths``.

    Runs the I2C content-based sync (``u19_pipeline.utils.imaging_behavior_sync``)
    and then applies the block-vs-session shift, because the two clocks do not
    share a zero: ``trial.start`` rides ViRMEn's ``vr.timeElapsed``, zeroed at
    block start, while NWB zeroes at ``log.session.start``. That offset is
    allocation and file-I/O cost during ViRMEn startup, so it differs per
    session and is read from each log rather than assumed. See
    ``docs/imaging_behavior_sync.md`` section 6.

    Args:
        tiff_paths: Split TIFFs in acquisition order.
        virmen_file: The session's ViRMEn behavior .mat file.
        n_samples: How many timestamps the imaging interface expects. A
            volumetric fastZ file reports one sample per *volume*, not per page,
            so the per-frame array is strided down to match. ``None`` returns
            the full per-frame array.

    Returns:
        ``(timestamps, diagnostics)`` — diagnostics carries the fit slope,
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

    n_frames = int(np.size(timestamps))
    if n_samples is not None and n_samples != n_frames:
        if n_samples <= 0 or n_frames % n_samples:
            raise ValueError(
                f"Cannot map {n_frames} imaging frame timestamps onto {n_samples} "
                f"interface samples: {n_frames} is not a whole multiple of {n_samples}. "
                f"Expected a volumetric fastZ stack (frames = volumes x slices)."
            )
        stride = n_frames // n_samples
        timestamps = timestamps[::stride][:n_samples]

    diagnostics = {
        "slope": float(slope),
        "fit_offset": float(offset),
        "residual_std_s": float(residual),
        "epoch_offset_s": float(epoch_offset),
        "n_frames": n_frames,
        "n_samples": int(np.size(timestamps)),
    }
    log.info(
        "  imaging sync: %d frames -> %d samples, clock slope %.9f, "
        "residual %.1f ms, block-vs-session offset %+.1f ms",
        n_frames,
        diagnostics["n_samples"],
        slope,
        residual * 1000,
        epoch_offset * 1000,
    )
    return timestamps, diagnostics


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
        tiff_paths = export_params.get("tiff_paths")
        if not tiff_paths:
            fov_numbers = export_params.get("fov_numbers") or []
            recording_ids = export_params.get("recording_ids") or []
            if recording_ids:
                tiff_paths = []
                for rid in recording_ids:
                    tiff_paths.extend(
                        resolve_imaging_paths({"recording_id": rid}, fov_numbers)
                    )
            else:
                # No explicit recordings: hand the session key over and let
                # resolve_imaging_paths do the session -> recording hop.
                session_key = {
                    k: job[k]
                    for k in ("subject_fullname", "session_date", "session_number")
                    if k in job
                }
                tiff_paths = resolve_imaging_paths(session_key, fov_numbers)

        if tiff_paths:
            # ScanImage BigTIFFs, not the generic TiffImagingInterface: only the
            # ScanImage reader understands their volumetric fastZ layout and the
            # per-frame headers the I2C sync depends on.
            source_data["ScanImageImaging"] = {
                "file_paths": [str(p) for p in tiff_paths]
            }
            log.info(f"  ScanImageImaging: {len(tiff_paths)} tiff file(s)")
        else:
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

    # Imaging gets its own timestamp array rather than the shared behavior one:
    # the interface reports one sample per volume for a fastZ stack, so a single
    # array cannot describe both streams. Build the converter once without
    # alignment to ask the interface how many samples it actually has, then
    # again with an array cut to fit.
    aligned_timestamps: dict = {}
    if "ScanImageImaging" in source_data:
        import numpy as np  # noqa: PLC0415

        probe = TowersNWBConverter(source_data=source_data)
        n_samples = int(
            np.size(
                probe.data_interface_objects[
                    "ScanImageImaging"
                ].get_original_timestamps()
            )
        )
        imaging_ts, diagnostics = imaging_timestamps_for_session(
            source_data["ScanImageImaging"]["file_paths"],
            virmen_file,
            n_samples=n_samples,
        )
        aligned_timestamps["ScanImageImaging"] = imaging_ts
        log.info(f"  imaging sync diagnostics: {diagnostics}")

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
