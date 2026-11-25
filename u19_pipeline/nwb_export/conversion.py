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
import re
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


IMAGING_DATA_KINDS = ("raw", "suite2p")
"""What an imaging export writes: the raw TIFF pixels, or the Suite2p results."""

_SPLIT_FOLDER = re.compile(r"^ROI\d+_z\d+$")
_SPLIT_FILE = re.compile(r"^(?P<stem>.*)_ROI\d+_z\d+_\d{5}\.tif$")


def _recording_restriction(key: dict):
    """A ``recording_id`` restriction for ``key``: kept as is, or found from an ``acquisition.Session`` key."""
    if "recording_id" in key:
        return [{"recording_id": key["recording_id"]}]
    from u19_pipeline.nwb_production_utils import recording_ids_for_session

    return [{"recording_id": rid} for rid in recording_ids_for_session(dict(key))]


def _data_roots(config_key: str) -> list:
    import datajoint as dj

    roots = dj.config.get("custom", {}).get(config_key, None) or []
    return [roots] if isinstance(roots, (str, Path)) else list(roots)


def _find_under_roots(relative, roots) -> Path | None:
    for root in roots:
        candidate = Path(root) / relative
        if candidate.exists():
            return candidate
    return None


def resolve_imaging_tiffs(key: dict, fov_numbers: list | None = None) -> list[Path]:
    """
    The TIFFs of the one ScanImage acquisition recorded for ``key``, as the converter should read them.

    ``key`` is a ``recording_id`` key or an ``acquisition.Session`` key. The files come from
    ``imaging_pipeline.TiffSplit.File``, under ``dj.config["custom"]["imaging_root_data_dir"]``. When they are
    the lab's per-ROI split copies (``ROI<r>_z<d>`` folders) and the recording folder still holds the original
    stack in ``originalStacks``, the stack is used instead: it keeps every ScanImage setting and the exact
    pixels, which the copies don't. Only stack files with the splits' acquisition stem are taken, so a folder
    holding other acquisitions doesn't leak into the export.

    The converter writes every field of an acquisition into one file, so ``fov_numbers`` may only name all of
    the recording's ``tiff_split`` rows (or be empty). It also converts one acquisition per file, so a key
    that resolves to several imaging recordings is refused.

    Raises:
        ValueError: If no imaging recording, or more than one, matches; or ``fov_numbers`` names a subset.
        FileNotFoundError: If a listed TIFF is missing under every imaging root.
    """
    from u19_pipeline import imaging_pipeline

    restriction = _recording_restriction(key)
    rows = (
        imaging_pipeline.TiffSplit.File * (imaging_pipeline.TiffSplit & restriction)
    ).fetch(
        "recording_id",
        "tiff_split",
        "file_number",
        "tiff_split_directory",
        "tiff_split_filename",
        as_dict=True,
    )
    recordings = sorted({int(row["recording_id"]) for row in rows})
    if not recordings:
        raise ValueError(f"No TiffSplit files for {key}; has the recording been split?")
    if len(recordings) > 1:
        raise ValueError(
            f"{key} has {len(recordings)} imaging recordings ({recordings}); one NWB file holds one acquisition. "
            f"Name one with export_parameters['recording_ids']."
        )
    splits = sorted({int(row["tiff_split"]) for row in rows})
    if fov_numbers and sorted({int(n) for n in fov_numbers}) != splits:
        raise ValueError(
            f"fov_numbers {sorted(fov_numbers)} names a subset of this recording's fields {splits}; the converter "
            f"writes every field of an acquisition into one file. Leave fov_numbers empty."
        )

    roots = _data_roots("imaging_root_data_dir")
    files = []
    for row in sorted(rows, key=lambda r: (r["tiff_split"], r["file_number"])):
        relative = Path(row["tiff_split_directory"]) / row["tiff_split_filename"]
        found = _find_under_roots(relative, roots)
        if found is None:
            raise FileNotFoundError(
                f"{relative} (TiffSplit.File) is not under any imaging_root_data_dir {roots}"
            )
        files.append(found)

    stems = {
        match["stem"] for match in (_SPLIT_FILE.match(f.name) for f in files) if match
    }
    parents = {f.parent.parent for f in files}
    if (
        len(stems) == 1
        and len(parents) == 1
        and all(_SPLIT_FOLDER.match(f.parent.name) for f in files)
    ):
        (stem,) = stems
        (recording_folder,) = parents
        stack_name = re.compile(rf"{re.escape(stem)}_\d{{5}}\.tif")
        stack = sorted(
            f
            for f in (recording_folder / "originalStacks").glob("*.tif")
            if stack_name.fullmatch(f.name)
        )
        if stack:
            log.info(
                "  imaging: original stack, %d file(s) in %s",
                len(stack),
                stack[0].parent,
            )
            return stack
        log.warning(
            "  imaging: %s has no originalStacks for %s, so the per-ROI copies are the source. The converter "
            "refuses them unless export_parameters['allow_incomplete_imaging'] is set.",
            recording_folder,
            stem,
        )
    unique = list(dict.fromkeys(files))
    log.info("  imaging: %d TIFF file(s) listed in TiffSplit.File", len(unique))
    return unique


def resolve_suite2p_dirs(
    key: dict, fov_numbers: list | None = None, paramset_idx: int | None = None
) -> list[Path]:
    """
    The Suite2p output folders (each holding ``plane0``, ...) for ``key``, one per ``tiff_split``.

    Read from ``imaging_element.ProcessingTask.processing_output_dir`` under the imaging roots, for tasks whose
    ``imaging_element.Processing`` finished. Suite2p writes its planes in a ``suite2p`` folder inside the output
    folder, which is used when present. A split with runs under several parameter sets needs ``paramset_idx``.

    Raises:
        ValueError: If a split has no finished run, or several and ``paramset_idx`` is not given.
        FileNotFoundError: If an output folder is missing under every imaging root.
    """
    from u19_pipeline.imaging_pipeline import imaging_element

    tasks = (
        imaging_element.ProcessingTask
        & imaging_element.Processing
        & _recording_restriction(key)
    )
    if paramset_idx is not None:
        tasks = tasks & {"paramset_idx": int(paramset_idx)}
    if fov_numbers:
        tasks = tasks & [{"tiff_split": int(n)} for n in fov_numbers]
    rows = tasks.fetch(
        "tiff_split", "paramset_idx", "processing_output_dir", as_dict=True
    )
    if not rows:
        raise ValueError(
            f"No finished Suite2p processing for {key} (paramset_idx={paramset_idx})."
        )
    by_split: dict = {}
    for row in rows:
        by_split.setdefault(int(row["tiff_split"]), []).append(row)
    roots = _data_roots("imaging_root_data_dir")
    folders = []
    for split, split_rows in sorted(by_split.items()):
        if len(split_rows) > 1:
            raise ValueError(
                f"tiff_split {split} has Suite2p runs under paramset_idx "
                f"{sorted(r['paramset_idx'] for r in split_rows)}; set export_parameters['paramset_idx']."
            )
        found = _find_under_roots(split_rows[0]["processing_output_dir"], roots)
        if found is None:
            raise FileNotFoundError(
                f"{split_rows[0]['processing_output_dir']} (ProcessingTask) is not under any imaging_root_data_dir {roots}"
            )
        folders.append(found / "suite2p" if (found / "suite2p").is_dir() else found)
    return folders


def imaging_data_kind(export_params: dict) -> str | None:
    """``"raw"``, ``"suite2p"`` or None (no imaging) for a job's export parameters."""
    if not export_params.get("include_imaging"):
        return None
    kind = export_params.get("imaging_data", "raw")
    if kind not in IMAGING_DATA_KINDS:
        raise ValueError(
            f"export_parameters['imaging_data'] is {kind!r}; expected one of {IMAGING_DATA_KINDS}."
        )
    return kind


def _number_list(raw) -> list:
    """A fov_numbers value (JSON or Python list text, a list, or NULL) as a list."""
    import ast
    import json

    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return list(raw)
    for parse in (json.loads, ast.literal_eval):
        try:
            return list(parse(raw))
        except (ValueError, SyntaxError, TypeError):
            continue
    return []


def with_imaging_modality(export_params: dict, modalities: list) -> dict:
    """
    ``export_params`` with the job's imaging modality applied.

    An ``imaging`` NwbExportModality row turns imaging on and picks what the
    converter writes from its ``modality_type``: IMAGING_RAW gives the raw TIFF
    pixels, IMAGING_PROCESSED the Suite2p results. Its ``fov_numbers`` are used
    unless export_parameters already set them. Raw and processed together would
    be two NWB files, and a job has one output path, so that is refused.
    """
    from u19_pipeline.nwb_export_enums import DataModalityTypeEnum

    imaging = [m for m in modalities if m.get("modality_name") == "imaging"]
    if not imaging:
        return export_params
    kinds = {
        int(DataModalityTypeEnum.IMAGING_RAW): "raw",
        int(DataModalityTypeEnum.IMAGING_PROCESSED): "suite2p",
    }
    wanted = {kinds.get(int(m.get("modality_type") or 0), "raw") for m in imaging}
    if len(wanted) > 1:
        raise NotImplementedError(
            "Raw and processed imaging are two NWB files; submit them as two jobs."
        )
    params = {**export_params, "include_imaging": True}
    params.setdefault("imaging_data", wanted.pop())
    if not params.get("fov_numbers"):
        fovs = [n for m in imaging for n in _number_list(m.get("fov_numbers"))]
        if fovs:
            params["fov_numbers"] = fovs
    return params


def build_source_data(
    job: dict,
    export_params: dict,
    virmen_file: Path | None,
    kilosort_dir: Path | None,
) -> dict:
    """
    Translate the DataJoint job record + export_params into a ``source_data``
    dict accepted by ``TowersNWBConverter``, for behavior and ephys. Imaging
    doesn't go through it; see :func:`run_imaging_conversion`.

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
            subject.Subject() * lab.User() & {"subject_fullname": subject_fullname}
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
        log.debug(
            "Optional configuration or sync information unavailable", exc_info=True
        )

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Conversion driver (ported from run_nwb_export.py PROCESSING block)
# ──────────────────────────────────────────────────────────────────────────────


def converter_metadata(metadata: dict) -> dict:
    """
    The DB metadata from :func:`query_metadata` in the form the converter's
    ``convert_session`` / ``convert_suite2p`` take (top-level NWBFile fields
    and a whole ``subject`` dict). Only what the DB knows is set; the rest keeps
    the converter's placeholders.
    """
    from zoneinfo import ZoneInfo

    out: dict = {}
    if metadata.get("experimenter"):
        out["experimenter"] = list(metadata["experimenter"])
    subject = {"species": "Mus musculus", "sex": metadata.get("subject_sex") or "U"}
    dob = metadata.get("subject_dob")
    if dob is not None:
        subject["date_of_birth"] = (
            dob if dob.tzinfo else dob.replace(tzinfo=ZoneInfo("America/New_York"))
        )
        subject["description"] = "Sex and date of birth from the U19 subject table."
    else:
        subject["age"] = "P90D"
        subject["description"] = (
            "Sex from the U19 subject table. PLACEHOLDER: age is not a real value."
        )
    out["subject"] = subject
    return out


def run_imaging_conversion(
    key: dict,
    export_params: dict,
    virmen_file: Path,
    output_path: Path,
    metadata: dict,
) -> None:
    """
    Write behavior plus imaging for one acquisition with ``tank_lab_to_nwb``.

    The converter does the imaging alignment itself, from the I2C packets in the
    TIFF headers, and verifies the file it writes against the sources. Nothing
    here computes frame times. ``imaging_data`` picks raw TIFF pixels (default)
    or Suite2p results; both carry the same frame times. Inputs come from the
    database unless ``export_parameters`` names them (``tiff_paths``,
    ``suite2p_dirs``). ``allow_incomplete_imaging`` lets the converter take TIFFs
    it can't show are the whole acquisition, and the file then says so.
    """
    import json
    import shutil

    from tank_lab_to_nwb.imaging.convert import conversion_report, convert_session
    from tank_lab_to_nwb.imaging.suite2p import convert_suite2p

    kind = imaging_data_kind(export_params)
    fov_numbers = export_params.get("fov_numbers") or []
    if export_params.get("recording_ids"):
        (recording_id, *others) = export_params["recording_ids"]
        if others:
            raise ValueError(
                "One NWB file holds one acquisition; give one recording in export_parameters['recording_ids']."
            )
        key = {"recording_id": int(recording_id)}

    explicit = export_params.get("tiff_paths")
    tiffs = (
        [Path(p) for p in explicit]
        if explicit
        else resolve_imaging_tiffs(key, fov_numbers)
    )
    allow_incomplete = bool(export_params.get("allow_incomplete_imaging", False))

    source_bytes = sum(p.stat().st_size for p in tiffs)
    free = shutil.disk_usage(output_path.parent).free
    if kind == "raw" and free < source_bytes:
        raise OSError(
            f"{output_path.parent}: {free / 1e9:.1f} GB free, and the TIFFs hold {source_bytes / 1e9:.1f} GB"
        )

    nwb_metadata = converter_metadata(metadata)
    if kind == "raw":
        log.info(
            "  Converting raw TIFFs + behavior with tank_lab_to_nwb (%d file(s))",
            len(tiffs),
        )
        convert_session(
            virmen_file,
            tiffs,
            output_path,
            metadata=nwb_metadata,
            allow_incomplete=allow_incomplete,
        )
    else:
        explicit_dirs = export_params.get("suite2p_dirs")
        folders = (
            [Path(p) for p in explicit_dirs]
            if explicit_dirs
            else resolve_suite2p_dirs(
                key, fov_numbers, export_params.get("paramset_idx")
            )
        )
        log.info(
            "  Converting Suite2p (%s) + behavior with tank_lab_to_nwb",
            ", ".join(map(str, folders)),
        )
        prepared = convert_suite2p(
            virmen_file,
            tiffs,
            folders if len(folders) > 1 else folders[0],
            output_path,
            metadata=nwb_metadata,
            allow_incomplete=allow_incomplete,
        )
        output_path.with_suffix(".report.json").write_text(
            json.dumps(conversion_report(prepared), indent=1)
        )


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

    Imaging jobs go to :func:`run_imaging_conversion`; behavior and ephys jobs
    build source_data and run ``TowersNWBConverter``. Either way the NWB file is
    written at ``output_path``, replacing any file there.

    Raises:
        ImportError: If ``tank_lab_to_nwb`` is not installed (the ``nwb`` extra).
        NotImplementedError: For imaging together with ephys, whose shared clock isn't decided yet.
        Exception: Propagated from the converter on conversion failure.
    """
    metadata = query_metadata(session_key)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    if imaging_data_kind(export_params) is not None:
        if export_params.get("include_ephys"):
            raise NotImplementedError(
                "Imaging and ephys in one file need one clock rule for both, which is still open "
                "(BrainCOGS/U19-pipeline-python#111). Export them as separate jobs."
            )
        if virmen_file is None:
            raise FileNotFoundError(
                "No Virmen .mat file provided. Cannot locate the behavioral data."
            )
        log.info(f"  Writing NWB to: {output}")
        run_imaging_conversion(
            session_key, export_params, Path(virmen_file), output, metadata
        )
    else:
        from tank_lab_to_nwb.convert_towers_task.towersnwbconverter import (
            TowersNWBConverter,
        )

        source_data = build_source_data(job, export_params, virmen_file, kilosort_dir)
        log.info(f"  source_data keys: {list(source_data.keys())}")
        converter = TowersNWBConverter(
            source_data=source_data, sync_timestamps=metadata["sync_timestamps"]
        )
        raw_metadata = converter.get_metadata()
        raw_metadata["NWBFile"]["session_description"] = (
            f"U19 pipeline export – {session_key['subject_fullname']} {session_key['session_date']}"
        )
        if metadata["experimenter"]:
            raw_metadata["NWBFile"]["experimenter"] = metadata["experimenter"]
        raw_metadata.setdefault("Subject", {})
        if metadata["subject_sex"]:
            raw_metadata["Subject"]["sex"] = metadata["subject_sex"]
        if metadata["subject_dob"] is not None:
            raw_metadata["Subject"]["date_of_birth"] = metadata["subject_dob"]
        log.info(f"  Writing NWB to: {output}")
        converter.run_conversion(
            nwbfile_path=str(output), metadata=raw_metadata, overwrite=True
        )

    log.info("  ✓ Conversion complete")
    size_gb = output.stat().st_size / (1024**3)
    log.info(f"  ✓ File size: {size_gb:.3f} GB")
    return size_gb
