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

    converter = TowersNWBConverter(
        source_data=source_data,
        sync_timestamps=metadata["sync_timestamps"],
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
