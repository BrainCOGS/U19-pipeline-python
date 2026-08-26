"""
CLI runner for a single NWB export job.

Given a job ID that was submitted through the website, this script drives the
full conversion pipeline:

    QUEUED → DATA_VALIDATION → PROCESSING → VALIDATION → COMPLETED
                                                         ↘ FAILED (any stage)

Usage
-----
Basic (behavior-only job):
    python scripts/run_nwb_export.py --job-id 42

Ephys job – specify where kilosort outputs live:
    python scripts/run_nwb_export.py --job-id 42 \\
        --virmen-file /data/behavior/jyanar_ya014_T_20240722_0.mat \\
        --kilosort-dir /data/ephys/jyanar_ya014/20240722_g0

Dry run (print plan, no DB writes):
    python scripts/run_nwb_export.py --job-id 42 --dry-run

Prerequisites
-------------
1. `dj_local_conf.json` must exist (or `DJ_CONN_STR` / `DJ_USER+DJ_PASS` env vars set).
2. `tank-lab-to-nwb` must be importable:
       pip install -e /path/to/tank-lab-to-nwb-clean
   or add it to PYTHONPATH.
3. The NWB output directory named in `output_filepath` must exist and be writable.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_nwb_export")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _connect_dj() -> None:
    """Ensure DataJoint is connected, trying conf_file_finding first."""
    try:
        from scripts.conf_file_finding import try_find_conf_file  # type: ignore

        try_find_conf_file()
    except Exception:
        pass  # may not exist in all environments

    import datajoint as dj

    dj.conn()


# Shared conversion logic lives in u19_pipeline.nwb_export.conversion so the CLI
# and the cronjob handler share one code path. Re-exported under the historical
# private names to keep this module's internal references working.
from u19_pipeline.nwb_export.conversion import (  # noqa: E402
    run_conversion_to_file as _run_conversion_to_file,
)

# ──────────────────────────────────────────────────────────────────────────────
# Status helpers
# ──────────────────────────────────────────────────────────────────────────────


def _transition(
    nwb_production, job_key: dict, new_status_id: int, dry_run: bool
) -> None:
    from u19_pipeline.nwb_export_enums import NwbExportStatusEnum  # type: ignore
    from u19_pipeline.nwb_production import update_job_status  # type: ignore

    new_status = NwbExportStatusEnum(new_status_id)
    log.info(f"  → {new_status.name}")
    if not dry_run:
        update_job_status(job_key, new_status)


def _fail(nwb_production, job_key: dict, exc: Exception, dry_run: bool) -> None:
    from u19_pipeline.nwb_export.error_capture import capture_exception  # type: ignore
    from u19_pipeline.nwb_export_enums import NwbExportStatusEnum  # type: ignore
    from u19_pipeline.nwb_production import update_job_status  # type: ignore

    tb = capture_exception(exc)
    log.error(f"Job FAILED: {tb['error_message']}")
    if not dry_run:
        update_job_status(
            job_key,
            NwbExportStatusEnum.FAILED,
            error_message=tb["error_message"],
            error_exception=tb["error_exception"],
        )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


def run(
    job_id: int,
    virmen_file: Path | None,
    kilosort_dir: Path | None,
    dry_run: bool,
) -> None:
    from u19_pipeline import nwb_production  # type: ignore
    from u19_pipeline.nwb_export_enums import NwbExportStatusEnum  # type: ignore

    job_key = {"nwb_job_id": job_id}

    # ── 1. Fetch job record ───────────────────────────────────────────────────
    log.info(f"Fetching job #{job_id} …")
    try:
        job = (nwb_production.NwbExportJob & job_key).fetch1()
    except Exception as exc:
        log.error(f"Job #{job_id} not found in NwbExportJob: {exc}")
        sys.exit(1)

    status_id = job["status_id"]
    status = NwbExportStatusEnum(status_id)
    log.info(f"  status      : {status.name}")
    log.info(f"  subject     : {job['subject_fullname']}")
    log.info(f"  session     : {job['session_date']}  #{job['session_number']}")
    log.info(f"  output_path : {job['output_filepath']}")

    if status not in (NwbExportStatusEnum.QUEUED, NwbExportStatusEnum.FAILED):
        log.error(
            f"Job is in state {status.name}. Only QUEUED or FAILED jobs can be (re-)run. "
            "Use --force to override (not yet implemented)."
        )
        sys.exit(1)

    export_params: dict = {}
    raw_params = job.get("export_parameters")
    if raw_params:
        try:
            export_params = json.loads(raw_params)
        except json.JSONDecodeError:
            # Streamlit stored it as a Python repr string in early versions
            import ast

            try:
                export_params = ast.literal_eval(raw_params)
            except Exception:
                log.warning(
                    f"Could not parse export_parameters: {raw_params!r}; proceeding with empty params"
                )

    session_key = {
        "subject_fullname": job["subject_fullname"],
        "session_date": str(job["session_date"]),
        "session_number": int(job["session_number"]),
    }

    log.info(f"  export_params: {export_params}")
    if dry_run:
        log.info(
            "[DRY RUN] Would transition: QUEUED → DATA_VALIDATION → PROCESSING → VALIDATION → COMPLETED"
        )
        log.info("[DRY RUN] Exiting without any DB writes or file operations.")
        return

    # ── 2. DATA_VALIDATION ────────────────────────────────────────────────────
    _transition(
        nwb_production, job_key, int(NwbExportStatusEnum.DATA_VALIDATION), dry_run
    )

    try:
        from u19_pipeline.nwb_production_utils import (
            validate_behavior_data_exists,  # type: ignore
        )

        ok, msg = validate_behavior_data_exists(session_key)
        if not ok:
            raise RuntimeError(f"Behavior data missing: {msg}")
        log.info("  ✓ behavior data found")
    except Exception as exc:
        _fail(nwb_production, job_key, exc, dry_run)
        sys.exit(1)

    # ── 3. PROCESSING ─────────────────────────────────────────────────────────
    _transition(nwb_production, job_key, int(NwbExportStatusEnum.PROCESSING), dry_run)

    try:
        from tank_lab_to_nwb.convert_towers_task.towersnwbconverter import (
            TowersNWBConverter,  # type: ignore
        )
    except ImportError:
        log.error(
            "tank-lab-to-nwb is not importable. Install it with:\n"
            "    pip install -e /path/to/tank-lab-to-nwb-clean\n"
            "or add it to PYTHONPATH."
        )
        _fail(
            nwb_production,
            job_key,
            ImportError("tank_lab_to_nwb not installed"),
            dry_run,
        )
        sys.exit(1)

    output_path = job["output_filepath"]
    try:
        size_gb = _run_conversion_to_file(
            job=job,
            export_params=export_params,
            session_key=session_key,
            virmen_file=virmen_file,
            kilosort_dir=kilosort_dir,
            output_path=output_path,
        )

        # Record actual file size
        nwb_production.NwbExportJob.update1({**job_key, "actual_file_size_gb": size_gb})

    except Exception as exc:
        log.error(traceback.format_exc())
        _fail(nwb_production, job_key, exc, dry_run)
        sys.exit(1)

    # ── 4. VALIDATION ─────────────────────────────────────────────────────────
    _transition(nwb_production, job_key, int(NwbExportStatusEnum.VALIDATION), dry_run)

    try:
        # Quick HDF5 integrity check: open the file and read root-level keys
        import h5py

        with h5py.File(output_path, "r") as f:
            top_keys = list(f.keys())
        log.info(f"  ✓ HDF5 readable; top-level groups: {top_keys}")
    except Exception as exc:
        _fail(nwb_production, job_key, exc, dry_run)
        sys.exit(1)

    # ── 5. COMPLETED ──────────────────────────────────────────────────────────
    _transition(nwb_production, job_key, int(NwbExportStatusEnum.COMPLETED), dry_run)
    log.info(f"Job #{job_id} completed successfully → {output_path}")


# ──────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--job-id",
        "-j",
        type=int,
        default=None,
        help=(
            "nwb_job_id of the QUEUED (or FAILED) job to process. "
            "If omitted, all non-terminal jobs are processed in submission order."
        ),
    )
    p.add_argument(
        "--virmen-file",
        type=Path,
        default=None,
        help=(
            "Absolute path to the Virmen behavioral .mat file. "
            "Required for behavior conversion."
        ),
    )
    p.add_argument(
        "--kilosort-dir",
        type=Path,
        default=None,
        help=(
            "Base directory containing <session>_imec<N>/ subdirectories "
            "with Kilosort outputs. Required for ephys conversion."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the execution plan without writing to DB or disk.",
    )
    return p.parse_args()


def _get_pending_job_ids() -> list[int]:
    """Return nwb_job_ids for all jobs not in a terminal state (COMPLETED / FAILED)."""

    from u19_pipeline import (
        nwb_production,  # type: ignore
    )
    from u19_pipeline.nwb_export_enums import NwbExportStatusEnum

    terminal_ids = [
        int(NwbExportStatusEnum.COMPLETED),
        int(NwbExportStatusEnum.FAILED),
    ]
    restriction = " AND ".join(f"status_id != {s}" for s in terminal_ids)
    job_ids = (
        (nwb_production.NwbExportJob & restriction)
        .fetch("nwb_job_id", order_by="submission_timestamp ASC")
        .tolist()
    )
    return job_ids


if __name__ == "__main__":
    args = _parse_args()

    _connect_dj()

    if args.job_id is not None:
        run(
            job_id=args.job_id,
            virmen_file=args.virmen_file,
            kilosort_dir=args.kilosort_dir,
            dry_run=args.dry_run,
        )
    else:
        job_ids = _get_pending_job_ids()
        if not job_ids:
            log.info("No pending (non-terminal) jobs found.")
        else:
            log.info(f"Found {len(job_ids)} pending job(s): {job_ids}")
            for job_id in job_ids:
                log.info(f"\n{'=' * 60}\nProcessing job #{job_id}\n{'=' * 60}")
                try:
                    run(
                        job_id=job_id,
                        virmen_file=args.virmen_file,
                        kilosort_dir=args.kilosort_dir,
                        dry_run=args.dry_run,
                    )
                except SystemExit:
                    log.warning(f"Job #{job_id} exited early — continuing to next job.")
                    continue
