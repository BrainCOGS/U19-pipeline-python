"""Globus transfer helpers for the processing DAGs.

Thin wrappers over ``u19_pipeline.automatic_job.clusters_paths_and_transfers``
so the orchestration logic lives in Airflow while the actual Globus calls stay
in the existing, proven code (endpoint IDs, path building, status polling).

All functions accept ``dry_run`` (default from the ``U19_AIRFLOW_DRY_RUN`` env
var). In dry-run mode they log what they *would* do and return a synthetic
success/COMPLETED result — letting the DAG and its tests run end-to-end without
a live Globus endpoint or cluster.

Return values mirror the wrapped functions: a dict with a ``status`` key whose
value is one of ``config.system_process`` (COMPLETED=1, SUCCESS=0, ERROR=-1),
plus ``task_id`` for newly requested transfers.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

# Mirror of u19_pipeline.automatic_job.params_config.system_process. Inlined so
# these tiny, stable status helpers don't import the whole pipeline (and its
# heavy scientific deps) just to read three constants.
COMPLETED = 1
SUCCESS = 0
ERROR = -1

_VALID_DIRECTIONS = ("to_cluster", "to_pni")


def _dry_run_default() -> bool:
    return os.environ.get("U19_AIRFLOW_DRY_RUN", "0") in ("1", "true", "True")


def request_transfer(job_id: int, rel_path: str, modality: str, direction: str, *, dry_run: bool | None = None) -> dict:
    """Request a Globus transfer.

    direction: ``"to_cluster"`` (raw PNI -> cluster) or ``"to_pni"`` (processed
    cluster -> PNI). Wraps ``globus_transfer_to_tiger`` / ``globus_transfer_to_pni``.
    """
    if direction not in _VALID_DIRECTIONS:
        raise ValueError(f"unknown direction {direction!r}; expected one of {_VALID_DIRECTIONS}")

    dry_run = _dry_run_default() if dry_run is None else dry_run
    if dry_run:
        fake = f"dryrun-task-{job_id}-{direction}"
        log.info("[dry_run] request_transfer job_id=%s dir=%s path=%s -> %s", job_id, direction, rel_path, fake)
        return {"status": SUCCESS, "task_id": fake}

    import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft

    if direction == "to_cluster":
        return ft.globus_transfer_to_tiger(job_id, rel_path, modality)
    return ft.globus_transfer_to_pni(job_id, rel_path, modality)


def check_transfer_status(task_id: str, *, dry_run: bool | None = None) -> dict:
    """Check the status of an in-flight Globus transfer.

    Returns ``{"status": <config.system_process value>}`` — COMPLETED(1) when
    done, SUCCESS(0) while in flight, ERROR(-1) on failure.
    """
    dry_run = _dry_run_default() if dry_run is None else dry_run
    if dry_run:
        log.info("[dry_run] check_transfer_status task_id=%s -> COMPLETED", task_id)
        return {"status": COMPLETED}

    import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft

    return ft.request_globus_transfer_status(str(task_id))


def transfer_succeeded(result: dict) -> bool:
    """True if a check_transfer_status result indicates COMPLETED."""
    return result.get("status") == COMPLETED


def transfer_failed(result: dict) -> bool:
    """True if a transfer result indicates ERROR."""
    return result.get("status") == ERROR
