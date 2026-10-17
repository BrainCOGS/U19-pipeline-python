"""
Retry-from-last-failed-stage helper (T048 / US6 / FR-033).

When a job enters FAILED state, the user can trigger a retry.  The retry
mechanism resets the job status to QUEUED so the orchestration pipeline
picks it up again on the next cron run.

Note:
    Unlike DANDI upload retries (which are automatic − see
    ``dandi/retry_policy.py``), pipeline-stage retries are *manual* — a
    user explicitly requests a retry after inspecting the failure reason.

Usage::

    from u19_pipeline.nwb_export.retry_service import retry_failed_job

    retry_failed_job({"nwb_job_id": 42})
"""

from __future__ import annotations

from typing import Any, Dict

from u19_pipeline.nwb_export_enums import NwbExportStatusEnum


def retry_failed_job(job_key: Dict[str, Any]) -> None:
    """Reset a FAILED job back to QUEUED for re-processing (FR-033).

    Validates that the current status is FAILED before resetting.
    Delegates the actual status update to ``update_job_status`` in
    ``nwb_production`` to preserve the audit log.

    Args:
        job_key: DataJoint primary-key dict for the NwbExportJob record
                 (e.g. ``{"nwb_job_id": 42}``).

    Raises:
        ValueError: If the job is not in FAILED state.
        KeyError:   If the job does not exist.
    """
    # Import here to avoid circular dependency at module load time
    from u19_pipeline import nwb_production  # type: ignore

    current_status, _ = nwb_production.get_job_status(job_key)

    if current_status != NwbExportStatusEnum.FAILED:
        raise ValueError(f"Cannot retry a job in state '{current_status.name}'. Only FAILED jobs can be retried.")

    # Reset to QUEUED — no direct FAILED→QUEUED transition in state_machine
    # (retry is an exceptional operation authorised explicitly here)
    nwb_production.update_job_status(
        job_key,
        NwbExportStatusEnum.QUEUED,
        error_message=None,
        error_exception=None,
    )
