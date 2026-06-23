"""Active-job discovery for the processing DAGs.

The processing DAGs are data-driven: the unit of work is "every active
``recording_process.Processing`` row for a modality", discovered at runtime.
This helper queries those job_ids so the DAG can dynamically map one task-group
per job.

"Active" mirrors the legacy
``RecProcessHandler.get_active_process_jobs`` definition: status strictly
between the ERROR id and the PROCESSED id.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def get_active_job_ids(modality: str = "electrophysiology") -> list[int]:
    """Return job_ids of active processing jobs for ``modality``.

    Active == ``JOB_STATUS_ERROR_ID < status_processing_id < JOB_STATUS_PROCESSED``
    (same window the cron's ``get_active_process_jobs`` uses).

    Returns a plain ``list[int]`` so it is XCom-serialisable for Airflow dynamic
    task mapping (``.expand``).
    """
    import u19_pipeline.automatic_job.params_config as config
    from u19_pipeline import recording, recording_process

    status_query = (
        f"status_processing_id > {config.JOB_STATUS_ERROR_ID} "
        f"and status_processing_id < {config.JOB_STATUS_PROCESSED}"
    )

    active = (
        recording.Recording.proj("recording_modality")
        * recording_process.Processing
        & {"recording_modality": modality}
        & status_query
    )
    job_ids = [int(j) for j in active.fetch("job_id")]
    log.info("get_active_job_ids(%s) -> %d job(s): %s", modality, len(job_ids), job_ids)
    return job_ids


def get_job_row(job_id: int) -> dict:
    """Fetch the full ``recording_process.Processing`` row (joined with recording
    modality/paths) for a single job_id, as a plain dict for use inside tasks.
    """
    from u19_pipeline import recording, recording_process

    row = (
        recording.Recording.proj("recording_modality", "recording_directory")
        * recording_process.Processing
        & {"job_id": job_id}
    ).fetch1()
    # Normalise to JSON-friendly types for XCom.
    return {k: (int(v) if hasattr(v, "__index__") else v) for k, v in row.items()}
