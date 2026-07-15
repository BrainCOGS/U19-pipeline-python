"""SLURM submission and polling helpers for the ephys processing DAG.

Wrap ``u19_pipeline.automatic_job.slurm_creator`` (file generation, sbatch,
``sacct`` status check) so orchestration lives in Airflow and the cluster calls
stay in proven code.

``dry_run`` (default from ``U19_AIRFLOW_DRY_RUN``) lets the DAG and tests run
without a cluster: submission returns a synthetic slurm id, polling returns the
terminal "next status" code.

The long-running poll (Kilosort runs for hours) should be driven by the
**deferrable** sensor in :mod:`u19.slurm_sensor`, not by calling
:func:`poll_slurm_job` in a loop inside a worker.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def _dry_run_default() -> bool:
    return os.environ.get("U19_AIRFLOW_DRY_RUN", "0") in ("1", "true", "True")


def submit_slurm_job(
    job_id: int,
    program_selection_params: dict,
    raw_directory: str,
    proc_directory: str,
    modality: str,
    *,
    dry_run: bool | None = None,
) -> dict:
    """Generate a ``.slurm`` file and submit it via ``sbatch``.

    Returns ``{"status": <system_process>, "slurm_id": str|None, "error": str}``.
    Wraps ``slurm_creator.generate_slurm_file`` + ``queue_slurm_file``.
    """
    dry_run = _dry_run_default() if dry_run is None else dry_run
    if dry_run:
        fake = f"dryrun-slurm-{job_id}"
        log.info("[dry_run] submit_slurm_job job_id=%s -> %s", job_id, fake)
        return {"status": 0, "slurm_id": fake, "error": ""}  # SUCCESS

    import u19_pipeline.automatic_job.params_config as config
    import u19_pipeline.automatic_job.slurm_creator as sc

    status, slurm_filepath = sc.generate_slurm_file(job_id, program_selection_params)
    if status != config.system_process["SUCCESS"]:
        return {"status": config.system_process["ERROR"], "slurm_id": None, "error": "slurm file generation failed"}

    status, slurm_jobid, error_message = sc.queue_slurm_file(
        job_id, program_selection_params, raw_directory, proc_directory, modality, slurm_filepath
    )
    return {"status": status, "slurm_id": slurm_jobid, "error": error_message}


def poll_slurm_job(slurm_job_id: str, program_selection_params: dict, *, dry_run: bool | None = None) -> dict:
    """Check a SLURM job's state once (single ``sacct`` call).

    Returns ``{"pipeline_status": <status_update_idx>, "error": str}`` where
    pipeline_status is NEXT_STATUS(1)/NO_CHANGE(0)/ERROR_STATUS(-1) — the same
    encoding the legacy ``check_slurm_job`` returns. Used by the deferrable
    sensor's trigger; not meant to be looped inside a worker.
    """
    dry_run = _dry_run_default() if dry_run is None else dry_run
    if dry_run:
        log.info("[dry_run] poll_slurm_job %s -> NEXT_STATUS", slurm_job_id)
        return {"pipeline_status": 1, "error": ""}  # NEXT_STATUS

    import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
    import u19_pipeline.automatic_job.slurm_creator as sc
    from u19_pipeline.utility import is_this_spock

    cluster = program_selection_params["process_cluster"]
    local_user = cluster == "spock" and is_this_spock()
    ssh_user = ft.cluster_vars[cluster]["user"]
    ssh_host = ft.cluster_vars[cluster]["hostname"]

    state_pipeline, error_message = sc.check_slurm_job(ssh_user, ssh_host, str(slurm_job_id), local_user=local_user)
    return {"pipeline_status": state_pipeline, "error": error_message}


def is_terminal(poll_result: dict) -> bool:
    """True if a poll result is terminal (job finished or errored)."""
    return poll_result.get("pipeline_status", 0) != 0


def is_success(poll_result: dict) -> bool:
    """True if a poll result indicates the SLURM job completed successfully."""
    return poll_result.get("pipeline_status", 0) == 1
