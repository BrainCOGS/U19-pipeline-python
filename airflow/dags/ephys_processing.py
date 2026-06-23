# NOTE: scaffold — bodies are stubs
"""Ephys recording processing DAG for U19.

Runs @hourly.  Polls for active processing jobs and drives each through the
linear pipeline: raw transfer → SLURM preprocessing → processed transfer →
DataJoint element populate.

All task bodies are TODO stubs.  Dynamic mapping is stubbed with a comment
placeholder — a linear task graph is wired instead so the DAG parses cleanly.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

# NOTE: u19 plugin imports are inside task bodies (TODO stubs) so the DAG
# parses even when plugins/ is not on PYTHONPATH at collection time.

# recording_process.Processing.status_processing_id values these tasks
# dual-write — mirror of u19_pipeline.automatic_job.params_config
# (recording_process_status_dict). Kept as named constants here so each task's
# dual_write_status(...) call is explicit; the implementation should import
# these from params_config rather than redeclaring, to stay in sync.
STATUS_ERROR = -1
STATUS_NEW = 0
STATUS_TRANSFER_REQUEST = 1  # RAW_FILE_TRANSFER_REQUEST
STATUS_RAW_TRANSFER_STARTED = 1
STATUS_RAW_TRANSFER_DONE = 2  # RAW_FILE_TRANSFER_END
STATUS_SLURM_SUBMITTED = 3  # JOB_QUEUE
STATUS_SLURM_DONE = 4  # JOB_FINISHED
STATUS_PROC_TRANSFER_STARTED = 5  # PROC_FILE_TRANSFER_REQUEST
STATUS_PROC_TRANSFER_DONE = 6  # PROC_FILE_TRANSFER_END
STATUS_COMPLETE = 7  # JOB_FINISHED_ELEMENT_WORKFLOW

default_args = {
    "owner": "u19",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="u19_ephys_processing",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["u19", "ephys", "processing"],
)
def u19_ephys_processing() -> None:
    """Ephys processing DAG.

    Intended flow (one task-group per active job)::

        request_raw_transfer
            → wait_raw_transfer
            → submit_slurm
            → wait_slurm
            → request_proc_transfer
            → wait_proc_transfer
            → populate_element

    Each step calls the matching plugin and calls dual_write_status after
    advancing the job state.

    TODO: replace the linear stub below with dynamic task-group mapping once
    ``get_active_jobs()`` is implemented, e.g.::

        @task_group
        def process_job(job_id: int) -> None:
            ...

        jobs = get_active_jobs()
        process_job.expand(job_id=jobs)
    """

    @task(task_id="get_active_jobs")
    def get_active_jobs() -> list[int]:
        """Return list of job_ids that are ready for processing.

        TODO: query recording_process.Processing for jobs in the
        'transfer_request' status; return their job_ids.

        Returns
        -------
        list[int]
            List of ``recording_process.Processing.job_id`` values.
        """
        # TODO: from u19_pipeline import recording_process
        #       jobs = (recording_process.Processing & {"status_processing_id": STATUS_TRANSFER_REQUEST}).fetch("job_id")
        #       return list(jobs)
        log.info("get_active_jobs stub — returning empty list")
        return []

    @task(task_id="request_raw_transfer")
    def request_raw_transfer(job_ids: list[int]) -> None:
        """Initiate Globus raw-data transfer from PNI to processing cluster.

        Wraps u19.transfers.globus_transfer.
        Calls dual_write_status after initiating each transfer.

        TODO: for job_id in job_ids:
                  task_id = globus_transfer(pni_ep, tiger_ep, raw_src, raw_dst, job_id=job_id)
                  dual_write_status({"job_id": job_id}, STATUS_RAW_TRANSFER_STARTED)
        """
        # TODO: from u19.transfers import globus_transfer
        #       from u19.status import dual_write_status
        pass

    @task(task_id="wait_raw_transfer")
    def wait_raw_transfer(job_ids: list[int]) -> None:
        """Poll until raw Globus transfer reaches SUCCEEDED state.

        Wraps u19.transfers.check_transfer_status.
        Calls dual_write_status when transfer completes.

        TODO: poll check_transfer_status(task_id) until SUCCEEDED/FAILED
              dual_write_status({"job_id": job_id}, STATUS_RAW_TRANSFER_DONE)
        """
        # TODO: from u19.transfers import check_transfer_status
        #       from u19.status import dual_write_status
        pass

    @task(task_id="submit_slurm")
    def submit_slurm(job_ids: list[int]) -> None:
        """Generate and submit a SLURM job for each active job.

        Wraps u19.slurm.submit_slurm_job.
        Calls dual_write_status after submission.

        TODO: for job_id in job_ids:
                  slurm_job_id = submit_slurm_job(job_id, program_selection_params)
                  dual_write_status({"job_id": job_id}, STATUS_SLURM_SUBMITTED)
        """
        # TODO: from u19.slurm import submit_slurm_job
        #       from u19.status import dual_write_status
        pass

    @task(task_id="wait_slurm")
    def wait_slurm(job_ids: list[int]) -> None:
        """Poll SLURM until the job reaches a terminal state.

        Wraps u19.slurm.poll_slurm_job.
        Calls dual_write_status when SLURM job completes.

        NOTE: should become a deferrable operator in Phase 2.

        TODO: for job_id in job_ids:
                  state = poll_slurm_job(slurm_job_id, job_id=job_id)
                  dual_write_status({"job_id": job_id}, STATUS_SLURM_DONE if state=="COMPLETED" else STATUS_ERROR)
        """
        # TODO: from u19.slurm import poll_slurm_job
        #       from u19.status import dual_write_status
        pass

    @task(task_id="request_proc_transfer")
    def request_proc_transfer(job_ids: list[int]) -> None:
        """Initiate Globus processed-data transfer from cluster back to PNI.

        Wraps u19.transfers.globus_transfer.
        Calls dual_write_status after initiating each transfer.

        TODO: for job_id in job_ids:
                  task_id = globus_transfer(tiger_ep, pni_ep, proc_src, proc_dst, job_id=job_id)
                  dual_write_status({"job_id": job_id}, STATUS_PROC_TRANSFER_STARTED)
        """
        # TODO: from u19.transfers import globus_transfer
        #       from u19.status import dual_write_status
        pass

    @task(task_id="wait_proc_transfer")
    def wait_proc_transfer(job_ids: list[int]) -> None:
        """Poll until processed-data Globus transfer reaches SUCCEEDED state.

        Wraps u19.transfers.check_transfer_status.
        Calls dual_write_status when transfer completes.

        TODO: poll check_transfer_status(task_id) until SUCCEEDED/FAILED
              dual_write_status({"job_id": job_id}, STATUS_PROC_TRANSFER_DONE)
        """
        # TODO: from u19.transfers import check_transfer_status
        #       from u19.status import dual_write_status
        pass

    @task(task_id="populate_element")
    def populate_element(job_ids: list[int]) -> None:
        """Populate DataJoint ephys element tables for completed jobs.

        Wraps u19_pipeline.automatic_job.ephys_element_populate.
        Calls dual_write_status after populate completes.

        TODO: for job_id in job_ids:
                  ephys_element_populate.run(job_id)
                  dual_write_status({"job_id": job_id}, STATUS_COMPLETE)
        """
        # TODO: import u19_pipeline.automatic_job.ephys_element_populate as ep
        #       from u19.status import dual_write_status
        pass

    # -------------------------------------------------------------------------
    # Wire linear stub graph
    # -------------------------------------------------------------------------
    t_jobs = get_active_jobs()
    t_req_raw = request_raw_transfer(t_jobs)
    t_wait_raw = wait_raw_transfer(t_req_raw)
    t_submit_slurm = submit_slurm(t_wait_raw)
    t_wait_slurm = wait_slurm(t_submit_slurm)
    t_req_proc = request_proc_transfer(t_wait_slurm)
    t_wait_proc = wait_proc_transfer(t_req_proc)
    populate_element(t_wait_proc)


u19_ephys_processing()
