# NOTE: scaffold — bodies are stubs
"""Imaging recording processing DAG for U19.

Runs @hourly.  Drives each imaging job through tiff acquisition, splitting,
and the DataJoint element processing chain.

All task bodies are TODO stubs.  Dynamic mapping is stubbed with a comment
placeholder — a linear/forked stub task graph is wired instead so the DAG
parses cleanly.

IMPORTANT — AcquiredTiff is the contended node:
    AcquiredTiff marks a recording as "claimed" for processing.  If two DAG
    runs or operators try to insert/process the same recording simultaneously,
    they will collide.  Guards against double-run (e.g. a DataJoint transaction
    lock check or an Airflow XCom-based claim token) MUST be added before this
    DAG goes live.  The owner of this step (Python operator vs. MATLAB SSH) is
    also an open question — see airflow/README.md.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

# NOTE: u19 plugin imports are inside task bodies (TODO stubs) so the DAG
# parses even when plugins/ is not on PYTHONPATH at collection time.

default_args = {
    "owner": "u19",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="u19_imaging_processing",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["u19", "imaging", "processing"],
)
def u19_imaging_processing() -> None:
    """Imaging processing DAG.

    Intended flow::

        acquired_tiff (owner TBD: python or MATLAB)
            → tiff_split
            ├─→ preprocess → processing → motion_correction
            │       → segmentation → fluorescence → activity   (python element chain)
            └─→ sync_imaging_behavior  (MATLAB; also needs recording chain + behavior file)

    TODO: replace the linear stub below with dynamic task-group mapping once
    ``get_active_jobs()`` is implemented::

        @task_group
        def process_imaging_job(job_id: int) -> None:
            ...

        jobs = get_active_jobs()
        process_imaging_job.expand(job_id=jobs)
    """

    @task(task_id="get_active_jobs")
    def get_active_jobs() -> list[int]:
        """Return list of job_ids ready for imaging processing.

        TODO: query recording_process.Processing for imaging jobs in the
        acquisition/tiff-ready status.

        Returns
        -------
        list[int]
            List of ``recording_process.Processing.job_id`` values.
        """
        # TODO: from u19_pipeline import recording_process
        #       jobs = (recording_process.Processing & {"status_processing_id": STATUS_TIFF_READY}).fetch("job_id")
        #       return list(jobs)
        log.info("get_active_jobs stub — returning empty list")
        return []

    @task(task_id="acquired_tiff")
    def acquired_tiff(job_ids: list[int]) -> None:
        """Mark recordings as acquired / insert AcquiredTiff rows.

        Owner TBD (Python operator or MATLAB SSH call).

        CONTENTION WARNING: AcquiredTiff is the contended node — guard against
        double-run before enabling this task in production (see module docstring).

        For MATLAB path:
            TODO: run_matlab_batch(..., step="populate_AcquiredTiff()")
        For Python path:
            TODO: call u19_pipeline.automatic_job.imaging_element.populate_acquired_tiff(job_ids)
        """
        # TODO: implement after owner decision is made
        pass

    @task(task_id="tiff_split")
    def tiff_split(upstream: None) -> None:
        """Split multi-page TIFFs into per-plane files.

        TODO: call u19_pipeline.automatic_job.imaging_element_populate.tiff_split(...)
        Also calls dual_write_status after completion.
        """
        # TODO: import u19_pipeline.automatic_job.imaging_element_populate as ip
        #       from u19.status import dual_write_status
        pass

    # -------------------------------------------------------------------------
    # Python element chain: preprocess → processing → motion_correction
    #                        → segmentation → fluorescence → activity
    # -------------------------------------------------------------------------

    @task(task_id="preprocess")
    def preprocess(upstream: None) -> None:
        """Run imaging preprocessing (e.g. dark-frame subtraction).

        TODO: call u19_pipeline.automatic_job.imaging_element_populate.preprocess(...)
        Calls dual_write_status after each step.
        """
        # TODO: import u19_pipeline.automatic_job.imaging_element_populate as ip
        #       from u19.status import dual_write_status
        pass

    @task(task_id="processing")
    def processing(upstream: None) -> None:
        """Run imaging processing step (scan-info extraction etc.).

        TODO: call u19_pipeline.automatic_job.imaging_element_populate.processing(...)
        Calls dual_write_status after each step.
        """
        # TODO: import u19_pipeline.automatic_job.imaging_element_populate as ip
        #       from u19.status import dual_write_status
        pass

    @task(task_id="motion_correction")
    def motion_correction(upstream: None) -> None:
        """Run motion correction (e.g. Suite2p / CaImAn).

        TODO: call u19_pipeline.automatic_job.imaging_element_populate.motion_correction(...)
        Calls dual_write_status after each step.
        """
        # TODO: import u19_pipeline.automatic_job.imaging_element_populate as ip
        #       from u19.status import dual_write_status
        pass

    @task(task_id="segmentation")
    def segmentation(upstream: None) -> None:
        """Run ROI segmentation.

        TODO: call u19_pipeline.automatic_job.imaging_element_populate.segmentation(...)
        Calls dual_write_status after each step.
        """
        # TODO: import u19_pipeline.automatic_job.imaging_element_populate as ip
        #       from u19.status import dual_write_status
        pass

    @task(task_id="fluorescence")
    def fluorescence(upstream: None) -> None:
        """Extract fluorescence traces.

        TODO: call u19_pipeline.automatic_job.imaging_element_populate.fluorescence(...)
        Calls dual_write_status after each step.
        """
        # TODO: import u19_pipeline.automatic_job.imaging_element_populate as ip
        #       from u19.status import dual_write_status
        pass

    @task(task_id="activity")
    def activity(upstream: None) -> None:
        """Compute dF/F / spike deconvolution.

        TODO: call u19_pipeline.automatic_job.imaging_element_populate.activity(...)
        Calls dual_write_status after each step.
        """
        # TODO: import u19_pipeline.automatic_job.imaging_element_populate as ip
        #       from u19.status import dual_write_status
        pass

    # -------------------------------------------------------------------------
    # MATLAB sync branch (fork from tiff_split)
    # NOTE: also needs the recording chain + behavior file on the mount
    # -------------------------------------------------------------------------

    @task(task_id="sync_imaging_behavior")
    def sync_imaging_behavior(upstream: None) -> None:
        """Sync imaging data to behavior via MATLAB.

        NOTE: this step also needs the recording chain populated and the
        behavior file accessible on the network mount.  Ensure both conditions
        hold before enabling in production.

        TODO: call run_matlab_batch(..., step="populate_ImagingSyncBehavior()")
        """
        # TODO: from u19.matlab import run_matlab_batch
        #       run_matlab_batch(..., "populate_ImagingSyncBehavior()")
        pass

    # -------------------------------------------------------------------------
    # Wire stub graph
    # -------------------------------------------------------------------------
    t_jobs = get_active_jobs()
    t_acquired = acquired_tiff(t_jobs)
    t_split = tiff_split(t_acquired)

    # Fork 1: python element chain
    t_pre = preprocess(t_split)
    t_proc = processing(t_pre)
    t_mc = motion_correction(t_proc)
    t_seg = segmentation(t_mc)
    t_fluor = fluorescence(t_seg)
    activity(t_fluor)

    # Fork 2: MATLAB sync (also from tiff_split)
    sync_imaging_behavior(t_split)


u19_imaging_processing()
