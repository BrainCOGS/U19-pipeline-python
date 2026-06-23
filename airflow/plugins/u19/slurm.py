"""SLURM job submission and polling stubs.

Wraps ``u19_pipeline.automatic_job.slurm_creator`` functions:

* :func:`~u19_pipeline.automatic_job.slurm_creator.generate_slurm_file` —
  writes a ``.slurm`` file from pipeline parameters.
* :func:`~u19_pipeline.automatic_job.slurm_creator.queue_slurm_file` —
  submits the generated file to the cluster via ``sbatch``.

And the job-status helper from ``u19_pipeline.automatic_job.recording_handler``
(``check_slurm_job`` / ``get_slurm_job_state``).

.. note::
    These functions should eventually become **deferrable operators** using
    Airflow's async/trigger pattern so the scheduler does not hold a worker
    slot while polling — see
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html
"""

from __future__ import annotations

from typing import Any


def submit_slurm_job(job_id: int, program_selection_params: dict, **kwargs: Any) -> str:
    """Generate and queue a SLURM job for a recording processing unit.

    Parameters
    ----------
    job_id:
        ``recording_process.Processing.job_id`` — the integer job identifier
        assigned when the processing row was inserted.
    program_selection_params:
        Dict containing at minimum ``process_cluster`` and the preprocessing
        tool selector. Forwarded verbatim to
        ``slurm_creator.generate_slurm_file``.
    **kwargs:
        Additional keyword arguments reserved for future use (e.g. overriding
        ``slurm_default`` values per-job).

    Returns
    -------
    str
        The SLURM job ID string returned by ``sbatch`` (e.g. ``"12345678"``).

    Notes
    -----
    Intended call sequence::

        generate_slurm_file(job_id, program_selection_params)
        queue_slurm_file(job_id, program_selection_params)

    Both live in ``u19_pipeline.automatic_job.slurm_creator``.
    """
    # TODO: call slurm_creator
    #   import u19_pipeline.automatic_job.slurm_creator as sc
    #   sc.generate_slurm_file(job_id, program_selection_params)
    #   return sc.queue_slurm_file(job_id, program_selection_params)
    raise NotImplementedError("submit_slurm_job is a scaffold stub")


def poll_slurm_job(slurm_job_id: str, job_id: int | None = None) -> str:
    """Return the current state string for a running SLURM job.

    Parameters
    ----------
    slurm_job_id:
        The SLURM-assigned job ID string (as returned by :func:`submit_slurm_job`).
    job_id:
        Optional ``recording_process.Processing.job_id`` for logging context.

    Returns
    -------
    str
        SLURM job state, e.g. ``"PENDING"``, ``"RUNNING"``, ``"COMPLETED"``,
        ``"FAILED"``.  Callers should loop / reschedule until a terminal state
        is reached.

    Notes
    -----
    Should be converted to a deferrable operator trigger before production use.
    Wraps ``check_slurm_job`` / ``get_slurm_job_state`` from
    ``u19_pipeline.automatic_job.recording_handler`` (or ``slurm_creator``).
    """
    # TODO: call recording_handler or slurm_creator job-state check
    #   import u19_pipeline.automatic_job.recording_handler as rh
    #   return rh.check_slurm_job(slurm_job_id)
    raise NotImplementedError("poll_slurm_job is a scaffold stub")
