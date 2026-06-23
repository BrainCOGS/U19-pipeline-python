"""Cross-system status dual-write helper.

This module encodes the status-update contract between the Airflow orchestration
layer and the DataJoint pipeline tables.  Every state transition in an ephys or
imaging processing job must be written to *both*:

* ``recording_process.Processing.status_processing_id`` — the current status
  integer for the job row.
* ``recording_process.LogStatus`` — an append-only audit log of all status
  transitions with timestamps.

Keeping both in sync is the dual-write contract.  Any Airflow operator that
advances a job's state must call :func:`dual_write_status` instead of updating
the tables directly.
"""

from __future__ import annotations


def dual_write_status(key: dict, status: int, log: bool = True) -> None:
    """Update job status in DataJoint and optionally append to the log table.

    Parameters
    ----------
    key:
        Primary-key dict for ``recording_process.Processing``, e.g.
        ``{"job_id": 42}``.
    status:
        Integer status code from ``recording_process.Status.status_processing_id``
        (see ``u19_pipeline.automatic_job.params_config.recording_process_status_list``
        for the full enum).
    log:
        If ``True`` (default), also insert a row into
        ``recording_process.LogStatus`` with the current timestamp.

    Notes
    -----
    Implementation should use a DataJoint transaction so the update and the log
    insert are atomic::

        with dj.conn().transaction:
            (recording_process.Processing & key)._update("status_processing_id", status)
            if log:
                recording_process.LogStatus.insert1({
                    **key,
                    "status_processing_id": status,
                    "log_timestamp": datetime.utcnow(),
                })
    """
    # TODO: implement dual write
    #   import datajoint as dj
    #   from u19_pipeline import recording_process
    #   from datetime import datetime
    #   with dj.conn().transaction:
    #       (recording_process.Processing & key)._update("status_processing_id", status)
    #       if log:
    #           recording_process.LogStatus.insert1({
    #               **key,
    #               "status_processing_id": status,
    #               "log_timestamp": datetime.utcnow(),
    #           })
    raise NotImplementedError("dual_write_status is a scaffold stub")
