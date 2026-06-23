"""Cross-system status dual-write helper.

This module encodes the status-update contract between the Airflow orchestration
layer and the DataJoint pipeline tables.  Every state transition in an ephys or
imaging processing job is written to *both*:

* ``recording_process.Processing.status_processing_id`` — the current status
  integer for the job row (read by the RecordingProcessJobGUI, MATLAB, Slack).
* ``recording_process.LogStatus`` — an append-only audit log of all status
  transitions with timestamps.

Keeping both in sync is the **dual-write contract** (see issue #95): Airflow owns
the real control flow, but the integer column survives as a projection so the
existing consumers keep working.  Any Airflow task that advances a job's state
must call :func:`dual_write_status` rather than touching the tables directly.

The actual table writes reuse the logic already proven in
``u19_pipeline.automatic_job.recording_process_handler.RecProcessHandler``
(``update_status_pipeline`` / ``update_job_id_log``) so behaviour matches the
legacy cron exactly.
"""

from __future__ import annotations

import logging
from datetime import datetime

log = logging.getLogger(__name__)


def dual_write_status(
    key: dict,
    new_status: int,
    old_status: int | None = None,
    error_message: str | None = None,
    error_exception: str | None = None,
    log_transition: bool = True,
) -> None:
    """Advance a recording-process job's status and append to the audit log.

    Parameters
    ----------
    key:
        Primary-key dict for ``recording_process.Processing`` — at minimum
        ``{"job_id": <int>}``.
    new_status:
        Target ``status_processing_id`` (see
        ``u19_pipeline.automatic_job.params_config.recording_process_status_dict``).
    old_status:
        The status the job is transitioning *from*.  Required when
        ``log_transition`` is True because ``LogStatus`` records both old and new
        ids.  If omitted, it is read from the current row.
    error_message / error_exception:
        Optional error strings written to the log row (cropped to the column
        widths 256 / 4096 as the legacy handler does).
    log_transition:
        If True (default) also insert a ``recording_process.LogStatus`` row.

    Notes
    -----
    The update and the log insert run inside a single DataJoint transaction so a
    crash cannot leave the status advanced without a corresponding log entry.
    """
    # Imported lazily so the DAG file parses without a DB/config present.
    import datajoint as dj

    from u19_pipeline import recording_process

    job_key = {"job_id": key["job_id"]}

    if old_status is None:
        old_status = int((recording_process.Processing & job_key).fetch1("status_processing_id"))

    # Crop error strings to the LogStatus column widths (matches legacy handler).
    if error_message is not None and len(error_message) >= 256:
        error_message = error_message[:255]
    if error_exception is not None and len(error_exception) >= 4096:
        error_exception = error_exception[:4095]

    connection = dj.conn()
    with connection.transaction:
        recording_process.Processing.update1({**job_key, "status_processing_id": new_status})

        if log_transition:
            recording_process.LogStatus.insert1(
                {
                    "job_id": key["job_id"],
                    "status_processing_id_old": old_status,
                    "status_processing_id_new": new_status,
                    "status_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "error_message": error_message,
                    "error_exception": error_exception,
                }
            )

    log.info("dual_write_status: job_id=%s %s -> %s", key["job_id"], old_status, new_status)
