"""Globus file transfer stubs.

Wraps ``u19_pipeline.automatic_job.clusters_paths_and_transfers``, which
contains the Globus SDK calls, endpoint IDs (PNI and Della/Tiger), and
transfer-status polling.
"""

from __future__ import annotations

from typing import Any


def globus_transfer(
    src_endpoint: str,
    dst_endpoint: str,
    src_path: str,
    dst_path: str,
    job_id: int | None = None,
    **kwargs: Any,
) -> str:
    """Initiate a Globus transfer and return the transfer task ID.

    Parameters
    ----------
    src_endpoint:
        Globus endpoint UUID for the source (e.g. PNI ``pni_ep_id``).
    dst_endpoint:
        Globus endpoint UUID for the destination (e.g. Della/Tiger ``tiger_ep_dir``).
    src_path:
        Absolute path on the source endpoint.
    dst_path:
        Absolute path on the destination endpoint.
    job_id:
        Optional ``recording_process.Processing.job_id`` for logging / status
        dual-write via :mod:`u19.status`.
    **kwargs:
        Additional options forwarded to the Globus SDK transfer document
        (e.g. ``sync_level``, ``notify_on_succeeded``).

    Returns
    -------
    str
        Globus transfer task ID string.

    Notes
    -----
    Wraps the transfer initiation logic in
    ``u19_pipeline.automatic_job.clusters_paths_and_transfers``.
    """
    # TODO: call clusters_paths_and_transfers
    #   import u19_pipeline.automatic_job.clusters_paths_and_transfers as ct
    #   return ct.transfer_request(src_endpoint, dst_endpoint, src_path, dst_path, ...)
    raise NotImplementedError("globus_transfer is a scaffold stub")


def check_transfer_status(task_id: str) -> str:
    """Return the current status of a Globus transfer task.

    Parameters
    ----------
    task_id:
        Globus transfer task ID returned by :func:`globus_transfer`.

    Returns
    -------
    str
        Globus task status string, e.g. ``"ACTIVE"``, ``"SUCCEEDED"``,
        ``"FAILED"``.  Callers should poll until a terminal state.

    Notes
    -----
    Wraps the status-check helper in
    ``u19_pipeline.automatic_job.clusters_paths_and_transfers``.
    Should become a deferrable operator in Phase 2.
    """
    # TODO: call clusters_paths_and_transfers status check
    #   import u19_pipeline.automatic_job.clusters_paths_and_transfers as ct
    #   return ct.check_transfer_status(task_id)
    raise NotImplementedError("check_transfer_status is a scaffold stub")
