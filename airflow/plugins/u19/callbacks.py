"""Airflow DAG/task callback helpers.

Callbacks must never raise — an exception inside a failure callback would mask
the original error and prevent Airflow from marking the task as failed.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


def slack_failure_callback(context: dict[str, Any]) -> None:
    """Post a Slack failure notification when an Airflow task fails.

    Intended to be passed as ``on_failure_callback`` on individual tasks or at
    the DAG level::

        @dag(on_failure_callback=slack_failure_callback, ...)
        def my_dag(): ...

    Parameters
    ----------
    context:
        Airflow task-instance context dict (injected automatically by the
        scheduler).  Useful keys: ``context["task_instance"]``,
        ``context["exception"]``, ``context["dag"]``.

    Notes
    -----
    Posts to Slack using ``u19_pipeline.utils.slack_utils`` and reads webhook
    URLs from ``lab.SlackWebhooks`` (same mechanism as the existing alert
    system in ``u19_pipeline.alert_system.main_alert_system``).

    This function is a **safe no-op** stub: it logs the failure but does not
    raise, ensuring the original task failure is not masked.
    """
    # TODO: implement Slack notification
    #   try:
    #       import u19_pipeline.utils.slack_utils as su
    #       import u19_pipeline.lab as lab
    #       ti = context.get("task_instance")
    #       dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    #       message = f":red_circle: Task `{ti.task_id}` in DAG `{dag_id}` failed.\n"
    #       message += f"Execution date: {context.get('execution_date')}\n"
    #       exc = context.get("exception")
    #       if exc:
    #           message += f"Exception: {exc}"
    #       slack_dict = {"slack_notification_channel": ["custom_alerts"]}
    #       webhooks = su.get_webhook_list(slack_dict, lab)
    #       for url in webhooks:
    #           su.send_slack_notification(url, message)
    #   except Exception:
    #       log.exception("slack_failure_callback itself raised — suppressing to avoid masking task failure")
    log.warning(
        "slack_failure_callback (stub): task %s in dag %s failed — Slack notification not yet implemented",
        context.get("task_instance"),
        context.get("dag"),
    )
