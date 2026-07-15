"""Deferrable SLURM-completion sensor for the ephys processing DAG.

Kilosort jobs run for *hours* (the cluster slurm configs request up to 30h).
A poke sensor would pin an Airflow worker slot for that entire time and starve
the pool under concurrent recordings. This deferrable sensor instead hands a
:class:`SlurmJobTrigger` to the **triggerer** process, which polls ``sacct``
asynchronously and resumes the task only when the job reaches a terminal state.

Terminal mapping comes from :func:`u19.slurm.poll_slurm_job`, which returns the
legacy ``status_update_idx`` codes:

* ``1`` NEXT_STATUS  -> job COMPLETED      -> sensor succeeds
* ``0`` NO_CHANGE    -> still PENDING/RUNNING -> keep waiting
* ``-1`` ERROR_STATUS -> FAILED/TIMEOUT/CANCELLED -> sensor raises

Requires the ``airflow triggerer`` process to be running (it is part of a
standard Airflow deployment).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

log = logging.getLogger(__name__)


class SlurmJobTrigger(BaseTrigger):
    """Async trigger that polls a SLURM job until it reaches a terminal state."""

    def __init__(self, slurm_id: str, program_selection_params: dict, poll_interval: float = 300.0) -> None:
        super().__init__()
        self.slurm_id = slurm_id
        self.program_selection_params = program_selection_params
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "u19.slurm_sensor.SlurmJobTrigger",
            {
                "slurm_id": self.slurm_id,
                "program_selection_params": self.program_selection_params,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        from u19 import slurm

        while True:
            # poll_slurm_job is a blocking subprocess (ssh/sacct); run it off the
            # event loop so the triggerer stays responsive for other triggers.
            result = await asyncio.to_thread(slurm.poll_slurm_job, self.slurm_id, self.program_selection_params)
            if slurm.is_terminal(result):
                yield TriggerEvent(
                    {
                        "slurm_id": self.slurm_id,
                        "success": slurm.is_success(result),
                        "error": result.get("error", ""),
                    }
                )
                return
            await asyncio.sleep(self.poll_interval)


class SlurmJobSensor(BaseSensorOperator):
    """Deferrable sensor: waits for a submitted SLURM job to finish.

    On COMPLETED the task succeeds; on FAILED/TIMEOUT/CANCELLED it raises so the
    Airflow task fails (which triggers the DAG's error handling + status
    dual-write to ERROR).
    """

    def __init__(self, slurm_id: str, program_selection_params: dict, poll_interval: float = 300.0, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.slurm_id = slurm_id
        self.program_selection_params = program_selection_params
        self.poll_interval = poll_interval

    def execute(self, context: Any) -> None:
        self.defer(
            trigger=SlurmJobTrigger(self.slurm_id, self.program_selection_params, self.poll_interval),
            method_name="execute_complete",
            timeout=timedelta(hours=48),
        )

    def execute_complete(self, context: Any, event: dict | None = None) -> None:
        event = event or {}
        if not event.get("success", False):
            raise RuntimeError(f"SLURM job {event.get('slurm_id')} failed: {event.get('error', 'unknown')}")
        log.info("SLURM job %s completed successfully", event.get("slurm_id"))
