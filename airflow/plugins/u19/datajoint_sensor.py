"""DataJoint table-populated sensor helpers.

Used to block downstream tasks until an upstream DataJoint Computed or
Imported table has rows for a given primary-key restriction — e.g., waiting
for an external MATLAB populate() to finish before running a Python populate.
"""

from __future__ import annotations

from typing import Any

from airflow.sdk import task


def dj_table_populated(table: Any, key: dict) -> bool:
    """Return True if *table* contains at least one row matching *key*.

    Parameters
    ----------
    table:
        A DataJoint table class or query expression (e.g. ``behavior.TowersSession``).
    key:
        Primary-key restriction dict, e.g. ``{"subject_fullname": "efonseca", "session_date": "2024-01-01"}``.

    Returns
    -------
    bool
        ``True`` when ``len(table & key) > 0``, ``False`` otherwise.

    Notes
    -----
    Intended to be called inside ``wait_for_dj_table`` or directly from a
    ShortCircuitOperator body. Does *not* handle DataJoint connection errors —
    callers should wrap with retry logic.
    """
    # TODO: implement using DataJoint fetch
    #   return len(table & key) > 0
    raise NotImplementedError("dj_table_populated is a scaffold stub")


@task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
def wait_for_dj_table(table_dotpath: str, key: dict) -> bool:
    """Airflow sensor task that pokes until *table_dotpath* has rows for *key*.

    Parameters
    ----------
    table_dotpath:
        Dotted import path to the DataJoint table, e.g.
        ``"u19_pipeline.behavior.TowersSession"``.
    key:
        Primary-key restriction dict forwarded to :func:`dj_table_populated`.

    Returns
    -------
    bool
        Passes ``True`` downstream when the table is populated (sensor fires).

    Notes
    -----
    Uses ``mode="reschedule"`` so the worker slot is released between pokes.
    Typical use: guard a Python populate task against an upstream MATLAB
    populate that runs on the MATLAB VM on an unknown schedule.

    Example usage in a DAG::

        wait = wait_for_dj_table.override(task_id="wait_towers_session")(
            table_dotpath="u19_pipeline.behavior.TowersSession",
            key={"session_date": "{{ ds }}"},
        )
    """
    # TODO: resolve table_dotpath via importlib, call dj_table_populated
    #   import importlib
    #   mod_path, attr = table_dotpath.rsplit(".", 1)
    #   table = getattr(importlib.import_module(mod_path), attr)
    #   return dj_table_populated(table, key)
    raise NotImplementedError("wait_for_dj_table is a scaffold stub")
