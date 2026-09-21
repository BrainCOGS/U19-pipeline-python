"""Shared pytest configuration.

Most modules under ``u19_pipeline`` call ``dj.schema(...)`` at import time,
which needs ``dj.config["custom"]["database.prefix"]`` (normally from a
``dj_local_conf.json`` next to the checkout) and a reachable MySQL server.
Tests that import such a module are marked ``db`` and are skipped, not
errored, when either is missing.

Usage in a test module whose *import* already needs the database::

    pytestmark = pytest.mark.db
    dsa = import_or_skip_db("u19_pipeline.alert_system.custom_alerts.disk_space_alert")

For tests where only the test body needs the database, ``@pytest.mark.db``
on the test/class is enough; ``pytest_collection_modifyitems`` below adds
the skip.

Pass ``--run-db`` to turn the skip into a hard failure (e.g. in an
environment that is supposed to have the database).
"""

import functools
import importlib

import pytest

DB_MARKER = "db"
_run_db = False  # set from --run-db in pytest_configure


def pytest_addoption(parser):
    parser.addoption(
        "--run-db",
        action="store_true",
        default=False,
        help="Fail instead of skipping tests marked 'db' when the DataJoint database is unavailable.",
    )


def pytest_configure(config):
    global _run_db
    _run_db = config.getoption("--run-db")


def _skip_or_fail(reason):
    if _run_db:
        pytest.fail(f"--run-db given but the database is unavailable: {reason}", pytrace=False)
    pytest.skip(reason, allow_module_level=True)


@functools.cache
def db_unavailable_reason():
    """Return None if a DataJoint database is usable, else a short reason."""
    import datajoint as dj

    custom = dj.config.get("custom") or {}
    if "database.prefix" not in custom:
        return "dj.config['custom']['database.prefix'] is not set (no dj_local_conf.json?)"
    try:
        dj.conn()
    except Exception as exc:  # DataJointError, OperationalError, ...
        return f"cannot connect to the DataJoint database: {exc!r}"
    return None


def import_or_skip_db(module_name):
    """Import ``module_name``; skip the calling test module if that needs a DB we don't have."""
    try:
        return importlib.import_module(module_name)
    except Exception as exc:
        reason = db_unavailable_reason() or repr(exc)
        _skip_or_fail(f"{module_name} needs a DataJoint database: {reason}")


def pytest_collection_modifyitems(config, items):
    if not any(item.get_closest_marker(DB_MARKER) for item in items):
        return
    reason = db_unavailable_reason()
    if reason is None:
        return
    if _run_db:
        pytest.fail(f"--run-db given but the database is unavailable: {reason}", pytrace=False)
    skip = pytest.mark.skip(reason=reason)
    for item in items:
        if item.get_closest_marker(DB_MARKER):
            item.add_marker(skip)
