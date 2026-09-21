"""Shared pytest configuration. See ``tests/dbskip.py`` for the ``db`` marker."""

import dbskip
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-db",
        action="store_true",
        default=False,
        help="Fail instead of skipping tests marked 'db' when the DataJoint database is unavailable.",
    )


def pytest_configure(config):
    dbskip.run_db = config.getoption("--run-db")


def pytest_runtest_setup(item):
    """Skip (or fail under ``--run-db``) ``db``-marked tests when no database is usable."""
    if item.get_closest_marker(dbskip.DB_MARKER) is None:
        return
    reason = dbskip.db_unavailable_reason()
    if reason is None:
        return
    if dbskip.run_db:
        pytest.fail(
            f"--run-db given but the database is unavailable: {reason}", pytrace=False
        )
    pytest.skip(reason)
