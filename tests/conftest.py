"""Shared pytest configuration. See ``tests/dbskip.py`` for the ``db`` marker.

``--require-extras`` turns ``pytest.importorskip`` skips into failures. CI uses it
in the job that installs the optional extras, so a broken install fails loudly
instead of quietly skipping every test that needs them.
"""

import dbskip
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-db",
        action="store_true",
        default=False,
        help="Fail instead of skipping tests marked 'db' when the DataJoint database is unavailable.",
    )
    parser.addoption(
        "--require-extras",
        action="store_true",
        default=False,
        help="Fail instead of skipping when pytest.importorskip cannot import a module.",
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


IMPORTORSKIP_REASON = "could not import"


def _fail_importorskip(report, config):
    """Turn a skipped ``report`` from ``pytest.importorskip`` into a failure under ``--require-extras``."""
    if not (report.skipped and config.getoption("--require-extras")):
        return
    if not isinstance(report.longrepr, tuple):
        return
    reason = report.longrepr[2].removeprefix("Skipped: ")
    if reason.startswith(IMPORTORSKIP_REASON):
        report.outcome = "failed"
        report.longrepr = f"--require-extras given but {reason}"


@pytest.hookimpl(wrapper=True)
def pytest_make_collect_report(collector):
    report = yield
    _fail_importorskip(report, collector.config)
    return report


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    _fail_importorskip(report, item.config)
    return report
