"""Skip (or, with ``--run-db``, fail) tests that need a DataJoint database.

Most modules under ``u19_pipeline`` call ``dj.schema(...)`` at import time,
which needs ``dj.config["custom"]["database.prefix"]`` (normally from a
``dj_local_conf.json`` next to the checkout) plus host/user/password and a
reachable MySQL server. This module lives outside ``conftest.py`` so test
files can import it under any pytest import mode (``pythonpath = ["tests"]``
in pyproject.toml puts it on ``sys.path``).

Usage in a test module whose *import* already needs the database::

    from dbskip import import_or_skip_db

    pytestmark = pytest.mark.db
    dsa = import_or_skip_db("u19_pipeline.alert_system.custom_alerts.disk_space_alert")

For tests where only the test body needs the database, ``@pytest.mark.db``
on the test/class is enough; ``tests/conftest.py`` adds the skip.
"""

import functools
import importlib

import pytest

DB_MARKER = "db"
REQUIRED_CONFIG_KEYS = ("database.host", "database.user", "database.password")

run_db = False  # set from --run-db by conftest.pytest_configure


@functools.cache
def db_unavailable_reason():
    """Return None if a DataJoint database is usable, else a short reason."""
    import datajoint as dj

    custom = dj.config.get("custom") or {}
    if "database.prefix" not in custom:
        return (
            "dj.config['custom']['database.prefix'] is not set (no dj_local_conf.json?)"
        )
    # dj.conn() prompts on stdin for anything missing here; never let a test
    # run block on a password prompt.
    missing = [k for k in REQUIRED_CONFIG_KEYS if not dj.config.get(k)]
    if missing:
        return f"dj.config is missing {', '.join(missing)}"
    try:
        dj.conn()
    except Exception as exc:  # DataJointError, OperationalError, ...
        return f"cannot connect to the DataJoint database: {exc!r}"
    return None


def import_or_skip_db(module_name):
    """Import ``module_name`` if a database is available.

    The availability check runs *before* the import: importing a module that
    declares a ``dj.schema`` triggers datajoint's own ``conn()``, which would
    prompt on stdin for missing credentials. When no database is available
    the calling test module is skipped (or failed under ``--run-db``) without
    importing anything. When one is available, any import error is a real
    bug and propagates unchanged.
    """
    reason = db_unavailable_reason()
    if reason is not None:
        message = f"{module_name} needs a DataJoint database: {reason}"
        if run_db:
            pytest.fail(
                f"--run-db given but the database is unavailable: {message}",
                pytrace=False,
            )
        pytest.skip(message, allow_module_level=True)
    return importlib.import_module(module_name)
