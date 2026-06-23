"""Pytest fixtures for the U19 Airflow plugin tests.

Two tiers:

* **unit** (default): no database, DataJoint and the cluster calls are mocked.
  Fast, runs in CI with only the ``airflow`` dependency group.
* **integration** (``-m integration``, opt-in): runs against the Dockerised
  MariaDB from the repo's ``docker-compose.yml`` (port 3307, schema prefix
  ``u19_test_``). Requires the full pipeline deps and a running container;
  NEVER touches the production ``datajoint00`` server.
"""

from __future__ import annotations

import os
import pathlib
import sys

import pytest

# Make the u19.* plugins importable just like Airflow does (plugins on path).
PLUGINS = pathlib.Path(__file__).resolve().parents[1] / "plugins"
sys.path.insert(0, str(PLUGINS))


@pytest.fixture(autouse=True)
def _dry_run_env(monkeypatch):
    """Default every test to dry-run so transfer/SLURM plugins never touch a cluster."""
    monkeypatch.setenv("U19_AIRFLOW_DRY_RUN", "1")


def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless explicitly selected and the env opts in."""
    if os.environ.get("U19_RUN_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="integration test — set U19_RUN_INTEGRATION=1 and start docker MariaDB")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
