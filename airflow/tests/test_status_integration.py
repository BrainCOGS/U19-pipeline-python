"""Integration test: dual-write status round-trip against Dockerised MariaDB.

Opt-in (``U19_RUN_INTEGRATION=1``). Requires the repo's docker-compose MariaDB:

    docker compose -f docker-compose.yml up -d mariadb

It validates the *dual-write semantics* (status update + LogStatus audit row in
one transaction) against a real DataJoint/MariaDB connection, using a minimal
standalone schema under the ``u19_test_`` prefix so it does not need the heavy
element packages or the production server.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

TEST_PREFIX = "u19_test_airflow_"


@pytest.fixture(scope="module")
def dj_conn():
    import datajoint as dj

    dj.config["database.host"] = "127.0.0.1"
    dj.config["database.port"] = 3307
    dj.config["database.user"] = "u19test"
    dj.config["database.password"] = "u19test_pw"
    conn = dj.conn(reset=True)
    yield conn
    # Teardown: drop the test schema.
    import contextlib

    with contextlib.suppress(Exception):
        dj.schema(TEST_PREFIX + "recording_process").drop(force=True)


@pytest.fixture(scope="module")
def processing_tables(dj_conn):
    """Minimal standalone Processing + LogStatus tables mirroring the real schema."""
    import datajoint as dj

    schema = dj.schema(TEST_PREFIX + "recording_process", connection=dj_conn)

    @schema
    class Status(dj.Lookup):
        definition = """
        status_processing_id: TINYINT(1)
        ---
        status_processing_definition: VARCHAR(256)
        """
        contents = [[-1, "error"], [0, "new"], [1, "raw transfer req"], [7, "complete"]]

    @schema
    class Processing(dj.Manual):
        definition = """
        job_id: INT(11) AUTO_INCREMENT
        ---
        -> Status
        """

    @schema
    class LogStatus(dj.Manual):
        definition = """
        log_id: INT(11) AUTO_INCREMENT
        ---
        -> Processing
        -> Status.proj(status_processing_id_old='status_processing_id')
        -> Status.proj(status_processing_id_new='status_processing_id')
        status_timestamp: DATETIME
        error_message=null: VARCHAR(256)
        error_exception=null: VARCHAR(4096)
        """

    return Status, Processing, LogStatus


def test_dual_write_advances_status_and_logs(monkeypatch, processing_tables):
    """dual_write_status should update Processing and append one LogStatus row, atomically."""
    import sys
    import types

    Status, Processing, LogStatus = processing_tables

    # Point u19.status's lazy `from u19_pipeline import recording_process` at our
    # standalone test tables.
    fake_rp = types.SimpleNamespace(Processing=Processing, LogStatus=LogStatus)
    fake_pkg = types.ModuleType("u19_pipeline")
    monkeypatch.setitem(sys.modules, "u19_pipeline", fake_pkg)
    monkeypatch.setitem(sys.modules, "u19_pipeline.recording_process", fake_rp)
    fake_pkg.recording_process = fake_rp

    Processing.insert1({"job_id": 1, "status_processing_id": 0})

    from u19.status import dual_write_status

    dual_write_status({"job_id": 1}, new_status=1, old_status=0)

    assert (Processing & {"job_id": 1}).fetch1("status_processing_id") == 1
    log_rows = (LogStatus & {"job_id": 1}).fetch(as_dict=True)
    assert len(log_rows) == 1
    assert log_rows[0]["status_processing_id_old"] == 0
    assert log_rows[0]["status_processing_id_new"] == 1

    # old_status inferred from current row when omitted
    dual_write_status({"job_id": 1}, new_status=7)
    assert (Processing & {"job_id": 1}).fetch1("status_processing_id") == 7
    assert len(LogStatus & {"job_id": 1}) == 2
