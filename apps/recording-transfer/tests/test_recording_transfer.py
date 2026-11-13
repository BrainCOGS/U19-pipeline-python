"""Transfer integrity and queue coordination checks; synthetic local files only."""

import json
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from u19_recording_transfer import app as transfer


@pytest.fixture
def queued(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.bin").write_bytes(b"test data" * 100)
    store = transfer.Store(tmp_path / "state")
    job = store.enqueue(source, tmp_path / "incoming", "imaging", 0)
    return store, job


def test_copy_integrity_and_safe_rerun(queued):
    store, job = queued
    stop = threading.Event()
    assert transfer.run_pending(store, lambda *a: None, stop) == 0
    destination = Path(job["destination"])
    assert (destination / "data.bin").read_bytes() == (
        Path(job["source"]) / "data.bin"
    ).read_bytes()
    manifest = json.loads((destination.parent / "transfer_complete.json").read_text())
    assert manifest["original_path"] == job["source"]
    assert manifest["files"][0]["sha256"] == transfer.digest(
        destination / "data.bin", stop
    )
    assert transfer.run_pending(store, lambda *a: None, stop) == 0
    assert store.read()[0]["status"] == "Complete"


def test_shared_lock_excludes_second_worker(queued):
    store, _ = queued
    lock = store.lock()
    try:
        with pytest.raises(RuntimeError, match="Another transfer"):
            transfer.run_pending(
                transfer.Store(store.directory), lambda *a: None, threading.Event()
            )
    finally:
        lock.unlock()


def test_demo_rejects_existing_queue(queued):
    store, _ = queued
    with pytest.raises(ValueError, match="empty"):
        transfer.demo_setup(store)


def test_demo_rejects_existing_files_without_queue(tmp_path):
    store = transfer.Store(tmp_path)
    existing = tmp_path / "notes.txt"
    existing.write_text("preserve existing data")
    with pytest.raises(ValueError, match="empty"):
        transfer.demo_setup(store)
    assert existing.read_text() == "preserve existing data"


def test_scheduled_process_skips_active_gui_copy(queued):
    store, _ = queued
    lock = store.lock()
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "u19_recording_transfer",
                "--run-pending",
                "--state-dir",
                str(store.directory),
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert "Another transfer" in result.stdout
        assert store.read()[0]["status"] == "Queued"
    finally:
        lock.unlock()


def test_retry_invalidates_completion_marker(queued):
    store, job = queued
    destination = Path(job["destination"])
    destination.mkdir(parents=True)
    marker = destination.parent / "transfer_complete.json"
    marker.write_text("{}")
    (Path(job["source"]) / "data.bin").unlink()
    assert transfer.run_pending(store, lambda *a: None, threading.Event()) == 1
    assert not marker.exists()


def test_retry_does_not_certify_extra_destination_files(queued):
    store, job = queued
    destination = Path(job["destination"])
    destination.mkdir(parents=True)
    (destination / "old_data.bin").write_bytes(b"from previous attempt")
    assert transfer.run_pending(store, lambda *a: None, threading.Event()) == 1
    assert not (destination.parent / "transfer_complete.json").exists()
    assert (destination / "old_data.bin").read_bytes() == b"from previous attempt"


def test_retry_recovers_after_manifest_published_before_queue_save(queued):
    store, job = queued
    stop = threading.Event()
    assert transfer.copy_job(job, lambda *a: None, stop)[0]
    # queue.json is still Queued, as after a crash before saving Complete.
    assert transfer.run_pending(store, lambda *a: None, stop) == 0
    assert store.read()[0]["status"] == "Complete"
    assert (Path(job["destination"]).parent / "transfer_complete.json").exists()


def test_changing_source_never_publishes_completion(queued):
    store, job = queued

    def change_after_copy(done, total, message):
        if message.startswith("Verifying"):
            (Path(job["source"]) / "new_file.txt").write_text("still recording")

    assert transfer.run_pending(store, change_after_copy, threading.Event()) == 1
    assert not (Path(job["destination"]).parent / "transfer_complete.json").exists()


def test_quiet_period_defers_copy(queued):
    store, job = queued
    job["quiet_minutes"] = 10
    store.save([job])
    assert transfer.run_pending(store, lambda *a: None, threading.Event()) == 0
    assert store.read()[0]["status"] == "Queued"
    assert not Path(job["destination"]).exists()
