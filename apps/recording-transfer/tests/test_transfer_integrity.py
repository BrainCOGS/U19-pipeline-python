"""Regression cases found during the pre-PR data-safety review."""

import os
from pathlib import Path
import subprocess
import threading
from unittest.mock import patch

import pytest

from u19_recording_transfer.app import Store, run_pending


def recording(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.bin").write_bytes(b"original recording payload")
    store = Store(tmp_path / "state")
    job = store.enqueue(source, tmp_path / "incoming", "imaging", 0)
    return source, store, job, Path(job["destination"])


def test_unreadable_nested_directory_never_publishes_completion(tmp_path):
    source, store, job, destination = recording(tmp_path)
    blocked = source / "blocked"
    blocked.mkdir()
    (blocked / "hidden.bin").write_bytes(b"must not omit")
    original = os.scandir

    def deny_directory(path):
        if Path(path) == blocked:
            raise PermissionError("simulated unreadable recording subfolder")
        return original(path)

    with patch("os.scandir", deny_directory):
        assert run_pending(store, lambda *a: None, threading.Event()) == 1
    assert store.read()[0]["status"] == "Failed"
    assert not (destination.parent / "transfer_complete.json").exists()


def test_existing_partial_hardlink_never_truncates_source(tmp_path):
    source, store, _, destination = recording(tmp_path)
    destination.mkdir(parents=True)
    old_partial = destination / "data.bin.transfer-partial"
    os.link(source / "data.bin", old_partial)
    run_pending(store, lambda *a: None, threading.Event())
    assert (source / "data.bin").read_bytes() == b"original recording payload"
    assert old_partial.read_bytes() == b"original recording payload"
    assert not (destination.parent / "transfer_complete.json").exists()


def test_existing_destination_link_rejected_before_outside_write(tmp_path):
    source, store, _, destination = recording(tmp_path)
    nested = source / "nested"
    nested.mkdir()
    (nested / "data.bin").write_bytes(b"new recording")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "data.bin").write_bytes(b"unrelated data")
    destination.mkdir(parents=True)
    link = destination / "nested"
    if os.name == "nt":
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(outside)],
            check=True,
            capture_output=True,
        )
    else:
        link.symlink_to(outside, target_is_directory=True)
    assert run_pending(store, lambda *a: None, threading.Event()) == 1
    assert (outside / "data.bin").read_bytes() == b"unrelated data"
    assert not (
        destination / "data.bin"
    ).exists()  # reject entire existing tree before writes
