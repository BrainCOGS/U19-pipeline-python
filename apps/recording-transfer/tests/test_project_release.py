import importlib.util
from pathlib import Path
import threading
import zipfile

import pytest

from u19_recording_transfer.app import Store, run_pending

PROJECT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "u19_build_release", PROJECT / "tools/build_release.py"
)
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


def test_release_contains_app_assets_and_separate_bootstrap_bundle(tmp_path):
    first = builder.build(PROJECT, tmp_path / "first")
    second = builder.build(PROJECT, tmp_path / "second")
    assert first["sha256"] == second["sha256"]
    with zipfile.ZipFile(tmp_path / "first" / first["archive"]) as archive:
        names = set(archive.namelist())
        assert "src/u19_recording_transfer/assets/braincogs.png" in names
        assert "uv.lock" in names
        assert not any(
            ".venv" in name or "__pycache__" in name or "machine.json" in name
            for name in names
        )
    with zipfile.ZipFile(
        tmp_path / "first" / f"u19-recording-transfer-setup-{first['version']}.zip"
    ) as archive:
        assert set(archive.namelist()) == {
            "bootstrap.py",
            "launch.ps1",
            "setup.ps1",
            "tasks.ps1",
            "machine.example.json",
            "DEPLOYMENT.md",
        }


def test_machine_roots_constrain_queue_and_revalidate_pending_jobs(tmp_path):
    local = tmp_path / "data"
    source = local / "session"
    source.mkdir(parents=True)
    (source / "data.bin").write_bytes(b"test")
    incoming = tmp_path / "incoming"
    machine = dict(source_roots=[str(local)], incoming_root=str(incoming))
    store = Store(tmp_path / "state", machine)
    with pytest.raises(ValueError, match="configured source"):
        store.enqueue(local, incoming, "imaging", 0)
    with pytest.raises(ValueError, match="configured storage"):
        store.enqueue(source, tmp_path / "somewhere_else", "imaging", 0)
    job = store.enqueue(source, incoming, "imaging", 0)
    job["destination"] = str(tmp_path / "not_approved/session")
    store.save([job])
    assert run_pending(store, lambda *args: None, threading.Event()) == 1
    assert not (tmp_path / "not_approved").exists()
