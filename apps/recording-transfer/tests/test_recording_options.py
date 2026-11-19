import json
import threading
from pathlib import Path

import pytest
from PyQt6.QtWidgets import QApplication

from u19_recording_transfer import app as transfer
from u19_recording_transfer.recording_cleanup import cleanup
from u19_recording_transfer.recording_options import demo_catalog, matching_sessions


def test_subject_and_date_matching_and_fallback():
    sessions = demo_catalog()["sessions"]
    matches, filtered = matching_sessions(
        r"C:\Data\efonseca\ef932_act131\11052026", sessions
    )
    assert filtered and len(matches) == 2
    assert {s["session_number"] for s in matches} == {1, 2}
    assert not matching_sessions(r"C:\Data\ef932_act131\11062026", sessions)[1]
    assert matching_sessions(r"D:\NPX_DATA\jyanar\jyanar_ya013", sessions) == (
        sessions,
        False,
    )
    assert matching_sessions(r"C:\Data\EFONSECA_EF932_ACT131\2026-11-05", sessions)[1]


@pytest.fixture(scope="module")
def app():
    instance = QApplication.instance() or QApplication([])
    yield instance


def test_first_screen_defaults_and_parameter_changes(app, tmp_path):
    window = transfer.Window(
        transfer.Store(tmp_path), demo=True, catalog=demo_catalog()
    )
    assert not window.add_surgery.isChecked()
    assert window.processing.currentData()["name"] == "PNI182-1plane-30fs-PPC"
    window.source.setText(r"C:\Data\efonseca\ef932_act131\11052026")
    assert window.session.count() == 3  # choose-later plus the two matching sessions
    window.session.setCurrentIndex(1)
    assert "20261105_g1" in window.preview.text()
    window.processing.setCurrentIndex(2)
    assert '"nplanes": 2' in window.parameter_details.toPlainText()
    preferences = window.registration_preferences()
    assert preferences["processing"]["id"] == "demo-imaging-other"
    assert preferences["confirmation_required"]
    window.modality.setCurrentText("electrophysiology")
    assert window.processing.currentData() is None  # never reuse an imaging profile
    window.close()


@pytest.fixture
def copied(tmp_path):
    source_root = tmp_path / "local"
    source = source_root / "recording"
    source.mkdir(parents=True)
    (source / "data.bin").write_bytes(b"recording payload")
    destination_root = tmp_path / "braininit"
    store = transfer.Store(tmp_path / "state")
    preferences = dict(
        add_surgery=False, processing={"id": 123}, confirmation_required=True
    )
    job = store.enqueue(source, destination_root, "imaging", 0, preferences)
    assert transfer.run_pending(store, lambda *a: None, threading.Event()) == 0
    return store, job, source_root, destination_root


def test_preferences_travel_with_copy(copied):
    _, job, _, _ = copied
    manifest = json.loads(
        (Path(job["destination"]).parent / "transfer_complete.json").read_text()
    )
    assert manifest["registration_preferences"]["processing"]["id"] == 123
    assert not manifest["registration_preferences"]["add_surgery"]


def test_cleanup_preview_then_explicit_apply(copied):
    store, job, source_root, destination_root = copied
    messages = []
    assert cleanup(store, source_root, destination_root, 0, report=messages.append) == 0
    assert Path(job["source"]).exists()
    assert any("WOULD DELETE" in message for message in messages)
    assert cleanup(store, source_root, destination_root, 0, apply=True) == 0
    assert not Path(job["source"]).exists()
    assert (Path(job["destination"]) / "data.bin").read_bytes() == b"recording payload"
    assert store.read()[0]["status"] == "Local deleted"
    assert transfer.run_pending(store, lambda *a: None, threading.Event()) == 0


def test_cleanup_retention_protects_recent_recording(copied):
    store, job, source_root, destination_root = copied
    assert cleanup(store, source_root, destination_root, 14, apply=True) == 0
    assert Path(job["source"]).exists()


def test_installed_cleanup_scopes_a_mixed_root_queue(copied, tmp_path):
    store, job, source_root, destination_root = copied
    other_source = tmp_path / "other-root/session"
    other_source.mkdir(parents=True)
    (other_source / "keep.bin").write_bytes(b"belongs to another approved root")
    jobs = store.read()
    jobs.append(
        dict(job, id="other-root-job", status="Complete", source=str(other_source))
    )
    store.save(jobs)
    assert (
        cleanup(store, source_root, destination_root, 14, skip_outside_root=True) == 0
    )
    assert (other_source / "keep.bin").exists()


@pytest.mark.parametrize(
    "change", ["source", "destination", "extra", "missing_manifest", "wrong_root"]
)
def test_cleanup_preserves_unverified_source(copied, tmp_path, change):
    store, job, source_root, destination_root = copied
    source, destination = Path(job["source"]), Path(job["destination"])
    if change == "source":
        (source / "data.bin").write_bytes(b"new local data")
    elif change == "destination":
        (destination / "data.bin").write_bytes(b"damaged remote data")
    elif change == "extra":
        (source / "new.bin").write_bytes(b"new recording file")
    elif change == "missing_manifest":
        (destination.parent / "transfer_complete.json").unlink()
    elif change == "wrong_root":
        source_root = tmp_path / "unrelated"
        source_root.mkdir()
    assert cleanup(store, source_root, destination_root, 0, apply=True) == 1
    assert (source / "data.bin").exists()
    assert store.read()[0]["status"] == "Complete"


@pytest.mark.parametrize("change", ["wrong_pc", "escaping_path", "source_is_root"])
def test_cleanup_rejects_unsafe_identity_or_paths(copied, change):
    store, job, source_root, destination_root = copied
    marker = Path(job["destination"]).parent / "transfer_complete.json"
    manifest = json.loads(marker.read_text())
    if change == "wrong_pc":
        manifest["source_pc"] = "some-other-recording-pc"
    elif change == "escaping_path":
        manifest["files"][0]["path"] = "../outside.bin"
    else:
        source_root = Path(job["source"])
    marker.write_text(json.dumps(manifest))
    assert cleanup(store, source_root, destination_root, 0, apply=True) == 1
    assert (Path(job["source"]) / "data.bin").exists()
