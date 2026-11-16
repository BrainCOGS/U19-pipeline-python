"""Rig catalog filtering, durable selection and immutable transfer provenance."""

import copy
import json
from pathlib import Path
import socket
import threading

import pytest

from u19_recording_transfer import app, rigs


ROWS = [
    dict(location="182-recording", system_type="recording", acquisition_type="2photon"),
    dict(
        location="188-Recording",
        system_type="recording",
        acquisition_type="electrophysiology",
    ),
    dict(location="Bezos2", system_type="rig", acquisition_type="2photon"),
    dict(location="Training", system_type="rig", acquisition_type="behavior"),
    dict(location="Unknown", system_type=None, acquisition_type=None),
    dict(location="Room", system_type="housing", acquisition_type="N/A"),
]


def test_filters_use_database_role_and_acquisition_type_without_guessing_names():
    assert [r["location"] for r in rigs.matching_locations(ROWS, "recording")] == [
        "182-recording",
        "188-Recording",
    ]
    assert rigs.matching_locations(ROWS, "recording", "electrophysiology") == [ROWS[1]]
    assert rigs.matching_locations(ROWS, "rig", "2photon") == [ROWS[2]]
    assert rigs.matching_locations(ROWS, "rig", "mesoscope") == []
    with pytest.raises(ValueError):
        rigs.profile_for(ROWS[4])


def test_selection_survives_new_process_and_updates_registration_source(tmp_path):
    machine = {
        "source_roots": [str(tmp_path / "local")],
        "incoming_root": str(tmp_path / "incoming"),
    }
    original = copy.deepcopy(machine)
    store = app.Store(tmp_path / "state", machine)
    rigs.save_profile(store, ROWS[0])
    restarted = app.Store(tmp_path / "state", original)
    assert restarted.machine["rig"]["location"] == "182-recording"
    assert restarted.machine["rig"]["source_pc"] == socket.gethostname()
    assert restarted.machine["modality"] == "imaging"
    assert restarted.machine["registration"]["system"] == "182-recording"
    assert "password" not in (tmp_path / "state/rig.json").read_text()


def test_recording_manifest_contains_rig_and_selection_change_blocks_pending_job(
    tmp_path,
):
    source = tmp_path / "local/recording"
    source.mkdir(parents=True)
    (source / "raw.tif").write_bytes(b"synthetic recording")
    machine = {
        "source_roots": [str(source.parent)],
        "incoming_root": str(tmp_path / "incoming"),
    }
    store = app.Store(tmp_path / "state", machine)
    profile = rigs.save_profile(store, ROWS[0])
    job = store.enqueue(source, Path(machine["incoming_root"]), "imaging", 0)
    rigs.save_profile(store, ROWS[2])
    assert app.run_pending(store, lambda *a: None, threading.Event()) == 1
    assert "another rig identity" in store.read()[0]["message"]
    assert not Path(job["destination"]).exists()
    rigs.save_profile(store, ROWS[0])
    assert app.run_pending(store, lambda *a: None, threading.Event()) == 0
    manifest = json.loads(app.completion_marker(store.read()[0]).read_text())
    assert manifest["source_rig"] == profile
    with pytest.raises(ValueError, match="modality"):
        store.enqueue(source, Path(machine["incoming_root"]), "electrophysiology", 0)


def test_profile_cannot_be_reused_on_another_pc(tmp_path):
    profile = rigs.profile_for(ROWS[0])
    profile["source_pc"] = "another-pc"
    with pytest.raises(ValueError, match="this PC"):
        rigs.apply_profile({"modality": "imaging"}, profile)


def test_rig_dialog_filters_roles_and_requires_explicit_choice():
    from PyQt6.QtWidgets import QApplication, QDialogButtonBox
    from u19_recording_transfer.rig_ui import RigDialog

    application = QApplication.instance() or QApplication([])
    dialog = RigDialog(None, ROWS, {})
    assert dialog.location.count() == 3
    assert not dialog.buttons.button(QDialogButtonBox.StandardButton.Save).isEnabled()
    dialog.acquisition.setCurrentIndex(dialog.acquisition.findData("electrophysiology"))
    assert dialog.location.count() == 2
    dialog.location.setCurrentIndex(1)
    assert dialog.location.currentData()["location"] == "188-Recording"
    assert dialog.buttons.button(QDialogButtonBox.StandardButton.Save).isEnabled()
    dialog.role.setCurrentIndex(dialog.role.findData("rig"))
    assert dialog.location.count() == 2
    dialog.reject()
    application.processEvents()
