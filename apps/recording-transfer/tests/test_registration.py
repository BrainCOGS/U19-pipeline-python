"""Run the real registration service/transaction against an in-memory DB adapter."""

from contextlib import contextmanager
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import threading

import pytest

from u19_recording_transfer import app
from u19_recording_transfer.registration import (
    DataJointAdapter,
    prepare,
    request_for,
    resolve_windows_root,
)
from u19_recording_transfer.registration_queue import preview
from u19_recording_transfer.registration_cli import main as cli


class Relation:
    def __init__(self, database, name, restrictions=()):
        self.database, self.name, self.restrictions = database, name, restrictions

    def __and__(self, key):
        return Relation(self.database, self.name, (*self.restrictions, key))

    def rows(self):
        return [
            row
            for row in self.database[self.name]
            if all(
                all(row.get(k) == v for k, v in key.items())
                for key in self.restrictions
            )
        ]

    def __len__(self):
        return len(self.rows())

    def fetch(self, *fields, as_dict=False):
        rows = self.rows()
        return (
            copy.deepcopy(rows)
            if as_dict or not fields
            else [row[fields[0]] for row in rows]
        )

    def fetch1(self, *fields):
        assert len(self.rows()) == 1
        row = self.rows()[0]
        return (
            copy.deepcopy(row)
            if not fields
            else row[fields[0]]
            if len(fields) == 1
            else tuple(row[f] for f in fields)
        )

    def insert1(self, row):
        if self.name == self.database.get("fail_on"):
            raise RuntimeError("simulated DefaultParams insertion failure")
        row = copy.deepcopy(row)
        if self.name == "recordings":
            self.database["last_id"] += 1
            row["recording_id"] = self.database["last_id"]
        self.database[self.name].append(row)


class Connection:
    def __init__(self, data):
        self.data = data
        self.queries = []
        self.refuse_lock = False

    def query(self, query, args=()):
        self.queries.append((query, args))
        value = (
            self.data["last_id"]
            if "LAST_INSERT_ID" in query
            else 0
            if self.refuse_lock and "GET_LOCK" in query
            else 1
        )
        return SimpleNamespace(fetchone=lambda: (value,))

    @property
    @contextmanager
    def transaction(self):
        before = copy.deepcopy(self.data)
        try:
            yield
        except Exception:
            for key in self.data:
                if isinstance(self.data[key], list):
                    self.data[key][:] = before[key]
                else:
                    self.data[key] = before[key]
            raise


class Adapter(DataJointAdapter):
    def __init__(self, root):
        self.root = root
        self.data = dict(
            last_id=0,
            fail_on=None,
            recordings=[],
            associated=[],
            parameters=[],
            sessions=[
                dict(
                    subject_fullname="efonseca_ef932_act131",
                    session_date="2026-11-05",
                    session_number=1,
                    session_location="RIG",
                    num_trials=100,
                )
            ],
            subjects=[
                dict(subject_fullname="efonseca_ef932_act131", user_id="efonseca")
            ],
            locations=[dict(location="PC")],
            surgeries=[dict(subject_fullname="efonseca_ef932_act131")],
            modalities=[
                dict(
                    recording_modality="imaging",
                    default_preprocess_param_steps_id=0,
                    default_paramset_idx=7,
                )
            ],
            processing=[dict(paramset_idx=7), dict(paramset_idx=8)],
            preprocessing=[dict(preprocess_param_steps_id=3)],
        )

        def relation(name):
            return Relation(self.data, name)

        recording = relation("recordings")
        recording.BehaviorSession = relation("associated")
        self.recording = SimpleNamespace(
            Recording=recording,
            DefaultParams=relation("parameters"),
            Modality=relation("modalities"),
        )
        self.acquisition = SimpleNamespace(Session=relation("sessions"))
        self.subject = SimpleNamespace(Subject=relation("subjects"))
        self.lab = SimpleNamespace(Location=relation("locations"))
        self.action = SimpleNamespace(Surgery=relation("surgeries"))
        self.elements = dict(
            imaging=SimpleNamespace(
                ProcessingParamSet=relation("processing"),
                PreprocessParamSteps=relation("preprocessing"),
            )
        )
        self.conn = Connection(self.data)

    def raw_root(self, modality):
        return str(self.root)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    from u19_recording_transfer import accounts

    config = tmp_path / "dj.json"
    config.write_text(json.dumps({"database.host": "synthetic-db"}))
    monkeypatch.setattr(
        accounts,
        "WindowsVault",
        lambda: SimpleNamespace(read=lambda name: ("test-user", "synthetic")),
    )
    source = tmp_path / "local/ef932_act131/11052026"
    source.mkdir(parents=True)
    (source / "scan.tif").write_bytes(b"synthetic recording payload")
    adapter = Adapter(tmp_path / "raw")
    machine = dict(
        modality="imaging",
        source_roots=[str(tmp_path / "local")],
        incoming_root=str(tmp_path / "incoming"),
        state_dir=str(tmp_path / "state"),
        quiet_minutes=0,
        registration=dict(
            enabled=True,
            dj_config=str(config),
            system="PC",
            behavior_rigs=["RIG"],
        ),
    )
    preferences = dict(behavior_session=adapter.data["sessions"][0], add_surgery=False)
    plan = preview(machine, source, "imaging", preferences, lambda path: adapter)
    store = app.Store(tmp_path / "state", machine)
    return source, adapter, machine, preferences, plan, store


def enqueue(setup):
    source, adapter, machine, preferences, plan, store = setup
    job = store.enqueue(
        source,
        Path(machine["incoming_root"]),
        "imaging",
        0,
        preferences,
        registration_plan=plan,
    )
    return job


def run(setup):
    return app.run_pending(
        setup[-1],
        lambda *a: None,
        threading.Event(),
        adapter_factory=lambda path: setup[1],
    )


def test_plan_is_read_only_and_uses_live_defaults_and_final_layout(setup):
    source, adapter, _, _, plan, _ = setup
    assert plan["processing"] == 7 and plan["preprocessing"] == 0
    assert (
        plan["recording_directory"]
        == "efonseca/efonseca_ef932_act131/20261105_g1/11052026"
    )
    assert not Path(plan["destination"]).exists()
    assert adapter.data["recordings"] == [] and not adapter.conn.queries
    assert source.exists()


def test_saved_rig_type_is_revalidated_before_database_commit(setup, monkeypatch):
    from u19_recording_transfer.rigs import profile_for

    source, adapter, machine, preferences, _, store = setup
    row = dict(location="PC", system_type="recording", acquisition_type="2photon")
    adapter.data["locations"][0].update(row)
    machine["rig"] = profile_for(row)
    plan = preview(machine, source, "imaging", preferences, lambda path: adapter)
    assert plan["source_rig"] == machine["rig"]
    store.enqueue(
        source,
        Path(machine["incoming_root"]),
        "imaging",
        0,
        preferences,
        registration_plan=plan,
    )
    original_copy = app.copy_job

    def copy_then_location_changes(*args, **kwargs):
        result = original_copy(*args, **kwargs)
        adapter.data["locations"][0]["acquisition_type"] = "electrophysiology"
        return result

    monkeypatch.setattr(app, "copy_job", copy_then_location_changes)
    assert run(setup) == 1
    assert "type changed in lab.Location" in store.read()[0]["message"]
    assert not adapter.data["recordings"]
    assert Path(plan["destination"]).exists()


def test_verified_copy_then_atomic_registration_and_safe_rerun(setup):
    job = enqueue(setup)
    assert run(setup) == 0
    rows = setup[1].data
    assert (
        len(rows["recordings"])
        == len(rows["associated"])
        == len(rows["parameters"])
        == 1
    )
    assert rows["recordings"][0]["status_recording_id"] == 2
    current = setup[-1].read()[0]
    assert current["status"] == "Registered" and current["recording_id"] == 1
    manifest = json.loads(app.completion_marker(current).read_text())
    assert manifest["registered_recording_id"] == 1
    assert setup[0].exists()
    assert run(setup) == 0 and len(rows["recordings"]) == 1


def test_db_failure_rolls_back_all_rows_and_retains_verified_files(setup):
    enqueue(setup)
    setup[1].data["fail_on"] = "parameters"
    assert run(setup) == 1
    assert setup[1].data["recordings"] == setup[1].data["associated"] == []
    current = setup[-1].read()[0]
    assert app.completion_marker(current).exists() and current["status"] == "Failed"
    setup[1].data["fail_on"] = None
    assert run(setup) == 0 and len(setup[1].data["recordings"]) == 1


def test_copy_only_entry_can_be_explicitly_upgraded_without_deleting_staging(setup):
    source, adapter, machine, preferences, plan, store = setup
    original = store.enqueue(
        source, Path(machine["incoming_root"]), "imaging", 0, preferences
    )
    assert app.run_pending(store, lambda *a: None, threading.Event()) == 0
    incoming = Path(original["destination"])
    assert not adapter.data["recordings"]
    upgraded = store.enqueue(
        source,
        Path(machine["incoming_root"]),
        "imaging",
        0,
        preferences,
        registration_plan=plan,
    )
    assert upgraded["id"] == original["id"]
    assert len(store.read()) == 1
    assert run(setup) == 0
    assert (incoming / "scan.tif").read_bytes() == (
        Path(plan["destination"]) / "scan.tif"
    ).read_bytes()
    assert (source / "scan.tif").exists()
    assert store.read()[0]["staged_copy"]["destination"] == str(incoming)


@pytest.mark.parametrize("change", ["storage", "rig", "owner", "parameters"])
def test_commit_revalidates_live_changes_after_transfer(setup, change):
    enqueue(setup)

    def change_after_copy(done, total, message):
        if message.startswith("Verified"):
            if change == "storage":
                setup[1].root = setup[1].root / "changed"
            elif change == "rig":
                setup[1].data["sessions"][0]["session_location"] = "OTHER"
            elif change == "owner":
                setup[1].data["subjects"][0]["user_id"] = "another-user"
            else:
                setup[1].data["processing"].clear()

    assert (
        app.run_pending(
            setup[-1],
            change_after_copy,
            threading.Event(),
            adapter_factory=lambda path: setup[1],
        )
        == 1
    )
    assert setup[1].data["recordings"] == []
    assert (Path(setup[4]["destination"]) / "scan.tif").exists()


def test_cleanup_of_registered_copy_requires_explicit_final_root(setup):
    from u19_recording_transfer.recording_cleanup import cleanup

    enqueue(setup)
    assert run(setup) == 0
    source, _, machine, _, plan, store = setup
    Path(machine["incoming_root"]).mkdir(parents=True, exist_ok=True)
    assert (
        cleanup(
            store,
            Path(machine["source_roots"][0]),
            Path(machine["incoming_root"]),
            0,
            apply=True,
            skip_outside_root=True,
        )
        == 0
    )
    assert source.exists()
    assert (
        cleanup(
            store,
            Path(machine["source_roots"][0]),
            Path(plan["raw_root"]),
            0,
            apply=True,
        )
        == 0
    )
    assert not source.exists()
    assert (Path(plan["destination"]) / "scan.tif").exists()


def test_crash_after_db_commit_recovers_without_duplicate_insert(setup, monkeypatch):
    enqueue(setup)
    original = app.atomic_json

    def lose_receipt(path, data):
        if isinstance(data, dict) and "registered_recording_id" in data:
            raise OSError("simulated receipt failure after database commit")
        return original(path, data)

    monkeypatch.setattr(app, "atomic_json", lose_receipt)
    assert run(setup) == 1 and len(setup[1].data["recordings"]) == 1
    monkeypatch.setattr(app, "atomic_json", original)
    assert run(setup) == 0
    assert setup[-1].read()[0]["status"] == "Registered"
    assert len(setup[1].data["recordings"]) == 1


@pytest.mark.parametrize("change", ["source", "destination"])
def test_recovery_never_overwrites_changed_registered_data(setup, monkeypatch, change):
    enqueue(setup)
    original = app.atomic_json

    def lose_receipt(path, data):
        if isinstance(data, dict) and "registered_recording_id" in data:
            raise OSError("simulated receipt failure")
        return original(path, data)

    monkeypatch.setattr(app, "atomic_json", lose_receipt)
    assert run(setup) == 1
    monkeypatch.setattr(app, "atomic_json", original)
    path = (
        setup[0] / "scan.tif"
        if change == "source"
        else Path(setup[4]["destination"]) / "scan.tif"
    )
    path.write_bytes(b"changed; must be preserved")
    assert run(setup) == 1
    assert path.read_bytes() == b"changed; must be preserved"
    assert len(setup[1].data["recordings"]) == 1


@pytest.mark.parametrize(
    "change", ["rig", "surgery", "processing", "demo", "add_surgery", "disabled"]
)
def test_invalid_prerequisites_do_not_copy_or_write(setup, change):
    source, adapter, machine, preferences, plan, _ = setup
    if change == "rig":
        adapter.data["sessions"][0]["session_location"] = "OTHER"
    elif change == "surgery":
        adapter.data["surgeries"].clear()
    elif change == "processing":
        preferences["processing"] = dict(id=99)
    elif change == "demo":
        preferences["catalog_demo"] = True
    elif change == "add_surgery":
        preferences["add_surgery"] = True
    else:
        machine["registration"]["enabled"] = False
    with pytest.raises(ValueError):
        preview(machine, source, "imaging", preferences, lambda path: adapter)
    assert not Path(plan["destination"]).exists() and not adapter.data["recordings"]


def test_approved_ids_are_frozen_but_live_ids_are_revalidated(setup):
    enqueue(setup)
    setup[1].data["modalities"][0]["default_paramset_idx"] = 8
    assert run(setup) == 0
    assert setup[1].data["parameters"][0]["paramset_idx"] == 7


def test_duplicate_session_rechecked_when_second_queued_job_executes(setup):
    enqueue(setup)
    source2 = setup[0].parent / "another_recording"
    source2.mkdir()
    (source2 / "scan.tif").write_bytes(b"second recording")
    plan2 = preview(setup[2], source2, "imaging", setup[3], lambda path: setup[1])
    setup[-1].enqueue(
        source2,
        Path(setup[2]["incoming_root"]),
        "imaging",
        0,
        setup[3],
        registration_plan=plan2,
    )
    assert run(setup) == 1
    assert len(setup[1].data["recordings"]) == 1
    assert not Path(plan2["destination"]).exists()


def test_advisory_lock_conflict_prevents_db_writes(setup):
    enqueue(setup)
    setup[1].conn.refuse_lock = True
    assert run(setup) == 1
    assert not setup[1].data["recordings"]
    assert Path(setup[4]["destination"]).is_dir()  # verified copy retained


def test_unowned_final_directory_is_preserved(setup):
    enqueue(setup)
    destination = Path(setup[4]["destination"])
    destination.mkdir(parents=True)
    existing = destination / "scan.tif"
    existing.write_bytes(b"previous data")
    assert run(setup) == 1
    assert existing.read_bytes() == b"previous data"
    assert not setup[1].data["recordings"]


@pytest.mark.parametrize("change", ["root", "disabled"])
def test_execution_rechecks_approved_storage_and_enablement(setup, change):
    enqueue(setup)
    if change == "root":
        setup[1].root = setup[1].root.parent / "changed-raw-root"
    else:
        setup[2]["registration"]["enabled"] = False
    assert run(setup) == 1
    assert not Path(setup[4]["destination"]).exists()
    assert not setup[1].root.exists()
    assert not setup[1].data["recordings"]


def test_cli_dry_run_does_not_enqueue_or_write(setup, tmp_path, capsys):
    source, adapter, machine, _, _, store = setup
    config = tmp_path / "machine.json"
    config.write_text(json.dumps(machine))
    args = [
        "--machine-config",
        str(config),
        "--source",
        str(source),
        "--subject",
        "efonseca_ef932_act131",
        "--date",
        "2026-11-05",
        "--session-number",
        "1",
    ]
    assert cli(args, lambda path: adapter) == 0
    assert "DRY RUN" in capsys.readouterr().out
    assert store.read() == [] and not adapter.data["recordings"]
    assert cli([*args, "--yes"], lambda path: adapter) == 0
    assert store.read()[0]["status"] == "Registered"


def test_root_mapping_is_boundary_aware_and_specific():
    paths = [
        dict(global_path="braininit", net_location="Z:\\"),
        dict(global_path="braininit/Data", net_location="Y:\\Data"),
    ]
    assert (
        resolve_windows_root("/braininit/Data/Raw/electrophysiology", paths)
        == "Y:\\Data\\Raw\\electrophysiology"
    )
    with pytest.raises(ValueError):
        resolve_windows_root("/braininit-other/Data", paths)


@pytest.mark.parametrize("approved", [False, True])
def test_gui_requires_explicit_confirmation_before_queueing_registration(
    setup, monkeypatch, approved
):
    from PyQt6.QtWidgets import QApplication, QMessageBox

    application = QApplication.instance() or QApplication([])
    window = app.Window(
        setup[-1], catalog=dict(sessions=[], processing=[], preprocessing=[])
    )
    window.source.setText(str(setup[0]))
    window.root.setText(setup[2]["incoming_root"])
    started = []
    monkeypatch.setattr(window, "start", lambda: started.append(True))
    monkeypatch.setattr(
        QMessageBox,
        "question",
        lambda *a: (
            QMessageBox.StandardButton.Yes
            if approved
            else QMessageBox.StandardButton.No
        ),
    )
    window.approve_registration(setup[4])
    assert bool(setup[-1].read()) == approved
    assert bool(started) == approved
    assert setup[1].data["recordings"] == []
    assert window.register_button.isEnabled()
    window.close()


def test_gui_metadata_suggestions_do_not_select_session(setup):
    from PyQt6.QtCore import QEventLoop, QTimer
    from PyQt6.QtWidgets import QApplication

    application = QApplication.instance() or QApplication([])
    (setup[0] / "run.ap.meta").write_text("fileCreateTime=2026-11-05T12:00:00\n")
    session = dict(
        setup[1].data["sessions"][0],
        user_id="efonseca",
        session_start_time="2026-11-05T11:55:00",
        session_end_time="2026-11-05T12:05:00",
    )
    window = app.Window(
        setup[-1], catalog=dict(sessions=[session], processing=[], preprocessing=[])
    )
    window.source.setText(str(setup[0]))
    assert window.session.currentIndex() == 0
    window.suggest_sessions()
    loop = QEventLoop()
    window.planner.finished.connect(loop.quit)
    QTimer.singleShot(3000, loop.quit)
    loop.exec()
    application.processEvents()
    assert not window.planner.isRunning()
    assert window.session.currentIndex() == 0
    assert "Suggested" in window.session.itemText(1)
    assert setup[-1].read() == []
    window.close()
