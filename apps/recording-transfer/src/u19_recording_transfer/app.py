"""Recording-folder copier and registration-preference demo. No database writes.

Source deletion requires --cleanup --apply-cleanup and explicit approved roots.

uv run u19-recording-transfer --demo
uv run u19-recording-transfer --run-pending
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import tempfile
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from PyQt6.QtCore import QLockFile, QThread, QTimer, Qt, pyqtSignal
from PyQt6.QtGui import QIcon, QPixmap
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QPlainTextEdit,
    QScrollArea,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from .recording_options import (
    demo_catalog,
    default_processing_index,
    load_catalog,
    matching_sessions,
)
from . import __version__


def now():
    return datetime.now(UTC).isoformat()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    reject_linked_ancestors(path.parent)
    descriptor, name = tempfile.mkstemp(
        prefix=".u19-json-", suffix=".tmp", dir=path.parent
    )
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class Store:
    def __init__(self, directory, machine=None):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / "queue.json"
        self.machine = machine

    def lock(self):
        lock = QLockFile(str(self.directory / "transfer.lock"))
        # Transfers can last hours. Only a dead process makes its lock stale.
        lock.setStaleLockTime(0)
        if not lock.tryLock(0):
            raise RuntimeError(
                "Another transfer or queue update is active. Try again after it finishes."
            )
        return lock

    def read(self):
        return (
            json.loads(self.path.read_text(encoding="utf-8"))
            if self.path.exists()
            else []
        )

    def save(self, jobs):
        atomic_json(self.path, jobs)

    def enqueue(self, source, root, modality, quiet_minutes, registration=None):
        source, root = source.resolve(), root.resolve()
        if not source.is_dir():
            raise ValueError("Choose an existing recording folder.")
        if root == source or root.is_relative_to(source) or source.is_relative_to(root):
            raise ValueError(
                "Source and incoming area must be separate directory trees."
            )
        if self.machine:
            approved = [Path(p).resolve() for p in self.machine["source_roots"]]
            if not any(source != p and source.is_relative_to(p) for p in approved):
                raise ValueError(
                    "Recording folder must be beneath a configured source root."
                )
            if root != Path(self.machine["incoming_root"]).resolve():
                raise ValueError(
                    "Incoming area must match this machine's configured storage path."
                )
        lock = self.lock()
        try:
            jobs = self.read()
            if any(
                j["source"] == str(source) and j["status"] != "Cancelled" for j in jobs
            ):
                raise ValueError(
                    "This folder is already queued. Use Run pending to retry it."
                )
            transfer_id = uuid.uuid4().hex
            destination = (
                root / modality / socket.gethostname() / transfer_id / source.name
            )
            job = dict(
                id=transfer_id,
                source=str(source),
                destination=str(destination),
                modality=modality,
                status="Queued",
                message="Waiting to copy",
                created_at=now(),
                quiet_minutes=quiet_minutes,
                registration=registration or {},
            )
            jobs.append(job)
            self.save(jobs)
            return job
        finally:
            lock.unlock()


def reject_linked_ancestors(path):
    for candidate in (path, *path.parents):
        if candidate.is_symlink() or candidate.is_junction():
            raise ValueError(f"Linked folders/files are not supported: {candidate}")


def inventory(source, allow_empty=False):
    result = {}
    reject_linked_ancestors(source)

    def visit(directory):
        # Unlike rglob, scandir propagates unreadable-directory and I/O errors.
        with os.scandir(directory) as entries:
            for entry in sorted(entries, key=lambda item: item.name):
                path = Path(entry.path)
                if entry.is_symlink() or path.is_junction():
                    raise ValueError(f"Linked folders/files are not supported: {path}")
                if entry.is_dir(follow_symlinks=False):
                    visit(path)
                elif entry.is_file(follow_symlinks=False):
                    stat = entry.stat(follow_symlinks=False)
                    result[path.relative_to(source).as_posix()] = (
                        stat.st_size,
                        stat.st_mtime_ns,
                    )
                else:
                    raise ValueError(f"Unsupported recording entry: {path}")

    visit(source)
    if not result and not allow_empty:
        raise ValueError("No files found in the selected folder.")
    return result


def digest(path, stop):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            if stop.is_set():
                raise InterruptedError("Stopped; run pending to retry.")
            value.update(chunk)
    return value.hexdigest()


def copy_job(job, report, stop, delay=0):
    source, destination = Path(job["source"]), Path(job["destination"])
    reject_linked_ancestors(destination)
    # A crash can publish completion before queue.json records it. Never leave
    # that old success signal visible while retrying or deferring this job.
    marker = destination.parent / "transfer_complete.json"
    marker.unlink(missing_ok=True)
    before = inventory(source)
    newest = max(stamp for _, stamp in before.values()) / 1e9
    if time.time() - newest < job["quiet_minutes"] * 60:
        return False, "Waiting for folder quiet period"
    if destination.exists():
        inventory(
            destination, allow_empty=True
        )  # reject existing links before any writes
    destination.mkdir(parents=True, exist_ok=True)
    total = sum(size for size, _ in before.values())
    copied = 0
    files = []
    for relative, (size, _stamp) in before.items():
        src, dst = source / relative, destination / relative
        if stop.is_set():
            raise InterruptedError("Stopped; run pending to retry.")
        report(copied, total, f"Checking {relative}")
        reject_linked_ancestors(dst)
        source_hash = digest(src, stop)
        if not dst.exists() or digest(dst, stop) != source_hash:
            dst.parent.mkdir(parents=True, exist_ok=True)
            reject_linked_ancestors(dst)
            descriptor, name = tempfile.mkstemp(
                prefix=".u19-transfer-", suffix=".partial", dir=dst.parent
            )
            partial = Path(name)
            try:
                with os.fdopen(descriptor, "wb") as writer, src.open("rb") as reader:
                    while chunk := reader.read(4 * 1024 * 1024):
                        if stop.is_set():
                            raise InterruptedError("Stopped; run pending to retry.")
                        writer.write(chunk)
                        copied += len(chunk)
                        report(copied, total, f"Copying {relative}")
                        if delay:
                            time.sleep(delay)
                    writer.flush()
                    os.fsync(writer.fileno())
                report(copied, total, f"Verifying {relative}")
                if digest(partial, stop) != source_hash:
                    raise RuntimeError(f"Verification failed: {relative}")
                reject_linked_ancestors(dst)
                os.replace(partial, dst)
            finally:
                partial.unlink(missing_ok=True)
        else:
            copied += size
        files.append(dict(path=relative, bytes=size, sha256=source_hash))
    if inventory(source) != before:
        raise RuntimeError(
            "Source changed during transfer. No completion manifest was published; retry when recording has finished."
        )
    actual = inventory(destination)
    if set(actual) != set(before) or any(
        actual[name][0] != before[name][0] for name in before
    ):
        raise RuntimeError(
            "Destination contains unexpected or incomplete files from an earlier attempt. "
            "No completion manifest was published; inspect the transfer folder before retrying."
        )
    atomic_json(
        marker,
        dict(
            version=1,
            transfer_id=job["id"],
            source_pc=socket.gethostname(),
            original_path=str(source),
            destination=str(destination),
            modality=job["modality"],
            registration_preferences=job.get("registration", {}),
            completed_at=now(),
            total_bytes=total,
            files=files,
        ),
    )
    report(total, total, "Verified • Ready to register on the website")
    return True, f"Verified {len(files)} files • Ready to register"


def run_pending(store, report, stop, delay=0):
    lock = store.lock()
    try:
        jobs = store.read()
        errors = 0
        for job in jobs:
            if job["status"] in ("Complete", "Local deleted") or stop.is_set():
                continue
            job.update(status="Copying", message="Checking source")
            store.save(jobs)
            try:
                if store.machine:
                    source = Path(job["source"]).resolve()
                    roots = [Path(p).resolve() for p in store.machine["source_roots"]]
                    destination = Path(job["destination"]).resolve()
                    incoming = Path(store.machine["incoming_root"]).resolve()
                    if not any(
                        source != p and source.is_relative_to(p) for p in roots
                    ) or not destination.is_relative_to(incoming):
                        raise ValueError(
                            "Queued transfer is outside this machine's approved storage roots."
                        )
                complete, message = copy_job(job, report, stop, delay)
                job.update(status="Complete" if complete else "Queued", message=message)
            except InterruptedError as exc:
                job.update(status="Queued", message=str(exc))
            except Exception as exc:
                errors += 1
                job.update(status="Failed", message=str(exc))
            store.save(jobs)
        return errors
    finally:
        lock.unlock()


class Worker(QThread):
    progress = pyqtSignal(int, str)
    outcome = pyqtSignal(str)

    def __init__(self, store, delay=0):
        super().__init__()
        self.store, self.delay = store, delay
        self.stop = threading.Event()

    def run(self):
        try:
            errors = run_pending(
                self.store,
                lambda done, total, message: self.progress.emit(
                    int(100 * done / max(total, 1)), message
                ),
                self.stop,
                self.delay,
            )
            self.outcome.emit(
                "Finished with errors; see queue."
                if errors
                else "Queue checked. Completed recordings are ready for website registration."
            )
        except Exception as exc:
            self.outcome.emit(str(exc))


STYLE = """
QMainWindow, QWidget { background: #f3f6fa; color: #203347; font: 10pt 'Segoe UI'; }
QLabel#title { font: 23pt 'Segoe UI'; font-weight: 600; }
QLabel#subtitle { color: #607388; }
QLabel#brand { color: #176c70; font: 9pt 'Segoe UI'; font-weight: 600; }
QLabel#logo { background: white; border: 1px solid #d5dfe8; border-radius: 6px; padding: 8px; }
QLineEdit, QComboBox, QSpinBox { background: white; border: 1px solid #cbd6e1;
    border-radius: 5px; padding: 8px; }
QPushButton { background: white; border: 1px solid #cbd6e1; border-radius: 5px;
    padding: 9px 15px; }
QPushButton#primary { background: #176c70; color: white; border: none; }
QPushButton:disabled { color: #98a5b3; background: #e7edf2; }
QTableWidget { background: white; border: 1px solid #d5dfe8; gridline-color: #edf1f5; }
QHeaderView::section { background: #e7eef5; border: none; padding: 10px; }
QProgressBar { border: 1px solid #cbd6e1; border-radius: 5px; background: white;
    text-align: center; min-height: 23px; }
QProgressBar::chunk { background: #44a4a1; }
"""


class Window(QMainWindow):
    def __init__(self, store, demo=False, catalog=None):
        super().__init__()
        self.store, self.demo, self.worker = store, demo, None
        self.catalog = catalog or dict(sessions=[], processing=[], preprocessing=[])
        self.setWindowTitle(
            f"BrainCOGS U19 • Recording transfer {__version__}"
            + (" • DEMO" if demo else "")
        )
        logo_path = Path(__file__).resolve().parent / "assets" / "braincogs.png"
        self.setWindowIcon(QIcon(str(logo_path)))
        self.resize(1120, 1040)
        self.setStyleSheet(STYLE)
        panel = QWidget()
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(panel)
        self.setCentralWidget(scroll)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(30, 24, 30, 24)
        layout.setSpacing(10)
        header = QHBoxLayout()
        heading = QVBoxLayout()
        brand = QLabel("BRAINCOGS  /  U19 PIPELINE")
        brand.setObjectName("brand")
        heading.addWidget(brand)
        title = QLabel("Copy recordings to cup")
        title.setObjectName("title")
        heading.addWidget(title)
        subtitle = QLabel("Select a folder → copy and verify → register on the website")
        subtitle.setObjectName("subtitle")
        heading.addWidget(subtitle)
        header.addLayout(heading, 1)
        logo = QLabel()
        logo.setObjectName("logo")
        logo.setAccessibleName("BrainCOGS logo")
        logo.setToolTip("BrainCOGS • U19 pipeline")
        logo.setPixmap(
            QPixmap(str(logo_path)).scaled(
                148,
                86,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
        )
        header.addWidget(logo, 0, Qt.AlignmentFlag.AlignVCenter)
        layout.addLayout(header)
        if demo:
            layout.addWidget(
                QLabel(
                    "DEMO   •   Synthetic recordings and a local destination. No cup or database access."
                )
            )
        form = QFormLayout()
        self.source = QLineEdit()
        self.source.setPlaceholderText(r"C:\Data\efonseca\ef932_act131\11052026")
        self.root = QLineEdit()
        self.root.setPlaceholderText("Choose the configured incoming area on cup")
        for label, field in [
            ("Recording folder", self.source),
            ("Incoming area", self.root),
        ]:
            row = QHBoxLayout()
            row.addWidget(field)
            button = QPushButton("Browse…")
            button.clicked.connect(lambda checked=False, f=field: self.browse(f))
            row.addWidget(button)
            form.addRow(label, row)
        self.modality = QComboBox()
        self.modality.addItems(["imaging", "electrophysiology"])
        form.addRow("Recording type", self.modality)
        self.quiet = QSpinBox()
        self.quiet.setRange(0, 1440)
        self.quiet.setValue(10)
        self.quiet.setSuffix(" minutes without file changes")
        form.addRow("Wait before copying", self.quiet)
        layout.addLayout(form)
        options_row = QHBoxLayout()
        options_row.setSpacing(24)
        registration_form = QFormLayout()
        parameter_form = QFormLayout()
        self.session = QComboBox()
        self.session.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.session.setMinimumContentsLength(18)
        registration_form.addRow("Behavior session", self.session)
        self.session_hint = QLabel()
        self.session_hint.setWordWrap(True)
        registration_form.addRow("", self.session_hint)
        self.add_surgery = QCheckBox("Add surgery when registering on the website")
        self.add_surgery.setChecked(False)
        registration_form.addRow("", self.add_surgery)
        self.processing = QComboBox()
        self.processing.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.processing.setMinimumContentsLength(18)
        parameter_form.addRow("Processing parameters", self.processing)
        self.preprocessing = QComboBox()
        self.preprocessing.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.preprocessing.setMinimumContentsLength(18)
        parameter_form.addRow("Preprocessing list", self.preprocessing)
        self.parameter_details = QPlainTextEdit()
        self.parameter_details.setReadOnly(True)
        self.parameter_details.setFixedHeight(110)
        parameter_form.addRow("Parameter details", self.parameter_details)
        self.catalog_button = QPushButton("Load sessions and parameter catalog…")
        self.catalog_button.clicked.connect(self.choose_catalog)
        registration_form.addRow("", self.catalog_button)
        self.registration_note = QLabel(
            "These choices travel with the copy for website confirmation; no database records are created here."
        )
        self.registration_note.setWordWrap(True)
        registration_form.addRow("", self.registration_note)
        options_row.addLayout(registration_form, 1)
        options_row.addLayout(parameter_form, 1)
        layout.addLayout(options_row)
        self.preview = QLabel()
        self.preview.setWordWrap(True)
        layout.addWidget(self.preview)
        for field in [self.source, self.root]:
            field.textChanged.connect(self.update_preview)
        self.source.textChanged.connect(self.fill_sessions)
        self.modality.currentTextChanged.connect(self.fill_parameters)
        self.modality.currentTextChanged.connect(self.update_preview)
        self.session.currentIndexChanged.connect(self.update_preview)
        self.processing.currentIndexChanged.connect(self.show_parameters)
        self.preprocessing.currentIndexChanged.connect(self.show_parameters)
        actions = QHBoxLayout()
        self.queue_button = QPushButton("Queue for later")
        self.queue_button.clicked.connect(lambda: self.enqueue(False))
        self.copy_button = QPushButton("Copy now")
        self.copy_button.setObjectName("primary")
        self.copy_button.clicked.connect(lambda: self.enqueue(True))
        self.run_button = QPushButton("Run pending")
        self.run_button.clicked.connect(self.start)
        self.stop_button = QPushButton("Stop after current block")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(lambda: self.worker.stop.set())
        for button in [
            self.queue_button,
            self.copy_button,
            self.run_button,
            self.stop_button,
        ]:
            actions.addWidget(button)
        layout.addLayout(actions)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Recording folder", "Status", "Details"])
        self.table.setColumnWidth(0, 215)
        self.table.setColumnWidth(1, 100)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setMinimumHeight(110)
        layout.addWidget(self.table, 1)
        self.progress = QProgressBar()
        layout.addWidget(self.progress)
        self.message = QLabel(
            "Ready. Originals stay on this PC; association happens on the website."
        )
        self.message.setWordWrap(True)
        layout.addWidget(self.message)
        footer = QLabel(
            "11 PM fallback: Task Scheduler runs this script with --run-pending.\n"
            "Only queued folders are copied. A busy copier causes the scheduled run to skip."
        )
        footer.setObjectName("subtitle")
        layout.addWidget(footer)
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self.refresh)
        self.refresh_timer.start(1000)
        self.fill_sessions()
        self.fill_parameters()
        self.update_preview()
        self.refresh()

    def choose_catalog(self):
        selected, _ = QFileDialog.getOpenFileName(
            self, "Load catalog", "", "JSON catalog (*.json)"
        )
        if selected:
            try:
                catalog = load_catalog(Path(selected))
                if catalog.get("demo") and not self.demo:
                    raise ValueError(
                        "Synthetic demo profiles cannot be loaded in the real transfer window."
                    )
                self.catalog = catalog
                self.fill_sessions()
                self.fill_parameters()
            except Exception as exc:
                QMessageBox.warning(self, "Unable to load catalog", str(exc))

    def fill_sessions(self):
        previous = self.session.currentData()
        options, matched = matching_sessions(
            self.source.text(), self.catalog["sessions"]
        )
        self.session.blockSignals(True)
        self.session.clear()
        self.session.addItem("Choose later on website / no behavior session", None)
        for session in options:
            self.session.addItem(
                f"{session['subject_fullname']} • {session['session_date']} • "
                f"session {session['session_number']} • {session.get('session_location', '')}",
                session,
            )
        if previous in options:
            self.session.setCurrentIndex(options.index(previous) + 1)
        self.session.blockSignals(False)
        self.session_hint.setText(
            f"{len(options)} matching subject/date sessions."
            if matched
            else f"No subject/date match; showing all {len(options)} loaded sessions."
        )
        self.update_preview()

    def fill_parameters(self):
        modality = self.modality.currentText()
        for kind, combo in [
            ("processing", self.processing),
            ("preprocessing", self.preprocessing),
        ]:
            combo.blockSignals(True)
            combo.clear()
            combo.addItem("Choose later on website", None)
            options = [
                option
                for option in self.catalog[kind]
                if option["modality"] == modality
            ]
            for option in options:
                combo.addItem(
                    f"{option['name']} • {option.get('method', kind)} • ID {option['id']}",
                    option,
                )
            default = (
                default_processing_index(options, modality)
                if kind == "processing"
                else None
            )
            if default is not None:
                combo.setCurrentIndex(default + 1)
            combo.blockSignals(False)
        self.show_parameters()

    def show_parameters(self):
        rows = []
        for label, combo in [
            ("PROCESSING", self.processing),
            ("PREPROCESSING", self.preprocessing),
        ]:
            option = combo.currentData()
            rows.append(
                label
                + (
                    f" • {option['name']}\n"
                    + json.dumps(option["details"], indent=2, default=str)
                    if option
                    else " • Choose later on website"
                )
            )
        self.parameter_details.setPlainText("\n\n".join(rows))

    def registration_preferences(self):
        return dict(
            behavior_session=self.session.currentData(),
            add_surgery=self.add_surgery.isChecked(),
            processing=self.processing.currentData(),
            preprocessing=self.preprocessing.currentData(),
            catalog_demo=bool(self.catalog.get("demo")),
            confirmation_required=True,
        )

    def browse(self, field):
        selected = QFileDialog.getExistingDirectory(self, "Select folder", field.text())
        if selected:
            field.setText(selected)

    def update_preview(self):
        leaf = Path(self.source.text()).name or "<recording folder>"
        self.preview.setText(
            f"Destination: {self.root.text() or '<incoming area>'} / "
            f"{self.modality.currentText()} / {socket.gethostname()} / <transfer ID> / {leaf}"
        )
        session = self.session.currentData()
        if session:
            relative = (
                f"{session['user_id']}/{session['subject_fullname']}/"
                f"{session['session_date'].replace('-', '')}_g{session['session_number']}/{leaf}"
            )
            self.preview.setText(
                self.preview.text()
                + f"\nFinal relative path after website confirmation: {relative}"
            )

    def enqueue(self, start):
        try:
            if not self.source.text().strip() or not self.root.text().strip():
                raise ValueError("Select both the recording folder and incoming area.")
            self.store.enqueue(
                Path(self.source.text()),
                Path(self.root.text()),
                self.modality.currentText(),
                self.quiet.value(),
                self.registration_preferences(),
            )
            self.message.setText(
                "Queued. Run pending now, or let the 11 PM task pick it up."
            )
            self.refresh()
            if start:
                self.start()
        except Exception as exc:
            QMessageBox.warning(self, "Unable to queue", str(exc))

    def start(self):
        if self.worker and self.worker.isRunning():
            return
        self.worker = Worker(self.store, delay=0.12 if self.demo else 0)
        self.worker.progress.connect(self.on_progress)
        self.worker.outcome.connect(self.message.setText)
        self.worker.finished.connect(self.finished)
        for button in [self.queue_button, self.copy_button, self.run_button]:
            button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.worker.start()

    def on_progress(self, value, message):
        self.progress.setValue(value)
        self.message.setText(message)

    def finished(self):
        for button in [self.queue_button, self.copy_button, self.run_button]:
            button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.refresh()

    def refresh(self):
        jobs = self.store.read()
        self.table.setRowCount(len(jobs))
        for row, job in enumerate(jobs):
            for col, value in enumerate(
                [Path(job["source"]).name, job["status"], job["message"]]
            ):
                item = QTableWidgetItem(value)
                item.setToolTip(
                    job["source"] + "\n→ " + job["destination"] + "\n" + job["message"]
                )
                self.table.setItem(row, col, item)

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            self.message.setText(
                "Copy running. Keep this window open, or stop and close after it finishes."
            )
            event.ignore()
        else:
            event.accept()


def demo_setup(store):
    lock = store.lock()
    try:
        if any(path.name != "transfer.lock" for path in store.directory.iterdir()):
            raise ValueError(
                "Demo requires an empty state directory; omit --state-dir for a fresh sandbox."
            )
        source = (
            store.directory
            / "sample_recordings"
            / "efonseca"
            / "ef932_act131"
            / "11052026"
        )
        source.mkdir(parents=True, exist_ok=True)
        for name in ["scan_00001.tif", "scan_00002.tif", "scan_00003.tif"]:
            (source / name).write_bytes(b"U19 SYNTHETIC DEMO DATA\n" * 650000)
        (source / "notes.txt").write_text(
            "Synthetic transfer demo, not acquisition data.\n"
        )
        return source, store.directory / "cup_incoming"
    finally:
        lock.unlock()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Validate imports, Qt platform and bundled assets, then exit",
    )
    parser.add_argument(
        "--machine-config",
        type=Path,
        help="Installed machine configuration (approved storage roots)",
    )
    parser.add_argument(
        "--demo", action="store_true", help="Use isolated synthetic data"
    )
    parser.add_argument(
        "--run-pending",
        action="store_true",
        help="No window; suitable for Task Scheduler",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help="Queue directory; use the same path for GUI and scheduled task",
    )
    parser.add_argument(
        "--screenshot-dir",
        type=Path,
        help="With --demo: capture real Qt widgets and exit",
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        help="Read-only JSON snapshot of sessions and parameter profiles",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Preview retained, verified local copies eligible for deletion",
    )
    parser.add_argument(
        "--apply-cleanup",
        action="store_true",
        help="With --cleanup: actually delete verified local recordings",
    )
    parser.add_argument(
        "--source-root", type=Path, help="Approved local recording root for cleanup"
    )
    parser.add_argument(
        "--destination-root",
        type=Path,
        help="Approved braininit incoming root for cleanup",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=14,
        help="Minimum days after verified completion before cleanup",
    )
    args = parser.parse_args()
    if args.self_test:
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
        application = QApplication([sys.argv[0]])
        logo = QPixmap(str(Path(__file__).parent / "assets" / "braincogs.png"))
        if logo.isNull():
            raise RuntimeError("Bundled BrainCOGS logo could not be loaded.")
        print(f"U19 Recording Transfer {__version__}: Qt and assets OK")
        return 0
    machine = None
    if args.machine_config and not args.demo:
        machine = json.loads(args.machine_config.read_text(encoding="utf-8"))
        if not machine.get("source_roots") or not machine.get("incoming_root"):
            parser.error("Machine config needs source_roots and incoming_root.")
        if not args.catalog and machine.get("catalog_path"):
            args.catalog = Path(machine["catalog_path"])
    if args.screenshot_dir and (not args.demo or args.run_pending or args.cleanup):
        parser.error("--screenshot-dir requires --demo and the GUI")
    if args.demo and args.run_pending:
        parser.error(
            "--demo creates fresh data; use --run-pending --state-dir with an existing demo queue"
        )
    if args.cleanup and (args.demo or args.run_pending):
        parser.error("--cleanup cannot be combined with --demo or --run-pending")
    if args.apply_cleanup and not args.cleanup:
        parser.error("--apply-cleanup requires --cleanup")
    if args.cleanup and (
        not args.source_root or not args.destination_root or args.retention_days < 0
    ):
        parser.error(
            "Cleanup requires --source-root, --destination-root, and nonnegative --retention-days"
        )
    state = args.state_dir or (
        Path(tempfile.mkdtemp(prefix="u19-transfer-demo-"))
        if args.demo
        else Path(os.environ.get("LOCALAPPDATA", str(Path.home())))
        / "U19"
        / "RecordingTransfer"
    )
    app = QApplication([sys.argv[0]])
    store = Store(state.resolve(), machine)
    if args.cleanup:
        from .recording_cleanup import cleanup

        try:
            if machine:
                if args.source_root.resolve() not in [
                    Path(p).resolve() for p in machine["source_roots"]
                ]:
                    raise ValueError("Cleanup root is not configured for this machine.")
                if (
                    args.destination_root.resolve()
                    != Path(machine["incoming_root"]).resolve()
                ):
                    raise ValueError(
                        "Cleanup destination is not configured for this machine."
                    )
                if (
                    args.apply_cleanup
                    and machine.get("cleanup", {}).get("enabled") is not True
                ):
                    raise ValueError(
                        "Actual cleanup is disabled in this machine's configuration."
                    )
            return int(
                bool(
                    cleanup(
                        store,
                        args.source_root,
                        args.destination_root,
                        args.retention_days,
                        args.apply_cleanup,
                        skip_outside_root=machine is not None,
                    )
                )
            )
        except (ValueError, RuntimeError) as exc:
            print(str(exc), flush=True)
            return 1
    if args.run_pending:
        try:
            return int(
                bool(
                    run_pending(
                        store, lambda d, t, m: print(m, flush=True), threading.Event()
                    )
                )
            )
        except RuntimeError as exc:
            print(exc, flush=True)
            return 0 if "Another transfer" in str(exc) else 1
    if args.demo:
        try:
            source, root = demo_setup(store)
        except (RuntimeError, ValueError) as exc:
            parser.error(str(exc))
    catalog = (
        demo_catalog()
        if args.demo
        else (load_catalog(args.catalog) if args.catalog else None)
    )
    if catalog and catalog.get("demo") and not args.demo:
        parser.error("Synthetic demo profiles require --demo.")
    window = Window(store, args.demo, catalog)
    if machine:
        window.root.setText(machine["incoming_root"])
        window.root.setReadOnly(True)
        window.modality.setCurrentText(machine.get("modality", "imaging"))
        window.quiet.setValue(machine.get("quiet_minutes", 10))
    if args.demo:
        window.source.setText(str(source))
        window.root.setText(str(root))
        window.quiet.setValue(0)
    window.show()
    if args.screenshot_dir:
        folder = args.screenshot_dir.resolve()
        folder.mkdir(parents=True, exist_ok=True)

        def capture(name):
            window.grab().save(str(folder / name))

        def begin():
            capture("01-ready.png")
            window.enqueue(True)
            QTimer.singleShot(500, lambda: capture("02-copying.png"))
            window.worker.finished.connect(done)

        def done():
            capture("03-complete.png")
            print(f"Demo state: {state}\nScreenshots: {folder}", flush=True)
            app.quit()

        QTimer.singleShot(350, begin)
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())
