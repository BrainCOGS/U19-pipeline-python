"""Explicit login management; password text never reaches transfer state."""

from PyQt6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QLineEdit,
    QPushButton,
)

from .credentials import (
    LoginError,
    WindowsVault,
    database_config,
    save_database_login,
    save_storage_login,
    share_root,
    target,
)


class LoginDialog(QDialog):
    def __init__(self, parent, machine, mode="settings"):
        super().__init__(parent)
        self.mode = mode
        self.setWindowTitle(
            "Change password"
            if mode == "password"
            else "Sign in"
            if mode == "signin"
            else "Connection logins"
        )
        self.worker = None
        self.machine = machine or {}
        layout = QFormLayout(self)
        self.kind = QComboBox()
        self.kind.addItems(["Storage share", "Database"])
        if (machine or {}).get("registration", {}).get("enabled"):
            self.kind.setCurrentIndex(1)
        self.kind.setEnabled(mode == "settings")
        self.address = QLabel()
        self.username = QLineEdit()
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.EchoMode.Password)
        self.note = QLabel(
            "Save tests the login first. Passwords are saved only in Windows Credential Manager for this Windows account."
        )
        self.note.setWordWrap(True)
        self.save_button = QPushButton("Test and save login")
        self.forget_button = QPushButton("Forget saved login")
        close = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        close.rejected.connect(self.reject)
        for label, widget in [
            ("Login for", self.kind),
            ("Server / share", self.address),
            ("Username", self.username),
            ("Password", self.password),
            ("", self.note),
            ("", self.save_button),
            ("", self.forget_button),
            ("", close),
        ]:
            layout.addRow(label, widget)
        self.kind.currentIndexChanged.connect(self.load_identity)
        self.save_button.clicked.connect(self.save_login)
        self.forget_button.clicked.connect(self.forget_login)
        self.load_identity()
        if mode == "password":
            self.username.setReadOnly(True)
            self.save_button.setText("Test and update password")
            self.forget_button.hide()
            self.note.setText(
                "Update the saved password for this user. This updates the app's login; it does not change the password on the server."
            )
        elif mode == "signin":
            self.save_button.setText("Sign in and save securely")
            self.forget_button.hide()

    def identity(self):
        if self.kind.currentIndex() == 1:
            path = self.machine.get("registration", {}).get("dj_config")
            if not path:
                raise LoginError(
                    "Configure registration.dj_config before saving a database login."
                )
            config = database_config(path)
            return (
                "database",
                config["database.host"],
                path,
                config.get("database.user", "") or "",
            )
        path = self.machine.get("incoming_root", "")
        share = share_root(path)
        if not share:
            raise LoginError("Storage login requires a configured UNC incoming path.")
        return "storage", share, path, ""

    def load_identity(self):
        self.password.clear()
        self.username.clear()
        try:
            kind, address, _, username = self.identity()
            self.address.setText(address)
            saved = WindowsVault().read(target(kind, address))
            self.username.setText(saved[0] if saved else username)
            self.note.setText(
                "Saved login found. Enter a password to replace it."
                if saved
                else "No saved login. Enter credentials to test and save for this Windows account."
            )
            self.save_button.setEnabled(True)
            self.forget_button.setEnabled(bool(saved))
        except Exception as exc:
            self.address.setText("")
            self.note.setText(
                str(exc)
                if isinstance(exc, LoginError)
                else "Could not read login settings."
            )
            self.save_button.setEnabled(False)
            self.forget_button.setEnabled(False)

    def save_login(self):
        from .app import PreviewWorker

        kind, _, path, _ = self.identity()
        username, password = self.username.text().strip(), self.password.text()
        self.password.clear()
        if not username or not password:
            self.note.setText("Enter a username and password.")
            return
        self.setEnabled(False)
        self.note.setText("Testing login…")

        def operation():
            try:
                save = save_database_login if kind == "database" else save_storage_login
                save(path, username, password)
                return "Login tested and saved in Windows Credential Manager."
            except Exception as exc:
                return (
                    str(exc)
                    if isinstance(exc, LoginError)
                    else "Login could not be saved. Check local configuration and Windows Credential Manager."
                )

        self.worker = PreviewWorker(operation)
        self.worker.result.connect(self.login_result)
        self.worker.finished.connect(self.worker_finished)
        self.worker.start()

    def login_result(self, message):
        self.note.setText(message)

    def worker_finished(self):
        worker, self.worker = self.worker, None
        worker.deleteLater()
        self.setEnabled(True)
        self.forget_button.setEnabled(True)

    def forget_login(self):
        try:
            kind, address, _, _ = self.identity()
            from .accounts import primary_identity, logout, disconnect_share

            vault = WindowsVault()
            if (kind, address) == primary_identity(self.machine):
                message = logout(self.machine, vault)
            else:
                saved = vault.read(target(kind, address))
                vault.forget(target(kind, address))
                if saved and kind == "storage":
                    disconnect_share(address)
                message = "Saved connection login removed."
            self.password.clear()
            self.note.setText(message)
            self.forget_button.setEnabled(False)
        except Exception:
            self.note.setText(
                "Could not remove the saved login from Windows Credential Manager."
            )

    def reject(self):
        if self.worker and self.worker.isRunning():
            return
        self.password.clear()
        super().reject()

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            event.ignore()
        else:
            self.password.clear()
            super().closeEvent(event)
