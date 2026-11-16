"""Credential boundaries, noninteractive authentication and native vault smoke."""

import json
import os
from types import SimpleNamespace
import uuid

import pytest

from u19_recording_transfer import credentials as c
from u19_recording_transfer import login_cli


class Vault:
    def __init__(self):
        self.items = {}

    def read(self, name):
        return self.items.get(name)

    def save(self, name, username, password):
        self.items[name] = (username, password)

    def forget(self, name):
        self.items.pop(name, None)


@pytest.fixture
def config(tmp_path):
    path = tmp_path / "dj.json"
    path.write_text(
        json.dumps(
            {
                "database.host": "db.example.org:3306",
                "database.user": "researcher",
                "database.password": "old-local-value",
                "custom": {"database.prefix": "test_"},
            }
        )
    )
    return path


def test_explicit_db_connection_uses_vault_account_not_cached_or_plaintext(config):
    vault = Vault()
    vault.save(c.target("database", "db.example.org:3306"), "new-user", "vault-value")
    calls = []
    connection = object()
    dj = SimpleNamespace(
        Connection=lambda *a, **kw: calls.append((a, kw)) or connection
    )
    assert c.connect_database(config, vault=vault, dj=dj)[1] is connection
    assert calls == [
        (
            ("db.example.org:3306", "new-user", "vault-value"),
            {"port": 3306, "use_tls": True},
        )
    ]


def test_missing_login_fails_without_prompt_or_plaintext_fallback(config):
    with pytest.raises(c.LoginError, match="login is missing"):
        c.connect_database(config, vault=Vault(), dj=object())


def test_db_error_never_exposes_provider_exception_or_password(config, caplog, capsys):
    def fail(*args, **kwargs):
        raise RuntimeError("provider echo secret-value")

    with pytest.raises(c.LoginError) as error:
        c.connect_database(
            config, login=("user", "secret-value"), dj=SimpleNamespace(Connection=fail)
        )
    assert (
        "secret-value" not in str(error.value) + caplog.text + capsys.readouterr().out
    )


def test_save_tests_before_replacing_and_migrates_plaintext_config(config):
    vault = Vault()
    closed = []

    def connect(path, login):
        assert login == ("new-user", "new-value")
        assert not vault.items
        return (
            None,
            SimpleNamespace(close=lambda: closed.append(True)),
            c.database_config(path),
        )

    c.save_database_login(config, "new-user", "new-value", vault, connect)
    assert closed == [True]
    assert vault.read(c.target("database", "db.example.org:3306")) == (
        "new-user",
        "new-value",
    )
    saved = json.loads(config.read_text())
    assert saved["database.user"] == "new-user" and "database.password" not in saved
    assert "new-value" not in config.read_text()
    assert saved["custom"] == {"database.prefix": "test_"}


def test_failed_validation_preserves_previous_login_and_config(config):
    vault = Vault()
    name = c.target("database", "db.example.org:3306")
    vault.save(name, "previous", "previous-value")
    before = config.read_bytes()

    def fail(*args, **kwargs):
        raise c.LoginError("Login failed")

    with pytest.raises(c.LoginError):
        c.save_database_login(config, "new-user", "new-value", vault, fail)
    assert vault.read(name) == ("previous", "previous-value")
    assert config.read_bytes() == before


def test_storage_login_reuses_unc_share_identity_and_never_disconnects():
    vault = Vault()
    calls = []
    root = r"\\cup.example.org\braininit\Data\Incoming"
    c.save_storage_login(
        root, r"LAB\researcher", "share-value", vault, lambda *a: calls.append(a)
    )
    if os.name == "nt":
        c.ensure_share(
            r"\\cup.example.org\braininit\Data\Raw\imaging",
            vault,
            lambda *a: calls.append(a),
        )
        assert calls[0] == calls[1]
    assert c.share_root(root) == r"\\cup.example.org\braininit"
    assert c.share_root(r"C:\Data") is None
    assert c.target("database", "host") != c.target("storage", "host")
    assert c.target("storage", root.upper()) == c.target("storage", root)


@pytest.mark.skipif(os.name != "nt", reason="Windows native Credential Manager")
def test_real_native_vault_unicode_roundtrip_and_forget():
    # Unique disposable test entry; never reads or alters real application logins.
    vault = c.WindowsVault()
    name = "BrainCOGS/U19RecordingTransfer/test-" + uuid.uuid4().hex
    try:
        assert vault.read(name) is None
        vault.save(name, "test-user", "synthetic-\u03b1-\U0001f512")
        assert vault.read(name) == ("test-user", "synthetic-\u03b1-\U0001f512")
        vault.save(name, "replacement", "synthetic-replacement")
        assert vault.read(name) == ("replacement", "synthetic-replacement")
    finally:
        vault.forget(name)
    assert vault.read(name) is None
    vault.forget(name)


def test_login_cli_uses_getpass_and_never_prints_secret(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "machine.json"
    machine.write_text(json.dumps({"incoming_root": r"\\example\share\incoming"}))
    calls = []
    monkeypatch.setattr(login_cli.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(login_cli, "getpass", lambda *a: "synthetic-secret")
    monkeypatch.setattr(login_cli, "save_storage_login", lambda *a: calls.append(a))
    assert (
        login_cli.main(
            [
                "--machine-config",
                str(machine),
                "--kind",
                "storage",
                "--username",
                "user",
            ]
        )
        == 0
    )
    assert calls == [(r"\\example\share\incoming", "user", "synthetic-secret")]
    assert "synthetic-secret" not in capsys.readouterr().out


def test_gui_masks_password_and_clears_on_cancel(monkeypatch):
    from PyQt6.QtWidgets import QApplication, QLineEdit
    from u19_recording_transfer import login_ui

    application = QApplication.instance() or QApplication([])
    monkeypatch.setattr(login_ui, "WindowsVault", Vault)
    dialog = login_ui.LoginDialog(None, {"incoming_root": r"\\example\share\incoming"})
    assert dialog.password.echoMode() == QLineEdit.EchoMode.Password
    dialog.password.setText("synthetic-secret")
    dialog.reject()
    assert dialog.password.text() == ""
    application.processEvents()


@pytest.mark.parametrize("succeeds", [True, False])
def test_gui_save_clears_password_and_redacts_provider_failures(monkeypatch, succeeds):
    from PyQt6.QtCore import QEventLoop, QTimer
    from PyQt6.QtWidgets import QApplication
    from u19_recording_transfer import login_ui

    application = QApplication.instance() or QApplication([])
    monkeypatch.setattr(login_ui, "WindowsVault", Vault)
    calls = []

    def save(*args):
        calls.append(args)
        if not succeeds:
            raise RuntimeError("provider echoed synthetic-secret")

    monkeypatch.setattr(login_ui, "save_storage_login", save)
    machine = {"incoming_root": r"\\example\share\incoming"}
    dialog = login_ui.LoginDialog(None, machine)
    dialog.username.setText("user")
    dialog.password.setText("synthetic-secret")
    dialog.save_login()
    assert dialog.password.text() == ""
    loop = QEventLoop()
    dialog.worker.finished.connect(loop.quit)
    QTimer.singleShot(3000, loop.quit)
    loop.exec()
    application.processEvents()
    assert dialog.worker is None
    assert calls == [(machine["incoming_root"], "user", "synthetic-secret")]
    assert "synthetic-secret" not in dialog.note.text() + json.dumps(machine)
    assert ("tested and saved" in dialog.note.text()) == succeeds
    dialog.reject()


def test_cli_refuses_password_prompt_without_terminal(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "machine.json"
    machine.write_text(json.dumps({"incoming_root": r"\\example\share\incoming"}))
    monkeypatch.setattr(login_cli.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(login_cli, "getpass", lambda *a: pytest.fail("must not prompt"))
    assert (
        login_cli.main(
            [
                "--machine-config",
                str(machine),
                "--kind",
                "storage",
                "--username",
                "user",
            ]
        )
        == 1
    )
    assert "interactive terminal" in capsys.readouterr().out


@pytest.mark.skipif(os.name != "nt", reason="Windows share API")
@pytest.mark.parametrize("code", [1219, 1326])
def test_share_errors_are_safe_and_do_not_disconnect(monkeypatch, code):
    class Call:
        def __call__(self, resource, password, username, flags):
            assert password == "synthetic-secret"
            assert flags == 0  # no persistent mapping or OS credential dialog
            return code

    monkeypatch.setattr(
        c.ctypes, "WinDLL", lambda *a: SimpleNamespace(WNetAddConnection2W=Call())
    )
    with pytest.raises(c.LoginError) as error:
        c.connect_share(r"\\example\share", ("user", "synthetic-secret"))
    assert "synthetic-secret" not in str(error.value)
    if code == 1219:
        assert "will not disconnect" in str(error.value)
