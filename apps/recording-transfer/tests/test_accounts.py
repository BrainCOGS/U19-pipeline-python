"""Single-user, logout and queue ownership boundaries without real credentials."""

import json
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from u19_recording_transfer import accounts, app, credentials, login_ui


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
def account(tmp_path, monkeypatch):
    config = tmp_path / "dj.json"
    config.write_text(
        json.dumps({"database.host": "synthetic-db", "database.user": "alice"})
    )
    machine = {
        "registration": {"enabled": True, "dj_config": str(config)},
        "incoming_root": r"\\synthetic\share\incoming",
    }
    vault = Vault()
    vault.save(credentials.target("database", "synthetic-db"), "alice", "synthetic")
    vault.save(
        credentials.target("storage", r"\\synthetic\share"), r"LAB\alice", "synthetic"
    )
    monkeypatch.setattr(accounts, "WindowsVault", lambda: vault)
    monkeypatch.setattr(login_ui, "WindowsVault", lambda: vault)
    return machine, vault, config


@pytest.mark.parametrize("username", ["bob", "Alice"])
def test_second_user_cannot_replace_active_database_login(account, username):
    machine, vault, config = account

    def connector(*a, **kw):
        pytest.fail("must log out before authenticating another account")

    with pytest.raises(credentials.LoginError, match="One user"):
        credentials.save_database_login(config, username, "synthetic", vault, connector)
    assert accounts.active_user(machine) == "alice"


def test_second_storage_account_cannot_replace_active_account(account):
    machine, vault, _ = account
    with pytest.raises(credentials.LoginError, match="One storage account"):
        credentials.save_storage_login(
            machine["incoming_root"],
            "bob",
            "synthetic",
            vault,
            lambda *a: pytest.fail("must not connect"),
        )


def test_logout_removes_both_credentials_and_blocks_unattended_work(account):
    machine, vault, _ = account
    disconnected = []
    accounts.logout(machine, vault, disconnected.append)
    assert vault.items == {} and disconnected == [r"\\synthetic\share"]
    with pytest.raises(credentials.LoginError, match="Signed out"):
        accounts.require_user(machine)


def test_logout_stays_signed_out_when_windows_cannot_close_share(account):
    machine, vault, _ = account

    def busy(share):
        raise credentials.LoginError("share busy")

    with pytest.raises(credentials.LoginError, match="share busy"):
        accounts.logout(machine, vault, busy)
    assert accounts.active_user(machine) is None
    assert not vault.items


def test_logout_never_disconnects_an_unmanaged_windows_share(account):
    machine, vault, _ = account
    vault.forget(credentials.target("storage", r"\\synthetic\share"))
    accounts.logout(machine, vault, lambda share: pytest.fail("not managed by app"))
    assert accounts.active_user(machine) is None


def test_queue_rejects_another_user_then_retries_for_its_owner(account, tmp_path):
    machine, vault, _ = account
    source = tmp_path / "local/recording"
    source.mkdir(parents=True)
    (source / "raw.tif").write_bytes(b"synthetic")
    machine.update(
        source_roots=[str(source.parent)], incoming_root=str(tmp_path / "incoming")
    )
    store = app.Store(tmp_path / "state", machine)
    job = store.enqueue(source, Path(machine["incoming_root"]), "imaging", 0)
    assert job["queued_by"] == "alice"
    vault.save(credentials.target("database", "synthetic-db"), "bob", "synthetic")
    assert app.run_pending(store, lambda *a: None, threading.Event()) == 1
    assert "belongs to alice" in store.read()[0]["message"]
    assert not Path(job["destination"]).exists()
    vault.forget(credentials.target("database", "synthetic-db"))
    assert app.run_pending(store, lambda *a: None, threading.Event()) == 1
    assert "Signed out" in store.read()[0]["message"]
    vault.save(credentials.target("database", "synthetic-db"), "alice", "synthetic")
    assert app.run_pending(store, lambda *a: None, threading.Event()) == 0
    assert store.read()[0]["status"] == "Complete"


def test_gui_account_controls_and_password_username_lock(account, tmp_path):
    from PyQt6.QtWidgets import QApplication

    application = QApplication.instance() or QApplication([])
    machine, vault, _ = account
    window = app.Window(app.Store(tmp_path / "state", machine))
    assert "alice" in window.account_label.text()
    assert window.change_user_button.isEnabled() and window.logout_button.isEnabled()
    dialog = login_ui.LoginDialog(window, machine, mode="password")
    assert dialog.username.text() == "alice" and dialog.username.isReadOnly()
    assert not dialog.kind.isEnabled()
    dialog.reject()
    window.worker = SimpleNamespace(isRunning=lambda: True)
    window.refresh_account_controls()
    assert all(not button.isEnabled() for button in window.account_buttons)
    window.worker = None
    vault.items.clear()
    window.refresh_account_controls()
    assert "Signed out" in window.account_label.text()
    assert window.sign_in_button.isEnabled()
    assert not window.copy_button.isEnabled() and not window.run_button.isEnabled()
    assert not window.register_button.isEnabled()
    window.close()
    application.processEvents()


@pytest.mark.parametrize("code", [0, 2250, 2401])
def test_native_logout_never_forces_open_files_closed(monkeypatch, code):
    class Cancel:
        def __call__(self, share, flags, force):
            assert share == r"\\synthetic\share" and flags == 0 and force is False
            return code

    monkeypatch.setattr(
        accounts.ctypes,
        "WinDLL",
        lambda *a: SimpleNamespace(WNetCancelConnection2W=Cancel()),
    )
    if code == 2401:
        with pytest.raises(credentials.LoginError, match="close open share files"):
            accounts.disconnect_share(r"\\synthetic\share")
    else:
        accounts.disconnect_share(r"\\synthetic\share")
