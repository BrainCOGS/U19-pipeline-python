"""One active researcher, inferred from the primary native credential entry."""

import ctypes
from ctypes import wintypes

from .credentials import LoginError, WindowsVault, database_config, share_root, target


def primary_identity(machine):
    machine = machine or {}
    registration = machine.get("registration", {})
    if registration.get("enabled"):
        path = registration.get("dj_config")
        if not path:
            raise LoginError("Configure registration.dj_config before signing in.")
        return "database", database_config(path)["database.host"]
    share = share_root(machine.get("incoming_root", ""))
    return ("storage", share) if share else None


def active_user(machine, vault=None):
    identity = primary_identity(machine)
    if not identity:
        return None  # Local-only demo/test copies have no remote account.
    login = (vault or WindowsVault()).read(target(*identity))
    return login[0] if login else None


def require_user(machine, expected=None, vault=None):
    identity = primary_identity(machine)
    if not identity:
        return None
    username = active_user(machine, vault)
    if not username:
        raise LoginError(
            "Signed out. Sign in before running transfers or registration."
        )
    matches = (
        username == expected
        if identity[0] == "database"
        else username.casefold() == (expected or "").casefold()
    )
    if expected and not matches:
        raise LoginError(f"This job belongs to {expected}. Change user to run it.")
    return username


def disconnect_share(share):
    api = ctypes.WinDLL("Mpr.dll")
    api.WNetCancelConnection2W.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.BOOL,
    ]
    api.WNetCancelConnection2W.restype = wintypes.DWORD
    # Never force-close files another application is using.
    code = api.WNetCancelConnection2W(share, 0, False)
    if code not in (0, 2250):
        raise LoginError(
            "App signed out. Windows could not close the storage connection; close open share files and disconnect it in Windows before switching storage accounts."
        )


def logout(machine, vault=None, disconnect=disconnect_share):
    machine = machine or {}
    vault = vault or WindowsVault()
    registration = machine.get("registration", {})
    identities = []
    if registration.get("dj_config"):
        identities.append(
            ("database", database_config(registration["dj_config"])["database.host"])
        )
    share = share_root(machine.get("incoming_root", ""))
    if share:
        identities.append(("storage", share))
    had_storage = False
    for kind, address in identities:
        if kind == "storage":
            had_storage = bool(vault.read(target(kind, address)))
        vault.forget(target(kind, address))
    if had_storage:
        disconnect(share)
    return "Signed out. Saved database and storage logins removed; queued recordings are retained."
