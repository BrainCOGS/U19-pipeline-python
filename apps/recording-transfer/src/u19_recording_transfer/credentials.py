"""Windows Credential Manager and explicit, noninteractive DB/share connections."""

import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath


class LoginError(RuntimeError):
    """Safe user-facing error; never includes provider exception text."""


class Credential(ctypes.Structure):
    _fields_ = [
        ("Flags", wintypes.DWORD),
        ("Type", wintypes.DWORD),
        ("TargetName", wintypes.LPWSTR),
        ("Comment", wintypes.LPWSTR),
        ("LastWritten", wintypes.FILETIME),
        ("CredentialBlobSize", wintypes.DWORD),
        ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
        ("Persist", wintypes.DWORD),
        ("AttributeCount", wintypes.DWORD),
        ("Attributes", ctypes.c_void_p),
        ("TargetAlias", wintypes.LPWSTR),
        ("UserName", wintypes.LPWSTR),
    ]


def target(kind, address):
    identity = f"{kind}:{address.casefold().rstrip('/\\')}"
    return (
        "BrainCOGS/U19RecordingTransfer/"
        + hashlib.sha256(identity.encode()).hexdigest()
    )


class WindowsVault:
    def __init__(self):
        if os.name != "nt":
            raise LoginError("Saved logins require Windows Credential Manager.")
        self.api = ctypes.WinDLL("Advapi32.dll", use_last_error=True)
        pointer = ctypes.POINTER(Credential)
        self.api.CredReadW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.POINTER(pointer),
        ]
        self.api.CredReadW.restype = wintypes.BOOL
        self.api.CredWriteW.argtypes = [pointer, wintypes.DWORD]
        self.api.CredWriteW.restype = wintypes.BOOL
        self.api.CredDeleteW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
        ]
        self.api.CredDeleteW.restype = wintypes.BOOL
        self.api.CredFree.argtypes = [ctypes.c_void_p]
        self.api.CredFree.restype = None

    def read(self, name):
        pointer = ctypes.POINTER(Credential)()
        if not self.api.CredReadW(name, 1, 0, ctypes.byref(pointer)):
            if ctypes.get_last_error() == 1168:
                return None
            raise LoginError("Windows could not read the saved login for this account.")
        try:
            row = pointer.contents
            try:
                if row.CredentialBlobSize > 2560:
                    raise ValueError
                return row.UserName, ctypes.string_at(
                    row.CredentialBlob, row.CredentialBlobSize
                ).decode("utf-16-le")
            except (ValueError, UnicodeError):
                raise LoginError(
                    "The saved login is invalid. Replace it in Logins…."
                ) from None
        finally:
            self.api.CredFree(pointer)

    def save(self, name, username, password):
        blob = password.encode("utf-16-le")
        if (
            not username
            or not password
            or len(blob) > 2560
            or "\0" in username
            or "\0" in password
        ):
            raise LoginError(
                "Enter a username and a nonempty password (at most 2560 encoded bytes)."
            )
        buffer = (ctypes.c_ubyte * len(blob)).from_buffer_copy(blob)
        row = Credential(
            Type=1,
            TargetName=name,
            UserName=username,
            CredentialBlobSize=len(blob),
            CredentialBlob=buffer,
            Persist=2,
        )
        try:
            if not self.api.CredWriteW(ctypes.byref(row), 0):
                raise LoginError(
                    "Windows could not save this login. Check Credential Manager policy."
                )
        finally:
            ctypes.memset(buffer, 0, len(blob))

    def forget(self, name):
        if not self.api.CredDeleteW(name, 1, 0) and ctypes.get_last_error() != 1168:
            raise LoginError("Windows could not remove the saved login.")


def database_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not config.get("database.host"):
        raise LoginError(
            "Set database.host in the local DataJoint configuration first."
        )
    return config


def connect_database(path, login=None, vault=None, dj=None):
    config = database_config(path)
    if login is None:
        login = (vault or WindowsVault()).read(
            target("database", config["database.host"])
        )
    if not login:
        raise LoginError(
            "Database login is missing. Open Logins… and save a database login first."
        )
    if dj is None:
        import datajoint as dj
    try:
        # Explicit connection avoids both terminal prompts and dj.conn's cached account.
        connection = dj.Connection(
            config["database.host"],
            login[0],
            login[1],
            port=config.get("database.port", 3306),
            use_tls=config.get("database.use_tls", True),
        )
    except Exception:
        raise LoginError(
            "Database login failed. Check the account, password, network and TLS settings in Logins…."
        ) from None
    return dj, connection, config


def share_root(path):
    anchor = PureWindowsPath(path).anchor
    return anchor.rstrip("\\") if anchor.startswith("\\\\") else None


def connect_share(share, login):
    class Resource(ctypes.Structure):
        _fields_ = [
            ("scope", wintypes.DWORD),
            ("type", wintypes.DWORD),
            ("display", wintypes.DWORD),
            ("usage", wintypes.DWORD),
            ("local", wintypes.LPWSTR),
            ("remote", wintypes.LPWSTR),
            ("comment", wintypes.LPWSTR),
            ("provider", wintypes.LPWSTR),
        ]

    if os.name != "nt":
        raise LoginError("Network-share login requires Windows.")
    api = ctypes.WinDLL("Mpr.dll")
    api.WNetAddConnection2W.argtypes = [
        ctypes.POINTER(Resource),
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.DWORD,
    ]
    api.WNetAddConnection2W.restype = wintypes.DWORD
    result = api.WNetAddConnection2W(
        ctypes.byref(Resource(type=1, remote=share)), login[1], login[0], 0
    )
    if result == 1219:
        raise LoginError(
            "Windows already has a connection to this server under another account. Close it in Windows before retrying; the app will not disconnect it."
        )
    if result:
        raise LoginError(
            f"Storage login failed (Windows code {result}). Check Logins… and network access."
        )


def ensure_share(path, vault=None, connector=connect_share):
    share = share_root(path)
    if share and os.name == "nt":
        login = (vault or WindowsVault()).read(target("storage", share))
        if login:
            connector(share, login)


def save_database_login(
    path, username, password, vault=None, connector=connect_database
):
    # Validate before replacing the last working saved login.
    _, connection, config = connector(path, login=(username, password))
    connection.close()
    (vault or WindowsVault()).save(
        target("database", config["database.host"]), username, password
    )
    # Explicit Save migrates the legacy config; the password is never written back.
    from .app import atomic_json

    config.pop("database.password", None)
    config["database.user"] = username
    atomic_json(Path(path), config)


def save_storage_login(path, username, password, vault=None, connector=connect_share):
    share = share_root(path)
    if not share:
        raise LoginError(
            "Storage login needs a configured UNC path such as \\\\server\\share."
        )
    connector(share, (username, password))
    (vault or WindowsVault()).save(target("storage", share), username, password)
