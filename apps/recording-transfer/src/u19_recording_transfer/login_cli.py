"""Manage native saved logins without putting passwords on the command line."""

import argparse
from getpass import getpass
import json
import sys

from .credentials import (
    LoginError,
    WindowsVault,
    database_config,
    save_database_login,
    save_storage_login,
    share_root,
    target,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--machine-config", required=True)
    parser.add_argument("--kind", choices=["database", "storage"], required=True)
    parser.add_argument("--username")
    parser.add_argument("--forget", action="store_true")
    args = parser.parse_args(argv)
    try:
        with open(args.machine_config, encoding="utf-8-sig") as stream:
            machine = json.load(stream)
        if args.kind == "database":
            path = machine["registration"]["dj_config"]
            address = database_config(path)["database.host"]
            save = save_database_login
        else:
            path = machine["incoming_root"]
            address = share_root(path)
            if not address:
                raise LoginError("Configure a UNC incoming_root for storage login.")
            save = save_storage_login
        if args.forget:
            WindowsVault().forget(target(args.kind, address))
            print("Saved login removed. Existing connections are unchanged.")
        else:
            if not sys.stdin.isatty():
                raise LoginError(
                    "Saving a login requires an interactive terminal or the Logins… dialog."
                )
            username = args.username or input("Username: ").strip()
            password = getpass("Password (stored in Windows Credential Manager): ")
            try:
                save(path, username, password)
            finally:
                password = None
            print("Login tested and saved for the current Windows account.")
        return 0
    except (EOFError, KeyboardInterrupt):
        print("Login cancelled.")
        return 1
    except Exception as exc:
        print(
            str(exc)
            if isinstance(exc, LoginError)
            else "Login failed. Check local settings and Windows Credential Manager."
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
