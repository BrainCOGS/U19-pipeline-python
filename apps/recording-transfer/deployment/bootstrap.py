"""Stable, standard-library launcher/updater. Installed once per workstation user.

The release directory and its virtual environment never move after creation.
All installed invocations use this launcher and hold one process lock throughout.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
import hashlib
import json
import logging
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import Request, urlopen
import uuid
import zipfile

LOGGER = logging.getLogger(__name__)

BOOTSTRAP_API = 1
STATE_SCHEMA = 1
MAX_ARCHIVE_BYTES = 100 * 1024 * 1024
MAX_EXTRACTED_BYTES = 150 * 1024 * 1024
VERSION = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\Z")


class BusyError(RuntimeError):
    pass


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def runtime_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    handle = (root / "runtime.lock").open("a+b")
    handle.seek(0, 2)
    if handle.tell() == 0:
        handle.write(b"0")
        handle.flush()
    handle.seek(0)
    locked = False
    try:
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as exc:
            raise BusyError(
                "Application, transfer, cleanup or update already active; skipping."
            ) from exc
        yield
    finally:
        if locked:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def release_path(root, version):
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise ValueError("Release version must be a plain major.minor.patch number.")
    path = root / "versions" / version
    if path.is_symlink() or path.is_junction():
        raise ValueError("Linked release directories are not supported.")
    if not path.resolve().is_relative_to((root / "versions").resolve()):
        raise ValueError("Release directory escaped the installation root.")
    return path


def config_for(root):
    config = read_json(root / "machine.json")
    if config.get("schema_version") != STATE_SCHEMA:
        raise ValueError("Unsupported machine configuration schema.")
    if not isinstance(config.get("source_roots"), list) or not config["source_roots"]:
        raise ValueError("Configure at least one source root.")
    for value in [
        *config["source_roots"],
        config.get("incoming_root", ""),
        config.get("state_dir", ""),
        config.get("uv_executable", ""),
    ]:
        if not isinstance(value, str) or not value or not Path(value).is_absolute():
            raise ValueError(
                "Source, incoming, state and uv paths must all be absolute."
            )
    cleanup = config.get("cleanup", {})
    if (
        not isinstance(cleanup.get("retention_days", 14), int)
        or cleanup.get("retention_days", 14) < 0
    ):
        raise ValueError("Retention must be a nonnegative number of days.")
    if not isinstance(config.get("python", "3.12"), str):
        raise TypeError("python must be a version string.")
    github_settings(config)
    return config


def source_bytes(source, limit, headers=None):
    if source.startswith("https://"):
        request = Request(
            source,
            headers={
                "User-Agent": "BrainCOGS-U19-RecordingTransfer/1",
                **(headers or {}),
            },
        )
        with urlopen(request, timeout=20) as response:
            if urlparse(response.geturl()).scheme != "https":
                raise ValueError("Refusing an HTTPS downgrade redirect.")
            data = response.read(limit + 1)
    elif "://" in source:
        raise ValueError("Release sources must be HTTPS URLs or filesystem paths.")
    else:
        with Path(source).open("rb") as handle:
            data = handle.read(limit + 1)
    if len(data) > limit:
        raise ValueError("Release source exceeds the size limit.")
    return data


def github_settings(config):
    settings = config.get("github", {})
    repository = settings.get("repository", "BrainCOGS/U19-pipeline-python")
    if not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9][A-Za-z0-9_.-]*", repository
    ):
        raise ValueError("GitHub repository must be owner/name.")
    channel = settings.get("channel", "stable")
    if channel not in ("stable", "pilot"):
        raise ValueError("GitHub channel must be stable or pilot.")
    prefix = settings.get("tag_prefix", "recording-transfer/v")
    if prefix != "recording-transfer/v":
        raise ValueError("Release tags must use the recording-transfer/v namespace.")
    pinned = settings.get("pinned_version")
    if pinned is not None and (
        not isinstance(pinned, str) or not VERSION.fullmatch(pinned)
    ):
        raise ValueError("Pinned version must be null or major.minor.patch.")
    return repository, channel, prefix, pinned


def github_release_manifest(config):
    """Discover only this application's published releases, not repo-wide latest."""
    repository, channel, prefix, pinned = github_settings(config)
    candidates = []
    for page in range(1, 11):
        url = f"https://api.github.com/repos/{repository}/releases?per_page=100&page={page}"
        rows = json.loads(
            source_bytes(
                url, 5 * 1024 * 1024, headers={"Accept": "application/vnd.github+json"}
            )
        )
        if not isinstance(rows, list):
            raise TypeError("GitHub did not return a release list.")
        for release in rows:
            tag = release.get("tag_name", "")
            if (
                not isinstance(tag, str)
                or not tag.startswith(prefix)
                or release.get("draft")
            ):
                continue
            version = tag[len(prefix) :]
            if not VERSION.fullmatch(version) or (
                channel == "stable" and release.get("prerelease")
            ):
                continue
            if pinned is None or pinned == version:
                candidates.append(
                    (tuple(map(int, version.split("."))), version, release)
                )
        if len(rows) < 100:
            break
    else:
        raise ValueError(
            "Release listing exceeded 1,000 entries; refusing an incomplete discovery result."
        )
    if not candidates:
        raise ValueError(
            f"No published recording-transfer release is available for channel {channel}."
        )
    _, version, release = max(candidates, key=lambda item: item[0])
    assets = release.get("assets", [])

    def asset_url(name):
        matches = [
            asset
            for asset in assets
            if asset.get("name") == name and asset.get("state") == "uploaded"
        ]
        if len(matches) != 1:
            raise ValueError(
                f"GitHub release must contain exactly one uploaded {name}."
            )
        url = matches[0].get("browser_download_url", "")
        parsed = urlparse(url)
        expected = f"/{repository}/releases/download/{release['tag_name']}/{name}"
        if (
            parsed.scheme != "https"
            or parsed.netloc != "github.com"
            or unquote(parsed.path) != expected
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Release asset URL does not belong to the selected GitHub release."
            )
        return url

    manifest_url = asset_url(f"release-{version}.json")
    manifest, archive_url = manifest_for(manifest_url)
    expected_archive = asset_url(f"u19-recording-transfer-{version}.zip")
    if manifest["version"] != version or unquote(archive_url) != unquote(
        expected_archive
    ):
        raise ValueError("GitHub release tag, manifest and archive do not agree.")
    return manifest, archive_url


def manifest_for(source):
    manifest = json.loads(source_bytes(source, 64 * 1024))
    if not VERSION.fullmatch(str(manifest.get("version", ""))):
        raise ValueError("Invalid release version.")
    if (
        manifest.get("bootstrap_api") != BOOTSTRAP_API
        or manifest.get("state_schema") != STATE_SCHEMA
    ):
        raise ValueError(
            "Release requires a different launcher/state schema; contact the maintainer."
        )
    if not re.fullmatch(r"[a-f0-9]{64}", str(manifest.get("sha256", ""))):
        raise ValueError("Release needs a SHA-256 archive checksum.")
    archive = manifest.get("archive")
    if not isinstance(archive, str) or not archive:
        raise ValueError("Release needs an archive path.")
    if source.startswith("https://"):
        archive = urljoin(source, archive)
        if not archive.startswith("https://"):
            raise ValueError("Remote releases require an HTTPS archive.")
    elif not archive.startswith("https://"):
        archive = str(Path(source).resolve().parent / archive)
    return manifest, archive


def extract_archive(archive, destination):
    """Validate every member before writing any; no arbitrary archive extractall."""
    with zipfile.ZipFile(archive) as bundle:
        members = bundle.infolist()
        if (
            len(members) > 2000
            or sum(member.file_size for member in members) > MAX_EXTRACTED_BYTES
        ):
            raise ValueError("Release archive exceeds extraction limits.")
        names = set()
        for member in members:
            path = PurePosixPath(member.filename)
            if (
                not path.parts
                or path.is_absolute()
                or ".." in path.parts
                or "\\" in member.filename
                or ":" in member.filename
                or any(part.endswith((".", " ")) for part in path.parts)
                or path.parts[0]
                not in ("pyproject.toml", "uv.lock", "README.md", "src")
                or stat.S_ISLNK(member.external_attr >> 16)
            ):
                raise ValueError(f"Unsafe release member: {member.filename}")
            normalized = member.filename.rstrip("/").casefold()
            if normalized in names:
                raise ValueError("Duplicate archive member.")
            names.add(normalized)
        for member in members:
            target = destination.joinpath(*PurePosixPath(member.filename).parts)
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with bundle.open(member) as reader, target.open("xb") as writer:
                    shutil.copyfileobj(reader, writer)


def python_for(version_dir):
    return (
        version_dir
        / ".venv"
        / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )


def health_check(version_dir, version):
    python = python_for(version_dir)
    result = subprocess.run(
        [str(python), "-I", "-m", "u19_recording_transfer", "--version"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    if result.stdout.strip() != version:
        raise ValueError(
            "Installed package version disagrees with the release manifest."
        )
    subprocess.run(
        [str(python), "-I", "-m", "u19_recording_transfer", "--self-test"],
        check=True,
        timeout=60,
    )


def install_release(root, config, source, allow_downgrade=False):
    manifest, archive_source = (
        manifest_for(source) if source else github_release_manifest(config)
    )
    version = manifest["version"]
    active_path = root / "active.json"
    active = read_json(active_path) if active_path.exists() else {}
    if active.get("current") == version:
        LOGGER.info("Already running approved release %s", version)
        return version
    if active and not allow_downgrade:

        def numbers(text):
            return tuple(map(int, text.split(".")))

        if numbers(version) < numbers(active["current"]):
            raise ValueError(
                "Channel requests a downgrade; use rollback or an explicit --allow-downgrade."
            )
    target = release_path(root, version)
    installed = target / "installed.json"
    if target.exists():
        if installed.exists():
            if read_json(installed).get("sha256") != manifest["sha256"]:
                raise ValueError(
                    "Installed version already exists with a different checksum. Publish a new version."
                )
            health_check(target, version)
        else:
            # Preserve a failed attempt, while allowing retries after network errors.
            failed = root / "failed" / (version + "-" + uuid.uuid4().hex)
            failed.parent.mkdir(exist_ok=True)
            if not target.resolve().is_relative_to(
                root.resolve()
            ) or not failed.resolve().is_relative_to(root.resolve()):
                raise ValueError(
                    "Failed-install preservation escaped the installation root."
                )
            target.rename(failed)
            LOGGER.info("Preserved incomplete install at %s", failed)
    if not installed.exists():
        payload = source_bytes(archive_source, MAX_ARCHIVE_BYTES)
        if hashlib.sha256(payload).hexdigest() != manifest["sha256"]:
            raise ValueError(
                "Release archive checksum mismatch; current installation unchanged."
            )
        target.mkdir(parents=True)
        # Failed installs remain inactive for inspection; never delete version trees.
        with tempfile.TemporaryFile() as handle:
            handle.write(payload)
            handle.seek(0)
            extract_archive(handle, target)
        import tomllib

        metadata = tomllib.loads(
            (target / "pyproject.toml").read_text(encoding="utf-8")
        )
        if (
            metadata["project"]["name"] != "u19-recording-transfer"
            or metadata["project"]["version"] != version
        ):
            raise ValueError(
                "Archive project identity/version disagrees with release manifest."
            )
        if not (target / "uv.lock").is_file():
            raise ValueError("Release is missing its dependency lockfile.")
        env = os.environ.copy()
        for key in (
            "VIRTUAL_ENV",
            "PYTHONPATH",
            "PYTHONHOME",
            "UV_PROJECT_ENVIRONMENT",
            "UV_WORKING_DIRECTORY",
            "UV_PROJECT",
        ):
            env.pop(key, None)
        env["UV_PROJECT_ENVIRONMENT"] = str(target / ".venv")
        logs = root / "logs"
        logs.mkdir(exist_ok=True)
        with (logs / f"install-{version}.log").open(
            "a", encoding="utf-8"
        ) as install_log:
            subprocess.run(
                [
                    config["uv_executable"],
                    "sync",
                    "--project",
                    str(target),
                    "--frozen",
                    "--no-dev",
                    "--no-editable",
                    "--python",
                    config.get("python", "3.12"),
                ],
                check=True,
                env=env,
                timeout=900,
                stdout=install_log,
                stderr=subprocess.STDOUT,
            )
        health_check(target, version)
        atomic_json(
            installed,
            dict(
                version=version,
                sha256=manifest["sha256"],
                state_schema=STATE_SCHEMA,
                installed_at=datetime.now(UTC).isoformat(),
            ),
        )
    # Rollback only changes this pointer; no shared-state migration takes place.
    atomic_json(active_path, dict(current=version, previous=active.get("current")))
    LOGGER.info("Activated %s (previous %s)", version, active.get("current"))
    return version


def rollback(root):
    active = read_json(root / "active.json")
    previous = active.get("previous")
    if not previous:
        raise ValueError("No previous release is available.")
    target = release_path(root, previous)
    if read_json(target / "installed.json").get("state_schema") != STATE_SCHEMA:
        raise ValueError("Previous version uses an incompatible state schema.")
    health_check(target, previous)
    atomic_json(
        root / "active.json", dict(current=previous, previous=active["current"])
    )
    LOGGER.info("Rolled back to %s", previous)
    return previous


def launch(root, config, mode, demo=False, output=None):
    active = read_json(root / "active.json")
    version = active["current"]
    target = release_path(root, version)
    if not (target / "installed.json").exists():
        raise ValueError(
            "Active release does not have a successful installation marker."
        )
    base = [str(python_for(target)), "-I", "-m", "u19_recording_transfer"]

    def execute(command):
        return subprocess.call(
            command, stdout=output, stderr=subprocess.STDOUT if output else None
        )

    state = Path(config["state_dir"])
    state.mkdir(parents=True, exist_ok=True)
    if demo:
        return execute([*base, "--demo"])
    base += ["--state-dir", str(state), "--machine-config", str(root / "machine.json")]
    if mode == "run":
        return execute(base)
    if mode == "copy":
        return execute([*base, "--run-pending"])
    cleanup = config.get("cleanup", {})
    errors = 0
    for source_root in config["source_roots"]:
        if not Path(source_root).is_dir():
            LOGGER.info("Cleanup skipped absent source root %s", source_root)
            continue
        command = [
            *base,
            "--cleanup",
            "--source-root",
            source_root,
            "--destination-root",
            config["incoming_root"],
            "--retention-days",
            str(cleanup.get("retention_days", 14)),
        ]
        if mode == "cleanup" and cleanup.get("enabled") is True:
            command.append("--apply-cleanup")
        errors |= execute(command)
    return errors


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "command",
        choices=[
            "run",
            "copy",
            "cleanup",
            "cleanup-preview",
            "update",
            "rollback",
            "status",
        ],
    )
    parser.add_argument(
        "--manifest", help="Override configured release source for this update"
    )
    parser.add_argument("--allow-downgrade", action="store_true")
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    logs = root / "logs"
    logs.mkdir(exist_ok=True)
    from logging.handlers import RotatingFileHandler

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            RotatingFileHandler(
                logs / "launcher.log", maxBytes=2_000_000, backupCount=4
            ),
            logging.StreamHandler(),
        ],
    )
    try:
        with runtime_lock(root):
            config = config_for(root)
            if args.command == "status":
                print(
                    json.dumps(
                        read_json(root / "active.json")
                        if (root / "active.json").exists()
                        else {"current": None},
                        indent=2,
                    )
                )
                return 0
            if args.command == "update":
                install_release(root, config, args.manifest, args.allow_downgrade)
                return 0
            if args.command == "rollback":
                rollback(root)
                return 0
            if args.command == "run" and config.get("auto_update_on_launch") is True:
                try:
                    install_release(root, config, None)
                except Exception:
                    LOGGER.exception(
                        "Update unavailable; continuing with current installed release."
                    )
            # Child output is retained for scheduled/hidden launches as well.
            app_log = logs / "application.log"
            if app_log.exists() and app_log.stat().st_size > 5_000_000:
                app_log.replace(logs / "application.previous.log")
            with app_log.open("a", encoding="utf-8") as log:
                return launch(root, config, args.command, args.demo, log)
    except BusyError as exc:
        LOGGER.info("%s", exc)
        return 0
    except Exception as exc:
        LOGGER.exception("Launcher operation failed")
        (root / "last_error.txt").write_text(str(exc), encoding="utf-8")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
