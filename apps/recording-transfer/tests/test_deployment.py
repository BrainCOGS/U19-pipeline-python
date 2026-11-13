"""Release activation, failure recovery, rollback and interprocess coordination."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

PROJECT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "u19_bootstrap", PROJECT / "deployment/bootstrap.py"
)
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


@pytest.fixture
def installation(tmp_path):
    root = tmp_path / "installed app"
    root.mkdir()
    config = dict(
        schema_version=1,
        source_roots=[str(tmp_path / "data")],
        incoming_root=str(tmp_path / "cup"),
        state_dir=str(root / "state"),
        uv_executable=str(Path(sys.executable).resolve()),
        python="3.14",
        cleanup=dict(enabled=False, retention_days=14),
    )
    bootstrap.atomic_json(root / "machine.json", config)
    return root, config


def release(tmp_path, version="0.1.0", extra=None):
    tmp_path.mkdir(parents=True, exist_ok=True)
    archive = tmp_path / f"{version}.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(
            "pyproject.toml",
            f'[project]\nname="u19-recording-transfer"\nversion="{version}"\n',
        )
        bundle.writestr("uv.lock", "version = 1\n")
        bundle.writestr(
            "src/u19_recording_transfer/__init__.py", f'__version__="{version}"'
        )
        if extra:
            bundle.writestr(extra, "unsafe")
    manifest = dict(
        version=version,
        sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
        archive=archive.name,
        bootstrap_api=1,
        state_schema=1,
    )
    path = tmp_path / f"release-{version}.json"
    bootstrap.atomic_json(path, manifest)
    return path


@pytest.fixture
def fake_install(monkeypatch):
    calls = []
    monkeypatch.setattr(bootstrap, "health_check", lambda *args: None)
    monkeypatch.setattr(
        bootstrap.subprocess, "run", lambda command, **kwargs: calls.append(command)
    )
    return calls


def test_install_update_rollback_preserves_state(installation, tmp_path, fake_install):
    root, config = installation
    queue = root / "state/queue.json"
    bootstrap.atomic_json(queue, [{"status": "Queued", "id": "keep-me"}])
    first = release(tmp_path / "release1")
    second = release(tmp_path / "release2", "0.2.0")
    bootstrap.install_release(root, config, str(first))
    bootstrap.install_release(root, config, str(second))
    assert bootstrap.read_json(root / "active.json") == dict(
        current="0.2.0", previous="0.1.0"
    )
    assert bootstrap.rollback(root) == "0.1.0"
    assert bootstrap.read_json(queue) == [{"status": "Queued", "id": "keep-me"}]
    assert bootstrap.read_json(root / "machine.json") == config
    assert "--frozen" in fake_install[0] and "--no-editable" in fake_install[0]


def test_checksum_failure_keeps_current_release(installation, tmp_path, fake_install):
    root, config = installation
    bootstrap.atomic_json(root / "active.json", dict(current="0.1.0", previous=None))
    path = release(tmp_path / "releases", "0.2.0")
    manifest = bootstrap.read_json(path)
    manifest["sha256"] = "0" * 64
    bootstrap.atomic_json(path, manifest)
    with pytest.raises(ValueError, match="checksum"):
        bootstrap.install_release(root, config, str(path))
    assert bootstrap.read_json(root / "active.json")["current"] == "0.1.0"
    assert not (root / "versions/0.2.0").exists()


def test_failed_install_can_retry_without_deleting_evidence(
    installation, tmp_path, monkeypatch, fake_install
):
    root, config = installation
    path = release(tmp_path / "releases")

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "uv sync")

    monkeypatch.setattr(bootstrap.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        bootstrap.install_release(root, config, str(path))
    assert not (root / "active.json").exists()
    monkeypatch.setattr(bootstrap.subprocess, "run", lambda *args, **kwargs: None)
    bootstrap.install_release(root, config, str(path))
    assert bootstrap.read_json(root / "active.json")["current"] == "0.1.0"
    assert len(list((root / "failed").iterdir())) == 1


@pytest.mark.parametrize(
    "member",
    [
        "../escape.py",
        "/absolute.py",
        "src/../../escape.py",
        "src/file:stream",
        "src/file. ",
    ],
)
def test_archive_paths_rejected_before_writing(tmp_path, member):
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as bundle:
        bundle.writestr("src/valid.py", "ok")
        bundle.writestr(member, "not ok")
    content.seek(0)
    target = tmp_path / "extract"
    with pytest.raises(ValueError, match="Unsafe"):
        bootstrap.extract_archive(content, target)
    assert not target.exists()


def test_schema_and_downgrade_guards(installation, tmp_path, fake_install):
    root, config = installation
    path = release(tmp_path / "releases")
    bootstrap.atomic_json(root / "active.json", dict(current="0.2.0", previous=None))
    with pytest.raises(ValueError, match="downgrade"):
        bootstrap.install_release(root, config, str(path))
    manifest = bootstrap.read_json(path)
    manifest["state_schema"] = 2
    bootstrap.atomic_json(path, manifest)
    with pytest.raises(ValueError, match="schema"):
        bootstrap.install_release(root, config, str(path))


def test_healthy_existing_version_reused(installation, tmp_path, fake_install):
    root, config = installation
    first = release(tmp_path / "release1")
    second = release(tmp_path / "release2", "0.2.0")
    bootstrap.install_release(root, config, str(first))
    bootstrap.install_release(root, config, str(second))
    bootstrap.rollback(root)
    bootstrap.install_release(root, config, str(second))
    assert (
        len(fake_install) == 2
    )  # no dependency reinstall for existing healthy release


def test_failed_health_check_never_activates_candidate(
    installation, tmp_path, fake_install, monkeypatch
):
    root, config = installation
    path = release(tmp_path / "candidate", "0.2.0")
    bootstrap.atomic_json(root / "active.json", dict(current="0.1.0", previous=None))

    def unhealthy(*args):
        raise RuntimeError("Qt health check failed")

    monkeypatch.setattr(bootstrap, "health_check", unhealthy)
    with pytest.raises(RuntimeError, match="health check failed"):
        bootstrap.install_release(root, config, str(path))
    assert bootstrap.read_json(root / "active.json")["current"] == "0.1.0"
    assert not (root / "versions/0.2.0/installed.json").exists()


def test_concurrent_updater_skips_active_application(installation):
    root, _ = installation
    with bootstrap.runtime_lock(root):
        result = subprocess.run(
            [
                sys.executable,
                str(PROJECT / "deployment/bootstrap.py"),
                "--root",
                str(root),
                "update",
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    assert result.returncode == 0
    assert "already active" in result.stderr
    assert not (root / "active.json").exists()


def test_offline_launch_uses_installed_version(installation, monkeypatch):
    root, config = installation
    config.update(auto_update_on_launch=True, github=dict(channel="stable"))
    bootstrap.atomic_json(root / "machine.json", config)
    monkeypatch.setattr(
        bootstrap,
        "install_release",
        lambda *a, **k: (_ for _ in ()).throw(OSError("offline")),
    )
    invoked = []
    monkeypatch.setattr(bootstrap, "launch", lambda *args: invoked.append(args) or 0)
    assert bootstrap.main(["--root", str(root), "run"]) == 0
    assert len(invoked) == 1


def test_cleanup_disabled_by_default_is_preview(installation, monkeypatch):
    root, config = installation
    Path(config["source_roots"][0]).mkdir()
    bootstrap.atomic_json(root / "active.json", dict(current="0.1.0", previous=None))
    target = root / "versions/0.1.0"
    target.mkdir(parents=True)
    bootstrap.atomic_json(target / "installed.json", dict(state_schema=1))
    commands = []
    monkeypatch.setattr(
        bootstrap.subprocess,
        "call",
        lambda command, **kw: commands.append(command) or 0,
    )
    bootstrap.launch(root, config, "cleanup")
    assert "--cleanup" in commands[0] and "--apply-cleanup" not in commands[0]
