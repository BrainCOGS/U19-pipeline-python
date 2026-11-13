"""Exercise real install/update/rollback in a retained temp directory.

No scheduled tasks, desktop shortcuts, production paths or database access.
"""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib

from build_release import build


def smoke(project, uv, python):
    root = Path(tempfile.mkdtemp(prefix="u19-deployment-smoke-"))
    install = root / "installation"
    install.mkdir()
    config = dict(
        schema_version=1,
        source_roots=[str(root / "local-data")],
        incoming_root=str(root / "incoming"),
        state_dir=str(install / "state"),
        uv_executable=str(Path(uv).resolve()),
        python=python,
        auto_update_on_launch=False,
        cleanup=dict(enabled=False, retention_days=14),
    )
    (install / "machine.json").write_text(json.dumps(config), encoding="utf-8")
    state = install / "state"
    state.mkdir()
    (state / "queue.json").write_text("[]", encoding="utf-8")
    original_config = (install / "machine.json").read_bytes()
    original_queue = (state / "queue.json").read_bytes()
    bootstrap = project / "deployment/bootstrap.py"

    def github_install(version):
        # The release is not published during tests. Simulate only GitHub's API
        # and asset transport; discovery, checksum, install and health checks run.
        spec = importlib.util.spec_from_file_location("smoke_bootstrap", bootstrap)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        original_fetch = module.source_bytes
        tag = "recording-transfer/v" + version
        base = (
            f"https://github.com/BrainCOGS/U19-pipeline-python/releases/download/{tag}/"
        )
        names = [f"release-{version}.json", f"u19-recording-transfer-{version}.zip"]
        release = dict(
            tag_name=tag,
            draft=False,
            prerelease=False,
            assets=[
                dict(name=name, state="uploaded", browser_download_url=base + name)
                for name in names
            ],
        )

        def fixture_fetch(source, limit, headers=None):
            if source.startswith("https://api.github.com/"):
                return json.dumps([release]).encode()
            if source.startswith(base):
                return original_fetch(str(feed / source.removeprefix(base)), limit)
            return original_fetch(source, limit, headers)

        module.source_bytes = fixture_fetch
        with module.runtime_lock(install):
            module.install_release(install, config, None)

    def command(*args, expected=0):
        result = subprocess.run(
            [sys.executable, str(bootstrap), "--root", str(install), *args],
            timeout=900,
            check=False,
        )
        if result.returncode != expected:
            raise RuntimeError(
                f"{args} returned {result.returncode}, expected {expected}"
            )

    current = tomllib.loads((project / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]["version"]
    numbers = list(map(int, current.split(".")))
    numbers[-1] += 1
    next_version = ".".join(map(str, numbers))
    feed = root / "feed"
    build(project, feed)
    github_install(current)
    candidate = root / "candidate-source"
    candidate.mkdir()
    for name in ("pyproject.toml", "uv.lock", "README.md", "DEPLOYMENT.md"):
        shutil.copyfile(project / name, candidate / name)
    shutil.copytree(
        project / "deployment",
        candidate / "deployment",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    shutil.copytree(
        project / "src",
        candidate / "src",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    for path in (
        candidate / "pyproject.toml",
        candidate / "src/u19_recording_transfer/__init__.py",
    ):
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                f'"{current}"', f'"{next_version}"'
            ),
            encoding="utf-8",
        )
    subprocess.run([uv, "lock", "--project", str(candidate)], check=True, timeout=120)
    build(candidate, feed)
    github_install(next_version)
    assert json.loads((install / "active.json").read_text()) == dict(
        current=next_version, previous=current
    )
    command("rollback")
    assert json.loads((install / "active.json").read_text())["current"] == current
    bad = json.loads((feed / f"release-{next_version}.json").read_text())
    bad["sha256"] = "0" * 64
    (feed / "corrupt.json").write_text(json.dumps(bad))
    command("update", "--manifest", str(feed / "corrupt.json"), expected=1)
    assert json.loads((install / "active.json").read_text())["current"] == current
    assert (install / "machine.json").read_bytes() == original_config
    assert (state / "queue.json").read_bytes() == original_queue
    interpreter = (
        install
        / "versions"
        / current
        / ".venv"
        / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    subprocess.run(
        [
            str(interpreter),
            "-I",
            "-m",
            "u19_recording_transfer",
            "--demo",
            "--screenshot-dir",
            str(root / "screenshots"),
        ],
        check=True,
        timeout=120,
    )
    print(
        f"PASS: installed {current}, updated to {next_version}, rolled back, rejected changed release, preserved config/queue, copied demo data."
    )
    print(f"Retained test installation and screenshots: {root}")
    return root


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--uv", default=shutil.which("uv"))
    parser.add_argument("--python", default="3.12")
    args = parser.parse_args()
    if not args.uv:
        parser.error("uv executable not found")
    smoke(args.project.resolve(), args.uv, args.python)
