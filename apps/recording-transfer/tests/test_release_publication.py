"""Exercise the actual publishing shell script using a fake gh CLI."""

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import tomllib

import pytest

PROJECT = Path(__file__).resolve().parents[1]
PUBLISHER = PROJECT.parents[1] / ".github/scripts/publish-recording-transfer.sh"


@pytest.mark.parametrize(
    "state,channel,success",
    [
        ("missing", "pilot", True),
        ("matching", "stable", True),
        ("mismatch", "pilot", False),
    ],
)
def test_publication_verifies_tag_before_upload(tmp_path, state, channel, success):
    bash = shutil.which("bash")
    if not bash and os.name == "nt":
        candidate = Path("C:/Program Files/Git/bin/bash.exe")
        bash = str(candidate) if candidate.exists() else None
    if not bash:
        pytest.skip("Bash is required to test the GitHub Actions publication script")
    binaries = tmp_path / "bin"
    binaries.mkdir()
    fake_gh = binaries / "gh"
    fake_gh.write_text(
        """#!/usr/bin/env bash
set -eu
printf '%s\\n' "$*" >> "$FAKE_GH_LOG"
if [[ "$1" == api && "$2" == */commits/* ]]; then
  case "$FAKE_TAG_STATE" in
    missing) exit 1 ;;
    matching) printf '%s\\n' "$RELEASE_COMMIT" ;;
    mismatch) printf '%040d\\n' 1 ;;
  esac
fi
""",
        encoding="utf-8",
        newline="\n",
    )
    fake_gh.chmod(0o755)
    artifacts = tmp_path / "assets"
    artifacts.mkdir()
    (artifacts / "candidate.zip").write_bytes(b"fixture only")
    log = tmp_path / "calls.log"
    environment = dict(
        os.environ,
        PATH=str(binaries) + os.pathsep + os.environ["PATH"],
        GH_REPO="BrainCOGS/U19-pipeline-python",
        RELEASE_TAG="recording-transfer/v0.1.0",
        RELEASE_COMMIT="a" * 40,
        RELEASE_CHANNEL=channel,
        ARTIFACT_DIR=artifacts.as_posix(),
        FAKE_TAG_STATE=state,
        FAKE_GH_LOG=log.as_posix(),
    )
    # Never expose real credentials to the fake CLI or accidentally use them.
    environment.pop("GH_TOKEN", None)
    environment.pop("GITHUB_TOKEN", None)
    result = subprocess.run(
        [bash, str(PUBLISHER)],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert (result.returncode == 0) == success, result.stderr
    calls = log.read_text()
    if success:
        assert "release create" in calls and "--verify-tag" in calls
        assert "--draft" in calls and "--latest=false" in calls
        assert ("--prerelease" in calls) == (channel == "pilot")
        assert "release edit" in calls
        assert ("git/refs" in calls) == (state == "missing")
    else:
        assert "release create" not in calls
        assert "another commit" in result.stderr


def test_release_version_identity():
    spec = importlib.util.spec_from_file_location(
        "release_spec", PROJECT / "tools/release_spec.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    current = tomllib.loads((PROJECT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]["version"]
    assert module.release_tag(PROJECT, current) == "recording-transfer/v" + current
    for version in ("../../bad", "99999.0.0", "0.1.0; echo bad"):
        with pytest.raises(ValueError):
            module.release_tag(PROJECT, version)
