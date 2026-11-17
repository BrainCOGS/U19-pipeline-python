"""Tests for updating the processing repository (BrainCogsEphysSorters) before queuing a job.

The checkout lives on the cluster and is pulled there (fast-forward only) before each
job is queued. If it can't be pulled (read-only mount, local commits, no network) the
job still runs, and the reason is reported so a Slack warning can be sent.
"""

import os
import shlex
import shutil
import stat
import subprocess

import pytest

from u19_pipeline.utils.slurm_utils import (
    repository_update_warning,
    update_repository,
    update_repository_command,
)

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git not found")


def git(cwd, *args):
    return subprocess.run(
        ["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def commit(repo, name):
    (repo / name).write_text(name)
    git(repo, "add", name)
    git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", name)
    return git(repo, "rev-parse", "--short", "HEAD")


@pytest.fixture
def upstream_and_checkout(tmp_path):
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    git(upstream, "init", "-q", "-b", "main")
    commit(upstream, "first")
    checkout = tmp_path / "checkout"
    subprocess.run(
        ["git", "clone", "-q", str(upstream), str(checkout)],
        check=True,
        capture_output=True,
    )
    return upstream, checkout


def test_fast_forwards_to_the_latest_commit(upstream_and_checkout):
    upstream, checkout = upstream_and_checkout
    latest = commit(upstream, "second")

    updated, message = update_repository(str(checkout), upstream.as_uri())

    assert updated
    assert message == latest
    assert git(checkout, "rev-parse", "--short", "HEAD") == latest


def test_up_to_date_checkout_is_updated(upstream_and_checkout):
    upstream, checkout = upstream_and_checkout

    updated, message = update_repository(str(checkout), upstream.as_uri())

    assert updated
    assert message == git(upstream, "rev-parse", "--short", "HEAD")


def test_submodule_moves_to_the_pinned_commit(
    upstream_and_checkout, tmp_path, monkeypatch
):
    upstream, checkout = upstream_and_checkout
    # Local file:// submodules are blocked by default in recent git
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "protocol.file.allow")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "always")
    sorter = tmp_path / "sorter"
    sorter.mkdir()
    git(sorter, "init", "-q", "-b", "main")
    commit(sorter, "v1")
    git(upstream, "submodule", "add", "-q", sorter.as_uri(), "sorters/sorter")
    git(upstream, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "add")
    assert update_repository(str(checkout), upstream.as_uri())[0]

    v2 = commit(sorter, "v2")
    git(upstream / "sorters" / "sorter", "pull", "-q", "origin", "main")
    git(upstream, "add", "sorters/sorter")
    git(upstream, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "bump")

    updated, message = update_repository(str(checkout), upstream.as_uri())

    assert updated, message
    assert git(checkout / "sorters" / "sorter", "rev-parse", "--short", "HEAD") == v2


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
def test_read_only_checkout_is_reported(upstream_and_checkout):
    upstream, checkout = upstream_and_checkout
    commit(upstream, "second")
    git_dir = checkout / ".git"
    modes = {p: p.stat().st_mode for p in [git_dir, *git_dir.rglob("*")] if p.is_dir()}
    for p in modes:
        p.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        updated, message = update_repository(str(checkout), upstream.as_uri())
    finally:
        for p, mode in modes.items():
            p.chmod(mode)

    assert not updated
    assert message


def test_diverged_checkout_is_not_merged(upstream_and_checkout):
    upstream, checkout = upstream_and_checkout
    commit(upstream, "second")
    local = commit(checkout, "local_change")

    updated, message = update_repository(str(checkout), upstream.as_uri())

    assert not updated
    assert message
    assert git(checkout, "rev-parse", "--short", "HEAD") == local


def test_missing_checkout_is_reported(tmp_path):
    updated, message = update_repository(str(tmp_path / "nope"), "https://x/y.git")

    assert not updated
    assert message


def test_command_through_ssh_quotes_the_paths():
    command = update_repository_command(
        "/mnt/dir with space/Repo", "https://github.com/o/r.git", "u19prod@host"
    )

    assert command[:4] == ["ssh", "-o", "BatchMode=yes", "u19prod@host"]
    assert shlex.split(command[4]) == [
        "git", "-C", "/mnt/dir with space/Repo", "pull", "--ff-only", "--quiet",
        "https://github.com/o/r.git", "HEAD",
        "&&", "git", "-C", "/mnt/dir with space/Repo", "submodule", "update", "--init",
        "--recursive", "--quiet",
        "&&", "git", "-C", "/mnt/dir with space/Repo", "rev-parse", "--short", "HEAD",
    ]  # fmt: skip


def test_warning_names_repository_host_and_job():
    warning = repository_update_warning(
        "BrainCogsEphysSorters",
        "/repo/dir",
        "u19prod@spock",
        42,
        "Read-only file system",
    )

    text = warning["text"]
    assert "BrainCogsEphysSorters" in text
    assert "/repo/dir" in text
    assert "u19prod@spock" in text
    assert "job 42" in text
    assert "Read-only file system" in text
