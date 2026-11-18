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

from u19_pipeline.utils import slurm_utils
from u19_pipeline.utils.slurm_utils import (
    checkout_behind,
    jobs_using_checkout,
    parse_running_jobs,
    repository_skip_note,
    repository_update_warning,
    running_jobs,
    running_jobs_command,
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


# --- jobs running from the checkout (#202) ---------------------------------------------

REPO = "/mnt/repos/U19-pipeline-python"
OTHER = "/mnt/repos/BrainCogsEphysSorters"


def test_running_jobs_are_listed_with_squeue_over_ssh():
    command = running_jobs_command("u19prod", "u19prod@host")

    assert command[:4] == ["ssh", "-o", "BatchMode=yes", "u19prod@host"]
    assert shlex.split(command[4]) == [
        "squeue", "-h", "-u", "u19prod", "-t", "RUNNING", "-o", "%j|%k",
    ]  # fmt: skip


def test_running_jobs_are_listed_locally_without_ssh_target():
    assert running_jobs_command("u19prod") == [
        "squeue", "-h", "-u", "u19prod", "-t", "RUNNING", "-o", "%j|%k",
    ]  # fmt: skip


def test_squeue_lines_are_name_and_comment():
    stdout = f"job_id_1477|{REPO}\n pupil_3|(null)\n\njob_id_8|\n"

    assert parse_running_jobs(stdout) == [
        ("job_id_1477", REPO),
        ("pupil_3", "(null)"),
        ("job_id_8", ""),
    ]


@pytest.mark.parametrize(
    "jobs, using",
    [
        ([], []),
        ([("job_id_1", REPO)], ["job_id_1"]),
        ([("job_id_1", OTHER)], []),
        # queued before sbatch tagged jobs with their checkout: could be using it
        ([("job_id_2", "(null)"), ("job_id_3", "")], ["job_id_2", "job_id_3"]),
        # other u19prod jobs (pupillometry, interactive) don't run from the checkout
        ([("pupil_9", REPO), ("bash", "(null)")], []),
        ([("job_id_1", OTHER), ("job_id_4", REPO)], ["job_id_4"]),
    ],
)
def test_jobs_using_the_checkout(jobs, using):
    assert jobs_using_checkout(jobs, REPO) == using


def fake_run(monkeypatch, result):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(slurm_utils.subprocess, "run", run)
    return calls


def test_running_jobs_parses_squeue(monkeypatch):
    calls = fake_run(
        monkeypatch, subprocess.CompletedProcess([], 0, f"job_id_5|{REPO}\n", "")
    )

    assert running_jobs("u19prod", "u19prod@host") == (True, [("job_id_5", REPO)])
    assert calls == [running_jobs_command("u19prod", "u19prod@host")]


@pytest.mark.parametrize(
    "result",
    [
        subprocess.CompletedProcess([], 1, "", "slurm_load_jobs error: timeout"),
        subprocess.CompletedProcess([], 255, "", ""),
        subprocess.TimeoutExpired("ssh", 60),
        OSError("ssh not found"),
    ],
)
def test_running_jobs_failure_is_reported(monkeypatch, result):
    fake_run(monkeypatch, result)

    ok, message = running_jobs("u19prod", "u19prod@host")

    assert not ok
    assert message


def test_checkout_at_the_latest_commit_is_not_behind(upstream_and_checkout):
    upstream, checkout = upstream_and_checkout

    behind, message = checkout_behind(str(checkout), upstream.as_uri())

    assert not behind
    assert message == git(checkout, "rev-parse", "--short", "HEAD")


def test_checkout_without_the_latest_commit_is_behind(upstream_and_checkout):
    upstream, checkout = upstream_and_checkout
    before = git(checkout, "rev-parse", "--short", "HEAD")
    latest = commit(upstream, "second")

    behind, message = checkout_behind(str(checkout), upstream.as_uri())

    assert behind
    assert before in message and latest in message
    # only looked: nothing was pulled
    assert git(checkout, "rev-parse", "--short", "HEAD") == before


def test_checkout_that_cannot_be_compared_counts_as_behind(tmp_path):
    behind, message = checkout_behind(str(tmp_path / "nope"), "https://x/y.git")

    assert behind
    assert message


def test_skip_note_names_repository_host_age_and_jobs():
    note = repository_skip_note(
        "U19-pipeline-python", REPO, "u19prod@spock", 26.4, ["job_id_1", "job_id_2"]
    )

    text = note["text"]
    assert "U19-pipeline-python" in text
    assert REPO in text
    assert "u19prod@spock" in text
    assert "26 h" in text
    assert "job_id_1, job_id_2" in text
