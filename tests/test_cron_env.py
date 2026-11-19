"""The u19proc cron wrappers (``u19_pipeline/**/call_*.sh``) and ``u19_pipeline/cron_env.sh``.

Every wrapper sources the shared ``cron_env.sh``, which picks the Python
environment from ``U19_CRON_ENV``: the conda env ``U19-pipeline_python_env3``
by default, or the checkout's uv ``.venv`` with ``U19_CRON_ENV=uv`` (#206).

The behaviour tests run the real wrappers the way the crontab does
(``source <wrapper>`` from bash in POSIX mode, which is what ``/bin/sh`` is on
u19proc) and also as ``bash <wrapper>``, with stub ``conda``, ``uv``, ``git``
and ``python`` executables that only log their arguments. The u19prod home and
checkout are replaced by temporary directories through ``U19_CRON_HOME`` and
``U19_CRON_REPO``, so nothing under /home/u19prod is read or written.
"""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED_SCRIPT = REPO_ROOT / "u19_pipeline" / "cron_env.sh"

CONDA_ENV = "U19-pipeline_python_env3"
UV_SYNC = "uv sync --locked --extra pipeline --no-dev"

# What each wrapper did before #206, read off the wrappers on master (b55740bb):
# the arguments of its `git pull` (None: it does not pull) and the arguments of
# each `python` it runs, in order. Under the default (conda) the wrappers must
# still do exactly this.
WRAPPERS = {
    "u19_pipeline/automatic_job/call_cronjob_automatic_job.sh": (
        "--autostash",
        ["./u19_pipeline/automatic_job/cronjob_automatic_job.py"],
    ),
    "u19_pipeline/automatic_job/call_pupillometry_queue_jobs.sh": (
        "",
        ["./u19_pipeline/automatic_job/pupillometry_handler_script.py"],
    ),
    "u19_pipeline/automatic_job/call_pupillometry_check_jobs.sh": (
        "",
        ["./u19_pipeline/automatic_job/pupillometry_check_handler_script.py"],
    ),
    "u19_pipeline/automatic_job/call_populate_missing_syncbehavior.sh": (
        None,
        ["./u19_pipeline/automatic_job/populate_missing_syncbehavior_ephys.py"],
    ),
    "u19_pipeline/alert_system/call_cronjob_alert.sh": (
        "",
        ["./u19_pipeline/alert_system/cronjob_alert.py"],
    ),
    "u19_pipeline/alert_system/live_monitor_alert/call_cronjob_live_monitor_alert.sh": (
        None,
        [
            "./u19_pipeline/alert_system/live_monitor_alert/cronjob_live_monitor_alert.py"
        ],
    ),
    "u19_pipeline/alert_system/live_session_stats_deletion/call_live_session_stats_deletion.sh": (
        None,
        [
            "./u19_pipeline/alert_system/live_session_stats_deletion/cronjob_live_session_stats_deletion.py"
        ],
    ),
    "u19_pipeline/alert_system/locked_tables_alert/call_cronjob_locked_tables.sh": (
        None,
        [
            "./u19_pipeline/alert_system/locked_tables_alert/cronjob_locked_tables_alert.py"
        ],
    ),
    # "olg" (sic): the script is cronjob_old_log_deletion.py. Kept as it was so
    # this change does not start deleting logs; see the PR.
    "u19_pipeline/alert_system/log_deletion/call_cronjob_old_log_deletion.sh": (
        None,
        ["./u19_pipeline/alert_system/log_deletion/cronjob_olg_log_deletion.py"],
    ),
    "u19_pipeline/alert_system/rig_maintenance/call_cronjob_check_rig_maintenance.sh": (
        None,
        ["-m u19_pipeline.alert_system.rig_maintenance.check_rig_maintenance"],
    ),
    "u19_pipeline/alert_system/schedule_check_alert/call_cronjob_schedule_check.sh": (
        None,
        ["./u19_pipeline/alert_system/schedule_check_alert/cronjob_schedule_check.py"],
    ),
    "u19_pipeline/alert_system/tech_alert/call_cronjob_tech_alert.sh": (
        None,
        [
            "./u19_pipeline/alert_system/tech_alert/tech_alert.py",
            "./u19_pipeline/alert_system/locked_tables_alert/cronjob_locked_tables_alert.py",
            "./u19_pipeline/alert_system/schedule_check_alert/cronjob_schedule_check.py",
        ],
    ),
    "u19_pipeline/alert_system/water_weigh_alert/call_cronjob_water_weigh_alert.sh": (
        None,
        ["./u19_pipeline/alert_system/water_weigh_alert/cronjob_water_weigh_alert.py"],
    ),
}

# How the wrapper is started: as the crontab does it (cron's /bin/sh is bash in
# POSIX mode on u19proc and spock), and directly with bash. bash is looked up
# here because the wrappers run with a PATH that holds only the stubs.
BASH = shutil.which("bash") or "/bin/bash"
INVOCATIONS = {
    "crontab-source": lambda w: [BASH, "--posix", "-c", 'source "$1"', "sh", w],
    "bash": lambda w: [BASH, w],
}


def _all_wrappers():
    return sorted(
        str(p.relative_to(REPO_ROOT))
        for p in REPO_ROOT.glob("u19_pipeline/**/call_*.sh")
    )


# --------------------------------------------------------------------------
# Static checks
# --------------------------------------------------------------------------


def test_wrapper_list_is_complete():
    """A new call_*.sh must be added to WRAPPERS, so the behaviour tests cover it."""
    assert _all_wrappers() == sorted(WRAPPERS)


@pytest.mark.parametrize("wrapper", _all_wrappers())
def test_wrapper_sources_shared_script(wrapper):
    path = REPO_ROOT / wrapper
    text = path.read_text()
    match = re.search(
        r'^source "\$\(dirname "\$\{BASH_SOURCE\[0\]\}"\)/([./]*cron_env\.sh)"$',
        text,
        re.MULTILINE,
    )
    assert match, f"{wrapper} does not source cron_env.sh next to it"
    assert (path.parent / match.group(1)).resolve() == SHARED_SCRIPT


@pytest.mark.parametrize("wrapper", _all_wrappers())
def test_wrapper_does_not_pick_its_own_env(wrapper):
    """Only cron_env.sh activates an env or runs python; wrappers call u19_cron_python."""
    lines = [
        line.strip()
        for line in (REPO_ROOT / wrapper).read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    for line in lines:
        assert "conda" not in line, f"{wrapper}: {line}"
        assert not re.match(r"(source|\.)\s+\S*(\.bashrc|\.bash_profile)", line), line
        assert not re.match(r"cd\s", line), f"{wrapper}: {line}"
        assert not re.match(r"(\S*/)?python3?\b", line), f"{wrapper}: {line}"
        assert ".venv" not in line, f"{wrapper}: {line}"


def test_shared_script_is_the_only_conda_activate():
    hits = [
        str(p.relative_to(REPO_ROOT))
        for p in REPO_ROOT.glob("u19_pipeline/**/*.sh")
        if re.search(r"^\s*conda activate", p.read_text(), re.MULTILINE)
    ]
    assert hits == ["u19_pipeline/cron_env.sh"]


# --------------------------------------------------------------------------
# Behaviour, with stub executables
# --------------------------------------------------------------------------

STUB = """#!/bin/bash
echo "{name} $*  [cwd=$PWD]" >> "$U19_TEST_LOG"
{extra}
"""


class Sandbox:
    """A fake u19prod home, checkout and PATH; runs a wrapper and returns its log."""

    def __init__(self, tmp_path: Path):
        self.home = tmp_path / "home"
        self.repo = tmp_path / "repo"
        self.bin = tmp_path / "bin"
        self.log = tmp_path / "calls.log"
        for d in (self.home, self.repo, self.bin):
            d.mkdir()
        self.log.touch()
        for name in (".bashrc", ".bash_profile"):
            (self.home / name).write_text(
                f'echo "source $U19_CRON_HOME/{name}" >> "$U19_TEST_LOG"\n'
            )
        # Tools the wrappers need besides the stubs (and nothing else, so a
        # real uv, conda or git on the host can never be picked up).
        for tool in ("dirname",):
            os.symlink(shutil.which(tool), self.bin / tool)
        self.stub("conda")
        self.stub("git")
        self.stub("python")
        self.stub(
            "uv",
            extra='if [ -n "$U19_TEST_UV_FAIL" ]; then echo "uv: sync failed" >&2; exit 3; fi',
        )
        venv_bin = self.repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        self.stub(
            "venv-python",
            path=venv_bin / "python",
            extra='echo "  VIRTUAL_ENV=$VIRTUAL_ENV PATH0=${PATH%%:*}" >> "$U19_TEST_LOG"',
        )

    def stub(self, name, path=None, extra=""):
        path = path or self.bin / name
        path.write_text(STUB.format(name=name, extra=extra))
        path.chmod(0o755)

    def run(self, wrapper, invocation="crontab-source", **env):
        full_env = {
            "PATH": str(self.bin),
            "HOME": str(self.home),
            "U19_CRON_HOME": str(self.home),
            "U19_CRON_REPO": str(self.repo),
            "U19_TEST_LOG": str(self.log),
        }
        full_env.update(env)
        make_cmd = INVOCATIONS.get(invocation, invocation)
        proc = subprocess.run(
            make_cmd(str(REPO_ROOT / wrapper)),
            env=full_env,
            cwd=self.home,  # cron starts jobs in the user's home directory
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.proc = proc
        return proc, self.log.read_text().splitlines()

    # The log lines the stubs and the fake rc files write.
    def sourced(self):
        return [f"source {self.home}/.bashrc", f"source {self.home}/.bash_profile"]

    def cmd(self, name, args):
        return f"{name} {args}  [cwd={self.repo}]"

    def venv(self):
        return (
            f"  VIRTUAL_ENV={self.repo / '.venv'} PATH0={self.repo / '.venv' / 'bin'}"
        )


@pytest.fixture
def sandbox(tmp_path):
    return Sandbox(tmp_path)


def _pull_lines(sb, pull):
    if pull is None:
        return []
    return [sb.cmd("git", f"pull {pull}".rstrip())]


@pytest.mark.parametrize("invocation", sorted(INVOCATIONS))
@pytest.mark.parametrize("wrapper", sorted(WRAPPERS))
@pytest.mark.parametrize("value", [None, "", "conda"])
def test_default_is_conda_as_before(sandbox, wrapper, invocation, value):
    """Unset, empty or "conda": the same sourced files, env, cd, pull and python as before."""
    pull, scripts = WRAPPERS[wrapper]
    env = {} if value is None else {"U19_CRON_ENV": value}
    proc, log = sandbox.run(wrapper, invocation, **env)
    assert proc.returncode == 0, proc.stderr
    assert log == [
        *sandbox.sourced(),
        # Activated before the cd, from cron's working directory (HOME).
        f"conda activate {CONDA_ENV}  [cwd={sandbox.home}]",
        *_pull_lines(sandbox, pull),
        *[sandbox.cmd("python", s) for s in scripts],
    ]
    # The wrapper still echoes the directory cron started it in.
    assert proc.stdout.splitlines()[0] == str(sandbox.home)


def test_conda_mode_returns_the_python_exit_status(sandbox):
    sandbox.stub("python", extra="exit 7")
    proc, log = sandbox.run("u19_pipeline/alert_system/call_cronjob_alert.sh")
    assert proc.returncode == 7
    assert log[-1] == sandbox.cmd(
        "python", "./u19_pipeline/alert_system/cronjob_alert.py"
    )


def test_conda_mode_runs_every_tech_alert_script_even_if_one_fails(sandbox):
    """As before: the three tech alert scripts run one after the other regardless."""
    sandbox.stub("python", extra="exit 1")
    wrapper = "u19_pipeline/alert_system/tech_alert/call_cronjob_tech_alert.sh"
    proc, log = sandbox.run(wrapper)
    assert [line for line in log if line.startswith("python ")] == [
        sandbox.cmd("python", s) for s in WRAPPERS[wrapper][1]
    ]


@pytest.mark.parametrize("invocation", sorted(INVOCATIONS))
@pytest.mark.parametrize("wrapper", sorted(WRAPPERS))
def test_uv_mode_syncs_then_runs_the_venv_python(sandbox, wrapper, invocation):
    pull, scripts = WRAPPERS[wrapper]
    proc, log = sandbox.run(wrapper, invocation, U19_CRON_ENV="uv")
    assert proc.returncode == 0, proc.stderr
    expected = [*sandbox.sourced(), *_pull_lines(sandbox, pull)]
    expected.append(sandbox.cmd("uv", UV_SYNC.removeprefix("uv ")))
    for s in scripts:
        expected += [sandbox.cmd("venv-python", s), sandbox.venv()]
    assert log == expected
    assert not any(line.startswith(("conda ", "python ")) for line in log)


def test_uv_mode_can_be_switched_on_on_the_crontab_line(sandbox):
    """`U19_CRON_ENV=uv source <wrapper>`, as the doc tells you to write it."""
    wrapper = "u19_pipeline/automatic_job/call_cronjob_automatic_job.sh"
    proc, log = sandbox.run(
        wrapper,
        lambda w: [BASH, "--posix", "-c", f"U19_CRON_ENV=uv source {w}"],
    )
    assert proc.returncode == 0, proc.stderr
    assert log[2:] == [
        sandbox.cmd("git", "pull --autostash"),
        sandbox.cmd("uv", UV_SYNC.removeprefix("uv ")),
        sandbox.cmd("venv-python", WRAPPERS[wrapper][1][0]),
        sandbox.venv(),
    ]


def test_uv_mode_can_be_switched_on_in_the_profile(sandbox):
    """U19_CRON_ENV=uv in u19prod's .bash_profile works like on the crontab line."""
    with (sandbox.home / ".bash_profile").open("a") as f:
        f.write("export U19_CRON_ENV=uv\n")
    proc, log = sandbox.run("u19_pipeline/alert_system/call_cronjob_alert.sh")
    assert proc.returncode == 0, proc.stderr
    assert sandbox.cmd("uv", UV_SYNC.removeprefix("uv ")) in log
    assert not any(line.startswith("conda ") for line in log)


@pytest.mark.parametrize("invocation", sorted(INVOCATIONS))
def test_unknown_value_fails_before_anything_runs(sandbox, invocation):
    wrapper = "u19_pipeline/automatic_job/call_cronjob_automatic_job.sh"
    proc, log = sandbox.run(wrapper, invocation, U19_CRON_ENV="mamba")
    assert proc.returncode != 0
    assert "U19_CRON_ENV" in proc.stderr
    assert "mamba" in proc.stderr
    assert log == sandbox.sourced()


@pytest.mark.parametrize("invocation", sorted(INVOCATIONS))
def test_uv_sync_failure_stops_the_wrapper(sandbox, invocation):
    wrapper = "u19_pipeline/alert_system/tech_alert/call_cronjob_tech_alert.sh"
    proc, log = sandbox.run(
        wrapper, invocation, U19_CRON_ENV="uv", U19_TEST_UV_FAIL="1"
    )
    assert proc.returncode != 0
    assert "uv sync" in proc.stderr
    assert log[-1] == sandbox.cmd("uv", UV_SYNC.removeprefix("uv "))
    assert not any("python" in line for line in log)


def test_uv_mode_syncs_once_per_wrapper(sandbox):
    wrapper = "u19_pipeline/alert_system/tech_alert/call_cronjob_tech_alert.sh"
    proc, log = sandbox.run(wrapper, U19_CRON_ENV="uv")
    assert proc.returncode == 0, proc.stderr
    assert sum(line.startswith("uv ") for line in log) == 1


def test_uv_mode_returns_the_python_exit_status(sandbox):
    sandbox.stub(
        "venv-python", path=sandbox.repo / ".venv" / "bin" / "python", extra="exit 5"
    )
    proc, _ = sandbox.run(
        "u19_pipeline/alert_system/call_cronjob_alert.sh", U19_CRON_ENV="uv"
    )
    assert proc.returncode == 5


@pytest.mark.parametrize("invocation", sorted(INVOCATIONS))
def test_uv_missing_gives_a_clear_error(sandbox, invocation):
    (sandbox.bin / "uv").unlink()
    wrapper = "u19_pipeline/automatic_job/call_cronjob_automatic_job.sh"
    proc, log = sandbox.run(wrapper, invocation, U19_CRON_ENV="uv")
    assert proc.returncode != 0
    assert "uv" in proc.stderr
    assert "PATH" in proc.stderr
    assert log == sandbox.sourced()


def test_missing_venv_gives_a_clear_error(sandbox):
    """No one-time build yet: say how to make it, and don't pull or run anything."""
    shutil.rmtree(sandbox.repo / ".venv")
    wrapper = "u19_pipeline/automatic_job/call_cronjob_automatic_job.sh"
    proc, log = sandbox.run(wrapper, U19_CRON_ENV="uv")
    assert proc.returncode != 0
    assert UV_SYNC in proc.stderr
    assert log == sandbox.sourced()
