# shellcheck shell=bash
#
# Picks the Python environment for the u19proc cron wrappers (call_*.sh).
#
# Every wrapper sources this file, then runs its own `git pull` (if it pulls)
# and its scripts through u19_cron_python:
#
#     source "$(dirname "${BASH_SOURCE[0]}")/../cron_env.sh"
#     git pull
#     u19_cron_python ./u19_pipeline/alert_system/cronjob_alert.py
#
# U19_CRON_ENV picks the environment. Set it on the crontab line or in
# u19prod's ~/.bash_profile (read below, before the value is checked):
#
#   unset, empty or "conda"  (default) the conda env U19-pipeline_python_env3,
#                            exactly as the wrappers did before #206.
#   "uv"                     the checkout's uv .venv: `uv sync --locked` after
#                            the wrapper's pull, then .venv/bin/python.
#
# Anything else stops the wrapper before it pulls or runs anything.
# See u19_pipeline/automatic_job/cron_uv_env.md.
#
# The crontab runs the wrappers with `source` from cron's /bin/sh (bash in
# POSIX mode), so this file runs in that shell: `exit` here ends the cron job.
# The setup below runs at the top level, not in a function, so that the rc
# files and `conda activate` run in the same scope as before.
#
# U19_CRON_HOME and U19_CRON_REPO replace the u19prod home and the checkout;
# they exist for the tests (tests/test_cron_env.py) and default to the
# production paths.

_u19_cron_home="${U19_CRON_HOME:-/home/u19prod@pu.win.princeton.edu}"
_u19_cron_repo="${U19_CRON_REPO:-$_u19_cron_home/Datajoint_projs/U19-pipeline_python/}"

# Keep this the one sync command: all wrappers share one .venv, and a sync
# with other extras would remove packages the next wrapper needs.
U19_CRON_UV_SYNC="uv sync --locked --extra pipeline --no-dev"

source "$_u19_cron_home/.bashrc"
source "$_u19_cron_home/.bash_profile"

case "${U19_CRON_ENV:-conda}" in
conda)
    _u19_cron_mode=conda
    conda activate U19-pipeline_python_env3
    ;;
uv)
    _u19_cron_mode=uv
    _u19_cron_venv="${_u19_cron_repo%/}/.venv"
    if ! command -v uv >/dev/null 2>&1; then
        echo "cron_env.sh: U19_CRON_ENV=uv but uv is not on PATH ($PATH)." \
            "Install uv for this user or add it to PATH in ~/.bash_profile," \
            "or unset U19_CRON_ENV to run from conda." >&2
        exit 127
    fi
    if [ ! -x "$_u19_cron_venv/bin/python" ]; then
        echo "cron_env.sh: U19_CRON_ENV=uv but $_u19_cron_venv/bin/python does not exist." \
            "Build it once: cd \"$_u19_cron_repo\" && $U19_CRON_UV_SYNC" \
            "(or unset U19_CRON_ENV to run from conda)." >&2
        exit 1
    fi
    ;;
*)
    echo "cron_env.sh: unknown U19_CRON_ENV='$U19_CRON_ENV';" \
        "use 'conda' (the default, also when unset) or 'uv'. Nothing was run." >&2
    exit 2
    ;;
esac

# No `|| exit`, as before #206. (In uv mode a missing checkout has already
# failed the .venv check above.)
# shellcheck disable=SC2164
cd "$_u19_cron_repo"

_u19_cron_synced=

# Syncs the .venv to uv.lock (once per wrapper, after its pull) and puts it
# first on PATH, so scripts that start `python` themselves get the same env.
# A failed sync ends the wrapper: no script runs on a half-updated .venv.
# uv locks the project and .venv/.lock, so wrappers that start in the same
# minute wait for each other instead of syncing at the same time.
_u19_cron_uv_sync() {
    [ -n "$_u19_cron_synced" ] && return 0
    echo "cron_env.sh: $U19_CRON_UV_SYNC (in $PWD)"
    if ! $U19_CRON_UV_SYNC; then
        echo "cron_env.sh: '$U19_CRON_UV_SYNC' failed in $PWD; not running any script." \
            "Set U19_CRON_ENV=conda (or unset it) to go back to conda." >&2
        exit 1
    fi
    _u19_cron_synced=1
    export VIRTUAL_ENV="$_u19_cron_venv"
    export PATH="$VIRTUAL_ENV/bin:$PATH"
}

# Runs `python "$@"` in the chosen environment and returns its exit status.
u19_cron_python() {
    if [ "$_u19_cron_mode" = uv ]; then
        _u19_cron_uv_sync
        "$VIRTUAL_ENV/bin/python" "$@"
    else
        python "$@"
    fi
}
