# u19proc cron jobs from uv

The cron wrappers on u19proc (every `u19_pipeline/**/call_*.sh`) can run their scripts from
the checkout's uv environment (`.venv`, built from `uv.lock`) instead of the conda env
`U19-pipeline_python_env3`. The default is still conda: until someone sets the switch below on
u19proc, the wrappers do exactly what they did before.

**Status: blocked on #54.** The u19proc VM is end of life and IT is procuring a new one. Turn
this on only on the new VM, after the checks under [Before you turn it on](#before-you-turn-it-on).

## The switch: `U19_CRON_ENV`

| `U19_CRON_ENV` | what a wrapper does |
|---|---|
| unset, empty or `conda` (default) | `source ~/.bashrc`, `source ~/.bash_profile`, `conda activate U19-pipeline_python_env3`, `cd` into the checkout, its own `git pull` (if it pulls), `python <script>`: the same commands as before. |
| `uv` | `source ~/.bashrc`, `source ~/.bash_profile`, check that `uv` is on PATH and `.venv/bin/python` exists, `cd` into the checkout, its own `git pull` (if it pulls), then `uv sync --locked --extra pipeline --no-dev` and `.venv/bin/python <script>`, with `.venv/bin` first on PATH. |
| anything else | prints an error naming the value and exits 2 before it pulls or runs anything. |

The logic is in `u19_pipeline/cron_env.sh`, which every wrapper sources; a wrapper only holds
its `git pull` (if any) and its `u19_cron_python <script>` lines. `tests/test_cron_env.py`
fails if a `call_*.sh` activates an env, runs `python` itself, or doesn't source `cron_env.sh`.

In `uv` mode:

- The sync runs after the wrapper's `git pull`, so `.venv` matches the `uv.lock` that was just
  pulled. It runs once per wrapper (`call_cronjob_tech_alert.sh` runs three scripts on one sync);
  when nothing changed it only checks `.venv` against the lock and installs nothing. u19proc has internet, so packages are
  downloaded right there.
- If the sync fails (for example `uv.lock` doesn't match `pyproject.toml`, or a download fails),
  uv's error and a `cron_env.sh: ... failed` line go to the cron log, the wrapper exits 1 and no
  script runs.
- `uv` not on PATH: exit 127 with a message. A missing `.venv`: exit 1 with the command that
  builds it. In both cases nothing is pulled or run.
- All wrappers use the same `.venv` and the same sync command (`--extra pipeline`: the job
  handler, the pupillometry handlers and `populate_missing_syncbehavior_ephys.py` import the
  element packages, paramiko and scp; the alerts need only the base dependencies). A sync with
  other extras would remove packages from the shared `.venv`, so don't run a plain `uv sync` or
  `uv run` in the production checkout; use `uv run --no-sync ...` or the same flags.
- Several wrappers can start in the same minute. uv takes an exclusive lock on the project and
  on `.venv/.lock` while it syncs, so a second sync waits for the first (seen with uv 0.13.0 on
  spock: `Waiting to acquire exclusive lock for .venv at .venv/.lock`). uv does not wait for
  scripts that are already running from `.venv`: if a pull brings a new `uv.lock`, a sync can
  replace packages under a script that is still running. That window is a pull that changes
  `uv.lock` while another cron script runs; the next run of that script uses the new env.
- `git pull --autostash` in the job handler is kept: the handler rewrites tracked files such as
  `SlurmFiles/slurm_real.slurm`. `.venv` is in `.gitignore`, so the sync doesn't touch what git
  sees.

## Before you turn it on

As `u19prod` on the (new) u19proc VM, in the checkout the crontab uses
(`/home/u19prod@pu.win.princeton.edu/Datajoint_projs/U19-pipeline_python`):

1. **glibc.** `ldd --version`. The locked wheels for Python 3.13 on x86_64 need glibc 2.28 or
   newer (RHEL / Rocky 8 or later): torch, torchvision, h5py and imagecodecs only have
   `manylinux_2_28` wheels, and numpy, scipy and pillow `manylinux_2_27`. On an older glibc uv
   falls back to building from source, which is what failed in #54.
2. **uv on PATH for cron.** `command -v uv` in a non-interactive shell that sources
   `~/.bashrc` and `~/.bash_profile` (cron's environment), for example
   `env -i HOME=$HOME bash -c 'source ~/.bashrc; source ~/.bash_profile; command -v uv'`. If it
   prints nothing, add uv's directory (usually `~/.local/bin`) to PATH in `~/.bash_profile`.
3. **One-time build of `.venv`** (Python 3.13 from `.python-version`; uv downloads a managed
   Python if there is none; the `pipeline` extra includes the CUDA build of torch, several GB):

   ```bash
   cd /home/u19prod@pu.win.princeton.edu/Datajoint_projs/U19-pipeline_python
   git pull --autostash
   uv sync --locked --extra pipeline --no-dev
   .venv/bin/python -c "import datajoint, element_array_ephys, element_calcium_imaging, element_interface, paramiko, scp; print('ok')"
   ```

   The cron wrappers refuse to build `.venv` themselves, so a missing one shows up in the
   cron log instead of a multi-GB download inside a cron job.

## Turning it on and off

Per wrapper, on its crontab line (start with one alert, then the job handler). Put the
assignment right before `source`; under `flock -c` it goes inside the quotes:

```text
0 3 * * * U19_CRON_ENV=uv source /home/u19prod\@pu.win.princeton.edu/Datajoint_projs/U19-pipeline_python/u19_pipeline/alert_system/call_cronjob_alert.sh  > /home/u19prod\@pu.win.princeton.edu/log/cronlog_alert 2>&1
*/30 * * * * flock -n -o /home/u19prod\@pu.win.princeton.edu/flock_dir/backup.lock -c 'U19_CRON_ENV=uv source /home/u19prod\@pu.win.princeton.edu/Datajoint_projs/U19-pipeline_python/u19_pipeline/automatic_job/call_cronjob_automatic_job.sh > /home/u19prod\@pu.win.princeton.edu/log/cronlog_automatic_job 2>&1'
```

For every wrapper at once: `export U19_CRON_ENV=uv` in u19prod's `~/.bash_profile` (only
`cron_env.sh` reads it, after sourcing the profile), or a line `U19_CRON_ENV=uv` at the top of
the crontab.

To go back to conda, delete that prefix or line (or set `U19_CRON_ENV=conda`). The conda env
and `.venv` can both stay in place, so switching back and forth needs nothing else. The cron
log of a wrapper that ran from uv has a `cron_env.sh: uv sync ...` line.

The pupillometry jobs that these wrappers queue on spock still load `anacondapy`, and the spock
ephys and imaging jobs have their own switches (`spock_uv_env.md`); this switch only changes the
processes on u19proc.
