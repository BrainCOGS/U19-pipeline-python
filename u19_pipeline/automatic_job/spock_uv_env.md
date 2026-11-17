# Spock ephys jobs from uv

Ephys jobs on spock can run from the uv environment of the sorters checkout
(`<electrophysiology_process_dir>/BrainCogsEphysSorters/.venv`, built from its `uv.lock`)
instead of the conda environment `u19_pipeline_python_env3`. Two keys under `custom` in the
pipeline's `dj_local_conf.json` control it:

```json
"custom": {
    "spock_ephys_env": "uv",
    "spock_uv_cache_dir": "/mnt/bucket/scratch/u19prod/uv_cache"
}
```

- `spock_ephys_env`: `"conda"` (the default, also when the key is missing) or `"uv"`. With
  `"conda"` the slurm script is byte for byte what it was before.
- `spock_uv_cache_dir` (optional, only read with `"uv"`): an absolute path for the uv cache.
  Without it uv uses the cache of the account that runs it (`~/.cache/uv`).

## What a uv job does

1. Before `sbatch`, on the login node over the existing ssh connection:
   `umask 002 && export UV_CACHE_DIR=<cache> UV_LINK_MODE=copy && cd <checkout> && uv sync --locked`
   (the first part only with `spock_uv_cache_dir`). Compute nodes have no internet, so this is
   where packages are downloaded. It takes under a second when nothing changed. If it fails,
   nothing is submitted and uv's message is stored in the job's `error_exception`.
2. In the job: `source ~/.bashrc` (PATH and umask of the account), `cd` into the checkout,
   check that `uv` is on PATH, `export UV_CACHE_DIR=<cache>` (with the key), then
   `uv sync --frozen --offline --check`, which installs nothing and stops the job within
   seconds when `.venv` is missing or does not match `uv.lock`. Then `source .venv/bin/activate`
   and `python -u main_script.py`.
3. The only module loaded is `matlab/R2024a`, as in the conda job, so a paramset that asks
   for native MATLAB Kilosort 2 / 3 still finds `matlab`. No anacondapy. Kilosort 4 is in
   `.venv`; the IBL atlas step runs with `.venv`'s python (BrainCogsEphysSorters). Kilosort
   2 / 2.5 / 3 can also run in Apptainer images through a container `sorter_version`
   (BrainCogsEphysSorters, SpikeInterface path), which needs no MATLAB on the node: Kilosort 2
   and 2.5 were run that way on spock, Kilosort 3 was not. Native MATLAB Kilosort 2 and
   3 were both run under `"uv"` on spock; they need BrainCOGS/BrainCogsEphysSorters#30 (without
   it the job can hang after the sort), and Kilosort 2 needs its mex files built once
   (`matlab -batch mexGPUall` in `sorters/Kilosort2/CUDA` on a GPU node). In the database no
   ephys job since job_id 602 has used anything but kilosort4.

## Shared uv cache

One cache on scratch saves a ~9.3 GB download per account and per checkout, and lets the
production account and individual users sync the same lock quickly.

One-time setup, as `u19prod` on `spockmk2-loginvm.pni.princeton.edu`:

```bash
mkdir /mnt/bucket/scratch/u19prod/uv_cache
chgrp g_u19_dj /mnt/bucket/scratch/u19prod/uv_cache
chmod 2775 /mnt/bucket/scratch/u19prod/uv_cache      # group-writable, setgid
ls -ld /mnt/bucket/scratch/u19prod/uv_cache           # drwxrwsr-x u19prod g_u19_dj
```

Every account that uses the cache must be in `g_u19_dj` (`id <user>`) and run uv with
`umask 002`. The pipeline's sync and job set it; a person syncing by hand runs
`umask 002; export UV_CACHE_DIR=/mnt/bucket/scratch/u19prod/uv_cache UV_LINK_MODE=copy` first.

Why this is safe, and what was and was not checked:

- **Concurrency.** uv's documentation (Concepts > Cache) says the cache "supports multiple
  concurrent readers and writers", that "it's safe to run multiple uv commands concurrently, even
  against the same virtual environment", and that uv locks the target environment while
  installing. Entries are written once and not modified. `uv cache clean` / `prune` wait for other
  uv processes (5 min timeout); run them only when nothing else uses the cache. uv's locks are file
  locks on NFS (`/mnt/bucket/scratch` is NFSv3); several processes on the login node were not
  tested against each other, and nothing was tested across accounts (only one account was available).
- **Permissions.** With the directory setgid and group-writable and `umask 002`, uv created only
  `g_u19_dj` directories with setgid and group-writable files (checked on 9.3 GB of cache: 0
  directories without setgid, 0 files of another group). Two files came out `rw-r--r--` (files uv
  extracts with their own mode when it builds a source distribution: the `u19-sorting` editable
  wheel and an sdist's `PKG-INFO`); they are read, never rewritten, so other accounts are not
  blocked by them.
- **Link mode.** `UV_LINK_MODE=copy`: `.venv` gets its own copies. The production checkout is on
  `/mnt/cup` and the cache on `/mnt/bucket`, so hard links are impossible anyway (uv's default
  `clone` falls back to hard links on one filesystem, copies across). Copies also keep `.venv`
  independent of a cache other accounts can write to, and of `uv cache clean`. Cost, measured on
  the login node with a warm cache: 8 min 31 s to fill a new `.venv` by copy, against 1 min 35 s
  with hard links; a cold cache (download) took 4 min 53 s; a sync with nothing to do 0.6 s.
- **Offline check on the node.** `uv sync --frozen --offline --check` reads the cache and writes a
  little to it (interpreter metadata), so the job needs the same writable cache. If it cannot
  create the directory, uv fails with `error: Failed to initialize cache at ...` and the job stops
  with the "missing or does not match uv.lock" message.
- **Python.** uv's Python builds are not in the cache: `.venv/bin/python` points at the Python uv
  installed for the account that ran the sync (`~u19prod/.local/share/uv/python`, readable by all).

## Updating the sorters

A `git pull` that changes `uv.lock` is picked up by the next job's sync. That sync replaces
packages in the `.venv` that running jobs use, as the `git pull` already replaces their code: pull
when `squeue -u u19prod` shows no ephys jobs, or run `uv sync --locked` by hand right after.

## Roll back

Remove `spock_ephys_env` (or set it to `"conda"`) and restart the handler. The conda
environments are untouched by the uv path.
