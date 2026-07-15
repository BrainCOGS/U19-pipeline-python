# U19 Pipeline — Airflow Phase-1 Scaffold

This directory contains the **Phase-1 Apache Airflow scaffold** for migrating the U19
neuroscience pipeline from bare cron jobs to Airflow orchestration. The project targets
**Airflow 3.x** (currently pinned to `apache-airflow==3.2.2`). Tracked in
[BrainCOGS/U19-pipeline-python#95](https://github.com/BrainCOGS/U19-pipeline-python/issues/95).

## Layout

```
airflow/
  README.md                     # this file
  requirements-airflow.txt      # Airflow + provider pins
  dags/
    nightly_populate.py         # @daily — behavior/optogenetics/pupillometry/alerts
    ephys_processing.py         # @hourly — ephys recording pipeline
    imaging_processing.py       # @hourly — imaging recording pipeline
  plugins/u19/
    __init__.py
    datajoint_sensor.py         # DataJoint table-populated sensor
    slurm.py                    # SLURM job submission/polling
    transfers.py                # Globus file transfer helpers
    matlab.py                   # SSH-based MATLAB batch runner
    status.py                   # Dual-write status + log helper
    callbacks.py                # Slack failure callback
```

## Status

**All task bodies are stubs** (`NotImplementedError` / `pass`). The DAGs parse and are
DagBag-importable, but no real work is performed. Each stub references the existing
`u19_pipeline` module that should be called once the task is fleshed out.

## DAG Validation in CI

A GitHub Actions workflow (`.github/workflows/airflow-dag-validation.yml`) uses **uv**
to `uv sync --only-group airflow` (Airflow 3.2.2, locked in `uv.lock`) and runs a
`DagBag` import-error check on every push and pull request. No Airflow database is
required — the DAGs are loaded in-process. The `airflow` dependency group is declared in
the project `pyproject.toml`.

## Open Infrastructure Questions

The following decisions are **not yet made** and block a production deployment:

1. **PNI hosting** — where does the Airflow webserver/scheduler run? (dedicated VM,
   existing `braincogs` server, or cloud?)
2. **MATLAB host** — which host runs the MATLAB batch processes, and what SSH
   `conn_id` ("matlab_host") maps to in the Airflow Connections table?
3. **AcquiredTiff owner** — `imaging_processing.py` notes the `AcquiredTiff` node is
   contended. Should it be owned by a Python operator or a MATLAB SSH call? Guards
   against double-run are needed before this DAG goes live.
4. **Metadata DB** — Airflow's metadata DB (Postgres recommended for production) needs
   to be provisioned and the connection string set before scheduler startup.
