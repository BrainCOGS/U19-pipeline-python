"""Slurm resources of an ephys job, worked out from its parameter sets.

The defaults in ``clusters_paths_and_transfers`` (4 cores, 1 GPU on spock) fit most jobs.
A job that runs through SpikeInterface (BrainCogsEphysSorters) does CPU work in parallel
chunks and may want more, and a paramset can say what it needs:

    {"clustering_method": ..., "sorter_params": {...},
     "resources": {"cpus": 16, "gpus": 1, "mem": "120G", "time": "12:00:00"}}

Every field of ``resources`` is optional. Without ``cpus`` the largest ``n_jobs`` named in
the job's paramsets is used; if there is none the cluster default stays. BrainCogsEphysSorters
reads the granted cores back from SLURM_CPUS_PER_TASK, so the request and the worker count
cannot drift apart. A job that does not use SpikeInterface gets no override at all.

Only ``resources`` of a namespaced clustering paramset (one with ``sorter_params``) is
read: in a legacy paramset every key is a sorter setting.

No imports beyond the standard library, so this can be used and tested without a database.
"""

import re

# Preprocessing methods that run SpikeInterface code on the CPU.
SPIKEINTERFACE_PREPROCESS_METHODS = {"spikeinterface", "dredge"}

RESOURCE_KEYS = {"cpus", "gpus", "mem", "time"}

_MEM_RE = re.compile(r"^[1-9][0-9]*[KMGT]$")
_TIME_RE = re.compile(r"^([0-9]+-)?[0-9]{1,3}:[0-5][0-9]:[0-5][0-9]$")


class EphysResourcesError(ValueError):
    """A ``resources`` entry slurm would not accept."""


def _is_count(value, minimum):
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def validate_resources(resources):
    """Return ``resources`` as a dict, or raise EphysResourcesError saying what is wrong."""

    if resources is None:
        return {}
    if not isinstance(resources, dict):
        raise EphysResourcesError(
            f"resources must be an object, got {type(resources).__name__}"
        )
    unknown = sorted(set(resources) - RESOURCE_KEYS)
    if unknown:
        raise EphysResourcesError(
            f"resources: unknown keys {unknown} (allowed: {sorted(RESOURCE_KEYS)})"
        )
    if "cpus" in resources and not _is_count(resources["cpus"], 1):
        raise EphysResourcesError(
            f"resources.cpus must be a whole number of cores >= 1, got {resources['cpus']!r}"
        )
    if "gpus" in resources and not _is_count(resources["gpus"], 0):
        raise EphysResourcesError(
            f"resources.gpus must be a whole number >= 0, got {resources['gpus']!r}"
        )
    if "mem" in resources and not (
        isinstance(resources["mem"], str) and _MEM_RE.match(resources["mem"])
    ):
        raise EphysResourcesError(
            f"resources.mem must look like '80G', got {resources['mem']!r}"
        )
    if "time" in resources and not (
        isinstance(resources["time"], str) and _TIME_RE.match(resources["time"])
    ):
        raise EphysResourcesError(
            f"resources.time must look like '30:00:00' or '2-00:00:00', got {resources['time']!r}"
        )
    return dict(resources)


def _is_namespaced(params):
    return isinstance(params, dict) and "sorter_params" in params


def _preprocess_steps(preparams):
    """(method, params) of every preprocessing step, whatever shape the list has."""

    steps = []
    for element in preparams or []:
        if isinstance(element, dict):
            steps.extend(element.items())
    return steps


def uses_spikeinterface(params, preparams):
    """True if the job runs SpikeInterface code: a namespaced clustering paramset, or a
    ``spikeinterface`` / ``dredge`` preprocessing step."""

    return _is_namespaced(params) or any(
        method in SPIKEINTERFACE_PREPROCESS_METHODS
        for method, _ in _preprocess_steps(preparams)
    )


def requested_n_jobs(params, preparams):
    """The largest whole ``n_jobs`` the job's paramsets name, or None.

    Looked for where BrainCogsEphysSorters reads job kwargs: ``si_params`` and the
    ``job_kwargs`` of ``preprocessing`` / ``postprocessing`` in a namespaced clustering
    paramset, and ``job_kwargs`` of SpikeInterface preprocessing steps. SpikeInterface
    also accepts -1 (all cores) and fractions; those say nothing about how many cores to
    ask for and are skipped.
    """

    found = []

    def collect(job_kwargs):
        if isinstance(job_kwargs, dict) and _is_count(job_kwargs.get("n_jobs"), 1):
            found.append(job_kwargs["n_jobs"])

    if _is_namespaced(params):
        collect(params.get("si_params"))
        for stage in ("preprocessing", "postprocessing"):
            if isinstance(params.get(stage), dict):
                collect(params[stage].get("job_kwargs"))
    for method, step_params in _preprocess_steps(preparams):
        if method in SPIKEINTERFACE_PREPROCESS_METHODS and isinstance(
            step_params, dict
        ):
            collect(step_params.get("job_kwargs"))
    return max(found, default=None)


def ephys_slurm_overrides(params, preparams):
    """Slurm options (as in the ``#SBATCH --<key>=<value>`` lines) this job changes.

    ``params`` is the clustering paramset, ``preparams`` the preprocessing step list
    (``[{method: params}, ...]``), both as written to the job's parameter files. A value
    of None means "remove this option" (``gres`` when the paramset asks for no GPU).
    Returns {} for a job that does not use SpikeInterface, or that asks for nothing.
    """

    if not uses_spikeinterface(params, preparams):
        return {}

    resources = validate_resources(
        params.get("resources") if _is_namespaced(params) else None
    )

    overrides = {}
    cpus = resources.get("cpus") or requested_n_jobs(params, preparams)
    if cpus:
        overrides["cpus-per-task"] = cpus
    if "gpus" in resources:
        overrides["gres"] = f"gpu:{resources['gpus']}" if resources["gpus"] else None
    for key in ("mem", "time"):
        if key in resources:
            overrides[key] = resources[key]
    return overrides


def apply_overrides(slurm_dict, overrides):
    """A copy of ``slurm_dict`` with the overrides applied (None removes the option)."""

    merged = dict(slurm_dict)
    for key, value in overrides.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = value
    return merged
