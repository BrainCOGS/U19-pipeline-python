"""The post-processing step list of ephys jobs, as BrainCogsEphysSorters reads it.

A job that selected a ``PostClusterParamSteps`` list (``ephys_postcluster.EphysPostParams``)
gets its steps written to ``postprocess_paramset_<job_id>.json``: an ordered list of
``{postcluster_method: params}``, the same shape as the preprocessing file. A job without
one gets ``[]``, which the sorters read as "no post-processing", so a file left on the
cluster by an earlier run of the same job id can never apply to a job that has none.

No imports beyond the standard library, so this can be used and tested without a database.
"""

# What BrainCogsEphysSorters' job_spec accepts as postcluster_method.
POSTPROCESS_METHODS = {"spikeinterface", "bombcell"}


def group_postparams(rows):
    """``{job_id: [{method: params}, ...]}`` from joined step rows, in step order.

    ``rows``: dicts with ``job_id``, ``step_number``, ``postcluster_method`` and
    ``params`` (EphysPostParams * PostClusterParamSteps.Step * PostClusterParamSet).
    """

    by_job = {}
    for row in sorted(rows, key=lambda r: (r["job_id"], r["step_number"])):
        by_job.setdefault(row["job_id"], []).append(
            {row["postcluster_method"]: row["params"]}
        )
    return by_job


def postparams_for_jobs(job_ids, rows):
    """The step list of each job in ``job_ids`` (``[]`` for a job without one)."""

    by_job = group_postparams(rows)
    return [by_job.get(job_id, []) for job_id in job_ids]


def as_step_list(postparams):
    """``postparams`` as a list, whatever a DataFrame cell made of it (NaN, None)."""

    return postparams if isinstance(postparams, list) else []
