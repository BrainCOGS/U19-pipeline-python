"""Create the ephys post-processing tables and their first rows (a one-time migration).

The tables are defined in ``u19_pipeline/ephys_postcluster.py``: PostClusterMethod,
PostClusterParamSet, PostClusterParamSteps (+ .Step) in the ephys element schema, and
EphysPostParams (a job's selected step list) in recording_process. Creating them alters
no existing table, and no existing job changes: a job without an EphysPostParams row has
no post-processing, exactly as before.

Rows this script adds, each only if it is not there yet:

  PostClusterMethod     ``spikeinterface`` and ``bombcell``
  PostClusterParamSet   SpikeInterface quality and template metrics; bombcell with its
                        default thresholds
  PostClusterParamSteps one list for each of the two, so a job can select either

Nothing is written unless ``--create`` is given. Run it from the repository, in its
environment (it imports ``u19_pipeline.ephys_postcluster``, which only needs datajoint):

    uv run python -m scripts.create_ephys_postcluster_tables --config dj_local_conf.json
    uv run python -m scripts.create_ephys_postcluster_tables --config dj_local_conf.json --create

The database account needs CREATE and REFERENCES rights on the ephys element and
recording_process schemas. The MATLAB package +pipeline_ephys_element (U19-pipeline-matlab)
needs matching PostCluster*.m definitions before RecordingProcessJobGUI can show them.
"""

import argparse
import copy
import datetime
import hashlib
import json
import uuid

METHODS = [
    {
        "postcluster_method": "spikeinterface",
        "postcluster_method_desc": "SpikeInterface sorting analyzer extensions",
    },
    {
        "postcluster_method": "bombcell",
        "postcluster_method_desc": "bombcell unit labels (good, mua, noise, non_soma), SpikeInterface port",
    },
]

# Waveforms, templates, noise levels and spike amplitudes give quality_metrics its SNR and
# amplitude metrics, as in insert_spikeinterface_paramsets.QUALITY_METRICS_EXTENSIONS.
# n_jobs is left out: BrainCogsEphysSorters takes it from the cores slurm gave the job.
SI_METRICS = {
    "extensions": {
        "random_spikes": {},
        "waveforms": {},
        "templates": {},
        "noise_levels": {},
        "spike_amplitudes": {},
        "quality_metrics": {},
        "template_metrics": {},
    }
}

# Every key optional: bombcell's default thresholds, and the extensions they need.
BOMBCELL_DEFAULTS = {}

# Column limits of the tables (ephys_postcluster.py), as in ephys_precluster.
METHOD_MAX_LENGTH = 16
PARAMSET_DESC_MAX_LENGTH = 128
STEPS_NAME_MAX_LENGTH = 32
STEPS_DESC_MAX_LENGTH = 128


def paramsets(date):
    """(key, method, description, params) of the paramsets, in insertion order.

    Descriptions are "<user>_<date>_<text>", which the website and RecordingProcessJobGUI
    split on "_"; the text has no underscore.
    """

    return [
        (
            "si_metrics",
            "spikeinterface",
            f"general-user_{date}_SpikeInterface quality and template metrics",
            copy.deepcopy(SI_METRICS),
        ),
        (
            "bombcell",
            "bombcell",
            f"general-user_{date}_bombcell default thresholds",
            copy.deepcopy(BOMBCELL_DEFAULTS),
        ),
    ]


def step_lists(date):
    """(name, description, [paramset keys]) of the step lists."""

    return [
        (
            "SpikeInterface metrics",
            f"general-user_{date}_SpikeInterface quality and template metrics",
            ["si_metrics"],
        ),
        ("bombcell", f"general-user_{date}_bombcell unit labels", ["bombcell"]),
    ]


def next_free_index(existing):
    """The index after the highest one in use (1 for an empty table), as a plain int."""

    return int(max(existing, default=0)) + 1


def _same(a, b):
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(
        b, sort_keys=True, default=str
    )


def plan(methods, paramset_rows, steps_rows, date):
    """Work out what to insert, given what the tables hold now (empty before --create).

    ``methods``: names in PostClusterMethod. ``paramset_rows``: PostClusterParamSet rows
    (paramset_idx, postcluster_method, params). ``steps_rows``: one row per step
    (postcluster_param_steps_id, paramset_idx), paramset_idx None for an empty list.
    """

    todo = {
        "PostClusterMethod": [
            m for m in METHODS if m["postcluster_method"] not in methods
        ],
        "PostClusterParamSet": [],
        "PostClusterParamSteps": [],
        "PostClusterParamSteps.Step": [],
    }

    idx_of = {}
    next_idx = next_free_index(row["paramset_idx"] for row in paramset_rows)
    for key, method, desc, params in paramsets(date):
        existing = [
            int(row["paramset_idx"])
            for row in paramset_rows
            if row["postcluster_method"] == method and _same(row["params"], params)
        ]
        if existing:
            idx_of[key] = existing[0]
            continue
        idx_of[key] = next_idx
        todo["PostClusterParamSet"].append(
            {
                "postcluster_method": method,
                "paramset_idx": next_idx,
                "paramset_desc": desc,
                "params": params,
            }
        )
        next_idx += 1

    lists = {}
    for row in sorted(steps_rows, key=lambda r: r["postcluster_param_steps_id"]):
        steps = lists.setdefault(row["postcluster_param_steps_id"], [])
        if row["paramset_idx"] is not None:
            steps.append(int(row["paramset_idx"]))
    next_id = next_free_index(lists)
    for name, desc, keys in step_lists(date):
        wanted = [idx_of[key] for key in keys]
        if wanted in lists.values():
            continue
        todo["PostClusterParamSteps"].append(
            {
                "postcluster_param_steps_id": next_id,
                "postcluster_param_steps_name": name,
                "postcluster_param_steps_desc": desc,
            }
        )
        todo["PostClusterParamSteps.Step"].extend(
            {
                "postcluster_param_steps_id": next_id,
                "step_number": number,
                "paramset_idx": idx,
            }
            for number, idx in enumerate(wanted, start=1)
        )
        next_id += 1
    return todo


def dict_to_uuid(key):
    """The paramset hash the element stores (element_interface.utils.dict_to_uuid)."""

    hashed = hashlib.md5()
    for k, v in sorted(key.items()):
        hashed.update(str(k).encode())
        hashed.update(str(v).encode())
    return uuid.UUID(hex=hashed.hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--create",
        action="store_true",
        help="create the tables and insert the rows (default: only show the plan)",
    )
    parser.add_argument(
        "--config",
        help="DataJoint config file (default: dj_local_conf.json in the current directory)",
    )
    args = parser.parse_args()

    import datajoint as dj

    if args.config:
        dj.config.load(args.config)
    prefix = dj.config["custom"]["database.prefix"]

    from u19_pipeline import ephys_postcluster

    element_name, job_name = ephys_postcluster.schema_names(prefix)
    print("tables:", [t.__name__ for t in ephys_postcluster.TABLES])
    print("schemas:", element_name, job_name)
    date = datetime.date.today().isoformat()

    if not args.create:
        tables = ephys_postcluster.tables()
        if tables is None:
            todo = plan([], [], [], date)
            print("The tables do not exist yet; --create would declare them and add:")
        else:
            todo = _plan_from(tables, date)
        for table, rows in todo.items():
            for row in rows:
                print(table, json.dumps(row, indent=2))
        print("\nDry run: nothing was written. Rerun with --create to apply.")
        return

    tables = ephys_postcluster.activate(prefix, create=True)
    todo = _plan_from(tables, date)
    with dj.conn().transaction:
        tables.PostClusterMethod.insert(todo["PostClusterMethod"])
        for row in todo["PostClusterParamSet"]:
            tables.PostClusterParamSet.insert1(
                {**row, "param_set_hash": dict_to_uuid(row["params"])}
            )
        tables.PostClusterParamSteps.insert(todo["PostClusterParamSteps"])
        tables.PostClusterParamSteps.Step.insert(todo["PostClusterParamSteps.Step"])
    for table, rows in todo.items():
        print(table, len(rows), "rows inserted")


def _plan_from(tables, date):
    steps = tables.PostClusterParamSteps
    return plan(
        list(tables.PostClusterMethod.fetch("postcluster_method")),
        tables.PostClusterParamSet.fetch(
            "paramset_idx", "postcluster_method", "params", as_dict=True
        ),
        (steps * steps.Step).fetch(
            "postcluster_param_steps_id", "paramset_idx", as_dict=True
        )
        + [
            {"postcluster_param_steps_id": steps_id, "paramset_idx": None}
            for steps_id in (steps - steps.Step).fetch("postcluster_param_steps_id")
        ],
        date,
    )


if __name__ == "__main__":
    main()
