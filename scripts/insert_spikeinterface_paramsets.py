"""Add the lookup rows that let an ephys job run through SpikeInterface.

BrainCogsEphysSorters reads a job's clustering paramset and preprocessing step list from
the two JSON files this pipeline writes. A clustering paramset with a ``sorter_params``
key is run through SpikeInterface instead of the direct Kilosort call; one that also has
a ``preprocessing`` step list describes the whole run (read, preprocess, sort,
postprocess). Existing rows, and the jobs that use them, are not touched.

Rows this script adds, each only if a row with the same params is not there yet:

  ClusteringParamSet   "full SpikeInterface path": SpikeInterface preprocessing, Kilosort 4
                       through SpikeInterface, quality metrics. Select it together with
                       the existing "Nothing" preprocessing.
  ClusteringParamSet   Kilosort 4 through SpikeInterface with quality metrics and no
                       SpikeInterface preprocessing.
  PreClusterMethod     ``spikeinterface``
  PreClusterParamSet   the same preprocessing steps as a preprocessing step, for jobs that
  PreClusterParamSteps keep an existing (direct Kilosort) clustering paramset.

Nothing is written unless ``--insert`` is given:

    python scripts/insert_spikeinterface_paramsets.py            # show what would be added
    python scripts/insert_spikeinterface_paramsets.py --insert   # add it

The database account needs insert rights on the ephys element schema.
"""

import argparse
import copy
import datetime
import json

# SpikeInterface's recipe for what CatGT does (phase_shift is CatGT's tshift, the global
# median reference its -gblcar), plus the 300 Hz high-pass of the lab's CatGT paramset.
# https://spikeinterface.readthedocs.io/en/stable/modules/preprocessing.html
PREPROCESSING_STEPS = {
    "steps": [
        {"name": "phase_shift", "kwargs": {}},
        {"name": "highpass_filter", "kwargs": {"freq_min": 300}},
        {
            "name": "common_reference",
            "kwargs": {"operator": "median", "reference": "global"},
        },
    ]
}

# quality_metrics on its own only gives the spike-train metrics (firing rate, ISI and
# refractory violations, presence ratio). With waveforms, templates, noise levels and
# spike amplitudes it adds SNR and the amplitude metrics; on a 10 min recording that cost
# about 4 min and 1.7 GB under <method>_output/spikeinterface (slurm 7465065).
QUALITY_METRICS_EXTENSIONS = {
    "random_spikes": {},
    "waveforms": {},
    "templates": {},
    "noise_levels": {},
    "spike_amplitudes": {},
    "quality_metrics": {},
}

# n_jobs is left out on purpose: BrainCogsEphysSorters takes it from the cores slurm gave
# the job (cpus-per-task in clusters_paths_and_transfers.slurm_dict_spock_default).
SI_KILOSORT4 = {
    "sorter_version": None,
    "sorter_params": {},
    "si_params": {"save_preprocessed_copy": True},
    "postprocessing": {"extensions": QUALITY_METRICS_EXTENSIONS},
}

CLUSTERING_METHOD = "kilosort4"
PRECLUSTER_METHOD = "spikeinterface"
PRECLUSTER_METHOD_DESC = "SpikeInterface preprocessing step list"

# Descriptions are "<user>_<date>_<text>": RecordingProcessJobGUI splits them on "_" into
# exactly those three parts to show user and date, and lists paramsets by <text>, so the
# text must not contain an underscore and must be unique.

# Column limits of the element tables (ephys_precluster.py).
METHOD_MAX_LENGTH = 16
PARAMSET_DESC_MAX_LENGTH = 128
STEPS_NAME_MAX_LENGTH = 32
STEPS_DESC_MAX_LENGTH = 128


def clustering_paramsets(date):
    """(description, params) of the two clustering paramsets, in insertion order."""

    full = {"preprocessing": copy.deepcopy(PREPROCESSING_STEPS), **SI_KILOSORT4}
    sort_only = copy.deepcopy(SI_KILOSORT4)
    return [
        (
            f"general-user_{date}_Full SpikeInterface path: SI preprocessing, kilosort4, quality metrics",
            full,
        ),
        (
            f"general-user_{date}_kilosort4 through SpikeInterface, quality metrics",
            sort_only,
        ),
    ]


def precluster_paramset(date):
    """(description, params) of the ``spikeinterface`` preprocessing paramset."""

    return (
        f"general-user_{date}_SpikeInterface phase shift, highpass 300 Hz, global median reference",
        copy.deepcopy(PREPROCESSING_STEPS),
    )


def precluster_steps(date):
    """(name, description) of the one-step preprocessing step list."""

    return ("SpikeInterface filters", f"general-user_{date}_SpikeInterface filters")


def next_free_index(existing):
    """The index after the highest one in use (0 for an empty table)."""

    return max(existing, default=-1) + 1


def find_params(rows, params):
    """paramset_idx of the row holding exactly these params, or None."""

    for row in rows:
        if _same(row["params"], params):
            return row["paramset_idx"]
    return None


def _same(a, b):
    """Compare two params dicts the way they are stored: by content, not key order."""

    return json.dumps(a, sort_keys=True, default=str) == json.dumps(
        b, sort_keys=True, default=str
    )


def plan(clustering_rows, precluster_rows, precluster_methods, steps_rows, date):
    """Work out what to insert, given what the tables hold now.

    Each argument is the current content of a table as a list of dicts (``precluster_methods``
    as a list of names). Returns a dict of lists of rows to insert; a paramset that already
    exists (same params) is left out, and the step list then points at the existing one.
    """

    todo = {
        "ClusteringParamSet": [],
        "PreClusterMethod": [],
        "PreClusterParamSet": [],
        "PreClusterParamSteps": [],
        "PreClusterParamSteps.Step": [],
    }

    next_idx = next_free_index(row["paramset_idx"] for row in clustering_rows)
    for desc, params in clustering_paramsets(date):
        if find_params(clustering_rows, params) is None:
            todo["ClusteringParamSet"].append(
                {
                    "clustering_method": CLUSTERING_METHOD,
                    "paramset_idx": next_idx,
                    "paramset_desc": desc,
                    "params": params,
                }
            )
            next_idx += 1

    if PRECLUSTER_METHOD not in precluster_methods:
        todo["PreClusterMethod"].append(
            {
                "precluster_method": PRECLUSTER_METHOD,
                "precluster_method_desc": PRECLUSTER_METHOD_DESC,
            }
        )

    desc, params = precluster_paramset(date)
    precluster_idx = find_params(precluster_rows, params)
    if precluster_idx is None:
        precluster_idx = next_free_index(row["paramset_idx"] for row in precluster_rows)
        todo["PreClusterParamSet"].append(
            {
                "precluster_method": PRECLUSTER_METHOD,
                "paramset_idx": precluster_idx,
                "paramset_desc": desc,
                "params": params,
            }
        )

    # A step list for this paramset on its own: one step, step_number 1 as in the others.
    single_step_lists = {
        steps_id
        for steps_id in {row["precluster_param_steps_id"] for row in steps_rows}
        if [
            row["paramset_idx"]
            for row in steps_rows
            if row["precluster_param_steps_id"] == steps_id
        ]
        == [precluster_idx]
    }
    if not single_step_lists:
        steps_id = next_free_index(
            row["precluster_param_steps_id"] for row in steps_rows
        )
        name, steps_desc = precluster_steps(date)
        todo["PreClusterParamSteps"].append(
            {
                "precluster_param_steps_id": steps_id,
                "precluster_param_steps_name": name,
                "precluster_param_steps_desc": steps_desc,
            }
        )
        todo["PreClusterParamSteps.Step"].append(
            {
                "precluster_param_steps_id": steps_id,
                "step_number": 1,
                "paramset_idx": precluster_idx,
            }
        )
    return todo


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--insert",
        action="store_true",
        help="write the rows (default: only show them)",
    )
    args = parser.parse_args()

    # Imported here so the planning above can be used, and tested, without a database.
    import datajoint as dj

    from u19_pipeline.ephys_pipeline import ephys_element

    date = datetime.date.today().isoformat()
    steps_table = ephys_element.PreClusterParamSteps
    todo = plan(
        ephys_element.ClusteringParamSet.fetch("paramset_idx", "params", as_dict=True),
        ephys_element.PreClusterParamSet.fetch("paramset_idx", "params", as_dict=True),
        list(ephys_element.PreClusterMethod.fetch("precluster_method")),
        (steps_table * steps_table.Step).fetch(
            "precluster_param_steps_id", "paramset_idx", as_dict=True
        )
        + [
            {"precluster_param_steps_id": steps_id, "paramset_idx": None}
            for steps_id in (steps_table - steps_table.Step).fetch(
                "precluster_param_steps_id"
            )
        ],
        date,
    )

    for table, rows in todo.items():
        for row in rows:
            print(table, json.dumps(row, indent=2))
    if not any(todo.values()):
        print("Nothing to add: every row is already there.")
        return
    if not args.insert:
        print("\nDry run: nothing was written. Rerun with --insert to add these rows.")
        return

    with dj.conn().transaction:
        for row in todo["ClusteringParamSet"]:
            ephys_element.ClusteringParamSet.insert_new_params(
                row["clustering_method"],
                row["paramset_idx"],
                row["paramset_desc"],
                row["params"],
            )
        ephys_element.PreClusterMethod.insert(todo["PreClusterMethod"])
        for row in todo["PreClusterParamSet"]:
            ephys_element.PreClusterParamSet.insert_new_params(
                row["precluster_method"],
                row["paramset_idx"],
                row["paramset_desc"],
                row["params"],
            )
        steps_table.insert(todo["PreClusterParamSteps"])
        steps_table.Step.insert(todo["PreClusterParamSteps.Step"])
    print("\nInserted.")


if __name__ == "__main__":
    main()
