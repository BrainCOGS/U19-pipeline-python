"""Export a read-only snapshot for the GUI. Reads existing tables; never inserts.

uv run --extra catalog u19-recording-catalog --dj-config <config.json> --output <catalog.json>
"""

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path


def json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def export(dj_config, since_days):
    import datajoint as dj

    dj.config.load(str(dj_config))
    prefix = dj.config.get("custom", {}).get("database.prefix", "u19_")
    acquisition = dj.VirtualModule("acquisition", prefix + "acquisition")
    subject = dj.VirtualModule("subject", prefix + "subject")
    since = (date.today() - timedelta(days=since_days)).isoformat()
    sessions = (
        (acquisition.Session * subject.Subject.proj("user_id"))
        & f'session_date >= "{since}"'
    ).fetch(
        "subject_fullname",
        "user_id",
        "session_date",
        "session_number",
        "session_location",
        as_dict=True,
    )
    catalog = dict(
        demo=False,
        exported_at=datetime.now().isoformat(),
        sessions=sessions,
        processing=[],
        preprocessing=[],
    )
    for modality, schema, processing_table, steps_table, steps_id, steps_name in [
        (
            "imaging",
            "pipeline_imaging_element",
            "ProcessingParamSet",
            "PreprocessParamSteps",
            "preprocess_param_steps_id",
            "preprocess_param_steps_name",
        ),
        (
            "electrophysiology",
            "pipeline_ephys_element",
            "ClusteringParamSet",
            "PreClusterParamSteps",
            "precluster_param_steps_id",
            "precluster_param_steps_name",
        ),
    ]:
        element = dj.VirtualModule(schema, prefix + schema)
        for row in getattr(element, processing_table).fetch(as_dict=True):
            catalog["processing"].append(
                dict(
                    id=int(row["paramset_idx"]),
                    name=row.get("paramset_desc", str(row["paramset_idx"])),
                    method=row.get(
                        "processing_method", row.get("clustering_method", "")
                    ),
                    modality=modality,
                    details=row,
                )
            )
        steps = getattr(element, steps_table)
        for row in steps.fetch(as_dict=True):
            catalog["preprocessing"].append(
                dict(
                    id=int(row[steps_id]),
                    name=row[steps_name],
                    modality=modality,
                    details=dict(
                        row,
                        steps=(steps.Step & {steps_id: row[steps_id]}).fetch(
                            as_dict=True
                        ),
                    ),
                )
            )
    return catalog


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dj-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--since-days", type=int, default=90)
    args = parser.parse_args()
    if args.since_days < 0:
        parser.error("--since-days must be nonnegative")
    catalog = export(args.dj_config, args.since_days)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(catalog, default=json_value, indent=2), encoding="utf-8"
    )
    print(
        f"Exported {len(catalog['sessions'])} sessions and {len(catalog['processing'])} processing profiles."
    )


if __name__ == "__main__":
    main()
