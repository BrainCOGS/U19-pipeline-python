"""Registration preferences carried with a transfer; no database writes."""

import json
import re
from datetime import date
from pathlib import PureWindowsPath

IMAGING_DEFAULT = "PNI182-1plane-30fs-PPC"


def load_catalog(path):
    catalog = json.loads(path.read_text(encoding="utf-8"))
    for collection in ("sessions", "processing", "preprocessing"):
        if not isinstance(catalog.get(collection), list):
            raise TypeError(f"Catalog requires a {collection} list.")
    for session in catalog["sessions"]:
        for field in ("subject_fullname", "session_date", "session_number", "user_id"):
            if field not in session:
                raise ValueError(f"Session is missing {field}.")
        date.fromisoformat(session["session_date"])
    for kind in ("processing", "preprocessing"):
        for option in catalog[kind]:
            if not all(
                field in option for field in ("id", "name", "modality", "details")
            ):
                raise ValueError(
                    f"Each {kind} option needs id, name, modality, and details."
                )
    return catalog


def matching_sessions(source, sessions):
    """Restrict only when both subject and date match; otherwise show all."""
    parts = [part.lower() for part in PureWindowsPath(str(source)).parts]
    dates = set()
    for part in parts:
        for token in re.findall(r"(?<!\d)(?:\d{4}-\d{2}-\d{2}|\d{8})(?!\d)", part):
            for candidate in (token, f"{token[4:]}-{token[:2]}-{token[2:4]}"):
                try:
                    dates.add(date.fromisoformat(candidate).isoformat())
                except ValueError:
                    continue
    matches = []
    for session in sessions:
        subject = session["subject_fullname"].lower()
        subject_match = any(
            part == subject
            or (any(c.isdigit() for c in part) and subject.endswith("_" + part))
            for part in parts
        )
        if subject_match and session["session_date"] in dates:
            matches.append(session)
    return (matches, True) if matches else (list(sessions), False)


def default_processing_index(options, modality):
    if modality == "imaging":
        for index, option in enumerate(options):
            if IMAGING_DEFAULT.lower() in option["name"].lower():
                return index
    return None


def demo_catalog():
    """Explicitly synthetic IDs, never suitable for production registration."""
    return dict(
        demo=True,
        sessions=[
            dict(
                subject_fullname="efonseca_ef932_act131",
                user_id="efonseca",
                session_date="2026-11-05",
                session_number=1,
                session_location="DEMO imaging rig",
            ),
            dict(
                subject_fullname="efonseca_ef932_act131",
                user_id="efonseca",
                session_date="2026-11-05",
                session_number=2,
                session_location="DEMO imaging rig",
            ),
            dict(
                subject_fullname="jyanar_ya013",
                user_id="jyanar",
                session_date="2026-11-06",
                session_number=1,
                session_location="DEMO ephys rig",
            ),
        ],
        processing=[
            dict(
                id="demo-imaging-default",
                name=IMAGING_DEFAULT,
                modality="imaging",
                method="suite2p",
                details={
                    "fs": 30,
                    "nplanes": 1,
                    "region": "PPC",
                    "note": "Synthetic demo profile; values are illustrative.",
                },
            ),
            dict(
                id="demo-imaging-other",
                name="Example imaging • 2 planes",
                modality="imaging",
                method="suite2p",
                details={"fs": 15, "nplanes": 2, "note": "Synthetic example"},
            ),
            dict(
                id="demo-ephys",
                name="Example ephys",
                modality="electrophysiology",
                method="kilosort",
                details={"note": "Synthetic example; choose a real DB profile later."},
            ),
        ],
        preprocessing=[
            dict(
                id="demo-no-preprocessing",
                name="No preprocessing",
                modality="imaging",
                details={"steps": [], "note": "Synthetic example"},
            ),
            dict(
                id="demo-ephys-steps",
                name="Example ephys preprocessing",
                modality="electrophysiology",
                details={"steps": ["Example CatGT step"], "note": "Synthetic example"},
            ),
        ],
    )
