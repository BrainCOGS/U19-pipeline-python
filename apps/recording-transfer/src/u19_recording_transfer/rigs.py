"""Read-only location catalog and persistent, per-PC source rig identity."""

import socket

MODALITIES = {
    "2photon": "imaging",
    "mesoscope": "imaging",
    "imaging": "imaging",
    "widefield": "imaging",
    "electrophysiology": "electrophysiology",
}


def location_rows(machine):
    from .credentials import connect_database
    from .accounts import require_user

    require_user(machine)
    path = (machine or {}).get("registration", {}).get("dj_config")
    if not path:
        raise ValueError(
            "Configure registration.dj_config and a database login to load lab.Location."
        )
    dj, connection, config = connect_database(path)
    try:
        prefix = config.get("custom", {}).get("database.prefix", "u19_")
        lab = dj.VirtualModule("lab", prefix + "lab", connection=connection)
        return (
            lab.Location & [{"system_type": "recording"}, {"system_type": "rig"}]
        ).fetch("location", "system_type", "acquisition_type", as_dict=True)
    finally:
        connection.close()


def matching_locations(rows, role, acquisition=None):
    return sorted(
        (
            dict(row)
            for row in rows
            if row.get("system_type") == role
            and row.get("acquisition_type") in MODALITIES
            and (not acquisition or row["acquisition_type"] == acquisition)
        ),
        key=lambda row: row["location"].casefold(),
    )


def profile_for(row):
    from .registration import safe_component

    if (
        row.get("system_type") not in ("recording", "rig")
        or row.get("acquisition_type") not in MODALITIES
    ):
        raise ValueError(
            "Select a recording computer or acquisition rig with a supported acquisition type."
        )
    return {
        "location": safe_component(row["location"]),
        "system_type": row["system_type"],
        "acquisition_type": row["acquisition_type"],
        "modality": MODALITIES[row["acquisition_type"]],
        "source_pc": socket.gethostname(),
    }


def identity(machine):
    return (machine or {}).get("rig")


def apply_profile(machine, profile):
    if not machine:
        raise ValueError("Use a machine configuration before selecting this PC's rig.")
    expected = profile_for(profile)
    if (
        profile.get("source_pc") != expected["source_pc"]
        or profile.get("modality") != expected["modality"]
    ):
        raise ValueError(
            "Saved rig identity does not match this PC or acquisition type. Re-select the rig on this PC."
        )
    machine["rig"] = expected
    machine["modality"] = expected["modality"]
    machine.setdefault("registration", {})["system"] = expected["location"]


def load_profile(store):
    load_machine_profile(store.machine, store.directory)


def load_machine_profile(machine, directory):
    from pathlib import Path

    path = Path(directory) / "rig.json"
    if path.exists() and machine is not None:
        import json

        apply_profile(machine, json.loads(path.read_text(encoding="utf-8")))


def save_profile(store, row):
    from .accounts import require_user
    from .app import atomic_json

    lock = store.lock()
    try:
        require_user(store.machine)
        profile = profile_for(row)
        if store.machine is None:
            raise ValueError("Rig selection requires a machine configuration.")
        atomic_json(store.directory / "rig.json", profile)
        apply_profile(store.machine, profile)
        return profile
    finally:
        lock.unlock()


def require_matching_job(machine, job):
    approved = job.get("source_rig")
    if approved and approved != identity(machine):
        raise ValueError(
            "This job was queued on another rig identity. Restore its original rig selection before running it."
        )
    if approved and job.get("modality") != approved["modality"]:
        raise ValueError("Queued recording modality does not match its source rig.")


def validate_live_profile(adapter, profile, modality, system):
    if not profile:
        return
    if profile.get("location") != system or profile.get("modality") != modality:
        raise ValueError(
            "Selected rig does not match the approved source location and recording modality."
        )
    row = (adapter.lab.Location & {"location": system}).fetch1()
    if any(
        row.get(field) != profile[field]
        for field in ("location", "system_type", "acquisition_type")
    ):
        raise ValueError(
            "Selected rig type changed in lab.Location. Reload and select this rig again."
        )


def label(machine):
    rig = identity(machine)
    if rig:
        role = (
            "Recording computer"
            if rig["system_type"] == "recording"
            else "Acquisition rig"
        )
        return f"{role}: {rig['location']} · {rig['acquisition_type']}"
    configured = (machine or {}).get("registration", {}).get("system")
    return (
        f"Configured location: {configured} · Select rig to confirm type"
        if configured
        else "Rig not selected"
    )
