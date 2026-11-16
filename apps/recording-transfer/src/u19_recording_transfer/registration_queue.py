"""Connect approved live registration plans to the verified copy queue."""

import hashlib
import json
from pathlib import Path

from .registration import DataJointAdapter, describe, prepare, request_for


def settings_for(machine):
    settings = (machine or {}).get("registration", {})
    if settings.get("enabled") is not True or not settings.get("dj_config"):
        raise ValueError(
            "Live registration is disabled; configure its local DB file explicitly."
        )
    if not settings.get("system") or not settings.get("behavior_rigs"):
        raise ValueError("Registration requires a system and allowed behavior rigs.")
    return settings


def preview(machine, source, modality, preferences, adapter_factory=DataJointAdapter):
    settings = settings_for(machine)
    adapter = adapter_factory(settings["dj_config"])
    return prepare(
        adapter,
        source,
        modality,
        settings["system"],
        settings["behavior_rigs"],
        preferences,
    )


def signature(plan):
    return {
        key: plan[key]
        for key in (
            "source",
            "system",
            "modality",
            "session",
            "user_id",
            "behavior_rigs",
            "processing",
            "preprocessing",
            "raw_root",
            "recording_directory",
            "destination",
            "allow_duplicate_session",
        )
    }


def verify_completed(job):
    from .app import completion_marker, digest, inventory
    import threading

    source, destination = Path(job["source"]), Path(job["destination"])
    manifest = json.loads(completion_marker(job).read_text(encoding="utf-8"))
    if (
        manifest["transfer_id"] != job["id"]
        or Path(manifest["original_path"]).resolve() != source.resolve()
        or Path(manifest["destination"]).resolve() != destination.resolve()
    ):
        raise ValueError(
            "Existing transfer manifest does not identify this approved copy."
        )
    expected = {item["path"]: item for item in manifest["files"]}
    if not expected or len(expected) != len(manifest["files"]):
        raise ValueError("Invalid completion manifest.")
    before, remote = inventory(source), inventory(destination)
    if set(before) != set(expected) or set(remote) != set(expected):
        raise ValueError(
            "Verified copy/source changed; registered files will not be overwritten."
        )
    for name, item in expected.items():
        if (
            digest(source / name, threading.Event()) != item["sha256"]
            or digest(destination / name, threading.Event()) != item["sha256"]
        ):
            raise ValueError(
                "Verified copy/source changed; registered files will not be overwritten."
            )
    if inventory(source) != before or inventory(destination) != remote:
        raise ValueError("Source/copy changed while verifying recovery.")
    return manifest


def run_registered_job(
    store, job, report, stop, delay=0, adapter_factory=DataJointAdapter
):
    from PyQt6.QtCore import QLockFile
    from .app import atomic_json, completion_marker, copy_job, reject_linked_ancestors

    settings = settings_for(store.machine)
    approved = job["registration_plan"]
    adapter = adapter_factory(settings["dj_config"])
    fresh = prepare(
        adapter,
        job["source"],
        job["modality"],
        settings["system"],
        settings["behavior_rigs"],
        request_for(approved),
    )
    if signature(fresh) != signature(approved):
        raise ValueError("Live registration plan changed; preview and confirm again.")
    destination = Path(fresh["destination"])
    from .credentials import ensure_share

    ensure_share(destination)
    reject_linked_ancestors(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    name = hashlib.sha256(
        (fresh["modality"] + ":" + fresh["recording_directory"].casefold()).encode()
    ).hexdigest()[:24]
    lock = QLockFile(str(destination.parent / f".u19-{name}.lock"))
    lock.setStaleLockTime(0)
    if not lock.tryLock(0):
        raise RuntimeError("Another transfer is writing the selected final directory.")
    try:
        owner_file = destination.parent / f".u19-{name}-owner.json"
        if owner_file.exists():
            owner = json.loads(owner_file.read_text(encoding="utf-8"))
            if owner.get("transfer_id") != job["id"] or owner.get("plan") != signature(
                approved
            ):
                raise ValueError(
                    "Final directory is owned by another transfer; nothing will be overwritten."
                )
        else:
            if destination.exists():
                raise ValueError(
                    "Final directory already exists without this transfer's ownership record."
                )
            atomic_json(
                owner_file, dict(transfer_id=job["id"], plan=signature(approved))
            )
        job["destination"] = str(destination)
        job["completion_marker"] = str(
            destination.parent / f".u19-{job['id']}-complete.json"
        )
        if fresh["existing_recording_id"] is not None:
            # Recovery after DB commit: only re-verify, never rewrite registered data.
            manifest = verify_completed(job)
        else:
            complete, message = copy_job(job, report, stop, delay)
            if not complete:
                return False, message
            manifest = verify_completed(job)
        if stop.is_set():
            raise InterruptedError(
                "Stopped before database registration; verified files retained."
            )
        identifier = adapter.commit(fresh)
        manifest["registered_recording_id"] = identifier
        manifest["registration_plan"] = signature(fresh)
        atomic_json(completion_marker(job), manifest)
        job["recording_id"] = identifier
        return True, f"Verified and registered • recording_id {identifier}"
    finally:
        lock.unlock()


__all__ = ["describe", "preview", "run_registered_job"]
