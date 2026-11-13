"""Opt-in local cleanup of this copier's verified, retained queue entries."""

import json
import socket
import threading
from datetime import UTC, datetime
from pathlib import Path


def checked_paths(job, source_root, destination_root):
    source, destination = Path(job["source"]), Path(job["destination"])
    # Check the named paths before resolution as well as their resolved targets.
    if (
        source.is_symlink()
        or source.is_junction()
        or destination.is_symlink()
        or destination.is_junction()
    ):
        raise ValueError("Linked recording roots are not eligible for cleanup.")
    source, destination = source.resolve(), destination.resolve()
    if source == source_root or not source.is_relative_to(source_root):
        raise ValueError(
            "Source is outside the approved source root (or is the root itself)."
        )
    if destination == destination_root or not destination.is_relative_to(
        destination_root
    ):
        raise ValueError("Copy is outside the approved braininit destination root.")
    if source.is_relative_to(destination_root) or destination.is_relative_to(
        source_root
    ):
        raise ValueError("Local source and storage destination must be separate trees.")
    # A symlink/junction in an intermediate component must not bypass root checks.
    for path, root in [
        (Path(job["source"]), source_root),
        (Path(job["destination"]), destination_root),
    ]:
        for parent in (path, *path.parents):
            if parent.is_symlink() or parent.is_junction():
                raise ValueError("Linked ancestors are not eligible for cleanup.")
            if parent.resolve() == root:
                break
    return source, destination


def cleanup(
    store,
    source_root,
    destination_root,
    retention_days=14,
    apply=False,
    report=print,
    skip_outside_root=False,
):
    # Import lazily to share exactly the same inventory/hash implementation.
    from .app import digest, inventory, now

    source_root, destination_root = source_root.resolve(), destination_root.resolve()
    if retention_days < 0:
        raise ValueError("Retention days must be nonnegative.")
    if not source_root.is_dir() or not destination_root.is_dir():
        raise ValueError("Both approved roots must exist and be accessible.")
    lock = store.lock()
    errors = 0
    try:
        jobs = store.read()
        for job in jobs:
            if job["status"] != "Complete":
                continue
            try:
                if skip_outside_root and not Path(
                    job["source"]
                ).resolve().is_relative_to(source_root):
                    continue
                source, destination = checked_paths(job, source_root, destination_root)
                manifest = json.loads(
                    (destination.parent / "transfer_complete.json").read_text(
                        encoding="utf-8"
                    )
                )
                if (
                    manifest["transfer_id"] != job["id"]
                    or manifest["source_pc"] != socket.gethostname()
                    or Path(manifest["original_path"]).resolve() != source
                    or Path(manifest["destination"]).resolve() != destination
                ):
                    raise ValueError(
                        "Completion manifest does not identify this PC and transfer."
                    )
                completed = datetime.fromisoformat(manifest["completed_at"])
                age = (datetime.now(UTC) - completed).total_seconds() / 86400
                if age < retention_days:
                    report(f"Retained ({age:.1f}/{retention_days} days): {source}")
                    continue
                expected = {item["path"]: item for item in manifest["files"]}
                if not expected or len(expected) != len(manifest["files"]):
                    raise ValueError("Manifest has no files or duplicate paths.")
                for relative in expected:
                    candidate = (source / relative).resolve()
                    if candidate == source or not candidate.is_relative_to(source):
                        raise ValueError(
                            "Manifest contains a path outside the recording folder."
                        )
                before = inventory(source)
                remote = inventory(destination)
                if set(before) != set(expected) or set(remote) != set(expected):
                    raise ValueError(
                        "Source/copy file set changed since transfer; preserving local recording."
                    )
                stop = threading.Event()
                for relative, item in expected.items():
                    if (
                        before[relative][0] != item["bytes"]
                        or remote[relative][0] != item["bytes"]
                        or digest(source / relative, stop) != item["sha256"]
                        or digest(destination / relative, stop) != item["sha256"]
                    ):
                        raise ValueError(
                            f"Hash/size mismatch: {relative}; preserving local recording."
                        )
                if inventory(source) != before or inventory(destination) != remote:
                    raise ValueError(
                        "Files changed while verifying; preserving local recording."
                    )
                if not apply:
                    report(f"WOULD DELETE verified local recording: {source}")
                    continue
                # Resolve/check again immediately before deleting only the verified files.
                checked_paths(job, source_root, destination_root)
                for relative, item in expected.items():
                    if (
                        digest(source / relative, stop) != item["sha256"]
                        or digest(destination / relative, stop) != item["sha256"]
                    ):
                        raise ValueError(
                            f"Files changed before deletion: {relative}; stopping cleanup."
                        )
                    (source / relative).unlink()
                for folder in sorted(
                    source.rglob("*"), key=lambda p: len(p.parts), reverse=True
                ):
                    if (
                        folder.is_dir()
                        and not folder.is_symlink()
                        and not folder.is_junction()
                    ):
                        folder.rmdir()
                source.rmdir()
                job.update(
                    status="Local deleted",
                    local_deleted_at=now(),
                    message="Verified copy retained on braininit",
                )
                store.save(jobs)
                report(f"Deleted local recording; verified copy retained: {source}")
            except Exception as exc:
                errors += 1
                report(f"SKIPPED {job['source']}: {exc}")
        return errors
    finally:
        lock.unlock()
