"""Dry-run-first CLI using the same registration service and queue as the GUI."""

import argparse
import json
from pathlib import Path
import threading

from .registration import DataJointAdapter, describe, local_key
from .registration_queue import preview, signature


def main(argv=None, adapter_factory=DataJointAdapter):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--machine-config", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--subject", required=True, help="Full database subject name")
    parser.add_argument(
        "--date", required=True, help="Behavior session date YYYY-MM-DD"
    )
    parser.add_argument("--session-number", type=int, required=True)
    parser.add_argument("--modality", choices=["imaging", "electrophysiology"])
    parser.add_argument("--processing-id", type=int)
    parser.add_argument("--preprocessing-id", type=int)
    parser.add_argument("--allow-duplicate-session", action="store_true")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Copy, verify and register the displayed plan",
    )
    args = parser.parse_args(argv)
    machine = json.loads(args.machine_config.read_text(encoding="utf-8-sig"))
    from .rigs import load_machine_profile

    load_machine_profile(
        machine, machine.get("state_dir", args.machine_config.parent / "state")
    )
    preferences = dict(
        behavior_session=dict(
            subject_fullname=args.subject,
            session_date=args.date,
            session_number=args.session_number,
        ),
        processing=None if args.processing_id is None else dict(id=args.processing_id),
        preprocessing=None
        if args.preprocessing_id is None
        else dict(id=args.preprocessing_id),
        allow_duplicate_session=args.allow_duplicate_session,
    )
    try:
        plan = preview(
            machine,
            args.source,
            args.modality or machine["modality"],
            preferences,
            adapter_factory,
        )
        print("SUBMIT" if args.yes else "DRY RUN • no copying or database writes")
        print(describe(plan))
        if plan["existing_recording_id"] is not None:
            print(
                f"Already registered as recording_id {plan['existing_recording_id']}; nothing changed."
            )
            return 0
        if not args.yes:
            return 0
        from .app import Store, run_pending

        store = Store(
            Path(machine.get("state_dir", args.machine_config.parent / "state")),
            machine,
        )
        lock = store.lock()
        try:
            matches = [
                job
                for job in store.read()
                if local_key(job["source"]) == local_key(str(args.source.resolve()))
            ]
        finally:
            lock.unlock()
        staged = (
            len(matches) == 1
            and matches[0]["status"] == "Complete"
            and "registration_plan" not in matches[0]
        )
        if matches and not staged:
            if (
                len(matches) != 1
                or "registration_plan" not in matches[0]
                or signature(matches[0]["registration_plan"]) != signature(plan)
            ):
                raise ValueError(
                    "Source is already queued with another intent. Resolve the existing queue entry first."
                )
            job = matches[0]
        else:
            job = store.enqueue(
                args.source,
                Path(machine["incoming_root"]),
                plan["modality"],
                machine.get("quiet_minutes", 10),
                preferences,
                registration_plan=plan,
            )
        errors = run_pending(
            store,
            lambda done, total, message: print(message, flush=True),
            threading.Event(),
            adapter_factory=adapter_factory,
            only_job_id=job["id"],
        )
        status = next(item for item in store.read() if item["id"] == job["id"])
        print(status["status"] + ": " + status["message"])
        return int(bool(errors))
    except Exception as exc:
        print(f"Registration stopped: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
