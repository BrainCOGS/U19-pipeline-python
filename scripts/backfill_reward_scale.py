#!/usr/bin/env python3
"""Backfill behavior.TowersBlock.reward_scale from the ViRMEn behavior logs.

reward_scale was declared `tinyint`, but ViRMEn moves the reward scale in 0.2
steps (see ViRMEn experiments/common/processKeypress.m and autoAdjustReward.m),
so every fractional value was collapsed to an integer on insert.  Once the
column has been widened to float, this script re-reads the source .mat logs and
restores the real values.

Finding the log for each session, in order:
  1. --path-map CSV     an explicit override for that session
  2. the recorded path  acquisition.SessionStarted.new_remote_path_behavior_file
  3. --search-dir DIR   a file with the same name anywhere under DIR

A log that did not come from the recorded path must prove it belongs to the
session: log.animal.name must equal the subject and log.session.start must be
within two minutes of the session's start time.  If several searched files pass,
the session is reported as ambiguous and skipped rather than guessed.

Each block is then verified before it is touched: the value in the database must
be the rounded or truncated form of the value in the file, and the block's
first_trial must agree.  Anything that fails is reported and left alone.

Runs read-only unless --commit is passed.

    python scripts/backfill_reward_scale.py --user efonseca
    python scripts/backfill_reward_scale.py --user efonseca --write-missing missing.csv
    python scripts/backfill_reward_scale.py --user efonseca --search-dir /mnt/archive/efonseca
    python scripts/backfill_reward_scale.py --user efonseca --path-map missing.csv --commit
"""

import argparse
import sys
from collections import Counter, defaultdict
from pathlib import Path

from scripts.conf_file_finding import try_find_conf_file

try_find_conf_file()

from u19_pipeline import acquisition, behavior, lab, subject  # noqa: E402
from u19_pipeline.utils import reward_scale_backfill as rsb  # noqa: E402
from u19_pipeline.utils.matlab_utils import convert_mat_file_to_dict  # noqa: E402

RECORDED = "recorded"
PATH_MAP = "path_map"
SEARCH = "search"


def load_log(path):
    return convert_mat_file_to_dict(str(path))["log"]


def resolve_log(row, path_table, path_map, search_index):
    """Find and load the log for one session.

    Returns (log, path, source, problem).  On failure log is None and problem
    says why; on success problem is None.
    """
    key = rsb.session_key(row["subject_fullname"], row["session_date"], row["session_number"])
    subject_fullname = row["subject_fullname"]
    start_time = row["session_start_time"]
    recorded = row["new_remote_path_behavior_file"]

    # An explicit override is authoritative: if it fails identity, report it
    # rather than silently falling through to some other file.
    if key in path_map:
        path = path_map[key]
        try:
            log = load_log(path)
        except Exception as err:
            return None, path, PATH_MAP, f"--path-map file unreadable: {type(err).__name__}: {err}"
        ok, reason = rsb.check_identity(log, subject_fullname, start_time, require=True)
        if not ok:
            return None, path, PATH_MAP, f"--path-map file rejected: {reason}"
        return log, path, PATH_MAP, None

    problems = []

    recorded_local = None
    try:
        recorded_local = Path(path_table.get_local_path2(recorded))
    except Exception as err:
        problems.append(f"recorded path not translatable: {type(err).__name__}: {err}")

    if recorded_local is not None:
        if recorded_local.is_file():
            try:
                log = load_log(recorded_local)
            except Exception as err:
                problems.append(f"recorded file unreadable: {type(err).__name__}: {err}")
            else:
                ok, reason = rsb.check_identity(log, subject_fullname, start_time, require=False)
                if ok:
                    return log, recorded_local, RECORDED, None
                problems.append(f"recorded file belongs to another session: {reason}")
        else:
            problems.append(f"recorded file not found at {recorded_local}")

    name = rsb.recorded_basename(recorded)
    candidates = [p for p in search_index.get(name, []) if p != recorded_local] if name else []
    passing = []
    for path in candidates:
        try:
            log = load_log(path)
        except Exception:
            continue
        if rsb.check_identity(log, subject_fullname, start_time, require=True)[0]:
            passing.append((path, log))

    if len(passing) == 1:
        path, log = passing[0]
        return log, path, SEARCH, None
    if len(passing) > 1:
        paths = ", ".join(str(p) for p, _ in passing)
        return None, None, SEARCH, f"ambiguous: {len(passing)} searched files match this session ({paths})"
    if candidates:
        problems.append(f"{len(candidates)} searched file(s) named {name} but none belong to this session")

    return None, None, None, "; ".join(problems) or "no log found"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--commit", action="store_true",
                        help="write the values (default is a read-only audit)")
    parser.add_argument("--after", metavar="YYYY-MM-DD",
                        help="only sessions on or after this date")
    parser.add_argument("--before", metavar="YYYY-MM-DD",
                        help="only sessions on or before this date")
    parser.add_argument("--user", nargs="+", metavar="USER_ID",
                        help="restrict to subjects owned by these lab.User ids")
    parser.add_argument("--subject", nargs="+", metavar="SUBJECT_FULLNAME",
                        help="restrict to these subject_fullnames")
    parser.add_argument("--limit", type=int,
                        help="stop after this many sessions (for a trial run)")
    parser.add_argument("--include-fallback", action="store_true",
                        help="also update blocks whose stored value is 0")
    parser.add_argument("--path-map", metavar="CSV", type=Path,
                        help="explicit session -> log file overrides "
                             "(columns: subject_fullname, session_date, session_number, path)")
    parser.add_argument("--search-dir", metavar="DIR", type=Path, action="append", default=[],
                        help="directory to search for logs missing from their recorded path; "
                             "repeatable")
    parser.add_argument("--write-missing", metavar="CSV", type=Path,
                        help="write sessions whose log could not be found, ready to fill in "
                             "and pass back as --path-map")
    args = parser.parse_args(argv)

    path_map = rsb.load_path_map(args.path_map) if args.path_map else {}

    if args.search_dir:
        print(f"Indexing {len(args.search_dir)} search director(y/ies)...", flush=True)
    search_index = rsb.build_basename_index(args.search_dir)
    if args.search_dir:
        print(f"  {sum(len(v) for v in search_index.values())} .mat files indexed")

    restriction = []
    if args.after:
        restriction.append(f'session_date >= "{args.after}"')
    if args.before:
        restriction.append(f'session_date <= "{args.before}"')
    if args.subject:
        names = ", ".join(f'"{name}"' for name in args.subject)
        restriction.append(f"subject_fullname IN ({names})")

    blocks = behavior.TowersBlock.proj("reward_scale", "first_trial")
    query = acquisition.SessionStarted.proj(
        "new_remote_path_behavior_file", "session_start_time") * blocks
    if restriction:
        query = query & " AND ".join(restriction)

    # Ownership lives on subject.Subject -> lab.User.  Restricting by the
    # subject_fullname prefix would look equivalent but is not: the prefix is a
    # naming convention, while the foreign key is what actually says who owns a
    # mouse, and subjects do get reassigned between users.
    if args.user:
        owners = ", ".join(f'"{user_id}"' for user_id in args.user)
        owned = subject.Subject & f"user_id IN ({owners})"
        if not owned:
            print(f"No subjects found for user(s): {', '.join(args.user)}")
            return 1
        query = query & owned

    rows = query.fetch(as_dict=True, order_by="subject_fullname, session_date, "
                                              "session_number, block")
    if not rows:
        print("No blocks matched.")
        return 0

    # One file read per session, not per block.
    sessions = defaultdict(list)
    for row in rows:
        sessions[rsb.session_key(row["subject_fullname"], row["session_date"],
                                 row["session_number"])].append(row)

    unused_overrides = set(path_map) - set(sessions)

    session_keys = sorted(sessions)
    if args.limit:
        session_keys = session_keys[:args.limit]

    subjects = sorted({key[0] for key in session_keys})
    print(f"{len(rows)} blocks across {len(sessions)} sessions "
          f"({len(session_keys)} to be processed)")
    print(f"{len(subjects)} subject(s): {', '.join(subjects)}")
    if not args.commit:
        print("AUDIT ONLY -- pass --commit to write")
    print()

    path_table = lab.Path()
    counts = Counter()
    sources = Counter()
    problems = []
    missing = []
    relocated = []

    for i, key in enumerate(session_keys, start=1):
        session_rows = sessions[key]
        first = session_rows[0]
        subject_fullname, session_date, session_number = key
        label = f"{subject_fullname} {session_date} #{session_number}"

        if i % 25 == 0 or i == len(session_keys):
            print(f"  [{i}/{len(session_keys)}] {label}", flush=True)

        log, path, source, problem = resolve_log(first, path_table, path_map, search_index)
        if log is None:
            counts["unresolved_session"] += 1
            problems.append((label, None, "unresolved", problem))
            missing.append((subject_fullname, session_date, session_number,
                            first["new_remote_path_behavior_file"]))
            continue
        sources[source] += 1
        if source != RECORDED:
            relocated.append((label, source, path))

        for row in session_rows:
            file_scale, file_first_trial = rsb.read_block_values(log, row["block"])
            if file_scale is None and file_first_trial is None:
                verdict = rsb.NO_BLOCK
            else:
                verdict = rsb.classify(row["reward_scale"], row["first_trial"],
                                       file_scale, file_first_trial)
            counts[verdict] += 1

            if verdict == rsb.MISMATCH:
                problems.append((label, row["block"], rsb.MISMATCH,
                                 f"db={row['reward_scale']} file={file_scale} "
                                 f"db_first_trial={row['first_trial']} "
                                 f"file_first_trial={file_first_trial} ({path})"))
                continue
            if verdict in (rsb.UNCHANGED, rsb.NO_VALUE, rsb.NO_BLOCK):
                continue
            if verdict == rsb.FALLBACK and not args.include_fallback:
                continue

            if args.commit:
                behavior.TowersBlock.update1({
                    "subject_fullname": subject_fullname,
                    "session_date": session_date,
                    "session_number": session_number,
                    "block": row["block"],
                    "reward_scale": file_scale,
                })
                counts["written"] += 1

    print("\n--- logs found via ---")
    for source in (RECORDED, PATH_MAP, SEARCH):
        if sources[source]:
            print(f"  {source:20s} {sources[source]}")

    print("\n--- blocks ---")
    for name in (rsb.OK, rsb.UNCHANGED, rsb.FALLBACK, rsb.MISMATCH, rsb.NO_VALUE,
                 rsb.NO_BLOCK, "unresolved_session", "written"):
        if counts[name]:
            print(f"  {name:20s} {counts[name]}")

    if relocated:
        print(f"\n--- {len(relocated)} session(s) read from outside the recorded path ---")
        for label, source, path in relocated[:50]:
            print(f"  {source:10s} {label}: {path}")
        if len(relocated) > 50:
            print(f"  ... and {len(relocated) - 50} more")

    if problems:
        print(f"\n--- {len(problems)} blocks/sessions needing attention ---")
        for label, block, kind, detail in problems[:50]:
            where = f"{label} block {block}" if block is not None else label
            print(f"  {kind:12s} {where}: {detail}")
        if len(problems) > 50:
            print(f"  ... and {len(problems) - 50} more")

    if unused_overrides:
        print(f"\nWARNING: {len(unused_overrides)} --path-map row(s) match no session in scope, "
              f"e.g. {sorted(unused_overrides)[0]}")

    if args.write_missing and missing:
        rsb.write_missing_csv(args.write_missing, missing)
        print(f"\nWrote {len(missing)} unresolved session(s) to {args.write_missing}. "
              f"Fill in the path column and re-run with --path-map {args.write_missing}.")

    if not args.commit and counts[rsb.OK]:
        print(f"\n{counts[rsb.OK]} blocks would be updated. Re-run with --commit.")

    return 1 if counts[rsb.MISMATCH] else 0


if __name__ == "__main__":
    sys.exit(main())
