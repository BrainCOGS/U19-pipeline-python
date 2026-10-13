"""Pre-fill action.ResponsibilitiesAlt from each subject's legacy schedule.

Dry run by default: writes a CSV report of what would be inserted. With
--write, inserts the converted rows. Subjects that already have explicit
responsibilities are never touched, so it is safe to run again.

Only ResponsibilitiesAlt is written, not ResponsibilitiesAltHistoric: a
Historic row means a person saved the subject on the website.

The conversion rules are in u19_pipeline/utils/responsibilities_conversion.py.

    python scripts/convert_legacy_responsibilities.py                # dry run
    python scripts/convert_legacy_responsibilities.py --write        # insert

Subjects whose researcher waters and weighs on paper on weekdays are chosen by
cage and/or owner; there is no default. Repeat either flag, with exact names:

    python scripts/convert_legacy_responsibilities.py \\
        --manual-weekday-cage CAGE --manual-weekday-user USER_ID

Uses the schema prefix from dj.config (u19_ or u19_test_).
"""

import argparse
import csv
import datetime
import re

import datajoint as dj

from u19_pipeline.utils.responsibilities_conversion import plan_conversions

REPORT_COLUMNS = (
    "subject_fullname",
    "user_id",
    "cage",
    "subject_status",
    "tech_responsibility",
    "manual_weekdays",
    "action",
    "schedule",
    "converted_schedule",
    "changed_days",
    "responsibilities",
)


def enum_values(table, attribute: str) -> frozenset[str]:
    """Allowed values of an enum attribute, from the declared table."""
    attribute_type = table.heading.attributes[attribute].type
    return frozenset(re.findall(r"'([^']*)'", attribute_type))


def fetch_subjects(action, subject, lab, today: datetime.date) -> list[dict]:
    """Each non-dead subject's SubjectStatus in effect today, owner and cage."""
    latest = dj.U("subject_fullname").aggr(
        action.SubjectStatus & f'effective_date <= "{today.isoformat()}"',
        effective_date="max(effective_date)",
    )
    status = (action.SubjectStatus & latest) & 'subject_status != "Dead"'
    rows = (
        status.proj("subject_status", "water_per_day", "schedule")
        * subject.Subject.proj("user_id")
        * lab.User.proj("tech_responsibility")
    ).fetch(as_dict=True)
    cages = dict(
        zip(*subject.CagingStatus.fetch("subject_fullname", "cage"), strict=True)
    )
    for row in rows:
        row["cage"] = cages.get(row["subject_fullname"])
    return sorted(rows, key=lambda row: row["subject_fullname"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--write", action="store_true", help="insert the converted rows"
    )
    parser.add_argument(
        "--manual-weekday-cage",
        action="append",
        dest="cages",
        default=[],
        help="cage whose subjects are watered/weighed on paper on weekdays "
        "(repeatable; exact match; no default)",
    )
    parser.add_argument(
        "--manual-weekday-user",
        action="append",
        dest="users",
        default=[],
        help="user_id whose subjects are watered/weighed on paper on weekdays "
        "(repeatable; exact match; no default)",
    )
    parser.add_argument(
        "--report",
        default=f"responsibilities_conversion_{datetime.date.today().isoformat()}.csv",
        help="CSV report path",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    cages = set(args.cages)
    users = set(args.users)

    prefix = dj.config["custom"]["database.prefix"]
    action = dj.create_virtual_module("action", prefix + "action")
    subject = dj.create_virtual_module("subject", prefix + "subject")
    lab = dj.create_virtual_module("lab", prefix + "lab")

    today = datetime.date.today()
    plans = plan_conversions(
        fetch_subjects(action, subject, lab, today),
        existing=set(action.ResponsibilitiesAlt.fetch("subject_fullname")),
        manual_weekday_cages=cages,
        manual_weekday_users=users,
        allowed_statuses=enum_values(action.ResponsibilitiesAlt, "subject_status"),
    )

    with open(args.report, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, REPORT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(plans)

    inserts = [p["record"] for p in plans if p["action"] == "insert"]
    counts: dict[str, int] = {}
    for p in plans:
        key = p["action"].split(":")[0]
        counts[key] = counts.get(key, 0) + 1
    print(f"Database prefix {prefix!r}; report written to {args.report}")
    print(", ".join(f"{n} {key}" for key, n in sorted(counts.items())))
    manual = [p["subject_fullname"] for p in plans if p["manual_weekdays"]]
    print(
        f"Manual weekday watering/weighing (cages {', '.join(sorted(cages)) or 'none'}; "
        f"users {', '.join(sorted(users)) or 'none'}): {', '.join(manual) or 'none'}"
    )
    for p in plans:
        if p["action"].startswith("error"):
            print(f"  {p['subject_fullname']}: {p['action']}")

    if not args.write:
        print("Dry run: nothing written. Re-run with --write to insert.")
        return
    with dj.conn().transaction:
        action.ResponsibilitiesAlt.insert(inserts)
    print(f"Inserted {len(inserts)} rows into {prefix}action.responsibilities_alt")


if __name__ == "__main__":
    main()
