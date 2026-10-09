"""Convert legacy SubjectStatus.schedule strings into explicit responsibilities.

The legacy schedule only says what the technician does each day ("Train",
"Water", "Weigh", "Transport", "Nothing"; Sunday first). How that splits
between the technician and the researcher follows
notebooks/create_tech_responsibilites_table.ipynb, with its day offset fixed
(it read the Sunday entry as Monday) and every day made valid for the website:

- Owner's lab has technician support (lab.User.tech_responsibility "yes"):
  Train  -> technician: Watering, Weighing, Training (no Training when water-
            restricted only); researcher: Nothing
  Water, Weigh -> technician: Watering, Weighing; researcher: Nothing
  Transport -> technician: Transport Only; researcher: the status's duties
  Nothing -> technician: Nothing; researcher: Watering, Weighing
- No technician support: technician: Transport Only every day; researcher:
  the status's duties on Train and Transport days, otherwise Watering, Weighing.
- AdLibWater, Dead and Missing subjects: Nothing for both, whatever the schedule.

Optionally, weekday watering and weighing become the researcher's Manual
Watering and Manual Weighing (done on paper; never alerted on).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field

from u19_pipeline.utils.responsibilities import (
    DAYS_OF_WEEK,
    WEEKEND_DAYS,
    AnyResponsibility,
    AssignmentStyle,
    DailyAssignment,
    ManualResponsibility,
    Responsibility,
    ResponsibleParty,
    dump_responsibilities,
    slot_keys,
    validate_daily_assignment,
)

LEGACY_TOKENS = ("Train", "OnlyTrain", "Water", "Weigh", "Transport", "Nothing")
WATER_RESTRICTED_STATUSES = frozenset({"InExperiments", "WaterRestrictionOnly"})
INACTIVE_STATUSES = frozenset({"AdLibWater", "Dead", "Missing"})

WATER_WEIGH = frozenset({Responsibility.WATERING, Responsibility.WEIGHING})
NOTHING = frozenset({Responsibility.NOTHING})
TRANSPORT = frozenset({Responsibility.TRANSPORT_ONLY})
STATUS_DUTIES = {
    "InExperiments": WATER_WEIGH | {Responsibility.TRAINING},
    "WaterRestrictionOnly": WATER_WEIGH,
}
MANUAL = {
    Responsibility.WATERING: ManualResponsibility.MANUAL_WATERING,
    Responsibility.WEIGHING: ManualResponsibility.MANUAL_WEIGHING,
}


@dataclass(frozen=True)
class Conversion:
    """A converted subject; json is None when error says why it could not be."""

    json: str | None = None
    days: dict[str, DailyAssignment] = field(default_factory=dict)
    legacy_schedule: str | None = None
    error: str | None = None


def _assignment(
    technician: frozenset[AnyResponsibility], owner: frozenset[AnyResponsibility]
) -> DailyAssignment:
    return DailyAssignment(technician=technician, owner=owner)


def convert_day(token: str, status: str, technician_supported: bool) -> DailyAssignment:
    """One legacy day for an active (water-restricted) subject."""
    status_duties = STATUS_DUTIES[status]
    if not technician_supported:
        owner = (
            status_duties
            if token in ("Train", "OnlyTrain", "Transport")
            else WATER_WEIGH
        )
        return _assignment(TRANSPORT, owner)
    match token:
        case "Train" | "OnlyTrain":
            return _assignment(status_duties, NOTHING)
        case "Water" | "Weigh":
            return _assignment(WATER_WEIGH, NOTHING)
        case "Transport":
            return _assignment(TRANSPORT, status_duties)
        case "Nothing":
            return _assignment(NOTHING, WATER_WEIGH)
        case _:
            raise ValueError(f"Unknown legacy duty {token!r}")


def make_manual(assignment: DailyAssignment) -> DailyAssignment:
    """Move watering and weighing to the researcher, done on paper."""
    technician = frozenset(assignment.technician - set(MANUAL))
    owner = frozenset((assignment.owner - set(MANUAL) - NOTHING) | set(MANUAL.values()))
    return _assignment(technician or NOTHING, owner)


def simplest_style(
    days: Mapping[str, DailyAssignment],
) -> tuple[AssignmentStyle, dict[str, DailyAssignment]]:
    """The simplest style that represents these seven days, and its slots."""
    weekdays = {days[d] for d in DAYS_OF_WEEK if d not in WEEKEND_DAYS}
    weekends = {days[d] for d in WEEKEND_DAYS}
    if len(weekdays | weekends) == 1:
        return AssignmentStyle.WEEKLY, {"weekly": days["Monday"]}
    if len(weekdays) == 1 and len(weekends) == 1:
        return AssignmentStyle.WORKWEEK_SPLIT, {
            "weekdays": days["Monday"],
            "weekends": days["Sunday"],
        }
    return AssignmentStyle.DAILY, {day: days[day] for day in DAYS_OF_WEEK}


def convert_legacy(
    schedule: str | None,
    status: str,
    technician_supported: bool,
    manual_weekdays: bool,
) -> Conversion:
    """Convert one subject's legacy schedule to the canonical JSON.

    Args:
        schedule: SubjectStatus.schedule, seven "/"-separated duties, Sunday first.
        status: SubjectStatus.subject_status.
        technician_supported: whether the owner's lab has technician support
            (lab.User.tech_responsibility == "yes").
        manual_weekdays: make weekday watering and weighing manual (paper).
    """
    if status in INACTIVE_STATUSES:
        days = {day: _assignment(NOTHING, NOTHING) for day in DAYS_OF_WEEK}
    elif status in STATUS_DUTIES:
        if not schedule:
            return Conversion(error="no schedule")
        tokens = schedule.split("/")
        if len(tokens) != len(DAYS_OF_WEEK):
            return Conversion(error=f"expected 7 days, got {len(tokens)}: {schedule!r}")
        try:
            days = {
                day: convert_day(token.strip(), status, technician_supported)
                for day, token in zip(DAYS_OF_WEEK, tokens, strict=True)
            }
        except ValueError as e:
            return Conversion(error=str(e))
        if manual_weekdays:
            days = {
                day: a if day in WEEKEND_DAYS else make_manual(a)
                for day, a in days.items()
            }
    else:
        return Conversion(error=f"unknown status {status!r}")

    require_watering = status in WATER_RESTRICTED_STATUSES
    for day, a in days.items():
        if errors := validate_daily_assignment(a.technician, a.owner, require_watering):
            return Conversion(error=f"{day}: {'; '.join(errors)}")

    style, slots = simplest_style(days)
    party_keys = {
        ResponsibleParty.TECHNICIAN.json_key: "technician",
        ResponsibleParty.OWNER.json_key: "owner",
    }
    record = {
        key: {
            json_key: getattr(slots[key], attr) for json_key, attr in party_keys.items()
        }
        for key in slot_keys(style)
    }
    return Conversion(
        json=dump_responsibilities(style, record),
        days=days,
        legacy_schedule="/".join(days[day].legacy_token() for day in DAYS_OF_WEEK),
    )


def plan_conversions(
    subjects: list[dict],
    existing: set[str],
    manual_weekday_cages: set[str],
    allowed_statuses: frozenset[str],
) -> list[dict]:
    """What to do for each subject, as rows for a review report.

    Args:
        subjects: one dict per subject with subject_fullname, user_id, cage,
            subject_status, water_per_day, schedule (the latest
            SubjectStatus) and tech_responsibility (the owner's lab.User).
        existing: subjects that already have a ResponsibilitiesAlt row; they
            are never overwritten.
        manual_weekday_cages: cages (exact names) whose subjects are watered
            and weighed on paper by the researcher on weekdays.
        allowed_statuses: subject_status values ResponsibilitiesAlt accepts.

    Each row's action is "insert", "skip: ..." or "error: ..."; record is the
    ResponsibilitiesAlt row to insert, or None.
    """
    plans = []
    for subject in subjects:
        name = subject["subject_fullname"]
        status = subject["subject_status"]
        manual = subject.get("cage") in manual_weekday_cages
        row = {
            "subject_fullname": name,
            "user_id": subject.get("user_id"),
            "cage": subject.get("cage"),
            "subject_status": status,
            "tech_responsibility": subject.get("tech_responsibility"),
            "manual_weekdays": manual,
            "schedule": subject.get("schedule"),
            "converted_schedule": None,
            "changed_days": "",
            "responsibilities": None,
            "record": None,
        }
        plans.append(row)
        if name in existing:
            row["action"] = "skip: already has explicit responsibilities"
            continue
        if status not in allowed_statuses:
            row["action"] = (
                f"skip: ResponsibilitiesAlt does not accept status {status!r}"
            )
            continue
        result = convert_legacy(
            subject.get("schedule"),
            status,
            technician_supported=subject.get("tech_responsibility") == "yes",
            manual_weekdays=manual,
        )
        if result.error:
            row["action"] = f"error: {result.error}"
            continue
        before = (subject.get("schedule") or "").split("/")
        after = (result.legacy_schedule or "").split("/")
        row["converted_schedule"] = result.legacy_schedule
        row["changed_days"] = (
            ", ".join(
                day
                for day, old, new in zip(DAYS_OF_WEEK, before, after, strict=False)
                if old.strip() != new
            )
            if len(before) == len(DAYS_OF_WEEK)
            else ""
        )
        row["responsibilities"] = result.json
        row["record"] = {
            "subject_fullname": name,
            "subject_status": status,
            "water_per_day": subject.get("water_per_day"),
            "responsibilities": result.json,
        }
        row["action"] = "insert"
    return plans
