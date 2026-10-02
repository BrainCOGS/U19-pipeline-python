"""Recurring schedule rules and their expansion into ``scheduler.Schedule`` rows.

``scheduler.Schedule`` holds one concrete row per (date, location, timeslot). Until
now the only way to fill future days was the nightly MATLAB job
``populate_schedule_for_tomorrow.m``, which copies today's rows to tomorrow. That
cannot express "run from date X to date Y" or "weekdays only".

``scheduler.ScheduleRule`` stores that intent instead. This module turns rules into
the rows that should exist in ``scheduler.Schedule`` over a window of dates, and
works out the inserts, updates and deletes needed to get there. Everything here is
pure Python on plain dicts and dataclasses -- no DataJoint import -- so it can be
unit tested without a database. ``apply_plan`` is the only function that writes,
and it takes the ``scheduler`` module as an argument.

Ownership rules for ``scheduler.Schedule`` rows inside the window:

* ``rule_id IS NULL`` rows were made by hand or by the legacy copy-forward job.
  The generator never modifies or deletes them. A rule that wants the same slot
  is reported as a conflict, unless the row is an empty slot (``subject_fullname
  IS NULL``), which the rule may fill.
* Rows whose ``rule_id`` matches a rule are owned by that rule. They are updated
  when the rule changes and deleted when the rule no longer occurs on that date --
  unless ``ScheduleRuleException`` marks that (rule, date) as ``override``, in
  which case the row is left exactly as it is.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

# Order matches ``datetime.date.weekday()``: Monday == 0.
WEEKDAY_NAMES: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

WEEKDAYS: frozenset[str] = frozenset(WEEKDAY_NAMES[:5])
WEEKENDS: frozenset[str] = frozenset(WEEKDAY_NAMES[5:])
EVERY_DAY: frozenset[str] = frozenset(WEEKDAY_NAMES)

PRESETS: dict[str, frozenset[str]] = {
    "Every day": EVERY_DAY,
    "Weekdays": WEEKDAYS,
    "Weekends": WEEKENDS,
}

ACTIVE = "active"
RULE_STATUSES = ("active", "ended", "cancelled")
EXCEPTION_ACTIONS = ("skip", "override")

SLOT_KEYS = ("date", "location", "timeslot")

# Matches ScheduleRule.experimenters_instructions; Schedule's own column is wider.
MAX_INSTRUCTIONS_LENGTH = 4096

# Secondary attributes of ``scheduler.Schedule`` that a rule controls.
PAYLOAD_KEYS = (
    "subject_fullname",
    "training_profile_id",
    "recording_profile_id",
    "input_output_profile_id",
    "experimenters_instructions",
    "level",
    "sublevel",
)


def weekday_name(day: datetime.date) -> str:
    return WEEKDAY_NAMES[day.weekday()]


def parse_weekdays(tokens: Iterable[str]) -> frozenset[str]:
    """Normalise weekday tokens ("mon", "MONDAY", "Mon") to the enum spelling.

    Raises ``ValueError`` on anything that is not a weekday, so a typo cannot
    silently produce a rule that never runs.
    """
    lookup = {name.lower(): name for name in WEEKDAY_NAMES}
    lookup.update(
        {
            datetime.date(2024, 1, 1 + i).strftime("%A").lower(): name
            for i, name in enumerate(WEEKDAY_NAMES)
        }
    )
    parsed = set()
    for token in tokens:
        key = str(token).strip().lower()
        if key not in lookup:
            raise ValueError(f"Not a weekday: {token!r}")
        parsed.add(lookup[key])
    return frozenset(parsed)


@dataclass(frozen=True)
class ScheduleRule:
    """In-memory mirror of one ``scheduler.ScheduleRule`` row plus its ``Day`` parts."""

    rule_id: int
    subject_fullname: str
    location: str
    timeslot: int
    start_date: datetime.date
    end_date: datetime.date | None
    weekdays: frozenset[str]
    training_profile_id: int
    recording_profile_id: int
    input_output_profile_id: int
    experimenters_instructions: str = ""
    level: int = 0
    sublevel: int = 0
    status: str = ACTIVE

    def payload(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in PAYLOAD_KEYS}

    @property
    def is_active(self) -> bool:
        return self.status == ACTIVE


def validate_rule(rule: ScheduleRule) -> list[str]:
    """Return human-readable problems with ``rule``; an empty list means it is valid."""
    problems = []
    if rule.end_date is not None and rule.end_date < rule.start_date:
        problems.append(
            f"End date {rule.end_date} is before start date {rule.start_date}."
        )
    if not rule.weekdays:
        problems.append("Pick at least one day of the week.")
    unknown = set(rule.weekdays) - set(WEEKDAY_NAMES)
    if unknown:
        problems.append(f"Unknown weekday(s): {', '.join(sorted(unknown))}.")
    if rule.status not in RULE_STATUSES:
        problems.append(f"Unknown status {rule.status!r}.")
    if rule.timeslot < 1:
        problems.append(f"Timeslot must be positive, got {rule.timeslot}.")
    if len(rule.experimenters_instructions) > MAX_INSTRUCTIONS_LENGTH:
        problems.append(
            f"Experimenter's instructions are limited to {MAX_INSTRUCTIONS_LENGTH} characters."
        )
    if rule.level < 0 or rule.sublevel < 0:
        problems.append("Level and sublevel must be 0 (auto) or positive.")
    return problems


def rule_occurs_on(rule: ScheduleRule, day: datetime.date) -> bool:
    """Whether an active ``rule`` wants a session on ``day`` (ignoring exceptions)."""
    if not rule.is_active:
        return False
    if day < rule.start_date:
        return False
    if rule.end_date is not None and day > rule.end_date:
        return False
    return weekday_name(day) in rule.weekdays


def date_ranges_overlap(
    a_start: datetime.date,
    a_end: datetime.date | None,
    b_start: datetime.date,
    b_end: datetime.date | None,
) -> bool:
    """Inclusive overlap test where ``None`` as an end date means open-ended."""
    a_before_b = a_end is not None and a_end < b_start
    b_before_a = b_end is not None and b_end < a_start
    return not (a_before_b or b_before_a)


def find_rule_conflicts(
    candidate: ScheduleRule, existing: Iterable[ScheduleRule]
) -> list[ScheduleRule]:
    """Active rules that would book the same rig slot on at least one common date.

    Two rules conflict when they share location and timeslot, their date ranges
    overlap, and they share a weekday. The candidate itself (same ``rule_id``, as
    when editing a rule) is ignored.
    """
    if not candidate.is_active:
        return []
    conflicts = []
    for other in existing:
        if other.rule_id == candidate.rule_id or not other.is_active:
            continue
        if (other.location, other.timeslot) != (candidate.location, candidate.timeslot):
            continue
        if not candidate.weekdays & other.weekdays:
            continue
        if not date_ranges_overlap(
            candidate.start_date, candidate.end_date, other.start_date, other.end_date
        ):
            continue
        if _first_shared_date(candidate, other) is None:
            continue
        conflicts.append(other)
    return conflicts


def _first_shared_date(a: ScheduleRule, b: ScheduleRule) -> datetime.date | None:
    """First date both rules occur on. Short ranges can overlap without sharing a weekday."""
    start = max(a.start_date, b.start_date)
    ends = [end for end in (a.end_date, b.end_date) if end is not None]
    # Shared weekdays repeat weekly, so an open-ended overlap resolves within 7 days.
    stop = min([*ends, start + datetime.timedelta(days=6)])
    day = start
    while day <= stop:
        if rule_occurs_on(a, day) and rule_occurs_on(b, day):
            return day
        day += datetime.timedelta(days=1)
    return None


def iter_dates(start: datetime.date, end: datetime.date) -> Iterable[datetime.date]:
    """Inclusive range of dates; empty when ``end < start``."""
    day = start
    while day <= end:
        yield day
        day += datetime.timedelta(days=1)


def expand_rule(
    rule: ScheduleRule,
    window_start: datetime.date,
    window_end: datetime.date,
    skip_dates: Iterable[datetime.date] = (),
    closures: Iterable[datetime.date] = (),
) -> list[datetime.date]:
    """Dates in ``[window_start, window_end]`` on which ``rule`` should produce a row."""
    blocked = set(skip_dates) | set(closures)
    return [
        day
        for day in iter_dates(window_start, window_end)
        if rule_occurs_on(rule, day) and day not in blocked
    ]


def expected_subjects_on(rules: Iterable[ScheduleRule], day: datetime.date) -> set[str]:
    """Subjects that some active rule schedules on ``day``."""
    return {rule.subject_fullname for rule in rules if rule_occurs_on(rule, day)}


def subjects_resting_on(
    rules: Iterable[ScheduleRule], today: datetime.date, tomorrow: datetime.date
) -> set[str]:
    """Subjects a rule runs today but deliberately not tomorrow (e.g. weekday-only on a Friday).

    The schedule-check alert uses this so a planned weekend drop is not reported as
    a suspicious schedule.
    """
    rules = list(rules)
    return expected_subjects_on(rules, today) - expected_subjects_on(rules, tomorrow)


def drop_planned_rest(
    schedule_df: Any, today: datetime.date, resting_subjects: set[str]
) -> Any:
    """Remove today's rows for subjects that are planned off tomorrow.

    The alert compares tomorrow's head-count against today's; without this a
    weekday-only cohort makes every Friday look like a broken schedule.
    """
    if schedule_df.empty or not resting_subjects:
        return schedule_df
    planned = (schedule_df["date"] == today) & schedule_df["subject_fullname"].isin(
        resting_subjects
    )
    return schedule_df.loc[~planned].reset_index(drop=True)


@dataclass(frozen=True)
class Conflict:
    date: datetime.date
    location: str
    timeslot: int
    rule_id: int
    existing_subject: str | None
    existing_rule_id: int | None


@dataclass
class MaterializationPlan:
    inserts: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[Conflict] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (self.inserts or self.updates or self.deletes)


def _slot(row: Mapping[str, Any]) -> tuple[datetime.date, str, int]:
    return (row["date"], row["location"], row["timeslot"])


def plan_materialization(
    rules: Iterable[ScheduleRule],
    existing_rows: Iterable[Mapping[str, Any]],
    window_start: datetime.date,
    window_end: datetime.date,
    exceptions: Mapping[tuple[int, datetime.date], str] | None = None,
    closures: Iterable[datetime.date] = (),
) -> MaterializationPlan:
    """Work out the writes that bring ``scheduler.Schedule`` in line with ``rules``.

    ``existing_rows`` are the ``scheduler.Schedule`` rows in the window (rows outside
    it are ignored, so the past is never rewritten). ``exceptions`` maps
    ``(rule_id, date)`` to ``"skip"`` or ``"override"``. ``closures`` are lab-wide
    dates on which no rule produces a row.
    """
    exceptions = dict(exceptions or {})
    closures = set(closures)
    plan = MaterializationPlan()
    if window_end < window_start:
        return plan

    in_window = [
        row for row in existing_rows if window_start <= row["date"] <= window_end
    ]
    by_slot: dict[tuple[datetime.date, str, int], Mapping[str, Any]] = {
        _slot(row): row for row in in_window
    }

    rules = sorted(rules, key=lambda r: r.rule_id)
    active_ids = {rule.rule_id for rule in rules if rule.is_active}

    def is_stale(row: Mapping[str, Any]) -> bool:
        """Owned by a rule that is no longer active and was not hand-edited."""
        owner = row.get("rule_id")
        return (
            owner is not None
            and owner not in active_ids
            and exceptions.get((owner, row["date"])) != "override"
        )

    claimed: set[tuple[datetime.date, str, int]] = set()
    for rule in rules:
        skips = [
            day
            for (rule_id, day), action in exceptions.items()
            if rule_id == rule.rule_id and action == "skip"
        ]
        for day in expand_rule(
            rule, window_start, window_end, skip_dates=skips, closures=closures
        ):
            slot = (day, rule.location, rule.timeslot)
            if exceptions.get((rule.rule_id, day)) == "override":
                claimed.add(slot)
                continue
            desired = {
                "date": day,
                "location": rule.location,
                "timeslot": rule.timeslot,
                **rule.payload(),
            }
            desired["rule_id"] = rule.rule_id
            current = by_slot.get(slot)

            if slot in claimed:
                # Two active rules on one slot; validation should have stopped this.
                plan.conflicts.append(
                    Conflict(
                        day, rule.location, rule.timeslot, rule.rule_id, None, None
                    )
                )
                continue
            if current is None:
                plan.inserts.append(desired)
            elif current.get("rule_id") == rule.rule_id:
                if any(current.get(key) != desired[key] for key in PAYLOAD_KEYS):
                    plan.updates.append(desired)
            elif (
                current.get("rule_id") is None
                and current.get("subject_fullname") is None
            ) or is_stale(current):
                plan.updates.append(desired)
            else:
                plan.conflicts.append(
                    Conflict(
                        day,
                        rule.location,
                        rule.timeslot,
                        rule.rule_id,
                        current.get("subject_fullname"),
                        current.get("rule_id"),
                    )
                )
                continue
            claimed.add(slot)

    for row in in_window:
        rule_id = row.get("rule_id")
        if rule_id is None or _slot(row) in claimed:
            continue
        if exceptions.get((rule_id, row["date"])) == "override":
            continue
        plan.deletes.append({key: row[key] for key in SLOT_KEYS})

    return plan


def apply_plan(scheduler: Any, plan: MaterializationPlan) -> None:
    """Write ``plan`` to the database in one transaction.

    ``scheduler`` is the DataJoint module (``u19_pipeline.scheduler`` or a virtual
    module) so callers choose the schema prefix.
    """
    connection = scheduler.Schedule.connection
    with connection.transaction:
        for key in plan.deletes:
            (scheduler.Schedule & key).delete_quick()
        for row in plan.updates:
            scheduler.Schedule.update1(row)
        if plan.inserts:
            scheduler.Schedule.insert(plan.inserts)


def rules_from_rows(
    rule_rows: Iterable[Mapping[str, Any]], day_rows: Iterable[Mapping[str, Any]]
) -> list[ScheduleRule]:
    """Build rules from ``ScheduleRule`` fetches and its ``ScheduleRule.Day`` part rows."""
    days: dict[int, set[str]] = {}
    for row in day_rows:
        days.setdefault(row["rule_id"], set()).add(row["weekday"])
    field_names = ScheduleRule.__dataclass_fields__.keys() - {"weekdays"}
    return [
        ScheduleRule(
            weekdays=frozenset(days.get(row["rule_id"], ())),
            **{key: row[key] for key in field_names if key in row},
        )
        for row in rule_rows
    ]


DEFAULT_HORIZON_DAYS = 14


def materialize_schedule(
    scheduler: Any,
    today: datetime.date | None = None,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    include_today: bool = False,
    dry_run: bool = False,
) -> MaterializationPlan:
    """Fetch rules and existing rows, plan the window, and (unless ``dry_run``) apply it.

    The nightly job runs with ``include_today=False`` so sessions already underway
    are never touched; the web app passes ``include_today=True`` right after a user
    saves a rule that starts today.
    """
    today = today or datetime.date.today()
    window_start = today if include_today else today + datetime.timedelta(days=1)
    window_end = today + datetime.timedelta(days=horizon_days)

    rules = rules_from_rows(
        (scheduler.ScheduleRule & {"status": ACTIVE}).fetch(as_dict=True),
        scheduler.ScheduleRule.Day.fetch(as_dict=True),
    )
    window = f'date between "{window_start.isoformat()}" and "{window_end.isoformat()}"'
    existing = (scheduler.Schedule & window).fetch(as_dict=True)
    exceptions = {
        (row["rule_id"], row["date"]): row["action"]
        for row in (scheduler.ScheduleRuleException & window).fetch(
            "rule_id", "date", "action", as_dict=True
        )
    }
    closures = (scheduler.LabClosure & window).fetch("date")

    plan = plan_materialization(
        rules, existing, window_start, window_end, exceptions, closures
    )
    if not dry_run and not plan.is_empty:
        apply_plan(scheduler, plan)
    return plan
