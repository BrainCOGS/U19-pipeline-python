"""Nightly scheduler housekeeping, ported from U19-pipeline-matlab.

Each ``main_*`` function replaces one call in the MATLAB nightly cron
(``scripts/populate_tables.m`` run by ``scripts/call_u19_night_cronjob.sh``):

=====================================  ==============================================
MATLAB                                 Python
=====================================  ==============================================
``reset_reweight_subjects()``          :func:`main_reset_reweight_subjects`
``populate_schedule_for_tomorrow()``   :func:`main_populate_schedule_for_tomorrow`
``populate_technician_schedule()``     :func:`main_populate_technician_schedule`
=====================================  ==============================================

The schedule copy-forward includes the rule-row handling from
U19-pipeline-matlab#62 (only rows without a ``rule_id`` are copied, and slots
already filled on the target date are skipped).

The date logic and the insert/notify flow are plain functions so they can be
tested without a database; the ``main_*`` wrappers do the DataJoint I/O.
"""

import calendar
import contextlib
import dataclasses
import datetime
import warnings
from collections import defaultdict

import datajoint as dj

import u19_pipeline.utils.slack_utils as su

LOOKBACK_DAYS = 5
MAX_FAILURES_SHOWN = 5
SCHEDULING_WEBHOOK = "rig_scheduling"
DEV_WEBHOOK = "dev_notifications"
DEVS_GROUP = "devs"


@dataclasses.dataclass(frozen=True)
class InsertFailure:
    index: int  # 1-based position in the rows passed to attempt_insert
    subject_fullname: str
    date: str
    error_message: str


# ---------------------------------------------------------------------------
# Date logic
# ---------------------------------------------------------------------------


def add_calendar_months(day, months):
    """``day + calmonths(months)`` in MATLAB: clamps to the last day of the month."""
    month_index = day.month - 1 + months
    year = day.year + month_index // 12
    month = month_index % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    return day.replace(year=year, month=month, day=min(day.day, last_day))


def next_month_bounds(today):
    """First and last day of the month after ``today``."""
    first = add_calendar_months(today.replace(day=1), 1)
    last = first.replace(day=calendar.monthrange(first.year, first.month)[1])
    return first, last


def tech_reference_week(today):
    """Sunday..Saturday week of this month that the technician schedule is copied from.

    Same as MATLAB: the first Saturday on or after (end of month - 13 days),
    then the Sunday after it and the Saturday after that.
    """
    end_of_month = today.replace(day=calendar.monthrange(today.year, today.month)[1])
    reference_point = end_of_month - datetime.timedelta(days=13)
    saturday = reference_point + datetime.timedelta(
        days=(5 - reference_point.weekday()) % 7
    )
    sunday = saturday + datetime.timedelta(days=1)
    return sunday, sunday + datetime.timedelta(days=6)


def build_tech_schedule_rows(today, reference_rows, max_shift_index):
    """TechSchedule rows for next month, repeating the reference week by weekday.

    Every reference row is copied to each date of next month with the same
    weekday. ``start_time``/``end_time`` keep their offset from the reference
    row's date, so shifts that run past midnight keep their duration.
    ``shift_index`` continues from ``max_shift_index`` (``None`` for an empty
    table).
    """
    by_weekday = defaultdict(list)
    for row in reference_rows:
        by_weekday[row["date"].weekday()].append(row)

    first, last = next_month_bounds(today)
    shift_index = max_shift_index or 0
    rows = []
    day = first
    while day <= last:
        new_midnight = datetime.datetime.combine(day, datetime.time())
        for ref in by_weekday[day.weekday()]:
            ref_midnight = datetime.datetime.combine(ref["date"], datetime.time())
            shift_index += 1
            rows.append(
                {
                    **ref,
                    "shift_index": shift_index,
                    "date": day,
                    "start_time": new_midnight + (ref["start_time"] - ref_midnight),
                    "end_time": new_midnight + (ref["end_time"] - ref_midnight),
                }
            )
        day += datetime.timedelta(days=1)
    return rows


# ---------------------------------------------------------------------------
# Schedule copy flow
# ---------------------------------------------------------------------------


def schedule_rows_for(rows, target_date, reset_levels=False):
    """Copies of Schedule ``rows`` moved to ``target_date``.

    Copies never carry a ``rule_id``: they are legacy rows, not rule-generated.
    """
    out = []
    for row in rows:
        new = {**row, "date": target_date}
        new.pop("rule_id", None)
        if reset_levels:
            new["level"] = 0
            new["sublevel"] = 0
        out.append(new)
    return out


def drop_occupied_slots(rows, occupied):
    """Rows whose ``(location, timeslot)`` is not in ``occupied``."""
    return [r for r in rows if (r["location"], r["timeslot"]) not in occupied]


def attempt_insert(table, rows, conn):
    """Insert ``rows`` in one transaction; if that fails, insert them one by one.

    Returns the rows that could not be inserted as :class:`InsertFailure`.
    Mirrors ``attempt_insert_schedule`` in ``populate_schedule_for_tomorrow.m``.
    """
    if not rows:
        return []

    try:
        conn.start_transaction()
        table.insert(rows)
        conn.commit_transaction()
        return []
    except Exception:
        with contextlib.suppress(Exception):
            conn.cancel_transaction()

    failures = []
    try:
        conn.start_transaction()
        for index, row in enumerate(rows, start=1):
            try:
                table.insert1(row)
            except Exception as e:
                failures.append(
                    InsertFailure(
                        index,
                        row.get("subject_fullname") or "",
                        str(row.get("date", "")),
                        str(e),
                    )
                )
        conn.commit_transaction()
    except Exception as e:
        with contextlib.suppress(Exception):
            conn.cancel_transaction()
        warnings.warn(f"Failed during individual inserts: {e}", stacklevel=2)
        failures = [
            InsertFailure(index, "", str(row.get("date", "")), "Transaction failure")
            for index, row in enumerate(rows, start=1)
        ]
    return failures


def copy_schedule_forward(
    today, fetch_day, count_sessions, occupied_slots, insert_rows, notify
):
    """Copy today's rig schedule to tomorrow, with levels reset to 0.

    Only legacy rows are copied; rule-generated rows (``rule_id`` set) are
    filled in by the ScheduleRule materializer instead. A slot that is already
    filled on the target date (booked in the web app, or from a rule) wins
    over the copy. If today has no sessions at all, today is first
    re-populated from the most recent of the previous ``LOOKBACK_DAYS`` days
    that has legacy entries.

    Callbacks:

    * ``fetch_day(date)``: legacy Schedule rows of that day (subject set,
      ``rule_id`` NULL)
    * ``count_sessions(date)``: number of rows of that day with a subject,
      rule-generated or not
    * ``occupied_slots(date)``: set of ``(location, timeslot)`` with a row
    * ``insert_rows(rows)``: list of :class:`InsertFailure`
    * ``notify(webhook_name, text)``: post to Slack

    Returns ``(rows_for_tomorrow, failures)``.
    """
    tomorrow = today + datetime.timedelta(days=1)
    source = fetch_day(today)

    if not source and count_sessions(today) > 0:
        print("All of today's sessions come from schedule rules; nothing to copy.")
        return [], []

    if not source:
        for days_back in range(1, LOOKBACK_DAYS + 1):
            candidate_date = today - datetime.timedelta(days=days_back)
            try:
                candidates = fetch_day(candidate_date)
            except Exception:
                candidates = []
            if not candidates:
                continue

            for_today = drop_occupied_slots(
                schedule_rows_for(candidates, today), occupied_slots(today)
            )
            failures = insert_rows(for_today)
            if failures:
                notify(
                    SCHEDULING_WEBHOOK,
                    f"Failed to populate today ({today}) from {candidate_date}:"
                    f" {len(failures)} failures",
                )
                source = for_today
            else:
                # for_today stands in when nothing was written (dry run)
                source = fetch_day(today) or for_today
            notify(
                SCHEDULING_WEBHOOK,
                f"Today ({today}) was empty — re-populated from {candidate_date}"
                f" ({len(candidates)} entries).",
            )
            break
        else:
            notify(
                SCHEDULING_WEBHOOK,
                f"No schedule entries found for today ({today}) nor in previous five days"
                f" — nothing to insert for {tomorrow}.",
            )
            return [], []

    rows = drop_occupied_slots(
        schedule_rows_for(source, tomorrow, reset_levels=True), occupied_slots(tomorrow)
    )
    if not rows:
        print(f"Every slot for {tomorrow} is already filled; nothing to copy.")
        return [], []
    return rows, insert_rows(rows)


def format_mentions(ids):
    """Slack mention markup: ``S...`` ids are user groups, anything else a user."""
    text = ""
    for mention_id in ids:
        if not mention_id:
            continue
        text += (
            f" <!subteam^{mention_id}>"
            if mention_id.startswith("S")
            else f" <@{mention_id}>"
        )
    return text


def _mrkdwn_section(text):
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def schedule_failure_message(target_date, failures, total, mention_ids=(), now=None):
    """Slack message for failed Schedule inserts (same layout as the MATLAB alert)."""
    now = now or datetime.datetime.now()
    title = "Schedule Insertion Failures"
    blocks = [
        _mrkdwn_section(
            f":x: *{title}*{format_mentions(mention_ids)} on {now:%d-%b-%Y %H:%M:%S}"
        ),
        {"type": "divider"},
        _mrkdwn_section(
            f"*Schedule insertion failures for {target_date}*\n\n"
            f"Total failed entries: *{len(failures)} out of {total}*"
        ),
    ]
    for failure in failures[:MAX_FAILURES_SHOWN]:
        blocks.append(
            _mrkdwn_section(
                f"*Entry {failure.index} - {failure.subject_fullname}*: \n"
                f"*Date*: {failure.date}\n*Error*: {failure.error_message}"
            )
        )
    if len(failures) > MAX_FAILURES_SHOWN:
        blocks.append(
            _mrkdwn_section(
                f"_... and {len(failures) - MAX_FAILURES_SHOWN} more failures not shown_"
            )
        )
    return {"blocks": blocks, "text": title}


# ---------------------------------------------------------------------------
# DataJoint wrappers
# ---------------------------------------------------------------------------


def _prefix():
    return dj.config["custom"]["database.prefix"]


def _module(name):
    return dj.create_virtual_module(name, _prefix() + name)


def send_to_webhook(webhook_name, message):
    """Post ``message`` (text or a Slack payload) to a ``lab.SlackWebhooks`` entry.

    Like the MATLAB helper, a missing webhook or a failed post only warns.
    """
    if isinstance(message, str):
        message = {"text": message}
    try:
        urls = (_module("lab").SlackWebhooks & {"webhook_name": webhook_name}).fetch(
            "webhook_url"
        )
        if len(urls) == 0:
            warnings.warn(
                f'Webhook "{webhook_name}" not found in lab.SlackWebhooks', stacklevel=2
            )
            return
        su.send_slack_notification(urls[0], message)
    except Exception as e:
        warnings.warn(
            f"Failed sending Slack notification to {webhook_name}: {e}", stacklevel=2
        )


def main_reset_reweight_subjects(dry_run=False):
    """Clear ``subject.Subject.need_reweight`` for every subject.

    Replaces ``reset_reweight_subjects()``.
    """
    conn = dj.conn()
    table = f"`{_prefix()}subject`.`subject`"
    if dry_run:
        (count,) = conn.query(
            f"SELECT COUNT(*) FROM {table} WHERE need_reweight <> 0"
        ).fetchone()
        print(f"[dry run] would reset need_reweight on {count} subjects")
        return count
    conn.query(f"UPDATE {table} SET need_reweight = 0")
    return None


def main_populate_schedule_for_tomorrow(today=None, dry_run=False):
    """Copy today's ``scheduler.Schedule`` to tomorrow.

    Replaces ``populate_schedule_for_tomorrow()``. Returns the rows meant for
    tomorrow.
    """
    today = today or datetime.date.today()
    scheduler = _module("scheduler")
    conn = dj.conn()

    schedule = scheduler.Schedule
    # Before the ScheduleRule migration there is no rule_id column and every
    # row is a legacy row.
    legacy = "subject_fullname IS NOT NULL"
    if "rule_id" in schedule.heading.names:
        legacy += " AND rule_id IS NULL"

    def fetch_day(day):
        return list((schedule & {"date": day} & legacy).fetch(as_dict=True))

    def count_sessions(day):
        return len(schedule & {"date": day} & "subject_fullname IS NOT NULL")

    def occupied_slots(day):
        return set(
            zip(*(schedule & {"date": day}).fetch("location", "timeslot"), strict=True)
        )

    def insert_rows(rows):
        if dry_run:
            if rows:
                print(
                    f"[dry run] would insert {len(rows)} Schedule rows for {rows[0]['date']}"
                )
            return []
        return attempt_insert(schedule, rows, conn)

    def notify(webhook_name, text):
        if dry_run:
            print(f"[dry run] would notify {webhook_name}: {text}")
        else:
            send_to_webhook(webhook_name, text)

    rows, failures = copy_schedule_forward(
        today, fetch_day, count_sessions, occupied_slots, insert_rows, notify
    )
    if not failures:
        if rows and not dry_run:
            print("All entries inserted successfully.")
        return rows

    mention_ids = []
    with contextlib.suppress(Exception):
        mention_ids = (
            (_module("lab").SlackGroups & {"group_name": DEVS_GROUP})
            .fetch("group_id")
            .tolist()
        )
    target_date = rows[0]["date"] if rows else "unknown"
    send_to_webhook(
        DEV_WEBHOOK,
        schedule_failure_message(target_date, failures, len(rows), mention_ids),
    )
    return rows


def main_populate_technician_schedule(today=None, dry_run=False):
    """Fill next month's ``scheduler.TechSchedule`` from a week of this month.

    Replaces ``populate_technician_schedule()``. Does nothing when there are
    already entries on or after the same day next month. Returns the rows
    inserted (or that would be, under ``dry_run``).
    """
    today = today or datetime.date.today()
    scheduler = _module("scheduler")
    tech = scheduler.TechSchedule

    if len(tech & f'date >= "{add_calendar_months(today, 1)}"') > 0:
        print("Technician schedule for next month already exists.")
        return []

    max_shift_index = (
        dj.U().aggr(tech, max_shift_index="max(shift_index)").fetch1("max_shift_index")
    )
    sunday, saturday = tech_reference_week(today)
    reference = (tech & f'date >= "{sunday}"' & f'date <= "{saturday}"').fetch(
        as_dict=True, order_by="shift_index"
    )
    rows = build_tech_schedule_rows(today, reference, max_shift_index)
    if not rows:
        print(f"No technician shifts between {sunday} and {saturday} to copy.")
        return []
    if dry_run:
        print(
            f"[dry run] would insert {len(rows)} TechSchedule rows ({rows[0]['date']} to {rows[-1]['date']})"
        )
        return rows

    with dj.conn().transaction:
        tech.insert(rows)
    return rows
