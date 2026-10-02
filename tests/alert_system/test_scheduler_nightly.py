import datetime as dt
from unittest.mock import MagicMock

import pytest

from u19_pipeline.alert_system.scheduler_nightly import scheduler_nightly as sn

D = dt.date


def local(*args):
    """Naive datetime: the scheduler tables store lab-local times."""
    return dt.datetime(*args)  # noqa: DTZ001


class TestAddCalendarMonths:
    @pytest.mark.parametrize(
        ("day", "months", "expected"),
        [
            (D(2026, 10, 2), 1, D(2026, 11, 2)),
            (D(2026, 12, 15), 1, D(2027, 1, 15)),  # year rollover
            (D(2026, 10, 31), 1, D(2026, 11, 30)),  # clamps like MATLAB calmonths
            (D(2027, 1, 31), 1, D(2027, 2, 28)),
            (D(2028, 1, 31), 1, D(2028, 2, 29)),  # leap year
            (D(2026, 10, 2), 0, D(2026, 10, 2)),
            (D(2026, 3, 31), -1, D(2026, 2, 28)),
        ],
    )
    def test_add(self, day, months, expected):
        assert sn.add_calendar_months(day, months) == expected


class TestNextMonthBounds:
    @pytest.mark.parametrize(
        ("today", "expected"),
        [
            (D(2026, 10, 2), (D(2026, 11, 1), D(2026, 11, 30))),
            (D(2026, 12, 31), (D(2027, 1, 1), D(2027, 1, 31))),
            (D(2028, 1, 31), (D(2028, 2, 1), D(2028, 2, 29))),
            (D(2027, 1, 1), (D(2027, 2, 1), D(2027, 2, 28))),
        ],
    )
    def test_bounds(self, today, expected):
        assert sn.next_month_bounds(today) == expected


class TestTechReferenceWeek:
    # MATLAB: Sunday after the first Saturday on/after (end of month - 13 days)
    @pytest.mark.parametrize(
        ("today", "expected"),
        [
            # eom 2026-10-31 (Sat); eom-13 = Oct 18 (Sun) -> Sat Oct 24
            (D(2026, 10, 2), (D(2026, 10, 25), D(2026, 10, 31))),
            # eom 2026-07-31 (Fri); eom-13 = Jul 18 is itself a Saturday
            (D(2026, 7, 1), (D(2026, 7, 19), D(2026, 7, 25))),
            # eom 2026-12-31 (Thu); eom-13 = Dec 18 (Fri) -> Sat Dec 19
            (D(2026, 12, 31), (D(2026, 12, 20), D(2026, 12, 26))),
            # February of a leap year: eom 2028-02-29 (Tue); eom-13 = Feb 16 (Wed)
            (D(2028, 2, 10), (D(2028, 2, 20), D(2028, 2, 26))),
        ],
    )
    def test_week(self, today, expected):
        sunday, saturday = sn.tech_reference_week(today)
        assert (sunday, saturday) == expected
        assert sunday.weekday() == 6
        assert saturday.weekday() == 5

    @pytest.mark.parametrize("day", range(1, 32))
    def test_week_always_inside_current_month(self, day):
        today = D(2026, 10, day)
        sunday, saturday = sn.tech_reference_week(today)
        assert sunday.month == saturday.month == 10


def _tech_row(index, day, shift="Day", user="tech1", start_h=9, hours=8):
    start = dt.datetime.combine(day, dt.time(start_h))
    return {
        "shift_index": index,
        "date": day,
        "shift": shift,
        "user_id": user,
        "tech_duty": "Training",
        "start_time": start,
        "end_time": start + dt.timedelta(hours=hours),
    }


def _reference_week(today):
    sunday, _ = sn.tech_reference_week(today)
    return [
        _tech_row(100 + i, sunday + dt.timedelta(days=i), user=f"tech{i}")
        for i in range(7)
    ]


class TestBuildTechScheduleRows:
    def test_one_row_per_day_matches_matlab(self):
        today = D(2026, 10, 2)
        rows = sn.build_tech_schedule_rows(today, _reference_week(today), 500)

        assert len(rows) == 30
        assert [r["date"] for r in rows] == [
            D(2026, 11, 1) + dt.timedelta(days=i) for i in range(30)
        ]
        assert [r["shift_index"] for r in rows] == list(range(501, 531))
        # Nov 1 2026 is a Sunday -> Sunday's tech (tech0)
        assert rows[0]["user_id"] == "tech0"
        assert all(r["user_id"] == f"tech{(r['date'].weekday() + 1) % 7}" for r in rows)
        assert rows[0]["start_time"] == local(2026, 11, 1, 9)
        assert rows[0]["end_time"] == local(2026, 11, 1, 17)

    def test_reference_rows_are_not_mutated(self):
        today = D(2026, 10, 2)
        reference = _reference_week(today)
        snapshot = [dict(r) for r in reference]
        sn.build_tech_schedule_rows(today, reference, 0)
        assert reference == snapshot

    def test_overnight_shift_keeps_its_duration(self):
        today = D(2026, 10, 2)
        sunday, _ = sn.tech_reference_week(today)
        evening = _tech_row(
            1, sunday, shift="Evening", start_h=17, hours=8
        )  # ends 01:00 next day
        rows = sn.build_tech_schedule_rows(today, [evening], 0)
        first = rows[0]
        assert first["start_time"] == local(2026, 11, 1, 17)
        assert first["end_time"] == local(2026, 11, 2, 1)

    def test_shift_starting_next_day_keeps_offset(self):
        today = D(2026, 10, 2)
        sunday, _ = sn.tech_reference_week(today)
        night = _tech_row(1, sunday, shift="Night")
        night["start_time"] = dt.datetime.combine(
            sunday + dt.timedelta(days=1), dt.time(1)
        )
        night["end_time"] = night["start_time"] + dt.timedelta(hours=8)
        first = sn.build_tech_schedule_rows(today, [night], 0)[0]
        assert first["start_time"] == local(2026, 11, 2, 1)
        assert first["end_time"] == local(2026, 11, 2, 9)

    def test_several_rows_per_weekday_are_all_copied(self):
        today = D(2026, 10, 2)
        sunday, _ = sn.tech_reference_week(today)
        reference = [
            _tech_row(1, sunday, user="a"),
            _tech_row(2, sunday, shift="Evening", user="b", start_h=17),
        ]
        rows = sn.build_tech_schedule_rows(today, reference, 0)
        # November 2026 has 5 Sundays
        assert len(rows) == 10
        assert [r["user_id"] for r in rows[:2]] == ["a", "b"]
        assert [r["shift_index"] for r in rows] == list(range(1, 11))

    def test_missing_weekday_leaves_gap(self):
        today = D(2026, 10, 2)
        reference = [r for r in _reference_week(today) if r["date"].weekday() != 2]
        rows = sn.build_tech_schedule_rows(today, reference, 0)
        assert len(rows) == 30 - 4  # four Wednesdays in Nov 2026
        assert all(r["date"].weekday() != 2 for r in rows)

    def test_empty_reference_week_gives_no_rows(self):
        assert sn.build_tech_schedule_rows(D(2026, 10, 2), [], 10) == []

    @pytest.mark.parametrize("max_index", [None, 0])
    def test_empty_table_starts_at_one(self, max_index):
        today = D(2026, 10, 2)
        rows = sn.build_tech_schedule_rows(today, _reference_week(today), max_index)
        assert rows[0]["shift_index"] == 1

    def test_february_leap_year(self):
        today = D(2028, 1, 15)
        rows = sn.build_tech_schedule_rows(today, _reference_week(today), 0)
        assert len(rows) == 29
        assert rows[-1]["date"] == D(2028, 2, 29)


def _sched_row(day, slot, subject="subj", level=3, sublevel=2):
    return {
        "date": day,
        "location": "165I-Rig1-T",
        "timeslot": slot,
        "subject_fullname": subject,
        "level": level,
        "sublevel": sublevel,
    }


class TestScheduleRowsFor:
    def test_retimes_and_resets_levels(self):
        src = [_sched_row(D(2026, 10, 2), 1)]
        out = sn.schedule_rows_for(src, D(2026, 10, 3), reset_levels=True)
        assert out == [{**src[0], "date": D(2026, 10, 3), "level": 0, "sublevel": 0}]
        assert src[0]["date"] == D(2026, 10, 2)  # source untouched

    def test_keeps_levels_by_default(self):
        out = sn.schedule_rows_for([_sched_row(D(2026, 10, 1), 1)], D(2026, 10, 2))
        assert (out[0]["level"], out[0]["sublevel"]) == (3, 2)

    def test_empty(self):
        assert sn.schedule_rows_for([], D(2026, 10, 2)) == []

    def test_strips_rule_id(self):
        src = [{**_sched_row(D(2026, 10, 2), 1), "rule_id": None}]
        out = sn.schedule_rows_for(src, D(2026, 10, 3))
        assert "rule_id" not in out[0]
        assert "rule_id" in src[0]


class TestDropOccupiedSlots:
    def test_drops_rows_whose_slot_is_taken(self):
        rows = [_sched_row(TODAY, 1), _sched_row(TODAY, 2)]
        assert sn.drop_occupied_slots(rows, {("165I-Rig1-T", 1)}) == [rows[1]]

    def test_same_timeslot_other_rig_is_kept(self):
        rows = [_sched_row(TODAY, 1)]
        assert sn.drop_occupied_slots(rows, {("165I-Rig2-T", 1)}) == rows

    def test_nothing_occupied(self):
        rows = [_sched_row(TODAY, 1)]
        assert sn.drop_occupied_slots(rows, set()) == rows

    def test_empty_rows(self):
        assert sn.drop_occupied_slots([], {("165I-Rig1-T", 1)}) == []


class FakeConn:
    def __init__(self, fail_commit=False):
        self.calls = []
        self.fail_commit = fail_commit

    def start_transaction(self):
        self.calls.append("start")

    def commit_transaction(self):
        self.calls.append("commit")
        if self.fail_commit:
            raise RuntimeError("commit failed")

    def cancel_transaction(self):
        self.calls.append("cancel")


class TestAttemptInsert:
    def test_empty_rows_does_nothing(self):
        table, conn = MagicMock(), FakeConn()
        assert sn.attempt_insert(table, [], conn) == []
        table.insert.assert_not_called()
        assert conn.calls == []

    def test_bulk_success(self):
        table, conn = MagicMock(), FakeConn()
        rows = [_sched_row(D(2026, 10, 3), 1)]
        assert sn.attempt_insert(table, rows, conn) == []
        table.insert.assert_called_once_with(rows)
        table.insert1.assert_not_called()
        assert conn.calls == ["start", "commit"]

    def test_bulk_failure_falls_back_to_individual_inserts(self):
        table, conn = MagicMock(), FakeConn()
        table.insert.side_effect = RuntimeError("duplicate")
        table.insert1.side_effect = [None, RuntimeError("dup slot 2"), None]
        rows = [_sched_row(D(2026, 10, 3), i, subject=f"s{i}") for i in (1, 2, 3)]

        failures = sn.attempt_insert(table, rows, conn)

        assert failures == [sn.InsertFailure(2, "s2", "2026-10-03", "dup slot 2")]
        assert table.insert1.call_count == 3
        assert conn.calls == ["start", "cancel", "start", "commit"]

    def test_missing_subject_reported_as_empty_string(self):
        table, conn = MagicMock(), FakeConn()
        table.insert.side_effect = RuntimeError("bulk")
        table.insert1.side_effect = RuntimeError("one")
        row = _sched_row(D(2026, 10, 3), 1, subject=None)
        assert sn.attempt_insert(table, [row], conn)[0].subject_fullname == ""

    def test_transaction_failure_marks_every_row_failed(self):
        table, conn = MagicMock(), FakeConn(fail_commit=True)
        table.insert.side_effect = RuntimeError("bulk")
        rows = [_sched_row(D(2026, 10, 3), i) for i in (1, 2)]

        with pytest.warns(UserWarning, match="commit failed"):
            failures = sn.attempt_insert(table, rows, conn)

        assert [f.error_message for f in failures] == ["Transaction failure"] * 2
        assert [f.index for f in failures] == [1, 2]
        assert conn.calls[-1] == "cancel"


class FakeSchedule:
    """In-memory stand-in for the callbacks used by copy_schedule_forward.

    Rows with a ``rule_id`` are rule-generated; only rows without one (and
    with a subject) are "legacy" rows that the copy-forward owns.
    """

    def __init__(self, days, fail_dates=()):
        self.days = {d: list(rows) for d, rows in days.items()}
        self.fail_dates = set(fail_dates)
        self.inserted = []
        self.messages = []

    def fetch_day(self, day):
        return [
            dict(r)
            for r in self.days.get(day, [])
            if r.get("rule_id") is None and r["subject_fullname"] is not None
        ]

    def count_sessions(self, day):
        return sum(r["subject_fullname"] is not None for r in self.days.get(day, []))

    def occupied_slots(self, day):
        return {(r["location"], r["timeslot"]) for r in self.days.get(day, [])}

    def insert_rows(self, rows):
        if rows and rows[0]["date"] in self.fail_dates:
            return [
                sn.InsertFailure(i, r["subject_fullname"], str(r["date"]), "boom")
                for i, r in enumerate(rows, 1)
            ]
        self.inserted.append(rows)
        for r in rows:
            self.days.setdefault(r["date"], []).append(r)
        return []

    def notify(self, webhook_name, message):
        self.messages.append((webhook_name, message))

    def run(self, today, fetch_day=None):
        return sn.copy_schedule_forward(
            today,
            fetch_day or self.fetch_day,
            self.count_sessions,
            self.occupied_slots,
            self.insert_rows,
            self.notify,
        )


TODAY = D(2026, 10, 2)
TOMORROW = D(2026, 10, 3)


class TestCopyScheduleForward:
    def test_copies_today_to_tomorrow_with_levels_reset(self):
        fake = FakeSchedule({TODAY: [_sched_row(TODAY, 1), _sched_row(TODAY, 2)]})
        rows, failures = fake.run(TODAY)
        assert failures == []
        assert [r["date"] for r in rows] == [TOMORROW, TOMORROW]
        assert all(r["level"] == 0 and r["sublevel"] == 0 for r in rows)
        assert fake.messages == []

    def test_empty_today_repopulates_from_most_recent_day(self):
        fake = FakeSchedule(
            {
                TODAY - dt.timedelta(days=2): [
                    _sched_row(TODAY - dt.timedelta(days=2), 1, subject="two_days")
                ],
                TODAY - dt.timedelta(days=4): [
                    _sched_row(TODAY - dt.timedelta(days=4), 1, subject="four_days")
                ],
            }
        )
        rows, failures = fake.run(TODAY)

        assert failures == []
        # today was filled first, levels kept; then tomorrow, levels reset
        assert fake.inserted[0][0]["date"] == TODAY
        assert fake.inserted[0][0]["level"] == 3
        assert [r["subject_fullname"] for r in rows] == ["two_days"]
        assert rows[0]["date"] == TOMORROW
        assert rows[0]["level"] == 0
        assert fake.messages == [
            (
                "rig_scheduling",
                "Today (2026-10-02) was empty — re-populated from 2026-09-30 (1 entries).",
            ),
        ]

    def test_lookback_limit_is_five_days(self):
        six = TODAY - dt.timedelta(days=6)
        fake = FakeSchedule({six: [_sched_row(six, 1)]})
        rows, failures = fake.run(TODAY)
        assert (rows, failures) == ([], [])
        assert fake.inserted == []
        assert fake.messages == [
            (
                "rig_scheduling",
                "No schedule entries found for today (2026-10-02) nor in previous five days — nothing to insert for 2026-10-03.",
            )
        ]

    def test_fifth_day_back_is_used(self):
        five = TODAY - dt.timedelta(days=5)
        fake = FakeSchedule({five: [_sched_row(five, 1)]})
        rows, _ = fake.run(TODAY)
        assert len(rows) == 1

    def test_failed_today_insert_still_fills_tomorrow_from_candidate(self):
        yday = TODAY - dt.timedelta(days=1)
        fake = FakeSchedule({yday: [_sched_row(yday, 1)]}, fail_dates={TODAY})
        rows, failures = fake.run(TODAY)

        assert failures == []
        assert [r["date"] for r in rows] == [TOMORROW]
        assert [m for _, m in fake.messages] == [
            "Failed to populate today (2026-10-02) from 2026-10-01: 1 failures",
            "Today (2026-10-02) was empty — re-populated from 2026-10-01 (1 entries).",
        ]

    def test_fetch_error_during_lookback_is_treated_as_empty(self):
        yday = TODAY - dt.timedelta(days=1)
        two = TODAY - dt.timedelta(days=2)
        fake = FakeSchedule({two: [_sched_row(two, 1)]})
        real_fetch = fake.fetch_day

        def flaky(day):
            if day == yday:
                raise RuntimeError("db hiccup")
            return real_fetch(day)

        rows, _ = fake.run(TODAY, fetch_day=flaky)
        assert len(rows) == 1

    def test_tomorrow_failures_are_returned(self):
        fake = FakeSchedule({TODAY: [_sched_row(TODAY, 1)]}, fail_dates={TOMORROW})
        rows, failures = fake.run(TODAY)
        assert len(rows) == 1
        assert [f.error_message for f in failures] == ["boom"]


class TestCopyScheduleForwardWithRules:
    """Behavior from U19-pipeline-matlab#62 (rule-generated Schedule rows)."""

    def test_rule_rows_are_not_copied(self):
        fake = FakeSchedule(
            {
                TODAY: [
                    _sched_row(TODAY, 1, subject="legacy"),
                    {**_sched_row(TODAY, 2, subject="ruled"), "rule_id": 7},
                ]
            }
        )
        rows, failures = fake.run(TODAY)
        assert failures == []
        assert [r["subject_fullname"] for r in rows] == ["legacy"]
        assert "rule_id" not in rows[0]

    def test_all_rule_rows_today_skips_lookback(self):
        yday = TODAY - dt.timedelta(days=1)
        fake = FakeSchedule(
            {
                TODAY: [{**_sched_row(TODAY, 1), "rule_id": 7}],
                yday: [_sched_row(yday, 1, subject="old")],
            }
        )
        assert fake.run(TODAY) == ([], [])
        assert fake.inserted == []
        assert fake.messages == []

    def test_today_with_only_null_subject_rows_is_empty(self):
        yday = TODAY - dt.timedelta(days=1)
        fake = FakeSchedule(
            {
                TODAY: [_sched_row(TODAY, 1, subject=None)],
                yday: [_sched_row(yday, 2, subject="old")],
            }
        )
        rows, _ = fake.run(TODAY)
        assert [r["subject_fullname"] for r in rows] == ["old"]

    def test_filled_slots_tomorrow_win_over_the_copy(self):
        fake = FakeSchedule(
            {
                TODAY: [_sched_row(TODAY, 1), _sched_row(TODAY, 2)],
                TOMORROW: [{**_sched_row(TOMORROW, 1, subject="booked"), "rule_id": 3}],
            }
        )
        rows, failures = fake.run(TODAY)
        assert failures == []
        assert [r["timeslot"] for r in rows] == [2]

    def test_every_slot_filled_tomorrow_inserts_nothing(self):
        fake = FakeSchedule(
            {
                TODAY: [_sched_row(TODAY, 1)],
                TOMORROW: [_sched_row(TOMORROW, 1, subject="booked")],
            }
        )
        assert fake.run(TODAY) == ([], [])
        assert fake.inserted == []
        assert fake.messages == []

    def test_lookback_skips_slots_already_filled_today(self):
        yday = TODAY - dt.timedelta(days=1)
        fake = FakeSchedule(
            {
                TODAY: [_sched_row(TODAY, 1, subject=None)],
                yday: [
                    _sched_row(yday, 1, subject="a"),
                    _sched_row(yday, 2, subject="b"),
                ],
            }
        )
        rows, failures = fake.run(TODAY)
        assert failures == []
        assert [r["timeslot"] for r in fake.inserted[0]] == [2]
        # message still reports the look-back day's entry count, as MATLAB does
        assert fake.messages[-1][1].endswith(
            "re-populated from 2026-10-01 (2 entries)."
        )


class TestScheduleFailureMessage:
    NOW = local(2026, 10, 2, 3, 0, 0)

    def _failures(self, n):
        return [
            sn.InsertFailure(i, f"s{i}", "2026-10-03", f"err{i}")
            for i in range(1, n + 1)
        ]

    def test_layout(self):
        msg = sn.schedule_failure_message(
            "2026-10-03", self._failures(2), 10, ["S123"], now=self.NOW
        )
        blocks = msg["blocks"]
        assert msg["text"] == "Schedule Insertion Failures"
        assert blocks[0]["text"]["text"] == (
            ":x: *Schedule Insertion Failures* <!subteam^S123> on 02-Oct-2026 03:00:00"
        )
        assert blocks[1] == {"type": "divider"}
        assert blocks[2]["text"]["text"] == (
            "*Schedule insertion failures for 2026-10-03*\n\nTotal failed entries: *2 out of 10*"
        )
        assert (
            blocks[3]["text"]["text"]
            == "*Entry 1 - s1*: \n*Date*: 2026-10-03\n*Error*: err1"
        )
        assert len(blocks) == 5

    def test_more_than_five_failures_are_truncated(self):
        msg = sn.schedule_failure_message(
            "2026-10-03", self._failures(7), 7, [], now=self.NOW
        )
        sections = msg["blocks"][3:]
        assert len(sections) == 6
        assert sections[-1]["text"]["text"] == "_... and 2 more failures not shown_"

    def test_exactly_five_failures_has_no_truncation_note(self):
        msg = sn.schedule_failure_message(
            "2026-10-03", self._failures(5), 5, [], now=self.NOW
        )
        assert len(msg["blocks"][3:]) == 5

    @pytest.mark.parametrize(
        ("ids", "expected"),
        [
            ([], ""),
            (["S1"], " <!subteam^S1>"),
            (["U9"], " <@U9>"),
            (["", None], ""),
        ],
    )
    def test_mentions(self, ids, expected):
        assert sn.format_mentions(ids) == expected
