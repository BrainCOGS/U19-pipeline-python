"""Tests for recurring schedule rules. Pure logic: no database needed."""

import datetime
from unittest.mock import MagicMock, call

import pandas as pd
import pytest

from u19_pipeline.utils import schedule_rules as sr

MON = datetime.date(2024, 1, 1)  # a Monday
TUE, WED, THU, FRI, SAT, SUN = (MON + datetime.timedelta(days=i) for i in range(1, 7))
NEXT_MON = MON + datetime.timedelta(days=7)


def day(offset: int) -> datetime.date:
    return MON + datetime.timedelta(days=offset)


def make_rule(**overrides) -> sr.ScheduleRule:
    fields = dict(
        rule_id=1,
        subject_fullname="ab1234_m1",
        location="165I-Rig1-T",
        timeslot=2,
        start_date=MON,
        end_date=None,
        weekdays=sr.EVERY_DAY,
        training_profile_id=10,
        recording_profile_id=20,
        input_output_profile_id=30,
    )
    fields.update(overrides)
    return sr.ScheduleRule(**fields)


def row_for(rule: sr.ScheduleRule, date: datetime.date, **overrides) -> dict:
    row = {
        "date": date,
        "location": rule.location,
        "timeslot": rule.timeslot,
        **rule.payload(),
        "rule_id": rule.rule_id,
    }
    row.update(overrides)
    return row


class TestWeekdayParsing:
    def test_presets_partition_the_week(self):
        assert sr.WEEKDAYS | sr.WEEKENDS == sr.EVERY_DAY
        assert not sr.WEEKDAYS & sr.WEEKENDS

    def test_weekday_name_matches_calendar(self):
        assert [sr.weekday_name(d) for d in (MON, FRI, SAT, SUN)] == [
            "Mon",
            "Fri",
            "Sat",
            "Sun",
        ]

    @pytest.mark.parametrize("token", ["Mon", "mon", "MON", " monday ", "Monday"])
    def test_accepts_common_spellings(self, token):
        assert sr.parse_weekdays([token]) == {"Mon"}

    def test_rejects_typos_instead_of_dropping_them(self):
        with pytest.raises(ValueError, match="Mnday"):
            sr.parse_weekdays(["Mon", "Mnday"])

    def test_empty_input_gives_empty_set(self):
        assert sr.parse_weekdays([]) == frozenset()

    def test_duplicates_collapse(self):
        assert sr.parse_weekdays(["sat", "Saturday", "SAT"]) == {"Sat"}


class TestValidateRule:
    def test_valid_rule_has_no_problems(self):
        assert sr.validate_rule(make_rule()) == []

    def test_end_before_start(self):
        assert any(
            "before start" in p
            for p in sr.validate_rule(
                make_rule(end_date=MON - datetime.timedelta(days=1))
            )
        )

    def test_single_day_rule_is_valid(self):
        assert sr.validate_rule(make_rule(start_date=MON, end_date=MON)) == []

    def test_no_weekdays(self):
        assert sr.validate_rule(make_rule(weekdays=frozenset())) == [
            "Pick at least one day of the week."
        ]

    def test_unknown_weekday(self):
        assert any(
            "Funday" in p
            for p in sr.validate_rule(make_rule(weekdays=frozenset({"Funday"})))
        )

    @pytest.mark.parametrize("timeslot", [0, -1])
    def test_non_positive_timeslot(self, timeslot):
        assert any(
            "Timeslot" in p for p in sr.validate_rule(make_rule(timeslot=timeslot))
        )

    def test_zero_level_means_auto_and_is_valid(self):
        assert sr.validate_rule(make_rule(level=0, sublevel=0)) == []

    def test_negative_level(self):
        assert sr.validate_rule(make_rule(level=-1))

    def test_instructions_at_limit_are_valid(self):
        assert (
            sr.validate_rule(
                make_rule(experimenters_instructions="x" * sr.MAX_INSTRUCTIONS_LENGTH)
            )
            == []
        )

    def test_instructions_over_limit(self):
        rule = make_rule(
            experimenters_instructions="x" * (sr.MAX_INSTRUCTIONS_LENGTH + 1)
        )
        assert any("instructions" in p for p in sr.validate_rule(rule))

    def test_unknown_status(self):
        assert sr.validate_rule(make_rule(status="paused"))


class TestRuleOccursOn:
    def test_start_and_end_are_inclusive(self):
        rule = make_rule(start_date=TUE, end_date=THU)
        assert [sr.rule_occurs_on(rule, d) for d in (MON, TUE, WED, THU, FRI)] == [
            False,
            True,
            True,
            True,
            False,
        ]

    def test_open_ended_runs_far_into_future(self):
        assert sr.rule_occurs_on(
            make_rule(end_date=None), MON + datetime.timedelta(days=3650)
        )

    def test_weekdays_only_skips_weekend(self):
        rule = make_rule(weekdays=sr.WEEKDAYS)
        assert [sr.rule_occurs_on(rule, d) for d in (FRI, SAT, SUN, NEXT_MON)] == [
            True,
            False,
            False,
            True,
        ]

    def test_weekends_only(self):
        rule = make_rule(weekdays=sr.WEEKENDS)
        assert [sr.rule_occurs_on(rule, d) for d in (FRI, SAT, SUN)] == [
            False,
            True,
            True,
        ]

    @pytest.mark.parametrize("status", ["ended", "cancelled"])
    def test_inactive_rule_never_occurs(self, status):
        assert not sr.rule_occurs_on(make_rule(status=status), MON)

    def test_empty_weekdays_never_occurs(self):
        assert not sr.rule_occurs_on(make_rule(weekdays=frozenset()), MON)


class TestDateRangesOverlap:
    @pytest.mark.parametrize(
        ("a", "b", "expected"),
        [
            ((MON, WED), (WED, FRI), True),  # touching on one day counts
            ((MON, TUE), (WED, FRI), False),  # adjacent, no shared day
            ((MON, None), (NEXT_MON, None), True),  # both open-ended
            ((MON, TUE), (WED, None), False),  # open-ended starts after closed ends
            ((WED, None), (MON, TUE), False),
            ((MON, None), (MON - datetime.timedelta(days=30), MON), True),
            ((MON, MON), (MON, MON), True),  # single-day ranges
        ],
    )
    def test_cases(self, a, b, expected):
        assert sr.date_ranges_overlap(*a, *b) is expected
        assert sr.date_ranges_overlap(*b, *a) is expected


class TestFindRuleConflicts:
    def test_same_slot_overlapping_conflicts(self):
        existing = make_rule(rule_id=2, subject_fullname="other")
        assert sr.find_rule_conflicts(make_rule(), [existing]) == [existing]

    def test_weekday_and_weekend_rules_share_a_slot(self):
        """The motivating case: one mouse weekdays, another weekends, same rig and slot."""
        weekday = make_rule(rule_id=1, weekdays=sr.WEEKDAYS)
        weekend = make_rule(rule_id=2, subject_fullname="other", weekdays=sr.WEEKENDS)
        assert sr.find_rule_conflicts(weekend, [weekday]) == []

    def test_sequential_date_ranges_share_a_slot(self):
        first = make_rule(rule_id=1, start_date=MON, end_date=FRI)
        second = make_rule(
            rule_id=2, subject_fullname="other", start_date=SAT, end_date=None
        )
        assert sr.find_rule_conflicts(second, [first]) == []

    def test_different_timeslot_or_location_never_conflicts(self):
        candidate = make_rule()
        others = [
            make_rule(rule_id=2, timeslot=3),
            make_rule(rule_id=3, location="165I-Rig2-T"),
        ]
        assert sr.find_rule_conflicts(candidate, others) == []

    def test_editing_a_rule_ignores_itself(self):
        rule = make_rule()
        assert sr.find_rule_conflicts(rule, [rule]) == []

    def test_inactive_rules_do_not_block(self):
        assert (
            sr.find_rule_conflicts(make_rule(), [make_rule(rule_id=2, status="ended")])
            == []
        )

    def test_inactive_candidate_conflicts_with_nothing(self):
        assert (
            sr.find_rule_conflicts(
                make_rule(status="cancelled"), [make_rule(rule_id=2)]
            )
            == []
        )

    def test_short_ranges_overlapping_without_a_shared_occurrence(self):
        # Ranges overlap on Tue-Wed, weekday sets share Mon, but Mon is outside the overlap.
        a = make_rule(
            rule_id=1, start_date=MON, end_date=WED, weekdays=frozenset({"Mon", "Tue"})
        )
        b = make_rule(
            rule_id=2, start_date=WED, end_date=FRI, weekdays=frozenset({"Mon", "Fri"})
        )
        assert sr.find_rule_conflicts(a, [b]) == []

    def test_shared_weekday_beyond_first_week_of_open_ended_overlap(self):
        a = make_rule(rule_id=1, start_date=MON, weekdays=frozenset({"Sun"}))
        b = make_rule(rule_id=2, start_date=TUE, weekdays=frozenset({"Sun"}))
        assert sr.find_rule_conflicts(a, [b]) == [b]


class TestExpandRule:
    def test_weekdays_over_two_weeks(self):
        dates = sr.expand_rule(make_rule(weekdays=sr.WEEKDAYS), MON, day(13))
        assert len(dates) == 10
        assert all(d.weekday() < 5 for d in dates)

    def test_window_clipped_by_rule_dates(self):
        rule = make_rule(start_date=WED, end_date=FRI)
        assert sr.expand_rule(rule, MON, NEXT_MON) == [WED, THU, FRI]

    def test_window_end_before_start_is_empty(self):
        assert sr.expand_rule(make_rule(), FRI, MON) == []

    def test_single_day_window(self):
        assert sr.expand_rule(make_rule(), WED, WED) == [WED]

    def test_skip_and_closures_remove_dates(self):
        assert sr.expand_rule(
            make_rule(), MON, WED, skip_dates=[TUE], closures=[WED]
        ) == [MON]

    def test_rule_entirely_before_window(self):
        assert (
            sr.expand_rule(
                make_rule(start_date=MON, end_date=TUE),
                NEXT_MON,
                NEXT_MON + datetime.timedelta(days=6),
            )
            == []
        )


class TestPlanMaterialization:
    def test_empty_schedule_gets_inserts(self):
        rule = make_rule(weekdays=sr.WEEKDAYS)
        plan = sr.plan_materialization([rule], [], MON, SUN)
        assert [row["date"] for row in plan.inserts] == [MON, TUE, WED, THU, FRI]
        assert all(row["rule_id"] == 1 for row in plan.inserts)
        assert not plan.updates and not plan.deletes and not plan.conflicts

    def test_no_rules_no_writes(self):
        assert sr.plan_materialization([], [], MON, SUN).is_empty

    def test_window_end_before_start_plans_nothing(self):
        assert sr.plan_materialization([make_rule()], [], FRI, MON).is_empty

    def test_rerun_is_idempotent(self):
        rule = make_rule()
        existing = [row_for(rule, d) for d in (MON, TUE)]
        plan = sr.plan_materialization([rule], existing, MON, TUE)
        assert plan.is_empty and not plan.conflicts

    def test_changed_rule_updates_its_rows(self):
        rule = make_rule(training_profile_id=11)
        existing = [row_for(rule, MON, training_profile_id=10)]
        plan = sr.plan_materialization([rule], existing, MON, MON)
        assert plan.updates == [row_for(rule, MON)]

    def test_fills_empty_slot_left_by_web_app(self):
        rule = make_rule()
        empty = row_for(rule, MON, subject_fullname=None, rule_id=None)
        plan = sr.plan_materialization([rule], [empty], MON, MON)
        assert plan.updates == [row_for(rule, MON)]

    def test_manual_booking_wins_and_is_reported(self):
        rule = make_rule()
        manual = row_for(rule, MON, subject_fullname="someone_else", rule_id=None)
        plan = sr.plan_materialization([rule], [manual], MON, MON)
        assert plan.is_empty
        assert plan.conflicts == [
            sr.Conflict(MON, rule.location, rule.timeslot, 1, "someone_else", None)
        ]

    def test_slot_owned_by_another_active_rule_is_a_conflict(self):
        mine = make_rule(rule_id=1)
        theirs = make_rule(rule_id=2, subject_fullname="other")
        plan = sr.plan_materialization([mine, theirs], [row_for(theirs, MON)], MON, MON)
        assert [(c.rule_id, c.existing_rule_id) for c in plan.conflicts] == [(1, 2)]
        assert plan.is_empty

    def test_row_from_ended_rule_is_taken_over_not_deleted(self):
        """A new rule on a slot whose previous rule ended replaces the stale row in one pass."""
        new = make_rule(rule_id=3, subject_fullname="successor")
        stale = row_for(make_rule(rule_id=2, subject_fullname="retired"), MON)
        plan = sr.plan_materialization([new], [stale], MON, MON)
        assert plan.updates == [row_for(new, MON)]
        assert not plan.deletes and not plan.conflicts

    def test_hand_edited_row_of_ended_rule_blocks_new_rule(self):
        new = make_rule(rule_id=3)
        edited = row_for(make_rule(rule_id=2, subject_fullname="retired"), MON)
        plan = sr.plan_materialization(
            [new], [edited], MON, MON, exceptions={(2, MON): "override"}
        )
        assert plan.is_empty
        assert plan.conflicts[0].existing_rule_id == 2

    def test_two_active_rules_on_one_slot_lowest_id_wins(self):
        a = make_rule(rule_id=1)
        b = make_rule(rule_id=2, subject_fullname="other")
        plan = sr.plan_materialization([b, a], [], MON, MON)
        assert [row["rule_id"] for row in plan.inserts] == [1]
        assert [c.rule_id for c in plan.conflicts] == [2]

    def test_ended_rule_rows_are_deleted(self):
        """Setting end_date or status removes future generated rows."""
        rule = make_rule(end_date=TUE)
        existing = [row_for(rule, d) for d in (MON, TUE, WED, THU)]
        plan = sr.plan_materialization([rule], existing, MON, THU)
        assert plan.deletes == [
            {"date": d, "location": rule.location, "timeslot": rule.timeslot}
            for d in (WED, THU)
        ]

    def test_rule_rows_deleted_when_rule_missing_from_active_list(self):
        rule = make_rule()
        plan = sr.plan_materialization([], [row_for(rule, MON)], MON, MON)
        assert len(plan.deletes) == 1

    def test_moving_rule_to_new_slot_deletes_old_and_inserts_new(self):
        old = make_rule(timeslot=2)
        moved = make_rule(timeslot=5)
        plan = sr.plan_materialization([moved], [row_for(old, MON)], MON, MON)
        assert plan.deletes == [{"date": MON, "location": old.location, "timeslot": 2}]
        assert plan.inserts[0]["timeslot"] == 5

    def test_legacy_rows_are_never_deleted(self):
        legacy = row_for(make_rule(), MON, rule_id=None)
        assert sr.plan_materialization([], [legacy], MON, MON).is_empty

    def test_rows_outside_window_untouched(self):
        rule = make_rule(start_date=NEXT_MON)
        past_and_future = [
            row_for(rule, MON - datetime.timedelta(days=1)),
            row_for(rule, NEXT_MON),
        ]
        assert sr.plan_materialization([rule], past_and_future, MON, SUN).is_empty

    def test_skip_exception_removes_existing_row(self):
        rule = make_rule()
        plan = sr.plan_materialization(
            [rule], [row_for(rule, TUE)], TUE, TUE, exceptions={(1, TUE): "skip"}
        )
        assert plan.deletes == [
            {"date": TUE, "location": rule.location, "timeslot": rule.timeslot}
        ]
        assert not plan.inserts

    def test_override_exception_leaves_hand_edited_row(self):
        rule = make_rule()
        edited = row_for(rule, TUE, level=7, experimenters_instructions="probe day")
        plan = sr.plan_materialization(
            [rule], [edited], TUE, TUE, exceptions={(1, TUE): "override"}
        )
        assert plan.is_empty and not plan.conflicts

    def test_override_row_kept_even_after_rule_ends(self):
        rule = make_rule(end_date=MON)
        edited = row_for(rule, TUE)
        plan = sr.plan_materialization(
            [rule], [edited], TUE, TUE, exceptions={(1, TUE): "override"}
        )
        assert plan.is_empty

    def test_exception_for_other_rule_does_not_apply(self):
        rule = make_rule(rule_id=1)
        plan = sr.plan_materialization(
            [rule], [], TUE, TUE, exceptions={(2, TUE): "skip"}
        )
        assert len(plan.inserts) == 1

    def test_lab_closure_blocks_all_rules(self):
        rules = [
            make_rule(rule_id=1),
            make_rule(rule_id=2, timeslot=3, subject_fullname="other"),
        ]
        plan = sr.plan_materialization(rules, [], MON, WED, closures=[TUE])
        assert sorted({row["date"] for row in plan.inserts}) == [MON, WED]

    def test_closure_deletes_previously_generated_rows(self):
        rule = make_rule()
        plan = sr.plan_materialization(
            [rule], [row_for(rule, TUE)], TUE, TUE, closures=[TUE]
        )
        assert len(plan.deletes) == 1

    def test_weekday_and_weekend_mice_alternate_on_one_slot(self):
        weekday = make_rule(rule_id=1, weekdays=sr.WEEKDAYS)
        weekend = make_rule(
            rule_id=2, subject_fullname="weekend_mouse", weekdays=sr.WEEKENDS
        )
        plan = sr.plan_materialization([weekday, weekend], [], MON, SUN)
        by_date = {row["date"]: row["subject_fullname"] for row in plan.inserts}
        assert by_date[FRI] == "ab1234_m1"
        assert by_date[SAT] == by_date[SUN] == "weekend_mouse"
        assert not plan.conflicts


class TestRulesFromRows:
    def test_joins_day_parts(self):
        rule_row = {**make_rule().__dict__, "created_by": "ab1234", "created_at": None}
        rule_row.pop("weekdays")
        days = [
            {"rule_id": 1, "weekday": "Sat"},
            {"rule_id": 1, "weekday": "Sun"},
            {"rule_id": 9, "weekday": "Mon"},
        ]
        (rule,) = sr.rules_from_rows([rule_row], days)
        assert rule.weekdays == sr.WEEKENDS
        assert rule == make_rule(weekdays=sr.WEEKENDS)

    def test_rule_without_days_never_runs(self):
        rule_row = {**make_rule().__dict__}
        rule_row.pop("weekdays")
        (rule,) = sr.rules_from_rows([rule_row], [])
        assert rule.weekdays == frozenset()
        assert sr.expand_rule(rule, MON, SUN) == []

    def test_null_end_date_is_open_ended(self):
        rule_row = {**make_rule(end_date=None).__dict__}
        rule_row.pop("weekdays")
        (rule,) = sr.rules_from_rows([rule_row], [{"rule_id": 1, "weekday": "Mon"}])
        assert rule.end_date is None


class TestRestingSubjects:
    def test_weekday_cohort_rests_friday_to_saturday(self):
        rules = [
            make_rule(weekdays=sr.WEEKDAYS),
            make_rule(rule_id=2, subject_fullname="daily", timeslot=3),
        ]
        assert sr.subjects_resting_on(rules, FRI, SAT) == {"ab1234_m1"}

    def test_nobody_rests_midweek(self):
        assert (
            sr.subjects_resting_on([make_rule(weekdays=sr.WEEKDAYS)], TUE, WED) == set()
        )

    def test_rule_ending_today(self):
        assert sr.subjects_resting_on([make_rule(end_date=TUE)], TUE, WED) == {
            "ab1234_m1"
        }

    def test_no_rules(self):
        assert sr.subjects_resting_on([], FRI, SAT) == set()

    def test_drop_planned_rest_removes_only_todays_rows(self):
        df = pd.DataFrame(
            [
                {"date": FRI, "location": "r1", "subject_fullname": "weekday_mouse"},
                {"date": FRI, "location": "r1", "subject_fullname": "daily"},
                {"date": SAT, "location": "r1", "subject_fullname": "weekday_mouse"},
            ]
        )
        out = sr.drop_planned_rest(df, FRI, {"weekday_mouse"})
        assert out.to_dict("records") == [df.iloc[1].to_dict(), df.iloc[2].to_dict()]

    def test_drop_planned_rest_empty_inputs(self):
        empty = pd.DataFrame()
        assert sr.drop_planned_rest(empty, FRI, {"x"}) is empty
        df = pd.DataFrame([{"date": FRI, "location": "r1", "subject_fullname": "a"}])
        assert sr.drop_planned_rest(df, FRI, set()) is df

    def test_drop_planned_rest_keeps_null_subject_rows(self):
        df = pd.DataFrame([{"date": FRI, "location": "r1", "subject_fullname": None}])
        assert len(sr.drop_planned_rest(df, FRI, {"a"})) == 1


class TestApplyAndMaterialize:
    def make_scheduler(
        self, rules=(), days=(), existing=(), exceptions=(), closures=()
    ):
        scheduler = MagicMock()
        scheduler.ScheduleRule.__and__.return_value.fetch.return_value = list(rules)
        scheduler.ScheduleRule.Day.fetch.return_value = list(days)
        scheduler.Schedule.__and__.return_value.fetch.return_value = list(existing)
        scheduler.ScheduleRuleException.__and__.return_value.fetch.return_value = list(
            exceptions
        )
        scheduler.LabClosure.__and__.return_value.fetch.return_value = list(closures)
        return scheduler

    def test_apply_plan_order_and_transaction(self):
        scheduler = MagicMock()
        plan = sr.MaterializationPlan(
            inserts=[{"i": 1}], updates=[{"u": 1}], deletes=[{"d": 1}]
        )
        sr.apply_plan(scheduler, plan)
        scheduler.Schedule.connection.transaction.__enter__.assert_called_once()
        scheduler.Schedule.__and__.assert_called_once_with({"d": 1})
        scheduler.Schedule.update1.assert_called_once_with({"u": 1})
        scheduler.Schedule.insert.assert_called_once_with([{"i": 1}])

    def test_apply_plan_skips_empty_insert(self):
        scheduler = MagicMock()
        sr.apply_plan(scheduler, sr.MaterializationPlan())
        scheduler.Schedule.insert.assert_not_called()

    def _rule_row(self):
        row = {**make_rule().__dict__}
        row.pop("weekdays")
        return row

    def test_nightly_window_starts_tomorrow(self):
        scheduler = self.make_scheduler(
            rules=[self._rule_row()],
            days=[{"rule_id": 1, "weekday": d} for d in sr.EVERY_DAY],
        )
        plan = sr.materialize_schedule(scheduler, today=MON, horizon_days=2)
        assert [row["date"] for row in plan.inserts] == [TUE, WED]
        scheduler.Schedule.insert.assert_called_once()
        assert (
            call('date between "2024-01-02" and "2024-01-03"')
            in scheduler.Schedule.__and__.call_args_list
        )

    def test_include_today_for_web_app(self):
        scheduler = self.make_scheduler(
            rules=[self._rule_row()],
            days=[{"rule_id": 1, "weekday": d} for d in sr.EVERY_DAY],
        )
        plan = sr.materialize_schedule(
            scheduler, today=MON, horizon_days=1, include_today=True
        )
        assert [row["date"] for row in plan.inserts] == [MON, TUE]

    def test_zero_horizon_nightly_does_nothing(self):
        scheduler = self.make_scheduler(
            rules=[self._rule_row()], days=[{"rule_id": 1, "weekday": "Mon"}]
        )
        plan = sr.materialize_schedule(scheduler, today=MON, horizon_days=0)
        assert plan.is_empty
        scheduler.Schedule.insert.assert_not_called()

    def test_dry_run_never_writes(self):
        scheduler = self.make_scheduler(
            rules=[self._rule_row()],
            days=[{"rule_id": 1, "weekday": d} for d in sr.EVERY_DAY],
        )
        plan = sr.materialize_schedule(
            scheduler, today=MON, horizon_days=3, dry_run=True
        )
        assert len(plan.inserts) == 3
        scheduler.Schedule.insert.assert_not_called()
        scheduler.Schedule.update1.assert_not_called()

    def test_exceptions_and_closures_are_read(self):
        scheduler = self.make_scheduler(
            rules=[self._rule_row()],
            days=[{"rule_id": 1, "weekday": d} for d in sr.EVERY_DAY],
            exceptions=[{"rule_id": 1, "date": TUE, "action": "skip"}],
            closures=[WED],
        )
        plan = sr.materialize_schedule(
            scheduler, today=MON, horizon_days=3, dry_run=True
        )
        assert [row["date"] for row in plan.inserts] == [THU]
