import datetime

import pandas as pd
import pytest

from u19_pipeline.alert_system.water_weigh_alert import alert_tiers as at

# MySQL returns naive datetimes in the lab's local (Eastern) wall-clock time
TODAY = datetime.datetime(2026, 10, 5)  # noqa: DTZ001


def make_subjects(rows):
    defaults = {
        "subject_status": "InExperiments",
        "schedule_today": "Train",
        "first_timeslot": None,
        "first_water_time": None,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def early_names(subject_data):
    return at.filter_subjects_for_tier(subject_data, at.AlertTier.EARLY)[
        "subject_fullname"
    ].tolist()


class TestResolveAlertTier:
    @pytest.mark.parametrize("hour", [0, 16, 18, 19])
    def test_before_cutoff_is_early(self, hour):
        now = datetime.datetime(2026, 10, 5, hour, 0, tzinfo=at.EASTERN)
        assert at.resolve_alert_tier(now=now) == at.AlertTier.EARLY

    @pytest.mark.parametrize("hour", [20, 21, 22, 23])
    def test_after_cutoff_is_all(self, hour):
        now = datetime.datetime(2026, 10, 5, hour, 0, tzinfo=at.EASTERN)
        assert at.resolve_alert_tier(now=now) == at.AlertTier.ALL

    def test_one_minute_before_cutoff(self):
        now = datetime.datetime(2026, 10, 5, 19, 59, tzinfo=at.EASTERN)
        assert at.resolve_alert_tier(now=now) == at.AlertTier.EARLY

    def test_cutoff_is_8_pm(self):
        now = datetime.datetime(2026, 10, 5, 20, 0, tzinfo=at.EASTERN)
        assert at.resolve_alert_tier(now=now) == at.AlertTier.ALL

    def test_aware_utc_time_is_converted_to_eastern(self):
        # 22:00 UTC is 18:00 EDT
        now = datetime.datetime(2026, 10, 5, 22, 0, tzinfo=datetime.UTC)
        assert at.resolve_alert_tier(now=now) == at.AlertTier.EARLY
        # 02:00 UTC the next day is 22:00 EDT
        now = datetime.datetime(2026, 10, 6, 2, 0, tzinfo=datetime.UTC)
        assert at.resolve_alert_tier(now=now) == at.AlertTier.ALL

    def test_winter_time_is_converted_to_eastern(self):
        # 00:30 UTC in January is 19:30 EST (it would be 20:30 in EDT)
        now = datetime.datetime(2026, 1, 6, 0, 30, tzinfo=datetime.UTC)
        assert at.resolve_alert_tier(now=now) == at.AlertTier.EARLY
        # 01:30 UTC in January is 20:30 EST
        now = datetime.datetime(2026, 1, 6, 1, 30, tzinfo=datetime.UTC)
        assert at.resolve_alert_tier(now=now) == at.AlertTier.ALL

    @pytest.mark.parametrize("tier", list(at.AlertTier))
    def test_explicit_tier_overrides_clock(self, tier):
        now = datetime.datetime(2026, 10, 5, 23, 0, tzinfo=at.EASTERN)
        assert at.resolve_alert_tier(tier, now=now) is tier

    @pytest.mark.parametrize("tier", list(at.AlertTier))
    def test_plain_string_becomes_enum_member(self, tier):
        # The cron script passes the --tier value through as a plain str
        result = at.resolve_alert_tier(str(tier))
        assert isinstance(result, at.AlertTier)
        assert result is tier

    @pytest.mark.parametrize("tier", ["", "EARLY", " early", "late"])
    def test_near_miss_strings_raise(self, tier):
        with pytest.raises(ValueError):
            at.resolve_alert_tier(tier)

    def test_unknown_tier_raises(self):
        with pytest.raises(ValueError):
            at.resolve_alert_tier("late")

    def test_defaults_to_current_time(self):
        assert isinstance(at.resolve_alert_tier(), at.AlertTier)

    def test_clock_reaches_every_tier(self):
        # Every AlertTier member must be selected at some hour of the day
        tiers_by_hour = {
            at.resolve_alert_tier(
                now=datetime.datetime(2026, 10, 5, hour, tzinfo=at.EASTERN)
            )
            for hour in range(24)
        }
        assert tiers_by_hour == set(at.AlertTier)


class TestTrainingTimeslot:
    def test_timeslot_boundary(self):
        subject_data = make_subjects(
            [
                {"subject_fullname": f"slot{slot}", "first_timeslot": slot}
                for slot in [0, 1, 4, 5, 6, 10]
            ]
        )
        assert early_names(subject_data) == ["slot0", "slot1", "slot4", "slot5"]

    def test_not_scheduled_is_excluded(self):
        subject_data = make_subjects(
            [{"subject_fullname": "unscheduled", "first_timeslot": None}]
        )
        assert early_names(subject_data) == []

    def test_timeslot_returned_as_string(self):
        # MySQL aggregates can come back as Decimal or str
        subject_data = make_subjects(
            [
                {"subject_fullname": "early", "first_timeslot": "3"},
                {"subject_fullname": "late", "first_timeslot": "7"},
            ]
        )
        assert early_names(subject_data) == ["early"]

    def test_trained_late_but_watered_early_is_excluded(self):
        # Training subjects are judged by their timeslot, not their water time
        subject_data = make_subjects(
            [
                {
                    "subject_fullname": "late_trainer",
                    "first_timeslot": 7,
                    "first_water_time": TODAY.replace(hour=9),
                }
            ]
        )
        assert early_names(subject_data) == []


class TestWaterOnly:
    @pytest.mark.parametrize(
        "status, schedule_today",
        [
            ("InExperiments", "Water"),
            ("InExperiments", "water"),
            ("WaterRestrictionOnly", None),
            ("WaterRestrictionOnly", "Train"),
        ],
    )
    def test_water_only_watered_in_the_morning(self, status, schedule_today):
        subject_data = make_subjects(
            [
                {
                    "subject_fullname": "s",
                    "subject_status": status,
                    "schedule_today": schedule_today,
                    "first_water_time": TODAY.replace(hour=10),
                }
            ]
        )
        assert early_names(subject_data) == ["s"]

    def test_water_cutoff_boundary(self):
        subject_data = make_subjects(
            [
                {
                    "subject_fullname": "15:59",
                    "schedule_today": "Water",
                    "first_water_time": TODAY.replace(hour=15, minute=59, second=59),
                },
                {
                    "subject_fullname": "16:00",
                    "schedule_today": "Water",
                    "first_water_time": TODAY.replace(hour=16),
                },
                {
                    "subject_fullname": "17:30",
                    "schedule_today": "Water",
                    "first_water_time": TODAY.replace(hour=17, minute=30),
                },
            ]
        )
        assert early_names(subject_data) == ["15:59"]

    def test_midnight_counts_as_early(self):
        subject_data = make_subjects(
            [
                {
                    "subject_fullname": "midnight",
                    "schedule_today": "Water",
                    "first_water_time": TODAY,
                }
            ]
        )
        assert early_names(subject_data) == ["midnight"]

    def test_not_watered_yet_waits_for_late_alert(self):
        subject_data = make_subjects(
            [
                {
                    "subject_fullname": "thirsty",
                    "schedule_today": "Water",
                    "first_water_time": None,
                }
            ]
        )
        assert early_names(subject_data) == []
        all_names = at.filter_subjects_for_tier(subject_data, at.AlertTier.ALL)
        assert all_names["subject_fullname"].tolist() == ["thirsty"]

    def test_training_subject_without_slot_is_excluded(self):
        subject_data = make_subjects(
            [
                {
                    "subject_fullname": "trainer",
                    "schedule_today": "Train",
                    "first_water_time": TODAY.replace(hour=10),
                },
                {
                    "subject_fullname": "no_schedule",
                    "schedule_today": None,
                    "first_water_time": None,
                },
            ]
        )
        assert early_names(subject_data) == []


class TestFilterSubjectsForTier:
    def test_all_tier_keeps_everyone(self):
        subject_data = make_subjects(
            [
                {"subject_fullname": "early", "first_timeslot": 1},
                {"subject_fullname": "late", "first_timeslot": 8},
            ]
        )
        result = at.filter_subjects_for_tier(subject_data, at.AlertTier.ALL)
        assert result["subject_fullname"].tolist() == ["early", "late"]

    @pytest.mark.parametrize("tier", list(at.AlertTier))
    def test_returns_range_index(self, tier):
        # Every frame in the alert is numbered 0..n-1, even after filtering
        subject_data = make_subjects(
            [
                {"subject_fullname": "late", "first_timeslot": 8},
                {"subject_fullname": "early", "first_timeslot": 1},
                {"subject_fullname": "early2", "first_timeslot": 2},
            ]
        ).set_axis([10, 20, 30])
        result = at.filter_subjects_for_tier(subject_data, tier)
        pd.testing.assert_index_equal(result.index, pd.RangeIndex(len(result)))

    def test_does_not_modify_input(self):
        subject_data = make_subjects(
            [{"subject_fullname": "late", "first_timeslot": 8}]
        ).set_axis([10])
        at.filter_subjects_for_tier(subject_data, at.AlertTier.ALL)
        assert subject_data.index.tolist() == [10]

    def test_empty_dataframe(self):
        subject_data = make_subjects([]).reindex(
            columns=[
                "subject_fullname",
                "subject_status",
                "schedule_today",
                "first_timeslot",
                "first_water_time",
            ]
        )
        assert at.filter_subjects_for_tier(subject_data, at.AlertTier.EARLY).empty

    @pytest.mark.parametrize("tier", list(at.AlertTier))
    def test_every_tier_is_handled(self, tier):
        # Fails if a new AlertTier member is added without a filter rule
        subject_data = make_subjects(
            [
                {"subject_fullname": "early", "first_timeslot": 1},
                {"subject_fullname": "late", "first_timeslot": 8},
            ]
        )
        result = at.filter_subjects_for_tier(subject_data, tier)
        assert set(result["subject_fullname"]) <= {"early", "late"}
        assert "early" in set(result["subject_fullname"])

    @pytest.mark.parametrize("tier", list(at.AlertTier))
    def test_every_tier_accepts_plain_string(self, tier):
        subject_data = make_subjects([{"subject_fullname": "s", "first_timeslot": 1}])
        pd.testing.assert_frame_equal(
            at.filter_subjects_for_tier(subject_data, str(tier)),
            at.filter_subjects_for_tier(subject_data, tier),
        )

    def test_unknown_tier_raises(self):
        with pytest.raises(ValueError):
            at.filter_subjects_for_tier(make_subjects([]), "late")


class TestTimeslotStartTime:
    @pytest.mark.parametrize(
        "timeslot, hour",
        [(1, 9), (2, 10), (3, 11), (4, 13), (5, 14), (6, 15), (7, 16), (8, 17)],
    )
    def test_matches_rig_schedule(self, timeslot, hour):
        # Slot 1 starts at 9 AM; 12-1 PM is lunch, so slot 4 starts at 1 PM
        assert at.timeslot_start_time(timeslot) == datetime.time(hour)

    def test_last_early_timeslot_starts_at_2_pm(self):
        assert at.timeslot_start_time(at.LAST_EARLY_TIMESLOT) == datetime.time(14)

    @pytest.mark.parametrize("timeslot", [-1, 0, 9, 100])
    def test_out_of_range_raises(self, timeslot):
        with pytest.raises(ValueError):
            at.timeslot_start_time(timeslot)


class TestFormatClockTime:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (datetime.time(0), "12 AM"),
            (datetime.time(9), "9 AM"),
            (datetime.time(11, 59), "11:59 AM"),
            (datetime.time(12), "12 PM"),
            (datetime.time(14), "2 PM"),
            (datetime.time(16, 30), "4:30 PM"),
            (datetime.time(23), "11 PM"),
        ],
    )
    def test_formats_12_hour_clock(self, value, expected):
        assert at.format_clock_time(value) == expected


class TestEarlyTierDescription:
    def test_names_slot_start_and_watering_cutoff(self):
        assert at.early_tier_description() == (
            "subjects trained in slot 5 (starts 2 PM) or earlier,"
            " or water-only and watered before 4 PM"
        )


class TestExplicitWaterOnly:
    def test_researcher_training_is_not_water_only(self):
        from u19_pipeline.alert_system.water_weigh_alert import explicit_duties
        from u19_pipeline.utils.responsibilities import DailyAssignment, Responsibility

        trains = DailyAssignment(
            frozenset({Responsibility.TRANSPORT_ONLY}),
            frozenset({Responsibility.WATERING, Responsibility.TRAINING}),
        )
        waters = DailyAssignment(
            frozenset({Responsibility.TRANSPORT_ONLY}),
            frozenset({Responsibility.WATERING}),
        )
        data = explicit_duties.apply_assignments(
            make_subjects(
                [{"subject_fullname": "trains"}, {"subject_fullname": "waters"}]
            ),
            {"trains": trains, "waters": waters},
        )
        # Both read "Transport" in schedule_today
        assert data["schedule_today"].tolist() == ["Transport", "Transport"]
        assert at.is_water_only_today(data).tolist() == [False, True]
