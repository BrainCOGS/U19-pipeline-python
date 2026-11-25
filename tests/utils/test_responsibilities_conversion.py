import datetime
import json

import pytest

from u19_pipeline.utils import responsibilities as r
from u19_pipeline.utils import responsibilities_conversion as conv

R = r.Responsibility
M = r.ManualResponsibility
WATER_WEIGH = {R.WATERING, R.WEIGHING}
EVERYTHING = {R.WATERING, R.WEIGHING, R.TRAINING}


def week(*tokens):
    """Seven tokens, Sunday first."""
    assert len(tokens) == 7
    return "/".join(tokens)


def days(result):
    return {
        day: (frozenset(a.technician), frozenset(a.owner))
        for day, a in result.days.items()
    }


def convert(
    schedule, status="InExperiments", technician_supported=True, manual_weekdays=False
):
    result = conv.convert_legacy(
        schedule, status, technician_supported, manual_weekdays
    )
    assert result.error is None, result.error
    return result


class TestPerDay:
    @pytest.mark.parametrize(
        ("token", "status", "technician", "owner"),
        [
            ("Train", "InExperiments", EVERYTHING, {R.NOTHING}),
            ("OnlyTrain", "InExperiments", EVERYTHING, {R.NOTHING}),
            # Training is not offered for water-restricted subjects
            ("Train", "WaterRestrictionOnly", WATER_WEIGH, {R.NOTHING}),
            ("Water", "InExperiments", WATER_WEIGH, {R.NOTHING}),
            ("Weigh", "WaterRestrictionOnly", WATER_WEIGH, {R.NOTHING}),
            ("Transport", "InExperiments", {R.TRANSPORT_ONLY}, EVERYTHING),
            ("Transport", "WaterRestrictionOnly", {R.TRANSPORT_ONLY}, WATER_WEIGH),
            # The researcher still waters and weighs a water-restricted subject
            ("Nothing", "InExperiments", {R.NOTHING}, WATER_WEIGH),
            ("Nothing", "WaterRestrictionOnly", {R.NOTHING}, WATER_WEIGH),
        ],
    )
    def test_with_technician_support(self, token, status, technician, owner):
        result = convert(week(*[token] * 7), status)
        assert set(days(result).values()) == {(frozenset(technician), frozenset(owner))}

    @pytest.mark.parametrize(
        ("token", "status", "owner"),
        [
            ("Train", "InExperiments", EVERYTHING),
            ("Water", "InExperiments", WATER_WEIGH),
            ("Weigh", "InExperiments", WATER_WEIGH),
            ("Transport", "InExperiments", EVERYTHING),
            ("Nothing", "InExperiments", WATER_WEIGH),
            ("Train", "WaterRestrictionOnly", WATER_WEIGH),
        ],
    )
    def test_without_technician_support_the_researcher_does_it(
        self, token, status, owner
    ):
        result = convert(week(*[token] * 7), status, technician_supported=False)
        assert days(result)["Monday"] == ({R.TRANSPORT_ONLY}, owner)

    @pytest.mark.parametrize("status", ["AdLibWater", "Dead", "Missing"])
    @pytest.mark.parametrize("supported", [True, False])
    def test_inactive_subjects_need_nothing(self, status, supported):
        result = convert(week(*["Train"] * 7), status, technician_supported=supported)
        assert set(days(result).values()) == {
            (frozenset({R.NOTHING}), frozenset({R.NOTHING}))
        }


class TestStyle:
    def test_same_every_day_is_weekly(self):
        result = convert(week(*["Train"] * 7))
        assert json.loads(result.json)["assignment_style"] == "weekly"

    def test_weekdays_and_weekends_is_workweek_split(self):
        result = convert(
            week("Water", "Train", "Train", "Train", "Train", "Train", "Water")
        )
        record = json.loads(result.json)
        assert record["assignment_style"] == "workweek_split"
        assert record["weekdays"] == {
            "technician": ["Watering", "Weighing", "Training"],
            "owner": ["Nothing"],
        }
        assert record["weekends"] == {
            "technician": ["Watering", "Weighing"],
            "owner": ["Nothing"],
        }

    def test_anything_else_is_daily_and_sunday_first(self):
        # The notebook this replaces read schedule[0] as Monday
        result = convert(
            week("Transport", "Train", "Water", "Train", "Train", "Train", "Nothing")
        )
        record = json.loads(result.json)
        assert record["assignment_style"] == "daily"
        assert record["Sunday"]["technician"] == ["Transport Only"]
        assert record["Monday"]["technician"] == ["Watering", "Weighing", "Training"]
        assert record["Tuesday"]["technician"] == ["Watering", "Weighing"]
        assert record["Saturday"]["technician"] == ["Nothing"]

    def test_saturday_and_sunday_must_match_for_a_split(self):
        result = convert(
            week("Water", "Train", "Train", "Train", "Train", "Train", "Nothing")
        )
        assert json.loads(result.json)["assignment_style"] == "daily"


class TestManualWeekdays:
    def test_weekdays_become_manual_for_the_researcher(self):
        result = convert(week(*["Train"] * 7), manual_weekdays=True)
        record = json.loads(result.json)
        assert record["assignment_style"] == "workweek_split"
        assert record["weekdays"] == {
            "technician": ["Training"],
            "owner": ["Manual Weighing", "Manual Watering"],
        }
        # Weekends are untouched
        assert record["weekends"] == {
            "technician": ["Watering", "Weighing", "Training"],
            "owner": ["Nothing"],
        }

    def test_technician_without_duties_left_does_nothing(self):
        result = convert(week(*["Water"] * 7), manual_weekdays=True)
        assert days(result)["Monday"] == (
            {R.NOTHING},
            {M.MANUAL_WATERING, M.MANUAL_WEIGHING},
        )

    def test_researcher_keeps_training(self):
        result = convert(week(*["Transport"] * 7), manual_weekdays=True)
        assert days(result)["Monday"] == (
            {R.TRANSPORT_ONLY},
            {R.TRAINING, M.MANUAL_WATERING, M.MANUAL_WEIGHING},
        )

    def test_without_technician_support(self):
        result = convert(
            week(*["Train"] * 7), technician_supported=False, manual_weekdays=True
        )
        assert days(result)["Wednesday"] == (
            {R.TRANSPORT_ONLY},
            {R.TRAINING, M.MANUAL_WATERING, M.MANUAL_WEIGHING},
        )
        assert days(result)["Sunday"] == ({R.TRANSPORT_ONLY}, EVERYTHING)

    def test_inactive_subjects_are_not_made_manual(self):
        result = convert(week(*["Train"] * 7), "AdLibWater", manual_weekdays=True)
        assert set(days(result).values()) == {
            (frozenset({R.NOTHING}), frozenset({R.NOTHING}))
        }


class TestEveryConversionIsValid:
    @pytest.mark.parametrize(
        "status",
        ["InExperiments", "WaterRestrictionOnly", "AdLibWater", "Dead", "Missing"],
    )
    @pytest.mark.parametrize("supported", [True, False])
    @pytest.mark.parametrize("manual", [True, False])
    @pytest.mark.parametrize("token", conv.LEGACY_TOKENS)
    def test_valid_and_readable(self, status, supported, manual, token):
        schedule = week(token, "Train", "Water", "Weigh", "Transport", "Nothing", token)
        result = convert(schedule, status, supported, manual)
        needs_water = status in conv.WATER_RESTRICTED_STATUSES
        for day, assignment in result.days.items():
            assert (
                r.validate_daily_assignment(
                    assignment.technician, assignment.owner, needs_water
                )
                == []
            )
            if needs_water:
                assert assignment.all & {R.WATERING, M.MANUAL_WATERING}
                assert assignment.all & {R.WEIGHING, M.MANUAL_WEIGHING}
            assert r.assignment_for_day(result.json, day) == assignment


class TestLegacyRoundTrip:
    @pytest.mark.parametrize("token", ["Train", "Water", "Transport", "Nothing"])
    def test_tokens_survive_with_technician_support(self, token):
        result = convert(week(*[token] * 7))
        assert result.legacy_schedule == week(*[token] * 7)

    def test_weigh_reads_back_as_water(self):
        # Both mean the technician waters and weighs; nobody trains
        assert convert(week(*["Weigh"] * 7)).legacy_schedule == week(*["Water"] * 7)


class TestErrors:
    @pytest.mark.parametrize(
        ("schedule", "match"),
        [
            (None, "no schedule"),
            ("", "no schedule"),
            ("Train/Train", "7"),
            (
                week("Train", "Train", "Train", "Bogus", "Train", "Train", "Train"),
                "Bogus",
            ),
        ],
    )
    def test_unusable_schedules(self, schedule, match):
        result = conv.convert_legacy(schedule, "InExperiments", True, False)
        assert result.json is None
        assert match in result.error

    def test_inactive_subjects_ignore_the_schedule(self):
        # Their schedule is never read, so a broken one does not matter
        assert conv.convert_legacy(None, "Dead", True, False).error is None

    def test_unknown_status(self):
        result = conv.convert_legacy(week(*["Train"] * 7), "Sleeping", True, False)
        assert "Sleeping" in result.error


def subject_row(name, schedule="Train/Train/Train/Train/Train/Train/Train", **kwargs):
    return {
        "subject_fullname": name,
        "user_id": name.split("_")[0],
        "subject_status": "InExperiments",
        "water_per_day": 1.5,
        "schedule": schedule,
        "tech_responsibility": "yes",
        "cage": "c1",
        **kwargs,
    }


ALLOWED = frozenset({"InExperiments", "WaterRestrictionOnly", "AdLibWater", "Dead"})


def plan(rows, existing=frozenset(), cages=(), allowed=ALLOWED, users=()):
    return {
        p["subject_fullname"]: p
        for p in conv.plan_conversions(
            rows,
            set(existing),
            set(cages),
            allowed,
            manual_weekday_users=set(users),
        )
    }


class TestPlanConversions:
    def test_insert_row_matches_responsibilities_alt(self):
        p = plan([subject_row("ab_m1")])["ab_m1"]
        assert p["action"] == "insert"
        assert p["record"] == {
            "subject_fullname": "ab_m1",
            "subject_status": "InExperiments",
            "water_per_day": 1.5,
            "responsibilities": p["responsibilities"],
        }
        assert (
            r.assignment_for_day(p["responsibilities"], "Monday").technician
            == EVERYTHING
        )

    def test_existing_records_are_never_overwritten(self):
        p = plan([subject_row("ab_m1")], existing={"ab_m1"})["ab_m1"]
        assert p["action"].startswith("skip")
        assert p["record"] is None

    def test_manual_weekday_cages(self):
        out = plan(
            [
                subject_row("ab_m1", cage="vr_DATcre_opto2"),
                subject_row("ab_m2", cage="other"),
            ],
            cages={"vr_DATcre_opto2", "vr_DATcre_opto3"},
        )
        assert out["ab_m1"]["manual_weekdays"] is True
        assert r.assignment_for_day(
            out["ab_m1"]["responsibilities"], "Monday"
        ).is_manual(R.WATERING)
        assert not r.assignment_for_day(
            out["ab_m1"]["responsibilities"], "Sunday"
        ).is_manual(R.WATERING)
        assert out["ab_m2"]["manual_weekdays"] is False

    def test_cage_match_is_exact(self):
        out = plan(
            [
                subject_row("ab_m1", cage="VR_DATCRE_OPTO2"),
                subject_row("ab_m2", cage=None),
            ],
            cages={"vr_DATcre_opto2"},
        )
        assert out["ab_m1"]["manual_weekdays"] is False
        assert out["ab_m2"]["manual_weekdays"] is False

    def test_manual_weekday_users(self):
        out = plan(
            [
                subject_row("ab_m1", user_id="ab"),
                subject_row("ab_m2", user_id="ab", cage="elsewhere"),
                subject_row("cd_m1", user_id="cd"),
            ],
            users={"ab"},
        )
        assert out["ab_m1"]["manual_weekdays"] is True
        assert out["ab_m2"]["manual_weekdays"] is True
        assert r.assignment_for_day(
            out["ab_m1"]["responsibilities"], "Monday"
        ).is_manual(R.WATERING)
        assert not r.assignment_for_day(
            out["ab_m1"]["responsibilities"], "Sunday"
        ).is_manual(R.WATERING)
        assert out["cd_m1"]["manual_weekdays"] is False

    def test_user_match_is_exact(self):
        out = plan(
            [
                subject_row("ab_m1", user_id="AB"),
                subject_row("ab_m2", user_id=None),
                subject_row("ab_m3", user_id=""),
            ],
            users={"ab"},
        )
        assert out["ab_m1"]["manual_weekdays"] is False
        assert out["ab_m2"]["manual_weekdays"] is False
        assert out["ab_m3"]["manual_weekdays"] is False

    def test_cage_or_user_makes_weekdays_manual(self):
        out = plan(
            [
                subject_row("ab_m1", user_id="ab", cage="paper"),
                subject_row("cd_m1", user_id="cd", cage="paper"),
                subject_row("ef_m1", user_id="ef", cage="other"),
            ],
            cages={"paper"},
            users={"ab"},
        )
        assert out["ab_m1"]["manual_weekdays"] is True
        assert out["cd_m1"]["manual_weekdays"] is True
        assert out["ef_m1"]["manual_weekdays"] is False

    def test_no_manual_users_by_default(self):
        out = conv.plan_conversions([subject_row("ab_m1")], set(), set(), ALLOWED)
        assert out[0]["manual_weekdays"] is False

    @pytest.mark.parametrize(
        ("value", "supported"),
        [("yes", True), ("no", False), ("N/A", False), (None, False)],
    )
    def test_technician_support(self, value, supported):
        p = plan([subject_row("ab_m1", tech_responsibility=value)])["ab_m1"]
        technician = r.assignment_for_day(p["responsibilities"], "Monday").technician
        assert (technician == EVERYTHING) is supported

    def test_status_not_allowed_by_the_table_is_skipped(self):
        # Missing is not in the ResponsibilitiesAlt.subject_status enum
        p = plan([subject_row("ab_m1", subject_status="Missing")])["ab_m1"]
        assert p["action"].startswith("skip")
        assert "Missing" in p["action"]

    def test_conversion_errors_are_reported_not_written(self):
        p = plan([subject_row("ab_m1", schedule="Train/Train")])["ab_m1"]
        assert p["action"].startswith("error")
        assert p["record"] is None

    def test_changed_days_lists_where_the_legacy_schedule_reads_differently(self):
        p = plan(
            [subject_row("ab_m1", schedule="Weigh/Train/Train/Train/Train/Train/Weigh")]
        )["ab_m1"]
        assert p["converted_schedule"] == "Water/Train/Train/Train/Train/Train/Water"
        assert p["changed_days"] == "Sunday, Saturday"

    def test_no_subjects(self):
        assert conv.plan_conversions([], set(), set(), ALLOWED) == []


class TestScriptEnumValues:
    def test_reads_the_declared_enum(self):
        from types import SimpleNamespace

        from scripts.convert_legacy_responsibilities import enum_values

        attribute = SimpleNamespace(
            type="enum('InExperiments','WaterRestrictionOnly','AdLibWater','Dead')"
        )
        table = SimpleNamespace(
            heading=SimpleNamespace(attributes={"subject_status": attribute})
        )
        assert enum_values(table, "subject_status") == {
            "InExperiments",
            "WaterRestrictionOnly",
            "AdLibWater",
            "Dead",
        }


class TestScriptArguments:
    def test_no_manual_cages_or_users_by_default(self):
        from scripts.convert_legacy_responsibilities import build_parser

        args = build_parser().parse_args([])
        assert args.cages == []
        assert args.users == []
        assert args.write is False

    def test_cages_and_users_are_repeatable(self):
        from scripts.convert_legacy_responsibilities import build_parser

        args = build_parser().parse_args(
            [
                "--manual-weekday-cage",
                "c1",
                "--manual-weekday-cage",
                "c2",
                "--manual-weekday-user",
                "ab",
                "--write",
            ]
        )
        assert args.cages == ["c1", "c2"]
        assert args.users == ["ab"]
        assert args.write is True


class TestHistoricRecords:
    RECORD = {
        "subject_fullname": "jdoe_1",
        "subject_status": "InExperiments",
        "water_per_day": 1.5,
        "responsibilities": '{"style": "weekly"}',
    }

    def test_copies_each_record_with_the_effective_date(self):
        historic = conv.historic_records([self.RECORD], datetime.date(2026, 11, 18))
        assert historic == [{**self.RECORD, "effective_date": "2026-11-18"}]

    def test_leaves_the_current_records_unchanged(self):
        record = dict(self.RECORD)
        historic = conv.historic_records([record], datetime.date(2026, 11, 18))
        historic[0]["responsibilities"] = "changed"
        assert record == self.RECORD
        assert "effective_date" not in record

    def test_no_records_gives_no_historic_rows(self):
        assert conv.historic_records([], datetime.date(2026, 11, 18)) == []

    def test_keeps_a_missing_water_amount(self):
        record = {**self.RECORD, "water_per_day": None}
        historic = conv.historic_records([record], datetime.date(2026, 1, 1))
        assert historic[0]["water_per_day"] is None
        assert historic[0]["effective_date"] == "2026-01-01"


def assignment(technician, owner):
    return r.DailyAssignment(frozenset(technician), frozenset(owner))


TECH_CARES = assignment({R.WEIGHING, R.WATERING, R.TRAINING}, {R.NOTHING})
OWNER_CARES = assignment({R.NOTHING}, {R.WATERING, R.WEIGHING})
ON_PAPER = assignment({R.NOTHING}, {M.MANUAL_WATERING, M.MANUAL_WEIGHING})


def whole_week(weekday, weekend):
    return {
        day: weekend if day in r.WEEKEND_DAYS else weekday for day in r.DAYS_OF_WEEK
    }


class TestDescribeDay:
    def test_names_both_parties_in_a_fixed_duty_order(self):
        assert (
            conv.describe_day(TECH_CARES)
            == "Technician: Watering, Weighing, Training; Owner: Nothing"
        )

    def test_manual_duties_say_they_are_on_paper(self):
        assert conv.describe_day(ON_PAPER) == (
            "Technician: Nothing; Owner: Manual Watering, Manual Weighing (on paper)"
        )

    def test_transport_only(self):
        day = assignment({R.TRANSPORT_ONLY}, {R.WATERING, R.WEIGHING})
        assert conv.describe_day(day) == (
            "Technician: Transport Only; Owner: Watering, Weighing"
        )


class TestSummarizeWeek:
    def test_same_every_day(self):
        assert conv.summarize_week(whole_week(TECH_CARES, TECH_CARES)) == (
            "Every day: " + conv.describe_day(TECH_CARES)
        )

    def test_weekdays_and_weekends_differ(self):
        assert conv.summarize_week(whole_week(ON_PAPER, OWNER_CARES)) == (
            "Weekdays: "
            + conv.describe_day(ON_PAPER)
            + " | Weekends: "
            + conv.describe_day(OWNER_CARES)
        )

    def test_one_weekday_differs(self):
        days = whole_week(TECH_CARES, TECH_CARES)
        days["Wednesday"] = OWNER_CARES
        assert conv.summarize_week(days) == "Varies by day (see the day columns)"

    def test_saturday_differs_from_sunday(self):
        days = whole_week(TECH_CARES, TECH_CARES)
        days["Saturday"] = OWNER_CARES
        assert conv.summarize_week(days) == "Varies by day (see the day columns)"

    def test_no_days(self):
        assert conv.summarize_week({}) == ""


class TestReadableReportColumns:
    SUBJECT = {
        "subject_fullname": "jdoe_1",
        "user_id": "jdoe",
        "cage": "cage1",
        "subject_status": "InExperiments",
        "water_per_day": 1.0,
        "schedule": week(*["Train"] * 7),
        "tech_responsibility": "yes",
    }

    def plan(self, subject, existing=frozenset(), cages=frozenset()):
        return conv.plan_conversions(
            [subject],
            existing=set(existing),
            manual_weekday_cages=set(cages),
            allowed_statuses=frozenset({"InExperiments", "WaterRestrictionOnly"}),
        )[0]

    def test_insert_rows_get_a_summary_and_one_column_per_day(self):
        row = self.plan(self.SUBJECT)
        assert row["action"] == "insert"
        assert row["summary"] == "Every day: " + conv.describe_day(TECH_CARES)
        for day in r.DAYS_OF_WEEK:
            assert row[day] == conv.describe_day(TECH_CARES)

    def test_manual_weekdays_show_on_the_weekday_columns(self):
        row = self.plan(self.SUBJECT, cages={"cage1"})
        assert "(on paper)" in row["Monday"]
        assert "(on paper)" not in row["Sunday"]
        assert row["summary"].startswith("Weekdays: ")

    def test_skipped_rows_leave_the_readable_columns_empty(self):
        row = self.plan(self.SUBJECT, existing={"jdoe_1"})
        assert row["action"].startswith("skip")
        assert row["summary"] == ""
        assert all(row[day] == "" for day in r.DAYS_OF_WEEK)

    def test_error_rows_leave_the_readable_columns_empty(self):
        row = self.plan({**self.SUBJECT, "schedule": None})
        assert row["action"].startswith("error"), row["action"]
        assert row["summary"] == ""
        assert all(row[day] == "" for day in r.DAYS_OF_WEEK)
