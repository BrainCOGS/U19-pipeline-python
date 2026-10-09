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
        require_watering = status in conv.WATER_RESTRICTED_STATUSES
        for day, assignment in result.days.items():
            assert (
                r.validate_daily_assignment(
                    assignment.technician, assignment.owner, require_watering
                )
                == []
            )
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


def plan(rows, existing=frozenset(), cages=(), allowed=ALLOWED):
    return {
        p["subject_fullname"]: p
        for p in conv.plan_conversions(rows, set(existing), set(cages), allowed)
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
