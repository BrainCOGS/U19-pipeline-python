import datetime
import json

import datajoint as dj
import pytest

from u19_pipeline.utils import responsibilities as r

R = r.Responsibility
M = r.ManualResponsibility
TECH = r.ResponsibleParty.TECHNICIAN
OWNER = r.ResponsibleParty.OWNER


def slot(technician, owner):
    return {
        "technician": [str(x) for x in technician],
        "owner": [str(x) for x in owner],
    }


class TestParseResponsibility:
    @pytest.mark.parametrize("member", [*R, *M])
    def test_round_trips_every_member(self, member):
        assert r.parse_responsibility(member.value) is member

    @pytest.mark.parametrize("value", ["", "watering", "Weigh", "Manual", " Watering"])
    def test_rejects_unknown_values(self, value):
        with pytest.raises(ValueError, match="Unknown responsibility"):
            r.parse_responsibility(value)

    def test_shared_and_manual_values_do_not_overlap(self):
        assert not {m.value for m in R} & {m.value for m in M}


class TestAllowedResponsibilities:
    def test_manual_is_researcher_only(self):
        assert not set(M) & r.allowed_responsibilities(TECH)
        assert set(M) <= r.allowed_responsibilities(OWNER)

    def test_shared_allowed_for_both(self):
        for party in r.ResponsibleParty:
            assert set(R) <= r.allowed_responsibilities(party)


class TestValidateDailyAssignment:
    def test_valid_split(self):
        assert r.validate_daily_assignment({R.WATERING, R.WEIGHING}, {R.TRAINING}) == []

    def test_manual_watering_satisfies_watering_requirement(self):
        errors = r.validate_daily_assignment({R.WEIGHING}, {M.MANUAL_WATERING})
        assert errors == []

    def test_watering_not_required_when_not_restricted(self):
        assert (
            r.validate_daily_assignment(
                {R.NOTHING}, {R.NOTHING}, require_watering=False
            )
            == []
        )

    def test_missing_watering(self):
        errors = r.validate_daily_assignment({R.WEIGHING}, {R.NOTHING})
        assert any("Watering" in e for e in errors)

    def test_both_nothing_is_not_an_overlap(self):
        errors = r.validate_daily_assignment({R.NOTHING}, {R.NOTHING})
        assert not any("both" in e for e in errors)

    def test_overlap_between_parties(self):
        errors = r.validate_daily_assignment({R.WATERING}, {R.WATERING})
        assert any("both" in e for e in errors)

    @pytest.mark.parametrize(
        ("technician", "owner"),
        [
            ({R.WEIGHING}, {M.MANUAL_WEIGHING, R.WATERING}),
            ({R.WATERING}, {M.MANUAL_WATERING}),
            (set(), {R.WEIGHING, M.MANUAL_WEIGHING, R.WATERING}),
            (set(), {R.WATERING, M.MANUAL_WATERING}),
        ],
    )
    def test_manual_conflicts_with_tracked_duty_across_parties(self, technician, owner):
        errors = r.validate_daily_assignment(technician or {R.NOTHING}, owner)
        assert any("Manual" in e and "conflicts" in e for e in errors)

    @pytest.mark.parametrize("manual", list(M))
    def test_technician_cannot_have_manual(self, manual):
        errors = r.validate_daily_assignment({manual, R.WATERING}, {R.NOTHING})
        assert any("researcher" in e.lower() for e in errors)

    @pytest.mark.parametrize("exclusive", [R.NOTHING, R.TRANSPORT_ONLY])
    def test_exclusive_responsibilities_must_be_alone(self, exclusive):
        errors = r.validate_daily_assignment({exclusive, R.WEIGHING}, {R.WATERING})
        assert any(str(exclusive) in e for e in errors)

    def test_empty_party_must_say_nothing(self):
        errors = r.validate_daily_assignment(set(), {R.WATERING})
        assert any("Nothing" in e for e in errors)


class TestAssignmentForDay:
    def test_weekly_applies_to_every_day(self):
        record = {
            "assignment_style": "weekly",
            "weekly": slot([R.WATERING], [R.NOTHING]),
        }
        for day in r.DAYS_OF_WEEK:
            assert r.assignment_for_day(record, day) == r.DailyAssignment(
                frozenset({R.WATERING}), frozenset({R.NOTHING})
            )

    @pytest.mark.parametrize(
        ("day", "key"),
        [
            ("Sunday", "weekends"),
            ("Saturday", "weekends"),
            ("Monday", "weekdays"),
            ("Friday", "weekdays"),
        ],
    )
    def test_workweek_split(self, day, key):
        record = {
            "assignment_style": "workweek_split",
            "weekdays": slot([R.TRAINING, R.WATERING], [R.NOTHING]),
            "weekends": slot([R.NOTHING], [M.MANUAL_WATERING]),
        }
        expected = r.parse_slot(record[key])
        assert r.assignment_for_day(record, day) == expected

    def test_daily_uses_the_day_key(self):
        record = {
            "assignment_style": "daily",
            **{d: slot([R.NOTHING], [R.WATERING]) for d in r.DAYS_OF_WEEK},
        }
        record["Tuesday"] = slot([R.WATERING, R.WEIGHING], [R.TRAINING])
        assert r.assignment_for_day(record, "Tuesday").technician == {
            R.WATERING,
            R.WEIGHING,
        }
        assert r.assignment_for_day(record, "Monday").technician == {R.NOTHING}

    def test_stale_keys_from_another_style_are_ignored(self):
        record = {
            "assignment_style": "weekly",
            "weekly": slot([R.WATERING], [R.NOTHING]),
            "Monday": slot([R.TRAINING], [R.NOTHING]),
        }
        assert r.assignment_for_day(record, "Monday").technician == {R.WATERING}

    def test_accepts_json_string(self):
        record = json.dumps(
            {"assignment_style": "weekly", "weekly": slot([R.WATERING], [R.NOTHING])}
        )
        assert r.assignment_for_day(record, "Monday").technician == {R.WATERING}

    @pytest.mark.parametrize(
        "record",
        [
            {},
            {"assignment_style": "weekly"},
            {
                "assignment_style": "fortnightly",
                "weekly": slot([R.WATERING], [R.NOTHING]),
            },
            {"assignment_style": "daily", "Sunday": slot([R.WATERING], [R.NOTHING])},
            {"assignment_style": "weekly", "weekly": slot(["Bogus"], [R.NOTHING])},
        ],
    )
    def test_malformed_records_raise(self, record):
        with pytest.raises(ValueError):
            r.assignment_for_day(record, "Monday")

    @pytest.mark.parametrize("party_value", [None, "Watering", {"Watering": 1}])
    def test_non_list_party_raises_type_error(self, party_value):
        record = {
            "assignment_style": "weekly",
            "weekly": {"technician": party_value, "owner": []},
        }
        with pytest.raises(TypeError, match="must be a list"):
            r.assignment_for_day(record, "Monday")

    def test_unknown_day_raises(self):
        record = {
            "assignment_style": "weekly",
            "weekly": slot([R.WATERING], [R.NOTHING]),
        }
        with pytest.raises(ValueError, match="day"):
            r.assignment_for_day(record, "Funday")

    def test_missing_party_is_empty(self):
        record = {"assignment_style": "weekly", "weekly": {"technician": ["Watering"]}}
        assert r.assignment_for_day(record, "Monday").owner == frozenset()


class TestDailyAssignment:
    def test_party_for(self):
        a = r.DailyAssignment(frozenset({R.WATERING}), frozenset({R.WEIGHING}))
        assert a.party_for(R.WATERING) is TECH
        assert a.party_for(R.WEIGHING) is OWNER
        assert a.party_for(R.TRAINING) is None

    def test_manual_is_not_tracked(self):
        a = r.DailyAssignment(
            frozenset({R.NOTHING}), frozenset({M.MANUAL_WATERING, M.MANUAL_WEIGHING})
        )
        assert not a.tracks(R.WATERING)
        assert not a.tracks(R.WEIGHING)
        assert a.is_manual(R.WATERING)
        assert a.is_manual(R.WEIGHING)

    def test_is_manual_false_for_tracked(self):
        a = r.DailyAssignment(frozenset({R.WATERING}), frozenset({R.WEIGHING}))
        assert not a.is_manual(R.WATERING)
        assert not a.is_manual(R.TRAINING)


class TestLegacyToken:
    @pytest.mark.parametrize(
        ("technician", "owner", "token"),
        [
            ({R.TRAINING, R.WATERING, R.WEIGHING}, {R.NOTHING}, "Train"),
            ({R.WATERING}, {R.TRAINING}, "Train"),
            ({R.WATERING, R.WEIGHING}, {R.NOTHING}, "Weigh"),
            ({R.WEIGHING}, {M.MANUAL_WATERING}, "Weigh"),
            ({R.WATERING}, {R.WEIGHING}, "Water"),
            ({R.TRANSPORT_ONLY}, {R.WATERING}, "Transport"),
            ({R.NOTHING}, {R.WATERING, R.WEIGHING}, "Nothing"),
            ({R.NOTHING}, {M.MANUAL_WATERING, M.MANUAL_WEIGHING}, "Nothing"),
            (set(), set(), "Nothing"),
        ],
    )
    def test_mapping(self, technician, owner, token):
        assert (
            r.DailyAssignment(frozenset(technician), frozenset(owner)).legacy_token()
            == token
        )

    def test_legacy_schedule_has_seven_days_sunday_first(self):
        record = {
            "assignment_style": "workweek_split",
            "weekdays": slot([R.TRAINING], [R.WATERING]),
            "weekends": slot([R.WATERING], [R.NOTHING]),
        }
        assert r.legacy_schedule(record) == "Water/Train/Train/Train/Train/Train/Water"


class TestResponsibilityFormat:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("legacy", "legacy"),
            ("preview", "preview"),
            ("explicit", "explicit"),
            (" Explicit ", "explicit"),
        ],
    )
    def test_parse(self, value, expected):
        assert r.parse_responsibility_format(value) == expected

    @pytest.mark.parametrize("value", [None, "", "new", "1"])
    def test_unknown_or_missing_falls_back_to_legacy(self, value):
        assert r.parse_responsibility_format(value) is r.ResponsibilityFormat.LEGACY

    @pytest.mark.parametrize(
        ("fmt", "acts", "edits"),
        [
            # Preview lets experimenters fill in and check the explicit
            # responsibilities while everything still acts on the legacy schedule
            (r.ResponsibilityFormat.LEGACY, False, False),
            (r.ResponsibilityFormat.PREVIEW, False, True),
            (r.ResponsibilityFormat.EXPLICIT, True, True),
        ],
    )
    def test_what_each_format_enables(self, fmt, acts, edits):
        assert fmt.acts_on_explicit is acts
        assert fmt.edits_explicit is edits


class FakeFeatureFlag:
    """Stands in for lab.FeatureFlag: a restriction, then fetch("flag_value")."""

    def __init__(self, values=(), error=None):
        self.values = list(values)
        self.error = error
        self.restrictions = []

    def __and__(self, restriction):
        self.restrictions.append(restriction)
        return self

    def fetch(self, attribute):
        assert attribute == "flag_value"
        if self.error:
            raise self.error
        return self.values


class TestGetResponsibilityFormat:
    def test_reads_the_flag_row(self):
        table = FakeFeatureFlag(values=["explicit"])
        assert r.get_responsibility_format(table) is r.ResponsibilityFormat.EXPLICIT
        assert table.restrictions == [{"flag_name": r.RESPONSIBILITY_FORMAT_FLAG}]

    def test_missing_row_is_legacy(self):
        assert (
            r.get_responsibility_format(FakeFeatureFlag())
            is r.ResponsibilityFormat.LEGACY
        )

    @pytest.mark.parametrize(
        "error",
        [
            dj.errors.MissingTableError("no table"),
            dj.errors.AccessError("SELECT command denied"),
        ],
    )
    def test_unreadable_flag_is_legacy(self, error):
        table = FakeFeatureFlag(error=error)
        assert r.get_responsibility_format(table) is r.ResponsibilityFormat.LEGACY


class TestTodayName:
    @pytest.mark.parametrize(
        ("date", "name"),
        [
            (datetime.date(2026, 10, 4), "Sunday"),
            (datetime.date(2026, 10, 5), "Monday"),
            (datetime.date(2026, 10, 10), "Saturday"),
        ],
    )
    def test_names(self, date, name):
        assert r.today_name(date) == name

    def test_matches_legacy_schedule_index(self):
        # get_subject_data indexes the schedule with (weekday() + 1) % 7
        for offset in range(7):
            date = datetime.date(2026, 10, 4) + datetime.timedelta(days=offset)
            assert r.DAYS_OF_WEEK.index(r.today_name(date)) == (date.weekday() + 1) % 7
