import pandas as pd
import pytest

from u19_pipeline.alert_system.water_weigh_alert import explicit_duties as ed
from u19_pipeline.utils import responsibilities
from u19_pipeline.utils.responsibilities import DailyAssignment

R = responsibilities.Responsibility
M = responsibilities.ManualResponsibility


def assignment(technician, owner):
    return DailyAssignment(frozenset(technician), frozenset(owner))


def make_subjects(rows):
    defaults = {"subject_status": "InExperiments", "schedule_today": "Train"}
    return pd.DataFrame([{**defaults, **row} for row in rows])


class TestApplyAssignments:
    def test_none_keeps_legacy_and_adds_empty_column(self):
        data = make_subjects([{"subject_fullname": "a", "schedule_today": "Water"}])
        out = ed.apply_assignments(data, None)
        assert out["schedule_today"].tolist() == ["Water"]
        assert out["assignment"].tolist() == [None]

    def test_overrides_schedule_only_for_subjects_with_assignment(self):
        data = make_subjects(
            [
                {"subject_fullname": "explicit", "schedule_today": "Nothing"},
                {"subject_fullname": "legacy", "schedule_today": "Weigh"},
            ]
        )
        a = assignment({R.TRAINING}, {R.WATERING})
        out = ed.apply_assignments(data, {"explicit": a})
        assert out["schedule_today"].tolist() == ["Train", "Weigh"]
        assert out["assignment"].tolist() == [a, None]

    def test_does_not_mutate_input(self):
        data = make_subjects([{"subject_fullname": "a", "schedule_today": "Nothing"}])
        ed.apply_assignments(data, {"a": assignment({R.WATERING}, {R.NOTHING})})
        assert data["schedule_today"].tolist() == ["Nothing"]
        assert "assignment" not in data

    def test_empty_frame(self):
        data = make_subjects([]).reindex(
            columns=["subject_fullname", "subject_status", "schedule_today"]
        )
        out = ed.apply_assignments(data, {"a": assignment({R.WATERING}, {R.NOTHING})})
        assert out.empty
        assert "assignment" in out


class TestKeepMask:
    def test_legacy_rows_use_legacy_mask(self):
        data = ed.apply_assignments(
            make_subjects([{"subject_fullname": "a"}, {"subject_fullname": "b"}]), None
        )
        legacy = pd.Series([True, False])
        assert ed.keep_mask(data, legacy).tolist() == [True, False]

    @pytest.mark.parametrize(
        ("technician", "owner", "kept"),
        [
            ({R.NOTHING}, {R.WATERING}, True),  # legacy "Nothing" would drop this
            ({R.WEIGHING}, {M.MANUAL_WATERING}, True),
            ({R.TRANSPORT_ONLY}, {R.TRAINING, M.MANUAL_WATERING}, True),
            ({R.NOTHING}, {M.MANUAL_WATERING, M.MANUAL_WEIGHING}, False),
            ({R.TRANSPORT_ONLY}, {M.MANUAL_WATERING}, False),
        ],
    )
    def test_explicit_rows_kept_when_something_is_tracked(
        self, technician, owner, kept
    ):
        data = ed.apply_assignments(
            make_subjects([{"subject_fullname": "a"}]),
            {"a": assignment(technician, owner)},
        )
        assert ed.keep_mask(data, pd.Series([not kept])).tolist() == [kept]

    @pytest.mark.parametrize("status", ["AdLibWater", "Dead", "Missing"])
    def test_explicit_rows_still_need_an_active_status(self, status):
        data = ed.apply_assignments(
            make_subjects([{"subject_fullname": "a", "subject_status": status}]),
            {"a": assignment({R.WATERING}, {R.NOTHING})},
        )
        assert ed.keep_mask(data, pd.Series([True])).tolist() == [False]

    def test_water_restriction_only_is_active(self):
        data = ed.apply_assignments(
            make_subjects(
                [{"subject_fullname": "a", "subject_status": "WaterRestrictionOnly"}]
            ),
            {"a": assignment({R.WATERING}, {R.NOTHING})},
        )
        assert ed.keep_mask(data, pd.Series([False])).tolist() == [True]


class TestDropUntrackedDuties:
    def make(self, assignments):
        rows = [
            {"subject_fullname": name, "current_need_water": 1.5, "need_weight": True}
            for name in [
                "legacy",
                "manual_water",
                "manual_weigh",
                "owner_does_all",
                "unassigned_weigh",
            ]
        ]
        return ed.drop_untracked_duties(
            ed.apply_assignments(make_subjects(rows), assignments)
        )

    def test_manual_and_unassigned_duties_are_not_alerted(self):
        out = self.make(
            {
                "manual_water": assignment({R.WEIGHING}, {M.MANUAL_WATERING}),
                "manual_weigh": assignment({R.WATERING}, {M.MANUAL_WEIGHING}),
                "owner_does_all": assignment({R.NOTHING}, {R.WATERING, R.WEIGHING}),
                "unassigned_weigh": assignment({R.WATERING}, {R.NOTHING}),
            }
        ).set_index("subject_fullname")
        assert out["current_need_water"].to_dict() == {
            "legacy": 1.5,
            "manual_water": 0,
            "manual_weigh": 1.5,
            "owner_does_all": 1.5,
            "unassigned_weigh": 1.5,
        }
        assert out["need_weight"].to_dict() == {
            "legacy": True,
            "manual_water": True,
            "manual_weigh": False,
            "owner_does_all": True,
            "unassigned_weigh": False,
        }

    def test_zero_need_stays_zero(self):
        data = ed.apply_assignments(
            make_subjects(
                [
                    {
                        "subject_fullname": "a",
                        "current_need_water": 0.0,
                        "need_weight": False,
                    }
                ]
            ),
            {"a": assignment({R.WATERING, R.WEIGHING}, {R.NOTHING})},
        )
        out = ed.drop_untracked_duties(data)
        assert out["current_need_water"].tolist() == [0.0]
        assert out["need_weight"].tolist() == [False]


class TestDutyUsesOwners:
    def test_no_assignment_uses_legacy_decision(self):
        assert ed.duty_uses_owners(None, R.WATERING, legacy_use_owners=True)
        assert not ed.duty_uses_owners(None, R.WATERING, legacy_use_owners=False)

    def test_technician_duty_tags_technicians(self):
        a = assignment({R.WATERING}, {R.WEIGHING})
        assert not ed.duty_uses_owners(a, R.WATERING, legacy_use_owners=True)
        assert ed.duty_uses_owners(a, R.WEIGHING, legacy_use_owners=False)

    def test_unassigned_duty_tags_owners(self):
        a = assignment({R.WATERING}, {R.NOTHING})
        assert ed.duty_uses_owners(a, R.WEIGHING, legacy_use_owners=False)


class TestParseAssignments:
    def test_skips_malformed_records(self, caplog):
        good = '{"assignment_style": "weekly", "weekly": {"technician": ["Watering"], "owner": ["Nothing"]}}'
        rows = [
            ("good", good),
            ("bad_json", "{"),
            ("bad_value", good.replace("Watering", "Bogus")),
            ("bad_party", good.replace('["Watering"]', '"Watering"')),
            ("not_object", "[]"),
            ("null", None),
        ]
        out = ed.parse_assignments(rows, "Monday")
        assert list(out) == ["good"]
        assert out["good"].technician == {R.WATERING}
        assert {"bad_json", "bad_value", "bad_party", "not_object", "null"} <= {
            r.args[0] for r in caplog.records
        }
