"""Run the shared JSON cases (tests/fixtures/responsibilities_cases.json).

The ViRMEn weighing GUI runs a byte-identical copy of the same file
(experiments/utility/WeighingGUI/tests/test_explicit_duties_for_day.m), so
both readers resolve every record the same way.
"""

import json
import pathlib

import pytest

from u19_pipeline.utils import responsibilities as r

CASES_FILE = (
    pathlib.Path(__file__).parents[1] / "fixtures" / "responsibilities_cases.json"
)
CASES = json.loads(CASES_FILE.read_text())["cases"]
DAY_CASES = [
    pytest.param(case["json"], day, expected, id=f"{case['name']}-{day}")
    for case in CASES
    for day, expected in case["days"].items()
]
ORDER = [*r.Responsibility, *r.ManualResponsibility]


def in_enum_order(values):
    return sorted(values, key=ORDER.index)


@pytest.mark.parametrize(("raw", "day", "expected"), DAY_CASES)
def test_case(raw, day, expected):
    if expected == "invalid":
        with pytest.raises((ValueError, TypeError)):
            r.assignment_for_day(raw, day)
        return
    assignment = r.assignment_for_day(raw, day)
    assert in_enum_order(assignment.technician) == expected["technician"]
    assert in_enum_order(assignment.owner) == expected["owner"]
    assert assignment.legacy_token() == expected["legacy_token"]
    assert (
        assignment.is_manual(r.Responsibility.WATERING) is expected["manual_watering"]
    )
    assert (
        assignment.is_manual(r.Responsibility.WEIGHING) is expected["manual_weighing"]
    )


def test_every_style_is_covered():
    styles = {
        json.loads(c["json"]).get("assignment_style") for c in CASES if c["canonical"]
    }
    assert styles == {style.json_key for style in r.AssignmentStyle}


@pytest.mark.parametrize(
    "case", [c for c in CASES if c["canonical"]], ids=lambda c: c["name"]
)
def test_canonical_cases_are_what_the_writer_produces(case):
    record = json.loads(case["json"])
    style = r.AssignmentStyle[record["assignment_style"].upper()]
    assert r.dump_responsibilities(style, record) == case["json"]


@pytest.mark.parametrize(
    "case",
    [c for c in CASES if all(v != "invalid" for v in c["days"].values())],
    ids=lambda c: c["name"],
)
def test_writer_output_resolves_the_same(case):
    record = json.loads(case["json"])
    style = r.AssignmentStyle[record["assignment_style"].upper()]
    dumped = r.dump_responsibilities(style, record)
    for day in case["days"]:
        assert r.assignment_for_day(dumped, day) == r.assignment_for_day(
            case["json"], day
        )
