import datetime

from u19_pipeline.utils import planned_responsibilities as pr

TODAY = datetime.date(2026, 10, 20)
WEEKLY_WATER = '{"assignment_style": "weekly", "weekly": {"technician": ["Watering"]}}'
WEEKLY_NOTHING = '{"assignment_style": "weekly", "weekly": {"technician": ["Nothing"]}}'


def historic(
    name, effective, responsibilities=WEEKLY_WATER, status="InExperiments", water=1.0
):
    return {
        "subject_fullname": name,
        "effective_date": effective,
        "subject_status": status,
        "water_per_day": water,
        "responsibilities": responsibilities,
    }


def current(name, responsibilities=WEEKLY_WATER, status="InExperiments", water=1.0):
    row = historic(name, TODAY, responsibilities, status, water)
    del row["effective_date"]
    return row


class TestRowsInEffect:
    def test_the_latest_row_on_or_before_today_wins(self):
        rows = [
            historic("a", datetime.date(2026, 10, 1), WEEKLY_NOTHING),
            historic("a", TODAY, WEEKLY_WATER),
            historic("a", datetime.date(2026, 10, 25), WEEKLY_NOTHING),  # planned
        ]
        assert pr.rows_in_effect(rows, TODAY)["a"]["responsibilities"] == WEEKLY_WATER

    def test_only_planned_rows_are_not_in_effect(self):
        assert (
            pr.rows_in_effect([historic("a", datetime.date(2026, 10, 21))], TODAY) == {}
        )

    def test_dates_may_be_strings_or_datetimes(self):
        rows = [
            historic("a", "2026-10-19", WEEKLY_NOTHING),
            historic("b", datetime.datetime(2026, 10, 20, 9, 0)),  # noqa: DTZ001 (MySQL is naive)
        ]
        assert set(pr.rows_in_effect(rows, TODAY)) == {"a", "b"}

    def test_the_row_has_only_the_current_table_columns(self):
        (row,) = pr.rows_in_effect([historic("a", TODAY)], TODAY).values()
        assert set(row) == set(pr.CURRENT_COLUMNS)


class TestPlanCurrentRows:
    def test_a_plan_whose_date_arrived_replaces_the_current_row(self):
        rows = pr.plan_current_rows(
            [historic("a", TODAY, WEEKLY_NOTHING, status="AdLibWater", water=0.0)],
            [current("a", WEEKLY_WATER)],
            TODAY,
        )
        assert rows == [
            {
                "subject_fullname": "a",
                "subject_status": "AdLibWater",
                "water_per_day": 0.0,
                "responsibilities": WEEKLY_NOTHING,
            }
        ]

    def test_a_current_row_that_matches_is_left_alone(self):
        assert pr.plan_current_rows([historic("a", TODAY)], [current("a")], TODAY) == []

    def test_water_compares_by_value(self):
        rows = pr.plan_current_rows(
            [historic("a", TODAY, water=1)], [current("a", water=1.0)], TODAY
        )
        assert rows == []

    def test_a_subject_without_a_current_row_gets_one(self):
        assert [
            r["subject_fullname"]
            for r in pr.plan_current_rows([historic("a", TODAY)], [], TODAY)
        ] == ["a"]

    def test_subjects_without_history_are_left_alone(self):
        # e.g. pre-filled by scripts/convert_legacy_responsibilities.py
        assert pr.plan_current_rows([], [current("converted")], TODAY) == []

    def test_a_future_plan_does_not_touch_the_current_row(self):
        rows = pr.plan_current_rows(
            [historic("a", datetime.date(2026, 10, 21), WEEKLY_NOTHING)],
            [current("a")],
            TODAY,
        )
        assert rows == []


class _Restricted:
    def __init__(self, rows):
        self.rows = rows

    def fetch(self, as_dict=False):
        return self.rows


class _Table:
    def __init__(self, rows):
        self.rows = rows
        self.writes = []
        self.restriction = None
        self.connection = self

    @property
    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __and__(self, restriction):
        self.restriction = restriction
        return _Restricted(self.rows)

    def fetch(self, as_dict=False):
        return self.rows

    def insert1(self, row, skip_duplicates=False):
        self.writes.append(("insert1", row["subject_fullname"]))

    def update1(self, row):
        self.writes.append(("update1", row["subject_fullname"]))


class _Action:
    def __init__(self, historic_rows, current_rows):
        self.ResponsibilitiesAltHistoric = _Table(historic_rows)
        self.ResponsibilitiesAlt = _Table(current_rows)


class TestApplyPlannedResponsibilities:
    def test_writes_only_changed_subjects(self):
        action = _Action(
            [historic("a", TODAY, WEEKLY_NOTHING), historic("b", TODAY)],
            [current("a"), current("b")],
        )
        assert pr.apply_planned_responsibilities(TODAY, action) == ["a"]
        assert action.ResponsibilitiesAlt.writes == [("insert1", "a"), ("update1", "a")]
        assert (
            action.ResponsibilitiesAltHistoric.restriction
            == 'effective_date <= "2026-10-20"'
        )

    def test_nothing_to_apply_writes_nothing(self):
        action = _Action([historic("a", TODAY)], [current("a")])
        assert pr.apply_planned_responsibilities(TODAY, action) == []
        assert action.ResponsibilitiesAlt.writes == []


class TestApplyOrReport:
    def test_a_failure_is_printed_not_raised(self, capsys):
        class Broken:
            @property
            def ResponsibilitiesAltHistoric(self):
                raise ConnectionError("database down")

        assert (
            pr.apply_planned_responsibilities_or_report(today=TODAY, action=Broken())
            == []
        )
        assert "database down" in capsys.readouterr().err

    def test_applied_subjects_are_returned(self):
        action = _Action([historic("a", TODAY, WEEKLY_NOTHING)], [current("a")])
        assert pr.apply_planned_responsibilities_or_report(
            today=TODAY, action=action
        ) == ["a"]
