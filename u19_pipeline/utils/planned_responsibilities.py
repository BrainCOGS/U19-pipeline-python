"""Apply planned explicit responsibilities once their effective date arrives.

The website writes a subject's responsibilities to action.ResponsibilitiesAltHistoric
with an effective_date. Today's are also written to action.ResponsibilitiesAlt, the
current row the water/weigh alert and the ViRMEn weighing GUI read; a later date is
only planned. This copies, for each subject, the latest Historic row in effect
(effective_date on or before today) into ResponsibilitiesAlt when they differ.

Running it again changes nothing, so it runs both as a nightly cronjob_alert.py job
and at the start of the water/weigh alert. Subjects with no Historic row in effect
(for example ones pre-filled by scripts/convert_legacy_responsibilities.py) are left
alone.
"""

import datetime
import traceback
from collections.abc import Iterable, Mapping
from typing import Any

import datajoint as dj

# Columns of a ResponsibilitiesAlt row; Historic adds effective_date
CURRENT_COLUMNS = (
    "subject_fullname",
    "subject_status",
    "water_per_day",
    "responsibilities",
)


def _as_date(value: datetime.date | str) -> datetime.date:
    if isinstance(value, datetime.datetime):
        return value.date()
    return (
        value
        if isinstance(value, datetime.date)
        else datetime.date.fromisoformat(value)
    )


def rows_in_effect(
    historic: Iterable[Mapping[str, Any]], today: datetime.date
) -> dict[str, dict[str, Any]]:
    """Each subject's latest Historic row with effective_date on or before today."""
    in_effect: dict[str, tuple[datetime.date, dict[str, Any]]] = {}
    for row in historic:
        effective = _as_date(row["effective_date"])
        if effective > today:
            continue
        name = row["subject_fullname"]
        if name not in in_effect or effective > in_effect[name][0]:
            in_effect[name] = (
                effective,
                {column: row.get(column) for column in CURRENT_COLUMNS},
            )
    return {name: row for name, (_, row) in in_effect.items()}


def _same(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    for column in CURRENT_COLUMNS:
        x, y = a.get(column), b.get(column)
        if column == "water_per_day" and x is not None and y is not None:
            if float(x) != float(y):
                return False
        elif x != y:
            return False
    return True


def plan_current_rows(
    historic: Iterable[Mapping[str, Any]],
    current: Iterable[Mapping[str, Any]],
    today: datetime.date,
) -> list[dict[str, Any]]:
    """ResponsibilitiesAlt rows to write so each matches its Historic row in effect."""
    current_by_subject = {row["subject_fullname"]: row for row in current}
    return [
        row
        for name, row in sorted(rows_in_effect(historic, today).items())
        if name not in current_by_subject or not _same(row, current_by_subject[name])
    ]


def _action_module() -> Any:
    # Virtual module: importing u19_pipeline.action would declare its tables
    return dj.create_virtual_module(
        "action", dj.config["custom"]["database.prefix"] + "action"
    )


def apply_planned_responsibilities(
    today: datetime.date | None = None, action: Any = None
) -> list[str]:
    """Write the responsibilities in effect today into ResponsibilitiesAlt.

    Returns the subjects whose current row changed.
    """
    # Lab-local, like CURDATE() in the alert's SQL and responsibilities.today_name
    today = today or datetime.date.today()
    action = action or _action_module()
    historic = (
        action.ResponsibilitiesAltHistoric & f'effective_date <= "{today.isoformat()}"'
    ).fetch(as_dict=True)
    current = action.ResponsibilitiesAlt.fetch(as_dict=True)
    rows = plan_current_rows(historic, current, today)
    if rows:
        with action.ResponsibilitiesAlt.connection.transaction:
            for row in rows:
                action.ResponsibilitiesAlt.insert1(row, skip_duplicates=True)
                action.ResponsibilitiesAlt.update1(row)
    return [row["subject_fullname"] for row in rows]


def apply_planned_responsibilities_or_report(**kwargs: Any) -> list[str]:
    """apply_planned_responsibilities for a caller that must go on if it fails.

    The water/weigh alert calls this before reading ResponsibilitiesAlt: a failure
    is printed and the alert reads the current rows as they are.
    """
    try:
        applied = apply_planned_responsibilities(**kwargs)
    except Exception:
        traceback.print_exc()
        return []
    if applied:
        print(f"Applied planned responsibilities for: {', '.join(applied)}")
    return applied


def main_apply_planned_responsibilities() -> None:
    applied = apply_planned_responsibilities()
    print(
        f"Applied planned responsibilities for {len(applied)} subject(s): {', '.join(applied)}"
    )
