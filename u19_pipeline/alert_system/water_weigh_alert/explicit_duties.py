"""Apply explicit responsibilities (action.ResponsibilitiesAlt) to the alert.

Only used when lab.FeatureFlag responsibility_format is "explicit". Subjects
without a usable explicit record keep the legacy schedule behavior, so a
partial migration still alerts on everyone.

Manual (paper) watering and weighing are never alerted on: our systems cannot
see them being done.
"""

import json
import logging
from collections.abc import Iterable, Mapping

import pandas as pd

from u19_pipeline.utils.responsibilities import (
    DailyAssignment,
    Responsibility,
    ResponsibleParty,
    assignment_for_day,
)

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = ("InExperiments", "WaterRestrictionOnly")
# An explicit subject is alerted on if someone does one of these in our systems
ALERTED_DUTIES = (
    Responsibility.WATERING,
    Responsibility.WEIGHING,
    Responsibility.TRAINING,
)


def parse_assignments(
    rows: Iterable[tuple[str, str | None]], day: str
) -> dict[str, DailyAssignment]:
    """Today's assignment per subject from (subject_fullname, responsibilities) rows.

    Malformed records are logged and skipped, so those subjects fall back to
    the legacy schedule.
    """
    assignments = {}
    for subject_fullname, responsibilities in rows:
        try:
            assignments[subject_fullname] = assignment_for_day(
                json.loads(responsibilities), day
            )
        except (TypeError, ValueError, AttributeError) as e:
            logger.warning(
                "%s: unusable explicit responsibilities, using legacy schedule (%s)",
                subject_fullname,
                e,
            )
    return assignments


def apply_assignments(
    subject_data: pd.DataFrame, assignments: Mapping[str, DailyAssignment] | None
) -> pd.DataFrame:
    """Add an "assignment" column and derive schedule_today from it.

    The column holds None for subjects without an explicit assignment (and for
    everyone when assignments is None), whose schedule_today is left as is.
    """
    subject_data = subject_data.copy()
    subject_data["assignment"] = pd.Series(
        [(assignments or {}).get(name) for name in subject_data["subject_fullname"]],
        index=subject_data.index,
        dtype=object,
    )
    has_assignment = subject_data["assignment"].notna()
    subject_data.loc[has_assignment, "schedule_today"] = subject_data.loc[
        has_assignment, "assignment"
    ].map(DailyAssignment.legacy_token)
    return subject_data


def keep_mask(subject_data: pd.DataFrame, legacy_mask: pd.Series) -> pd.Series:
    """Which subjects the alert considers.

    Subjects with an explicit assignment are kept when they have an active
    status and someone does a tracked duty; the rest use legacy_mask.
    """
    legacy_mask = pd.Series(legacy_mask.to_numpy(), index=subject_data.index)
    explicit = subject_data["assignment"].map(
        lambda a: a is not None and any(a.tracks(duty) for duty in ALERTED_DUTIES)
    )
    active = subject_data["subject_status"].isin(ACTIVE_STATUSES)
    has_assignment = subject_data["assignment"].notna()
    return legacy_mask.where(~has_assignment, explicit & active).astype(bool)


def drop_untracked_duties(subject_data: pd.DataFrame) -> pd.DataFrame:
    """Stop alerting on water/weight that nobody does in our systems.

    For explicit subjects, no water is requested unless someone is assigned
    Watering, and no weight unless someone is assigned Weighing. That covers
    manual (paper) watering and weighing.
    """
    subject_data = subject_data.copy()
    assignment = subject_data["assignment"]
    has_assignment = assignment.notna()

    def untracked(duty: Responsibility) -> pd.Series:
        return has_assignment & ~assignment.map(
            lambda a: a is not None and a.tracks(duty)
        )

    subject_data.loc[untracked(Responsibility.WATERING), "current_need_water"] = 0
    subject_data.loc[untracked(Responsibility.WEIGHING), "need_weight"] = False
    return subject_data


def duty_uses_owners(
    assignment: DailyAssignment | None,
    duty: Responsibility,
    legacy_use_owners: bool,
) -> bool:
    """Whether to tag the owners (not the on-duty technicians) for this duty."""
    if assignment is None:
        return legacy_use_owners
    return assignment.party_for(duty) is not ResponsibleParty.TECHNICIAN
