"""Two-tier selection of subjects for the water/weigh Slack alert.

The early runs (6 and 7 PM ET) only report subjects whose duties should be done
by then: subjects trained in an early timeslot and water-only subjects that
were watered before 4 PM. The late runs (10 and 11 PM ET) report every subject.
"""

import datetime
from enum import StrEnum
from typing import assert_never
from zoneinfo import ZoneInfo

import pandas as pd

EASTERN = ZoneInfo("America/New_York")


class AlertTier(StrEnum):
    """Which subjects a water/weigh alert run reports."""

    EARLY = "early"  # 6/7 PM: early-timeslot trainers and water-only subjects
    ALL = "all"  # 10/11 PM: every subject


# Subjects scheduled in this timeslot or earlier belong to the early tier
LAST_EARLY_TIMESLOT = 5
# Water-only subjects watered before this time (ET) belong to the early tier
WATERING_CUTOFF = datetime.timedelta(hours=16)
# Runs at or after this hour (ET) report all subjects
ALL_TIER_START_HOUR = 21


def resolve_alert_tier(
    tier: str | None = None, now: datetime.datetime | None = None
) -> AlertTier:
    """Return the requested tier, or pick one from the current Eastern time.

    Raises ValueError if tier is not an AlertTier value.
    """
    if tier is not None:
        return AlertTier(tier)

    if now is None:
        now = datetime.datetime.now(EASTERN)
    elif now.tzinfo is not None:
        now = now.astimezone(EASTERN)

    return AlertTier.EARLY if now.hour < ALL_TIER_START_HOUR else AlertTier.ALL


def is_water_only_today(subject_data: pd.DataFrame) -> pd.Series:
    """Subjects that are only expected to be watered (not trained) today."""
    schedule_today = subject_data["schedule_today"].astype("string").str.lower()
    return (schedule_today == "water").fillna(False) | (
        subject_data["subject_status"] == "WaterRestrictionOnly"
    )


def early_tier_mask(subject_data: pd.DataFrame) -> pd.Series:
    """Subjects that belong in the early (6/7 PM) alert.

    - Subjects scheduled on a rig in timeslot LAST_EARLY_TIMESLOT or earlier.
    - Water-only subjects first watered before WATERING_CUTOFF. Water-only
      subjects not watered yet wait for the late alert.
    """
    first_timeslot = pd.to_numeric(subject_data["first_timeslot"], errors="coerce")
    trains_early = (first_timeslot <= LAST_EARLY_TIMESLOT).fillna(False)

    first_water_time = pd.to_datetime(subject_data["first_water_time"])
    time_of_day = first_water_time - first_water_time.dt.normalize()
    watered_early = (time_of_day < WATERING_CUTOFF).fillna(False)

    return trains_early | (is_water_only_today(subject_data) & watered_early)


def filter_subjects_for_tier(subject_data: pd.DataFrame, tier: str) -> pd.DataFrame:
    """Keep only the subjects reported in the given alert tier.

    Raises ValueError if tier is not an AlertTier value.
    """
    match AlertTier(tier):
        case AlertTier.ALL:
            return subject_data
        case AlertTier.EARLY:
            return subject_data.loc[early_tier_mask(subject_data)]
        case unhandled:
            assert_never(unhandled)
