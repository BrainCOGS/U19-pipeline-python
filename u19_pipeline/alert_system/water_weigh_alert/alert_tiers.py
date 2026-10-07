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


# Rig schedule, copied from tech_calendar_streamlit
# (streamlit_web_interface/tabs/rig_schedule.py, convert_timeslot_to_datetime).
# Keep the two in sync if the schedule changes.
TIMESLOTS = range(1, 9)
SCHEDULE_START_HOUR = 8  # slot 1 starts one hour later, at 9 AM
LUNCH_TIMESLOT = 4  # 12-1 PM is lunch, so this slot and later start an hour later

# Subjects scheduled in this timeslot (starts 2 PM) or earlier belong to the
# early tier
LAST_EARLY_TIMESLOT = 5
# Water-only subjects watered before this time (ET) belong to the early tier
WATERING_CUTOFF = datetime.timedelta(hours=16)
# Runs at or after this hour (ET) report all subjects
ALL_TIER_START_HOUR = 21


def timeslot_start_time(timeslot: int) -> datetime.time:
    """Start time (ET) of a rig schedule timeslot.

    Raises ValueError if timeslot is not in TIMESLOTS.
    """
    if timeslot not in TIMESLOTS:
        raise ValueError(f"Timeslot {timeslot} is outside {TIMESLOTS}")
    hour = SCHEDULE_START_HOUR + timeslot
    if timeslot >= LUNCH_TIMESLOT:
        hour += 1
    return datetime.time(hour)


def format_clock_time(value: datetime.time) -> str:
    """Format a time on the 12-hour clock, e.g. "2 PM" or "4:30 PM"."""
    hour = value.hour % 12 or 12
    minutes = f":{value.minute:02d}" if value.minute else ""
    suffix = "AM" if value.hour < 12 else "PM"
    return f"{hour}{minutes} {suffix}"


def early_tier_description() -> str:
    """Who the early alert reports, for the alert title."""
    slot_start = format_clock_time(timeslot_start_time(LAST_EARLY_TIMESLOT))
    cutoff_hour, cutoff_minute = divmod(WATERING_CUTOFF.seconds // 60, 60)
    watering_cutoff = format_clock_time(datetime.time(cutoff_hour, cutoff_minute))
    return (
        f"subjects trained in slot {LAST_EARLY_TIMESLOT} (starts {slot_start})"
        f" or earlier, or water-only and watered before {watering_cutoff}"
    )


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

    The result is renumbered 0..n-1. Raises ValueError if tier is not an
    AlertTier value.
    """
    match AlertTier(tier):
        case AlertTier.ALL:
            selected = subject_data
        case AlertTier.EARLY:
            selected = subject_data.loc[early_tier_mask(subject_data)]
        case unhandled:
            assert_never(unhandled)
    return selected.reset_index(drop=True)
