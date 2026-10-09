"""Explicit per-subject responsibilities shared by the pipeline, the website and
the weighing GUI.

There are two formats for "who does what for this subject today":

- Legacy: ``action.SubjectStatus.schedule``, seven "/"-separated technician
  duties starting on Sunday, e.g. "Water/Train/Train/Train/Train/Train/Water".
- Explicit: ``action.ResponsibilitiesAlt.responsibilities``, a JSON document
  written by tech_calendar_streamlit (tabs/assigning_responsibilities.py) that
  lists the responsibilities of the technician and of the researcher (owner).

The JSON, as written by ``dump_responsibilities``::

    {"assignment_style": "weekly" | "workweek_split" | "daily",
     <slot>: {"technician": [<responsibility>, ...], "owner": [...]}, ...}

where the slots are "weekly"; "weekdays" and "weekends" (Saturday, Sunday);
or "Sunday" ... "Saturday". Responsibilities are the exact Responsibility and
ManualResponsibility values, unique and in enum order. Readers treat a
missing or null party as no responsibilities and ignore other keys.
tests/fixtures/responsibilities_cases.json pins how every reader resolves
records; the weighing GUI runs a copy of it.

Which one the systems act on is the ``responsibility_format`` row of
``lab.FeatureFlag``: "legacy", then "preview" (experimenters fill in and check
the explicit responsibilities; nothing acts on them yet), then "explicit". A
missing table, row or unknown value means legacy, so the flag can be flipped
back without a deploy. In explicit mode the website writes both formats, so the
legacy schedule stays usable after a rollback.

This module does not connect to the database itself; ``get_responsibility_format``
takes the FeatureFlag table as an argument. The ViRMEn weighing GUI
(experiments/utility/WeighingGUI) mirrors these strings and the JSON layout;
keep the two in sync.
"""

import datetime
import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import datajoint as dj

logger = logging.getLogger(__name__)

# Order of the legacy "/"-separated schedule
DAYS_OF_WEEK = (
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
)
WEEKEND_DAYS = frozenset({"Saturday", "Sunday"})


def today_name(today: datetime.date | None = None) -> str:
    """Name of the day as in DAYS_OF_WEEK, e.g. "Monday"; defaults to today."""
    if today is None:
        # Lab-local, like CURDATE() in the alert's SQL
        today = datetime.date.today()
    return DAYS_OF_WEEK[(today.weekday() + 1) % 7]


class ResponsibleParty(StrEnum):
    # The JSON keys are the lowercased member names: "technician" and "owner"
    TECHNICIAN = "Technician"
    OWNER = "Owner/Co-owner"

    @property
    def json_key(self) -> str:
        return self.name.lower()


class AssignmentStyle(StrEnum):
    # The JSON stores the lowercased member name, e.g. "workweek_split"
    DAILY = "Specify each day"
    WEEKLY = "Specify for the whole week"
    WORKWEEK_SPLIT = "Specify by weekday/weekend"

    @property
    def json_key(self) -> str:
        return self.name.lower()


class Responsibility(StrEnum):
    """Responsibilities either party can take on, tracked by our systems."""

    WATERING = "Watering"
    WEIGHING = "Weighing"
    TRANSPORT_ONLY = "Transport Only"
    TRAINING = "Training"
    NOTHING = "Nothing"


class ManualResponsibility(StrEnum):
    """Researcher-only responsibilities recorded on paper.

    Our systems cannot see these, so nothing alerts on them.
    """

    MANUAL_WEIGHING = "Manual Weighing"
    MANUAL_WATERING = "Manual Watering"


type AnyResponsibility = Responsibility | ManualResponsibility

# Each manual responsibility replaces, and so conflicts with, a tracked one
MANUAL_REPLACES: Mapping[ManualResponsibility, Responsibility] = {
    ManualResponsibility.MANUAL_WEIGHING: Responsibility.WEIGHING,
    ManualResponsibility.MANUAL_WATERING: Responsibility.WATERING,
}

# Responsibilities that cannot be combined with any other for the same party
EXCLUSIVE_RESPONSIBILITIES = (Responsibility.NOTHING, Responsibility.TRANSPORT_ONLY)


def parse_responsibility(value: str) -> AnyResponsibility:
    """Return the responsibility with this value.

    Raises ValueError if the value is not a Responsibility or
    ManualResponsibility value.
    """
    for enum_cls in (Responsibility, ManualResponsibility):
        try:
            return enum_cls(value)
        except ValueError:
            pass
    raise ValueError(f"Unknown responsibility: {value!r}")


def allowed_responsibilities(party: ResponsibleParty) -> set[AnyResponsibility]:
    """Responsibilities this party may be assigned."""
    allowed: set[AnyResponsibility] = set(Responsibility)
    if party is ResponsibleParty.OWNER:
        allowed |= set(ManualResponsibility)
    return allowed


def validate_daily_assignment(
    technician: Iterable[AnyResponsibility],
    owner: Iterable[AnyResponsibility],
    require_watering: bool = True,
) -> list[str]:
    """Problems with one day's assignment, as messages for the user.

    An empty list means the assignment is valid. require_watering should be
    True for water-restricted subjects, where someone must water every day.
    """
    selections = {
        ResponsibleParty.TECHNICIAN: set(technician),
        ResponsibleParty.OWNER: set(owner),
    }
    both = selections[ResponsibleParty.TECHNICIAN] | selections[ResponsibleParty.OWNER]
    errors: list[str] = []

    if require_watering and not (
        {Responsibility.WATERING, ManualResponsibility.MANUAL_WATERING} & both
    ):
        errors.append(
            f'Please ensure "{Responsibility.WATERING}" or'
            f' "{ManualResponsibility.MANUAL_WATERING}" is assigned'
        )

    # Both parties doing nothing is fine (e.g. a dead subject)
    if overlapping := (
        selections[ResponsibleParty.TECHNICIAN]
        & selections[ResponsibleParty.OWNER] - {Responsibility.NOTHING}
    ):
        errors.append(
            "Cannot assign same responsibilities to both: "
            + ", ".join(sorted(overlapping))
        )

    for manual, tracked in MANUAL_REPLACES.items():
        if manual in both and tracked in both:
            errors.append(f"'{manual}' conflicts with '{tracked}'; choose one")

    for party, selected in selections.items():
        if not selected:
            errors.append(
                f"If there are no responsibilities for {party}, "
                f"please explicitly assign '{Responsibility.NOTHING}'."
            )
            continue
        if not_allowed := selected - allowed_responsibilities(party):
            errors.append(
                f"{', '.join(sorted(not_allowed))} can only be assigned to the researcher"
                f" ({ResponsibleParty.OWNER}), not the {party}"
            )
        if len(selected) > 1:
            errors.extend(
                f"Cannot have '{exclusive}' with other responsibilities for {party}"
                for exclusive in EXCLUSIVE_RESPONSIBILITIES
                if exclusive in selected
            )

    return errors


@dataclass(frozen=True)
class DailyAssignment:
    """Who is responsible for what on one day."""

    technician: frozenset[AnyResponsibility]
    owner: frozenset[AnyResponsibility]

    @property
    def all(self) -> frozenset[AnyResponsibility]:
        return self.technician | self.owner

    def party_for(self, duty: AnyResponsibility) -> ResponsibleParty | None:
        """The party assigned this duty, or None if nobody is."""
        if duty in self.technician:
            return ResponsibleParty.TECHNICIAN
        if duty in self.owner:
            return ResponsibleParty.OWNER
        return None

    def tracks(self, duty: Responsibility) -> bool:
        """Whether someone does this duty in a way our systems record."""
        return duty in self.all

    def is_manual(self, duty: Responsibility) -> bool:
        """Whether this duty is done on paper, outside our systems."""
        return any(
            tracked == duty and manual in self.all
            for manual, tracked in MANUAL_REPLACES.items()
        )

    def legacy_token(self) -> str:
        """The technician's duty in the legacy schedule ("Train", "Water", ...).

        The legacy schedule only describes the technician (the weighing GUI
        shows technicians the subjects that are not "Transport" or "Nothing"),
        so only their duties count, most involved first. The researcher's
        duties, manual or not, leave no trace: check the assignment itself
        for who trains.
        """
        if Responsibility.TRAINING in self.technician:
            return "Train"
        if Responsibility.WATERING in self.technician:
            return "Water"
        if Responsibility.WEIGHING in self.technician:
            return "Weigh"
        if Responsibility.TRANSPORT_ONLY in self.technician:
            return "Transport"
        return "Nothing"


def parse_slot(slot: Mapping[str, Any]) -> DailyAssignment:
    """Parse one {"technician": [...], "owner": [...]} entry.

    A missing, null or empty party means no responsibilities (MATLAB's
    jsondecode cannot tell null from []). Raises TypeError if the slot is not
    an object or a party is not a list of strings, and ValueError on unknown
    responsibilities.
    """
    if not isinstance(slot, Mapping):
        raise TypeError(f"Expected a {{technician, owner}} object, got {slot!r}")
    parsed: dict[ResponsibleParty, frozenset[AnyResponsibility]] = {}
    for party in ResponsibleParty:
        values = slot.get(party.json_key)
        if values is None:
            values = []
        if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
            raise TypeError(
                f"{party.json_key!r} must be a list of strings, got {values!r}"
            )
        parsed[party] = frozenset(parse_responsibility(v) for v in values)
    return DailyAssignment(
        technician=parsed[ResponsibleParty.TECHNICIAN],
        owner=parsed[ResponsibleParty.OWNER],
    )


def slot_keys(style: AssignmentStyle) -> tuple[str, ...]:
    """JSON keys holding the assignments under this style, in written order."""
    match style:
        case AssignmentStyle.WEEKLY:
            return ("weekly",)
        case AssignmentStyle.WORKWEEK_SPLIT:
            return ("weekdays", "weekends")
        case AssignmentStyle.DAILY:
            return DAYS_OF_WEEK


def slot_key(style: AssignmentStyle, day: str) -> str:
    """JSON key holding the assignment for this day under this style."""
    if day not in DAYS_OF_WEEK:
        raise ValueError(f"Unknown day {day!r}, expected one of {DAYS_OF_WEEK}")
    match style:
        case AssignmentStyle.WEEKLY:
            return "weekly"
        case AssignmentStyle.WORKWEEK_SPLIT:
            return "weekends" if day in WEEKEND_DAYS else "weekdays"
        case AssignmentStyle.DAILY:
            return day


def parse_assignment_style(record: Mapping[str, Any]) -> AssignmentStyle:
    """The record's style; the stored value is the exact lowercase json_key."""
    style_name = record.get("assignment_style")
    for style in AssignmentStyle:
        if style_name == style.json_key:
            return style
    raise ValueError(f"Unknown assignment_style {style_name!r}")


def assignment_for_day(record: Mapping[str, Any] | str, day: str) -> DailyAssignment:
    """The assignment for a day (e.g. "Monday") from a responsibilities JSON.

    Raises ValueError (including invalid JSON) or TypeError if the record is
    malformed. Keys left over from another assignment style are ignored.
    """
    if isinstance(record, str):
        record = json.loads(record)
    if not isinstance(record, Mapping):
        raise TypeError(f"Expected a responsibilities object, got {record!r}")
    key = slot_key(parse_assignment_style(record), day)
    if key not in record:
        raise ValueError(f"Responsibilities have no {key!r} entry for {day}")
    return parse_slot(record[key])


def dump_responsibilities(
    style: AssignmentStyle, slots: Mapping[str, Mapping[str, Iterable[str]]]
) -> str:
    """The canonical responsibilities JSON, as stored in ResponsibilitiesAlt.

    Only this style's keys are written, in slot_keys order, each as
    {"technician": [...], "owner": [...]} with unique values in enum order.
    Other keys in slots (e.g. from another style) are dropped. Raises
    ValueError if a slot is missing or a responsibility is unknown.
    """
    order = [*Responsibility, *ManualResponsibility]
    record: dict[str, Any] = {"assignment_style": style.json_key}
    for key in slot_keys(style):
        if key not in slots:
            raise ValueError(f"Missing {key!r} for assignment style {style.json_key!r}")
        slot = slots[key]
        assignment = parse_slot(
            {party: [str(v) for v in values or []] for party, values in slot.items()}
        )
        record[key] = {
            ResponsibleParty.TECHNICIAN.json_key: [
                str(v) for v in sorted(assignment.technician, key=order.index)
            ],
            ResponsibleParty.OWNER.json_key: [
                str(v) for v in sorted(assignment.owner, key=order.index)
            ],
        }
    return json.dumps(record)


def legacy_schedule(record: Mapping[str, Any] | str) -> str:
    """The legacy SubjectStatus.schedule string for a responsibilities JSON."""
    return "/".join(
        assignment_for_day(record, day).legacy_token() for day in DAYS_OF_WEEK
    )


class ResponsibilityFormat(StrEnum):
    """Which responsibilities format the systems act on."""

    LEGACY = "legacy"  # action.SubjectStatus.schedule
    # The website edits action.ResponsibilitiesAlt so experimenters can check it,
    # but everything (alert, weighing GUI) still acts on the legacy schedule
    PREVIEW = "preview"
    EXPLICIT = "explicit"  # action.ResponsibilitiesAlt

    @property
    def acts_on_explicit(self) -> bool:
        """Whether alerts and GUIs follow action.ResponsibilitiesAlt."""
        return self is ResponsibilityFormat.EXPLICIT

    @property
    def edits_explicit(self) -> bool:
        """Whether the website shows the explicit responsibilities page."""
        return self in (ResponsibilityFormat.PREVIEW, ResponsibilityFormat.EXPLICIT)


RESPONSIBILITY_FORMAT_FLAG = "responsibility_format"


def parse_responsibility_format(value: str | None) -> ResponsibilityFormat:
    """Parse a flag value, falling back to legacy if it is missing or unknown."""
    try:
        return ResponsibilityFormat(str(value).strip().lower())
    except ValueError:
        if value is not None:
            logger.warning(
                "Unknown %s %r, using %s",
                RESPONSIBILITY_FORMAT_FLAG,
                value,
                ResponsibilityFormat.LEGACY,
            )
        return ResponsibilityFormat.LEGACY


def get_responsibility_format(feature_flag: dj.Table) -> ResponsibilityFormat:
    """Read the responsibility_format flag.

    Args:
        feature_flag: the lab.FeatureFlag table, from u19_pipeline.lab or a
            virtual module of the lab schema.

    Returns LEGACY if the row does not exist, or if the flag cannot be read
    (legacy is the safe default to fall back to).
    """
    try:
        values = (feature_flag & {"flag_name": RESPONSIBILITY_FORMAT_FLAG}).fetch(
            "flag_value"
        )
    except dj.errors.DataJointError:
        logger.warning(
            "Cannot read %s, using %s",
            RESPONSIBILITY_FORMAT_FLAG,
            ResponsibilityFormat.LEGACY,
            exc_info=True,
        )
        return ResponsibilityFormat.LEGACY
    return parse_responsibility_format(values[0] if len(values) else None)
