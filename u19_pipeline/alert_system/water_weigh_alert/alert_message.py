"""Slack blocks for the water/weigh alert.

Nothing here touches the database: the caller fetches the lab manager Slack
handles (for the subjects from notifiable_subjects) and passes them in.
"""

from collections.abc import Iterable

import pandas as pd

from u19_pipeline.alert_system.water_weigh_alert.alert_tiers import (
    AlertTier,
    early_tier_description,
)

DIVIDER = {"type": "divider"}
TAGS_COLUMN = "responsible_slack_tags"


def section_block(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def format_tags(tags) -> str:
    """Turn a responsible_slack_tags value (str, list, set, None or NaN) into text."""
    if isinstance(tags, (list, tuple, set)):
        return " ".join(str(tag) for tag in tags)
    if tags is None or pd.isna(tags):
        return ""
    return str(tags).strip()


def notifiable_subjects(
    subjects_not_watered: pd.DataFrame,
    subjects_not_weighted: pd.DataFrame,
    missing_transport: pd.DataFrame,
) -> list[str]:
    """Subjects whose lab managers are tagged in the alert header."""
    return sorted(
        set(subjects_not_watered["subject_fullname"])
        | set(subjects_not_weighted["subject_fullname"])
        | set(missing_transport["subject_fullname"])
    )


def _header_block(lab_manager_handles: list[str], tier: AlertTier) -> dict:
    tier_text = ""
    if tier == AlertTier.EARLY:
        tier_text = f" (early check: {early_tier_description()})"

    lab_manager_text = "\n\n"
    if lab_manager_handles:
        managers = "Lab Managers" if len(lab_manager_handles) > 1 else "Lab Manager"
        handles = ", ".join(f"<@{handle}>" for handle in lab_manager_handles)
        lab_manager_text += (
            f"{managers} {handles}, please be advised that your labs' subjects"
            " are listed below."
        )

    return section_block(
        ":rotating_light: *Subjects Status Alert *" + tier_text + lab_manager_text
    )


def _section(title: str, lines: Iterable[str]) -> dict:
    lines = list(lines)
    if not lines:
        return section_block(f"*{title}:* None\n")
    return section_block(f"*{title}:*\n" + "".join(f"{line}\n" for line in lines))


def _rows(subjects: pd.DataFrame) -> list[dict]:
    return subjects.to_dict("records")


def format_alert_message(
    subjects_not_watered: pd.DataFrame,
    subjects_not_weighted: pd.DataFrame,
    subjects_not_trained: pd.DataFrame,
    missing_transport: pd.DataFrame,
    lab_manager_handles: list[str],
    tier: AlertTier | str = AlertTier.ALL,
) -> list[dict]:
    """Build the alert's Slack blocks.

    Every frame has a subject_fullname column. responsible_slack_tags is
    optional on the water, weighing and transport frames.
    """
    water_lines = []
    for row in _rows(subjects_not_watered):
        line = f"*{row['subject_fullname']}* : {row['current_need_water']} ml"
        if tags := format_tags(row.get(TAGS_COLUMN)):
            line += f" {tags}"
        water_lines.append(line)

    weighing_lines = []
    for row in _rows(subjects_not_weighted):
        line = f"*{row['subject_fullname']}* : "
        if tags := format_tags(row.get(TAGS_COLUMN)):
            line += f" {tags}"
        weighing_lines.append(line)

    training_lines = [
        f"*{row['subject_fullname']}* : {row['scheduled_rig']}"
        for row in _rows(subjects_not_trained)
    ]

    transport_lines = []
    for row in _rows(missing_transport):
        line = f"*{row['subject_fullname']}*"
        if tags := format_tags(row.get(TAGS_COLUMN)):
            line += f" : {tags}"
        transport_lines.append(line)

    sections = [
        _header_block(lab_manager_handles, AlertTier(tier)),
        _section("Subjects missing water", water_lines),
        _section("Subjects missing weighing", weighing_lines),
        _section("Subjects missing training", training_lines),
        _section("Subjects missing transport", transport_lines),
    ]

    blocks = []
    for block in sections:
        blocks += [block, DIVIDER]
    return blocks
