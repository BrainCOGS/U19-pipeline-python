import numpy as np
import pandas as pd
import pytest

from u19_pipeline.alert_system.water_weigh_alert import alert_message as am
from u19_pipeline.alert_system.water_weigh_alert.alert_tiers import AlertTier

DIVIDER = {"type": "divider"}


def section(text):
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def full_frames():
    watered = pd.DataFrame(
        {
            "subject_fullname": ["jy_001", "jy_002"],
            "current_need_water": ["1.0", "0.5"],
            "responsible_slack_tags": ["<@U1> <@U2>", ""],
        }
    )
    weighted = pd.DataFrame(
        {
            "subject_fullname": ["jy_001", "ab_010"],
            "need_weight": [True, True],
            "responsible_slack_tags": ["<@U1>", ""],
        }
    )
    trained = pd.DataFrame(
        {"subject_fullname": ["cd_020"], "scheduled_rig": ["165I-Rig1-T (slot 2)"]}
    )
    transport = pd.DataFrame(
        {
            "subject_fullname": ["ef_030", "gh_040"],
            "responsible_slack_tags": ["<@U3>", np.nan],
        }
    )
    return watered, weighted, trained, transport


def empty_frames():
    return (
        pd.DataFrame(
            columns=[
                "subject_fullname",
                "current_need_water",
                "responsible_slack_tags",
            ]
        ),
        pd.DataFrame(
            columns=["subject_fullname", "need_weight", "responsible_slack_tags"]
        ),
        pd.DataFrame(columns=["subject_fullname", "scheduled_rig"]),
        pd.DataFrame(columns=["subject_fullname", "responsible_slack_tags"]),
    )


def section_texts(blocks):
    return [b["text"]["text"] for b in blocks if b["type"] == "section"]


WATER_SECTION = section(
    "*Subjects missing water:*\n*jy_001* : 1.0 ml <@U1> <@U2>\n*jy_002* : 0.5 ml\n"
)
WEIGHING_SECTION = section(
    "*Subjects missing weighing:*\n*jy_001* :  <@U1>\n*ab_010* : \n"
)
TRAINING_SECTION = section(
    "*Subjects missing training:*\n*cd_020* : 165I-Rig1-T (slot 2)\n"
)
TRANSPORT_SECTION = section(
    "*Subjects missing transport:*\n*ef_030* : <@U3>\n*gh_040*\n"
)


class TestFormatAlertMessage:
    """Expected blocks were captured from the message builder before the refactor,
    then transport was moved up to second (subjects not returned are the most
    urgent after water)."""

    def test_full_early_alert_with_two_managers(self):
        blocks = am.format_alert_message(
            *full_frames(), lab_manager_handles=["UM1", "UM2"], tier=AlertTier.EARLY
        )
        assert blocks == [
            section(
                ":rotating_light: *Subjects Status Alert * (early check: subjects"
                " trained in slot 5 (starts 2 PM) or earlier, or water-only and"
                " watered before 4 PM)\n\nLab Managers <@UM1>, <@UM2>, please be"
                " advised that your labs' subjects are listed below."
            ),
            DIVIDER,
            WATER_SECTION,
            DIVIDER,
            TRANSPORT_SECTION,
            DIVIDER,
            WEIGHING_SECTION,
            DIVIDER,
            TRAINING_SECTION,
            DIVIDER,
        ]

    def test_full_late_alert_with_one_manager(self):
        blocks = am.format_alert_message(
            *full_frames(), lab_manager_handles=["UM1"], tier=AlertTier.ALL
        )
        assert blocks == [
            section(
                ":rotating_light: *Subjects Status Alert *\n\nLab Manager <@UM1>,"
                " please be advised that your labs' subjects are listed below."
            ),
            DIVIDER,
            WATER_SECTION,
            DIVIDER,
            TRANSPORT_SECTION,
            DIVIDER,
            WEIGHING_SECTION,
            DIVIDER,
            TRAINING_SECTION,
            DIVIDER,
        ]

    def test_empty_alert_without_managers(self):
        blocks = am.format_alert_message(*empty_frames(), lab_manager_handles=[])
        assert blocks == [
            section(":rotating_light: *Subjects Status Alert *\n\n"),
            DIVIDER,
            section("*Subjects missing water:* None\n"),
            DIVIDER,
            section("*Subjects missing transport:* None\n"),
            DIVIDER,
            section("*Subjects missing weighing:* None\n"),
            DIVIDER,
            section("*Subjects missing training:* None\n"),
            DIVIDER,
        ]

    def test_defaults_to_late_alert_title(self):
        blocks = am.format_alert_message(*empty_frames(), lab_manager_handles=[])
        assert "early check" not in section_texts(blocks)[0]

    def test_tier_as_plain_string(self):
        early = am.format_alert_message(
            *empty_frames(), lab_manager_handles=[], tier=AlertTier.EARLY
        )
        from_string = am.format_alert_message(
            *empty_frames(), lab_manager_handles=[], tier="early"
        )
        assert from_string == early

    def test_frames_without_tag_column(self):
        watered, weighted, trained, transport = full_frames()
        texts = section_texts(
            am.format_alert_message(
                watered.drop(columns="responsible_slack_tags"),
                weighted.drop(columns="responsible_slack_tags"),
                trained,
                transport.drop(columns="responsible_slack_tags"),
                lab_manager_handles=[],
            )
        )
        assert "*jy_001* : 1.0 ml\n" in texts[1]
        assert texts[2] == "*Subjects missing transport:*\n*ef_030*\n*gh_040*\n"
        assert "*jy_001* : \n" in texts[3]

    def test_index_does_not_matter(self):
        # Callers filter rows, so frames can arrive with a non-default index
        shifted = [f.set_axis(range(10, 10 + len(f))) for f in full_frames()]
        assert am.format_alert_message(
            *shifted, lab_manager_handles=[]
        ) == am.format_alert_message(*full_frames(), lab_manager_handles=[])


class TestNotifiableSubjects:
    def test_combines_water_weighing_and_transport(self):
        watered, weighted, _, transport = full_frames()
        assert am.notifiable_subjects(watered, weighted, transport) == [
            "ab_010",
            "ef_030",
            "gh_040",
            "jy_001",
            "jy_002",
        ]

    def test_empty(self):
        watered, weighted, _, transport = empty_frames()
        assert am.notifiable_subjects(watered, weighted, transport) == []


class TestFormatTags:
    @pytest.mark.parametrize(
        "tags, expected",
        [
            ("<@U1> <@U2>", "<@U1> <@U2>"),
            ("  <@U1> ", "<@U1>"),
            ("", ""),
            ("   ", ""),
            (None, ""),
            (np.nan, ""),
            (pd.NA, ""),
            (["<@U1>", "<@U2>"], "<@U1> <@U2>"),
            (("<@U1>",), "<@U1>"),
            ({"<@U1>"}, "<@U1>"),
            ([], ""),
        ],
    )
    def test_normalizes_to_text(self, tags, expected):
        assert am.format_tags(tags) == expected


class TestHeader:
    @pytest.mark.parametrize(
        "handles, expected",
        [
            ([], "\n\n"),
            (
                ["UM1"],
                (
                    "\n\nLab Manager <@UM1>, please be advised that your labs'"
                    " subjects are listed below."
                ),
            ),
            (
                ["UM1", "UM2", "UM3"],
                (
                    "\n\nLab Managers <@UM1>, <@UM2>, <@UM3>, please be advised that"
                    " your labs' subjects are listed below."
                ),
            ),
        ],
    )
    def test_lab_manager_text(self, handles, expected):
        blocks = am.format_alert_message(*empty_frames(), lab_manager_handles=handles)
        assert section_texts(blocks)[0] == (
            ":rotating_light: *Subjects Status Alert *" + expected
        )
