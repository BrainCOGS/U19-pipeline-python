import datetime
import json
import os
import pathlib
import re
import time
from datetime import date, timedelta

import datajoint as dj
import pandas as pd
from dotenv import load_dotenv
from icalevents.icalevents import events

import u19_pipeline.lab as lab
import u19_pipeline.utils.slack_utils as su
from u19_pipeline import subject
from u19_pipeline.utils.subject_metadata import fetch_slack_handles_for_lab_managers_by_subject

slack_configuration_dictionary = {
    "slack_notification_channel": ["subject_health"],
}

# Query from file
QUERY_FILE = pathlib.Path(pathlib.Path(__file__).resolve().parent, "get_subject_data.sql").as_posix()


def get_subject_data():
    with open(QUERY_FILE, "r", encoding="utf-8") as file:
        subject_query = file.read()

    conn = dj.conn()
    subject_data = pd.DataFrame(conn.query(subject_query, as_dict=True).fetchall())

    # Get subject fullname only once
    subject_data = subject_data.loc[:, ~subject_data.columns.duplicated()]
    # subject_data = subject_data.rename({'new_subject_fullname': 'subject_fullname'}, axis=1)

    # Get column list
    cols = subject_data.columns.tolist()

    # Remove and insert the column
    cols.insert(0, cols.pop(cols.index("subject_fullname")))
    subject_data = subject_data.reindex(columns=cols)

    # Get today's responsibility index (0 = Sunday, 6 = Saturday)
    today_idx = (datetime.datetime.today().weekday() + 1) % 7
    subject_data["schedule_today"] = subject_data["schedule"].str.split("/")
    subject_data["schedule_today"] = subject_data["schedule_today"].apply(
        lambda x: x[today_idx] if isinstance(x, list) and len(x) == 7 else None
    )

    subject_data = subject_data.loc[
        (
            ((subject_data["schedule_today"] != "Nothing") & (subject_data["subject_status"] == "InExperiments"))
            | (subject_data["subject_status"] == "WaterRestrictionOnly")
        ),
        :,
    ]

    subject_data = subject_data.reset_index(drop=True)

    with pd.option_context("future.no_silent_downcasting", True):
        subject_data["earned"] = subject_data["earned"].fillna(0).infer_objects(copy=False)
        subject_data["received"] = subject_data["received"].fillna(0).infer_objects(copy=False)
        subject_data["supplement"] = subject_data["supplement"].fillna(0).infer_objects(copy=False)
        subject_data["prescribed_extra_supplement_amount"] = (
            subject_data["prescribed_extra_supplement_amount"].fillna(0).infer_objects(copy=False)
        )

    # Calculated fields
    subject_data["already_water"] = subject_data["supplement"] > 0
    subject_data["upper_cage"] = subject_data["cage"].str.upper()
    subject_data["total_water_received"] = subject_data["received"]
    subject_data.loc[subject_data["total_water_received"].isnull(), "total_water_received"] = 0

    subject_data["need_supplement"] = 0
    subject_data["need_supplement"] = (
        subject_data["total_water_received"] < (subject_data["water_per_day"] - 0.05)
    ) & ~subject_data["already_water"]

    subject_data.loc[subject_data["already_received"].isnull(), "already_received"] = 0
    subject_data["need_extra_water_now"] = 0
    subject_data.loc[
        ((subject_data["prescribed_extra_supplement_amount"] > 0) & (subject_data["already_received"] == 0)),
        "need_extra_water_now",
    ] = 1

    subject_data["water_status"] = "Already Watered"
    subject_data.loc[subject_data["need_supplement"] == 1, "water_status"] = "Need Supplement"
    subject_data.loc[subject_data["need_extra_water_now"] == 1, "water_status"] = "Need Extra Supplement"

    subject_data["need_water"] = 0
    subject_data.loc[(subject_data["need_supplement"] | subject_data["need_extra_water_now"]), "need_water"] = 1

    subject_data["current_need_water"] = subject_data["suggested_water"]
    subject_data.loc[subject_data["need_extra_water_now"] == 1, "current_need_water"] = subject_data.loc[
        subject_data["need_extra_water_now"] == 1, "prescribed_extra_supplement_amount"
    ]

    subject_data["training_status"] = 0
    subject_data.loc[~subject_data["scheduled_rig"].isnull(), "training_status"] = 1
    subject_data.loc[~subject_data["session_location"].isnull(), "training_status"] = 2

    subject_data["training_status_label"] = "Not scheduled"
    subject_data.loc[subject_data["training_status"] == 1, "training_status_label"] = "Scheduled"
    subject_data.loc[subject_data["training_status"] == 2, "training_status_label"] = "Training Started"

    # Calculated fields
    subject_data["already_weighted"] = ~subject_data["weight"].isnull()
    subject_data["need_weight"] = ~subject_data["already_weighted"] | subject_data["need_reweight"]

    subject_data["weight_status"] = "Already Weighted"
    subject_data.loc[subject_data["already_weighted"] == 0, "weight_status"] = "Need Weight"
    subject_data.loc[subject_data["need_reweight"] == 1, "weight_status"] = "REWEIGHT"

    return subject_data


def fetch_and_parse_icalevents(weburl: str):
    """Fetch and filter iCal events for VR-related duties.

    Args:
        weburl (str): url to the iCal feed
        logger (Logger): Local Python logger

    Returns:
        list[dict]: _description_
    """

    start_date = datetime.datetime.now() - timedelta(days=1)
    end_date = datetime.datetime.now() + timedelta(days=2)
    vr_events = events(url=weburl, start=start_date, end=end_date)

    # Define mapping of substrings to event types and their corresponding colors
    vr_types = [
        (r"as\s*vr\s*water\s*at", "VR Water", "blue", "VR Watering only"),
        (r"as\s*vr\s*train\s*at", "VR Train", "orange", "VR Onboarding"),
        (r"as\s*vr\s*at", "Regular VR", "green", "All VR Duties"),
        (r"as\s*vr(?:\s*(?:with|w)\s*)?brody(?:\s*mice)?\s*at", "VR Brody Mice", "purple", " VR with Brody (Mice)"),
    ]

    filtered_events: list[dict[str, str | int]] = []

    # Consolidate lab clean-up events
    lab_clean_up_events = [
        event for event in vr_events if re.search(r"as\s*lab*\s*clean*\s*up*\s*at", event.summary.lower())
    ]
    non_lab_clean_up_events = [
        event for event in vr_events if not re.search(r"as\s*lab*\s*clean*\s*up*\s*at", event.summary.lower())
    ]
    if lab_clean_up_events:
        consolidated_event = {
            "start": str(min(event.start for event in lab_clean_up_events)),
            "end": str(max(event.end for event in lab_clean_up_events)),
            "title": "Lab - Lab Cleanup (Watering and No training duties)",
            "id": "lab_cleanup_consolidated",
            "type": "Regular VR",
            "color": "black",
            "personnel": "Lab",
            "duties": "Lab Cleanup",
        }
        filtered_events.append(consolidated_event)

    # Remove individual lab cleanup events from the filtered events
    vr_events = non_lab_clean_up_events

    for event in vr_events:
        summary = event.summary.lower()
        for pattern, event_type, color, duties in vr_types:
            if re.search(pattern, summary):
                person = event.summary.split("(")[0].strip()
                filtered_events.append(
                    {
                        "start": str(event.start),
                        "end": str(event.end),
                        "title": person + " - " + duties,
                        "id": event.uid,
                        "type": event_type,
                        "color": color,
                        "personnel": person,
                        "duties": duties,
                    }
                )

    return filtered_events


def get_responsible_user_slack(subject_data: pd.DataFrame) -> pd.DataFrame:
    """Add a column with Slack tags for the responsible user(s) per subject.

    Logic:
    - If schedule_today is None or 'Transport', tag co-owners (lab managers).
    - Otherwise, tag the on-duty technician for that schedule token/day if available,
      falling back to co-owners when no tech is found.

    We fetch user <-> slack info only once and then map by user_id.
    """

    people_and_their_managers_query = (
        lab.User().proj("user_id", user_slack="slack")
        * lab.UserLab()
        * (
            lab.Lab()
            * lab.LabManager().proj("lab", "lab_manager")
            * lab.User().proj(lab_manager="user_id", manager_slack="slack")
        )
    )

    technician_manager_slack = (
        (lab.LabManager().proj("lab", "lab_manager") & 'lab = "technician"')
        * lab.User().proj(lab_manager="user_id", manager_slack="slack")
    ).fetch("manager_slack", as_dict=True)
    print(technician_manager_slack)
    technician_manager_slack = [item["manager_slack"] for item in technician_manager_slack]

    people_and_their_managers: pd.DataFrame = people_and_their_managers_query.fetch(format="frame")

    load_dotenv()

    wheniwork_url = os.getenv("WHENIWORK_ICAL_URL", "")

    events = fetch_and_parse_icalevents(wheniwork_url)

    today = date.today()

    events = [
        e
        for e in events
        if datetime.datetime.fromisoformat(e["start"]).date() == today
        or datetime.datetime.fromisoformat(e["end"]).date() == today
    ]
    tech_personnel: list[str] = [e["personnel"] for e in events]
    techs = lab.User().fetch(format="frame")

    tech_slack = []

    for tech in tech_personnel:
        tech_row = techs[techs["full_name"].str.lower() == tech.lower()]
        if not tech_row.empty:
            tech_uid = tech_row.iloc[0]["slack"]
            tech_slack.append(tech_uid)

    if tech_slack:
        tech_slack.append(technician_manager_slack[0])  # Fallback to default technician manager slack

    coowner_dataframe: pd.DataFrame = (
        (
            subject.SubjectCoowners()
            * lab.User().proj(
                "slack",
                coowner="user_id",
            )
            & "active = 1"
        )
        .proj("slack")
        .fetch(format="frame")
        .reset_index()
    )

    def resolve_responsible_slack(row):
        schedule_today = row.get("schedule_today")
        lab_name = row.get("lab")

        token = str(schedule_today).strip() if schedule_today is not None else None
        use_coowners = (token is None) or (token.lower() == "transport") or (token.lower() == "nothing")

        slack_tags = []

        if not use_coowners:
            # Try technicians first
            for uid in tech_slack:
                slack_tags.append(uid)

        # Fallback or default: co-owners (lab managers)
        if use_coowners or not slack_tags:
            # Add the co-owners
            # Add the owners
            temp = people_and_their_managers.query(f"user_id == '{row.get('user_id')}'")
            # print(temp)
            slack_tags += temp["manager_slack"].tolist()
            slack_tags += temp["user_slack"].tolist()
            slack_tags += coowner_dataframe.query(f"subject_fullname == '{row['subject_fullname']}'")["slack"].tolist()

        slack_tags = set(slack_tags)  # Remove duplicates

        return " ".join(f"<@{item}>" for item in slack_tags)

    subject_data = subject_data.copy()
    subject_data["responsible_slack_tags"] = subject_data.apply(resolve_responsible_slack, axis=1)
    return subject_data


def find_unreturned_subjects() -> pd.DataFrame:
    import datajoint as dj

    action = dj.create_virtual_module("action", "u19_action")
    today = datetime.date.today()

    # Obtain the last transport date
    local_transport_data: pd.DataFrame
    local_transport_data = (
        dj.U("subject_fullname")
        .aggr(
            action.Transport() & f'DATE(transport_out_datetime) = "{today}"',
            # transport_out_datetime="MAX(transport_out_datetime)",
            last_transport="MAX(transport_out_datetime)",
            transport_in_datetime="MAX(transport_in_datetime)",
        )
        .fetch(format="frame")
    )

    unreturned_subjects: pd.DataFrame = local_transport_data[local_transport_data["transport_in_datetime"].isnull()]

    return unreturned_subjects


def slack_alert_message_format_weight_water(
    subjects_not_watered: pd.DataFrame,
    subjects_not_weighted: pd.DataFrame,
    subjects_not_trained: pd.DataFrame,
    missing_transport: pd.DataFrame,
    individual_alert: bool = False,
):
    now = datetime.datetime.now()
    datestr = now.strftime("%d-%b-%Y %H:%M:%S")

    print(missing_transport)
    temp_missing_transport = missing_transport.copy().reset_index()
    notifiable_subjects = set(
        temp_missing_transport["subject_fullname"].tolist()
        + subjects_not_watered["subject_fullname"].tolist()
        + subjects_not_weighted["subject_fullname"].tolist()
    )

    slack_handles: list[str] = fetch_slack_handles_for_lab_managers_by_subject(notifiable_subjects)
    lab_manager_text = "\n\n"
    if len(slack_handles) >= 1:
        lab_manager_text += "Lab Manager"
    if len(slack_handles) > 1:
        lab_manager_text += "s"

    slack_handles_formatted = ", ".join("<@" + handle + ">" for handle in slack_handles)
    if slack_handles_formatted:
        lab_manager_text += (
            " " + slack_handles_formatted + ", please be advised that your labs' subjects are listed below."
        )

    msep = dict()
    msep["type"] = "divider"

    # Title#
    m1 = dict()
    m1["type"] = "section"
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"

    m1_1["text"] = ":rotating_light: *Subjects Status Alert *" + lab_manager_text
    m1["text"] = m1_1

    # Info for subjects missing water
    m2 = dict()
    m2["type"] = "section"
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    if subjects_not_watered.empty:
        m2_1["text"] = "*Subjects missing water:* None\n"
    else:
        m2_1["text"] = "*Subjects missing water:*" + "\n"
        for i in range(subjects_not_watered.shape[0]):
            subject_name = subjects_not_watered.loc[i, "subject_fullname"]
            need_water_ml = str(subjects_not_watered.loc[i, "current_need_water"])

            # Tag responsible user(s) if available
            if "responsible_slack_tags" in subjects_not_watered.columns:
                tags = subjects_not_watered.loc[i, "responsible_slack_tags"]
            else:
                tags = []

            if isinstance(tags, (list, tuple, set)):
                tag_str = " ".join(str(t) for t in tags)
            else:
                tag_str = str(tags) if tags else ""

            line = f"*{subject_name}* : {need_water_ml} ml"
            if tag_str:
                line += f" {tag_str}"

            m2_1["text"] += line + "\n"
    m2["text"] = m2_1

    # Info for subjects missing weighing
    m4 = dict()
    m4["type"] = "section"
    m4_1 = dict()
    m4_1["type"] = "mrkdwn"

    if subjects_not_weighted.empty:
        m4_1["text"] = "*Subjects missing weighing:* None\n"
    else:
        m4_1["text"] = "*Subjects missing weighing:*" + "\n"
        for i in range(subjects_not_weighted.shape[0]):
            # m4_1["text"] += "*" + subjects_not_weighted.loc[i, "subject_fullname"] + "*\n"

            subject_name = subjects_not_weighted.loc[i, "subject_fullname"]

            # Tag responsible user(s) if available
            if "responsible_slack_tags" in subjects_not_weighted.columns:
                tags = subjects_not_weighted.loc[i, "responsible_slack_tags"]
            else:
                tags = []

            if isinstance(tags, (list, tuple, set)):
                tag_str = " ".join(str(t) for t in tags)
            else:
                tag_str = str(tags) if tags else ""

            line = f"*{subject_name}* : "
            if tag_str:
                line += f" {tag_str}"

            m4_1["text"] += line + "\n"
    m4["text"] = m4_1

    # Info for subjects missing training
    m5 = dict()
    m5["type"] = "section"
    m5_1 = dict()
    m5_1["type"] = "mrkdwn"

    if subjects_not_trained.empty:
        m5_1["text"] = "*Subjects missing training:* None\n"
    else:
        m5_1["text"] = "*Subjects missing training:*" + "\n"
        for i in range(subjects_not_trained.shape[0]):
            m5_1["text"] += (
                "*"
                + subjects_not_trained.loc[i, "subject_fullname"]
                + "* : "
                + subjects_not_trained.loc[i, "scheduled_rig"]
                + "\n"
            )
    m5["text"] = m5_1

    # Info for missing transport
    m6 = dict()
    m6["type"] = "section"
    m6_1 = dict()
    m6_1["type"] = "mrkdwn"

    if missing_transport.empty:
        m6_1["text"] = "*Subjects missing transport:* None\n"
    else:
        m6_1["text"] = "*Subjects missing transport:*" + "\n"
        for i in range(missing_transport.shape[0]):
            subject_name = missing_transport.index[i]
            # Try to get responsible slack tags for this subject
            tags = ""
            try:
                tags = missing_transport.iloc[i].get("responsible_slack_tags", "")  # may be string or iterable
            except Exception:
                tags = ""
            # Normalize tags to a string
            if pd.isna(tags):
                tag_str = ""
            elif isinstance(tags, (list, tuple, set)):
                tag_str = " ".join(str(t) for t in tags)
            else:
                tag_str = str(tags).strip() if tags else ""

            line = f"*{subject_name}*"
            if tag_str:
                line += f" : {tag_str}"
            m6_1["text"] += line + "\n"
    m6["text"] = m6_1

    message = dict()
    message["blocks"] = [m1, msep, m2, msep, m4, msep, m5, msep, m6, msep]
    # msg_groups = [m1, m2, m4, m5, m6]
    msg_groups = [m2]
    # message["blocks"] = [m6, msep]
    message["text"] = "Subject Status Alert"

    if not individual_alert:
        message["blocks"] = [m1, msep, m2, msep, m4, msep, m5, msep, m6, msep]
        return [message]
    else:
        msg = []
        msg_copy = message.copy()
        for msg_block in msg_groups:
            # msg_copy["blocks"] = [m1, msep, msg_block, msep]
            msg_copy["blocks"] = [msg_block]
            msg.append(msg_copy.copy())
        return msg

    # return message


SLACK_MAX_CHARS = 3_000  # conservative margin below Slack's limit


def estimate_size(payload):
    return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))


def _make_section_block(text: str, text_type: str = "mrkdwn") -> dict:
    """Create a section block with the given text."""
    return {"type": "section", "text": {"type": text_type, "text": text}}


def _estimate_section_size(text: str, text_type: str = "mrkdwn") -> int:
    """Estimate the JSON-encoded size of a section block with this text."""
    block = _make_section_block(text, text_type)
    return estimate_size({"blocks": [block]})


def split_text_to_fit(text: str, max_payload_size: int, text_type: str = "mrkdwn") -> list[str]:
    """Split text by lines so each chunk fits in max_payload_size when JSON-encoded.

    Uses actual JSON size estimation rather than raw character count.
    """
    lines = text.split("\n")
    chunks = []
    current_lines = []

    for line in lines:
        test_lines = current_lines + [line]
        test_text = "\n".join(test_lines)

        if _estimate_section_size(test_text, text_type) <= max_payload_size:
            # Line fits, add it
            current_lines.append(line)
        else:
            # Line doesn't fit
            if current_lines:
                # Save current chunk and start new one with this line
                chunks.append("\n".join(current_lines))
                current_lines = [line]

                # Check if even a single line is too big
                if _estimate_section_size(line, text_type) > max_payload_size:
                    # Single line too big - this shouldn't happen often, but handle it
                    # by truncating (better than crashing)
                    print("Warning: Single line too large, may be truncated")
            else:
                # Even first line doesn't fit - add it anyway (will be handled later)
                current_lines = [line]

    # Don't forget the last chunk
    if current_lines:
        chunks.append("\n".join(current_lines))

    return chunks if chunks else [text]


def _split_large_block(block: dict, max_size: int) -> list[dict]:
    """Split a single block that's too large into smaller blocks.

    Returns a list of smaller blocks that can be batched by the caller.
    """
    if block.get("type") == "divider":
        return [block]

    if block.get("type") == "section":
        text_obj = block.get("text", {})
        text = text_obj.get("text", "")
        text_type = text_obj.get("type", "mrkdwn")

        # Split text into chunks that fit when JSON-encoded
        chunks = split_text_to_fit(text, max_size, text_type)

        return [_make_section_block(chunk, text_type) for chunk in chunks]
    else:
        return [block]


def send_slack_blocks_safely(webhook: str, blocks: list[dict], max_size: int = SLACK_MAX_CHARS):
    """Send blocks together when possible, splitting only when necessary.

    Strategy:
    1. First, expand any blocks that are too large into smaller blocks
    2. Then greedily batch all blocks together until we hit the size limit
    3. Send each batch
    """
    from requests.exceptions import HTTPError

    if not blocks:
        return

    # Step 1: Expand large blocks into smaller ones
    expanded_blocks = []
    for block in blocks:
        single_payload = {"blocks": [block]}
        if estimate_size(single_payload) <= max_size:
            expanded_blocks.append(block)
        else:
            smaller_blocks = _split_large_block(block, max_size)
            expanded_blocks.extend(smaller_blocks)

    # Step 2: Greedily batch expanded blocks together
    batches = []
    current_batch = []

    for block in expanded_blocks:
        test_batch = current_batch + [block]
        test_payload = {"blocks": test_batch}

        if estimate_size(test_payload) <= max_size:
            current_batch.append(block)
        else:
            if current_batch:
                batches.append(current_batch)
            current_batch = [block]

    if current_batch:
        batches.append(current_batch)

    # Step 3: Send each batch with error handling
    for batch in batches:
        payload = {"blocks": batch}
        try:
            su.send_slack_notification(webhook, payload)
            time.sleep(0.5)
        except HTTPError as e:
            if "400" in str(e):
                # Batch rejected - split and retry with smaller size limit
                _send_blocks_individually(webhook, batch, max_size // 2)
            else:
                raise


def _send_blocks_individually(webhook: str, blocks: list[dict], max_size: int):
    """Fallback: send blocks one at a time, splitting if needed."""
    from requests.exceptions import HTTPError

    for block in blocks:
        single_payload = {"blocks": [block]}

        if estimate_size(single_payload) <= max_size:
            try:
                su.send_slack_notification(webhook, single_payload)
                time.sleep(0.5)
            except HTTPError as e:
                if "400" in str(e):
                    # Still too large, split further
                    smaller = _split_large_block(block, max_size)
                    for small_block in smaller:
                        try:
                            su.send_slack_notification(webhook, {"blocks": [small_block]})
                            time.sleep(0.5)
                        except HTTPError:
                            print("Failed to send block even after splitting")
                else:
                    raise
        else:
            # Block too large, split it
            smaller = _split_large_block(block, max_size)
            for small_block in smaller:
                try:
                    su.send_slack_notification(webhook, {"blocks": [small_block]})
                    time.sleep(0.5)
                except HTTPError:
                    print("Failed to send small block")


def main_water_weigh_alert():
    dj.conn()

    subject_data = get_subject_data()

    subject_data = get_responsible_user_slack(subject_data)

    # print(subject_data.columns)

    subject_data = subject_data.loc[~subject_data["subject_fullname"].str.contains("test"), :]
    subjects_not_watered = subject_data.loc[
        subject_data["current_need_water"] > 0, ["subject_fullname", "current_need_water", "responsible_slack_tags"]
    ]
    subjects_not_watered = subjects_not_watered.reset_index(drop=True)
    subjects_not_watered["current_need_water"] = subjects_not_watered["current_need_water"].apply(lambda x: f"{x:.1f}")
    # subjects_not_watered = subjects_not_watered.head()

    subjects_not_weighted = subject_data.loc[
        subject_data["need_weight"], ["subject_fullname", "need_weight", "responsible_slack_tags"]
    ]
    subjects_not_weighted = subjects_not_weighted.reset_index(drop=True)
    # subjects_not_weighted = subjects_not_weighted.head()

    subjects_not_trained = subject_data.loc[subject_data["training_status"] == 1, ["subject_fullname", "scheduled_rig"]]
    subjects_not_trained = subjects_not_trained.reset_index(drop=True)
    # subjects_not_trained = subjects_not_trained.head()

    subject_not_returned = find_unreturned_subjects()

    # Join responsible_slack_tags from subject_data on subject_fullname
    subject_not_returned = subject_not_returned.merge(
        subject_data[["subject_fullname", "responsible_slack_tags"]],
        on="subject_fullname",
        how="left",
    ).set_index("subject_fullname")

    webhooks_list = su.get_webhook_list(slack_configuration_dictionary, lab)

    # Send alert
    slack_json_messages = slack_alert_message_format_weight_water(
        subjects_not_watered, subjects_not_weighted, subjects_not_trained, missing_transport=subject_not_returned
    )


    # Send each message's blocks safely (splitting large blocks as needed)
    for this_webhook in webhooks_list:
        for message in slack_json_messages:
            blocks = message.get("blocks", [])
            send_slack_blocks_safely(this_webhook, blocks)


if __name__ == "__main__":
    main_water_weigh_alert()
