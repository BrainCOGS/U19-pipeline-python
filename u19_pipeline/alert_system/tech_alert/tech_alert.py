import datetime
import re
import sys
from datetime import timedelta
from pprint import pprint

import datajoint as dj
from icalevents.icalevents import events

from u19_pipeline.utils import slack_utils as su


def tech_schedule():
    lab = dj.create_virtual_module("lab", "u19_lab")

    slack_configuration_dictionary = {"webhook_name": "wheniwork_ical_url"}
    url = (lab.SlackWebhooks & slack_configuration_dictionary).fetch1("webhook_url")
    schedule_data = fetch_and_parse_icalevents(url)

    return schedule_data


def get_webhook_list(lab):
    # Get webhook lists
    slack_configuration_dictionary = {"slack_notification_channel": ["rigs_scheduling"]}
    webhooks_list = []
    query_slack_webhooks = [{"webhook_name": x} for x in slack_configuration_dictionary["slack_notification_channel"]]
    webhooks_list += (lab.SlackWebhooks & query_slack_webhooks).fetch("webhook_url").tolist()
    return webhooks_list


def slack_alert_message_format_tech_alert(schedule_data):
    shifts = schedule_data
    today = datetime.date.today()
    shifts_today = [shift for shift in shifts if shift["date"] == today]

    # Divider #

    msep = dict()
    msep["type"] = "divider"

    # Title#
    m1 = dict()
    m1["type"] = "section"
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = "🗓 *Today's Tech Schedule:*"
    m1["text"] = m1_1

    message = dict()
    message["blocks"] = [m1, msep]
    message["text"] = "🗓 *Today's Tech Schedule:*"

    # Info#
    m2 = dict()
    m2["type"] = "section"
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    # If there are shifts today, list them; otherwise provide a short fallback
    if shifts_today:
        m2_1["text"] = "\n".join(
            f"*{shift['personnel']}* ({shift['duties']}) is expected to work today between "
            f"{shift['start'].strftime('%I:%M %p')} and {shift['end'].strftime('%I:%M %p')} "
            "(actual times may vary)."
            for shift in shifts_today
        )
    else:
        # Ensure we don't send an empty section block to Slack (it will reject it)
        m2_1["text"] = "- <!channel> No technician is scheduled for today. Please assign someone for coverage"
    m2["text"] = m2_1
    message["blocks"].append(m2)
    message["blocks"].append(msep)

    # Check for "Watering Only", "Off", or no one scheduled
    next_week = today + datetime.timedelta(weeks=1)
    upcoming_shifts = [shift for shift in shifts if shift["date"] <= next_week]

    alerts = []
    # Today + next 4 days
    for day in range(start = 1, stop = 7):
        date_to_check = today + datetime.timedelta(days=day)
        shifts_on_date = [shift for shift in upcoming_shifts if shift["date"] == date_to_check and not is_off(shift)]

        if not shifts_on_date:
            alerts.append(
                f"- <!channel> No technician is scheduled for {date_to_check.strftime('%A, %B %d')}. Please assign someone for coverage."
            )
        else:
            for shift in shifts_on_date:
                if day == 1:
                    alerts.append(
                        f"- Tomorrow ({date_to_check.strftime('%A, %B %d')}): {shift['personnel']} is scheduled for '{shift['duties']}'."
                    )
                elif day == 2:
                    alerts.append(
                        f"- Overmorrow ({date_to_check.strftime('%A, %B %d')}): {shift['personnel']} is scheduled for '{shift['duties']}'."
                    )
                elif "Lab Cleanup" in shift["duties"]:
                    alerts.append(
                        f"- Experimenters, {date_to_check.strftime('%A, %B %d')} will have no training due to lab cleanup."
                    )
                if shift["duties"] in ["Watering Only", "Off"]:
                    alerts.append("Experimenters, please make arrangements if you need to train.")
        date_to_check = datetime.date.today() + datetime.timedelta(days=day)
        shifts_on_date = [shift for shift in upcoming_shifts if shift["date"] == date_to_check]

        if not shifts_on_date:
            alerts.append(f"No one is scheduled for {date_to_check.strftime('%A, %B %d')}.")
        else:
            for shift in shifts_on_date:
                if shift["duties"] in ["Watering Only"]:
                    alerts.append(
                        f"- {shift['personnel']} is scheduled for '{shift['duties']}' on {date_to_check.strftime('%A, %B %d')}."
                    )

    if alerts:
        alert_text = "\n".join(alerts)
        m3 = dict()
        m3["type"] = "section"
        m3_1 = dict()
        m3_1["type"] = "mrkdwn"
        m3_1["text"] = f"*:rotating_light: Upcoming Shifts: :rotating_light:*\n{alert_text}"
        m3["text"] = m3_1
        message["blocks"].append(m3)

    return message


def is_not_training(duties):
    return duties not in ["Watering Only", "Off"]


def is_off(shift):
    return shift["duties"] == "Off"


def main_technician_alert():
    import datajoint as dj

    dj.conn()
    lab = dj.create_virtual_module("lab", "u19_lab")
    webhooks_list = get_webhook_list(lab)

    nearby_date_schedule = tech_schedule()

    slack_json_message = slack_alert_message_format_tech_alert(nearby_date_schedule)

    # Send alert
    for this_webhook in webhooks_list:
        su.send_slack_notification(this_webhook, slack_json_message)
    pprint(slack_json_message)

    return slack_json_message


def generate_sample_data():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    sample_data = [
        {
            "date": yesterday,
            "personnel": "John Doe",
            "start": datetime.time(9, 0),
            "end": datetime.time(17, 0),
            "duties": "General Duties",
        },
        {
            "date": today,
            "personnel": "Jane Smith",
            "start": datetime.time(10, 0),
            "end": datetime.time(18, 0),
            "duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=1),
            "personnel": "Bob Johnson",
            "start": datetime.time(8, 0),
            "end": datetime.time(16, 0),
            "duties": "Watering Only",
        },
        {
            "date": today + datetime.timedelta(days=1),
            "personnel": "Alice Johnson",
            "start": datetime.time(8, 0),
            "end": datetime.time(16, 0),
            "duties": "Watering Only",
        },
        {
            "date": today + datetime.timedelta(days=2),
            "personnel": "Bob Brown",
            "start": datetime.time(9, 0),
            "end": datetime.time(17, 0),
            "duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=3),
            "personnel": "Charlie Davis",
            "start": datetime.time(10, 0),
            "end": datetime.time(18, 0),
            "duties": "Off",
        },
        {
            "date": today + datetime.timedelta(days=4),
            "personnel": "Eve Wilson",
            "start": datetime.time(9, 0),
            "end": datetime.time(17, 0),
            "duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=5),
            "personnel": "Frank White",
            "start": datetime.time(8, 0),
            "end": datetime.time(16, 0),
            "duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=6),
            "personnel": "Grace Black",
            "start": datetime.time(10, 0),
            "end": datetime.time(18, 0),
            "duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=7),
            "personnel": "Hank Green",
            "start": datetime.time(9, 0),
            "end": datetime.time(17, 0),
            "duties": "General Duties",
        },
    ]

    return sample_data


def test_slack_alert_message_format_tech_alert():
    sample_data = generate_sample_data()
    message = slack_alert_message_format_tech_alert(sample_data)
    pprint(message)


def main_loop():
    return main_technician_alert()


def fetch_and_parse_icalevents(weburl: str):
    """Fetch and filter iCal events for VR-related duties.

    Args:
        weburl (str): url to the iCal feed
        logger (Logger): Local Python logger

    Returns:
        list[dict]: _description_
    """

    start_date = datetime.date.today() - timedelta(weeks=2)
    end_date = datetime.date.today() + timedelta(weeks=2)
    try:
        vr_events = events(url=weburl, start=start_date, end=end_date)
    except Exception as e:
        print(f"Error fetching events from WhenIWork: {e}")
        return []

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
        unique_days = sorted(set(event.start.date() for event in lab_clean_up_events))
        for day in unique_days:
            filtered_events.append(
                {
                    "date": day,
                    "personnel": "Lab",
                    "duties": "Lab Cleanup (no training)",
                    "start": datetime.time(9, 0),
                    "end": datetime.time(17, 0),
                }
            )

    # Remove individual lab cleanup events from the filtered events
    vr_events = non_lab_clean_up_events

    for event in vr_events:
        summary = event.summary.lower()
        for pattern, event_type, color, duties in vr_types:
            if re.search(pattern, summary):
                person = event.summary.split("(")[0].strip()
                filtered_events.append(
                    {
                        "date": event.start.date(),
                        "start": event.start,
                        "end": event.end,
                        "title": person + " - " + duties,
                        "id": event.uid,
                        "type": event_type,
                        "color": color,
                        "personnel": person,
                        "duties": duties,
                    }
                )

    return filtered_events


if __name__ == "__main__":
    print("Command-line arguments:", sys.argv)
    if len(sys.argv) < 2:
        main_loop()
    else:
        if sys.argv[1].lower() == "test":
            test_slack_alert_message_format_tech_alert()
        else:
            raise ValueError(f"Unrecognized argument {sys.argv[1]} -- use 'test' to run in test mode.")
