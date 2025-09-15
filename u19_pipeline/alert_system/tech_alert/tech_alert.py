import datetime
import sys

# import datajoint as dj
from pprint import pprint

from u19_pipeline.utils import slack_utils as su


def tech_schedule():
    import datajoint as dj

    scheduler = dj.create_virtual_module("scheduler", "u19_scheduler")
    lab = dj.create_virtual_module("lab", "u19_lab")

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    next_week = today + datetime.timedelta(weeks=1)

    schedule_query = scheduler.TechSchedule * lab.User() & f'date BETWEEN "{yesterday}" AND "{next_week}"'
    schedule_data = schedule_query.fetch(as_dict=True)
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

    # Info#
    m2 = dict()
    m2["type"] = "section"
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    m2_1["text"] = "\n".join(
        f"*{shift['full_name']}* is expected to work today between "
        f"{shift['start_time'].strftime('%I:%M %p')} and {shift['end_time'].strftime('%I:%M %p')} "
        f"(actual times may vary). Duties: {shift['tech_duties']}"
        for shift in shifts_today
    )

    m2["text"] = m2_1

    message = dict()
    message["blocks"] = [m1, msep, m2, msep]
    message["text"] = "🗓 *Today's Tech Schedule:*"

    # Check for "Watering Only", "Off", or no one scheduled
    next_week = datetime.date.today() + datetime.timedelta(weeks=1)
    upcoming_shifts = [shift for shift in shifts if shift["date"] <= next_week]

    alerts = []
    # Today + next 7 days
    for day in range(8):
        date_to_check = datetime.date.today() + datetime.timedelta(days=day)
        shifts_on_date = [shift for shift in upcoming_shifts if shift["date"] == date_to_check and not is_off(shift)]

        if not shifts_on_date:
            alerts.append(
                f"\n<!channel> No technician is scheduled for {date_to_check.strftime('%A, %B %d')}. Please assign someone for coverage."
            )
        else:
            for shift in shifts_on_date:
                if day == 1:
                    alerts.append(
                        f"\nTomorrow ({date_to_check.strftime('%A, %B %d')}): {shift['full_name']} is scheduled for '{shift['tech_duties']}'."
                    )
                elif day == 2:
                    alerts.append(
                        f"\nOvermorrow ({date_to_check.strftime('%A, %B %d')}): {shift['full_name']} is scheduled for '{shift['tech_duties']}'."
                    )
                if shift["tech_duties"] in ["Watering Only", "Off"]:
                    alerts.append("Experimenters, please make arrangements if you need to train.")
        date_to_check = datetime.date.today() + datetime.timedelta(days=day)
        shifts_on_date = [shift for shift in upcoming_shifts if shift["date"] == date_to_check]

        if not shifts_on_date:
            alerts.append(f"No one is scheduled for {date_to_check.strftime('%A, %B %d')}.")
        else:
            for shift in shifts_on_date:
                if shift["tech_duties"] in ["Watering Only", "Off"]:
                    alerts.append(
                        f"{shift['full_name']} is scheduled for '{shift['tech_duties']}' on {date_to_check.strftime('%A, %B %d')}."
                    )

    if alerts:
        alert_text = "\n".join(alerts)
        m3 = dict()
        m3["type"] = "section"
        m3_1 = dict()
        m3_1["type"] = "mrkdwn"
        m3_1["text"] = f"*Upcoming Alerts:*\n{alert_text}"
        m3["text"] = m3_1
        message["blocks"].append(m3)

    return message


def is_not_training(tech_duties):
    return tech_duties not in ["Watering Only", "Off"]


def is_off(shift):
    return shift["tech_duties"] == "Off"


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

    return slack_json_message


def generate_sample_data():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    sample_data = [
        {
            "date": yesterday,
            "full_name": "John Doe",
            "start_time": datetime.time(9, 0),
            "end_time": datetime.time(17, 0),
            "tech_duties": "General Duties",
        },
        {
            "date": today,
            "full_name": "Jane Smith",
            "start_time": datetime.time(10, 0),
            "end_time": datetime.time(18, 0),
            "tech_duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=1),
            "full_name": "Bob Johnson",
            "start_time": datetime.time(8, 0),
            "end_time": datetime.time(16, 0),
            "tech_duties": "Watering Only",
        },
        {
            "date": today + datetime.timedelta(days=1),
            "full_name": "Alice Johnson",
            "start_time": datetime.time(8, 0),
            "end_time": datetime.time(16, 0),
            "tech_duties": "Watering Only",
        },
        {
            "date": today + datetime.timedelta(days=2),
            "full_name": "Bob Brown",
            "start_time": datetime.time(9, 0),
            "end_time": datetime.time(17, 0),
            "tech_duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=3),
            "full_name": "Charlie Davis",
            "start_time": datetime.time(10, 0),
            "end_time": datetime.time(18, 0),
            "tech_duties": "Off",
        },
        {
            "date": today + datetime.timedelta(days=4),
            "full_name": "Eve Wilson",
            "start_time": datetime.time(9, 0),
            "end_time": datetime.time(17, 0),
            "tech_duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=5),
            "full_name": "Frank White",
            "start_time": datetime.time(8, 0),
            "end_time": datetime.time(16, 0),
            "tech_duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=6),
            "full_name": "Grace Black",
            "start_time": datetime.time(10, 0),
            "end_time": datetime.time(18, 0),
            "tech_duties": "General Duties",
        },
        {
            "date": today + datetime.timedelta(days=7),
            "full_name": "Hank Green",
            "start_time": datetime.time(9, 0),
            "end_time": datetime.time(17, 0),
            "tech_duties": "General Duties",
        },
    ]

    return sample_data


def test_slack_alert_message_format_tech_alert():
    sample_data = generate_sample_data()
    message = slack_alert_message_format_tech_alert(sample_data)
    pprint(message)


def main_loop():
    from scripts.conf_file_finding import try_find_conf_file

    try_find_conf_file()


if __name__ == "__main__":
    print("Command-line arguments:", sys.argv)
    if len(sys.argv[1]) < 2:
        main_loop()
    else:
        if sys.argv[1].lower() == "test":
            test_slack_alert_message_format_tech_alert()
        else:
            raise ValueError(f"Unrecognized argument {sys.argv[1]} -- use 'test' to run in test mode.")
