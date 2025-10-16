import datetime
import pathlib
import time

import datajoint as dj
import u19_pipeline.lab as lab
import pandas as pd

import u19_pipeline.utils.slack_utils as su

slack_configuration_dictionary = {
    'slack_notification_channel': ['subject_health'],
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

    # Get today's responsibility based on schedule "Nothing/Nothing etc" field
    today_idx = (datetime.datetime.today().weekday() + 1) % 7
    subject_data["schedule_today"] = subject_data["schedule"].str.split("/")
    subject_data["schedule_today"] = subject_data["schedule_today"].apply(lambda x: x[today_idx])

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


def find_unreturned_subjects() -> pd.DataFrame:
    import datajoint as dj

    action = dj.create_virtual_module("action", "u19_action")
    today = datetime.date.today()

    # Obtain the last transport date
    local_transport_data: pd.DataFrame
    local_transport_data = dj.U("subject_fullname").aggr(
        action.Transport() & f'DATE(transport_out_datetime) = "{today}"',
        # transport_out_datetime="MAX(transport_out_datetime)",
        last_transport="MAX(transport_out_datetime)",
        transport_in_datetime="MAX(transport_in_datetime)",
    ).fetch(format='frame')

    unreturned_subjects: pd.DataFrame = local_transport_data[
    local_transport_data["transport_in_datetime"].isnull()
    ]


    return unreturned_subjects


def slack_alert_message_format_weight_water(subjects_not_watered, subjects_not_weighted, subjects_not_trained, missing_transport):
    now = datetime.datetime.now()
    datestr = now.strftime("%d-%b-%Y %H:%M:%S")

    msep = dict()
    msep["type"] = "divider"

    # Title#
    m1 = dict()
    m1["type"] = "section"
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ":rotating_light: *Subjects Status Alert *"
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
            m2_1["text"] += (
                "*"
                + subjects_not_watered.loc[i, "subject_fullname"]
                + "* : "
                + str(subjects_not_watered.loc[i, "current_need_water"])
                + " ml\n"
            )
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
            m4_1["text"] += "*" + subjects_not_weighted.loc[i, "subject_fullname"] + "*\n"
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
            m6_1["text"] += "*" + missing_transport.index[i] + "*\n"
    m6["text"] = m6_1

    message = dict()
    message["blocks"] = [m1, msep, m2, msep, m4, msep, m5, msep, m6, msep]
    message["text"] = "Subject Status Alert"

    return message


def main_water_weigh_alert():
    dj.conn()

    subject_data = get_subject_data()

    subject_data = subject_data.loc[~subject_data["subject_fullname"].str.contains("test"), :]
    subjects_not_watered = subject_data.loc[
        subject_data["current_need_water"] > 0, ["subject_fullname", "current_need_water"]
    ]
    subjects_not_watered = subjects_not_watered.reset_index(drop=True)
    subjects_not_watered["current_need_water"] = subjects_not_watered["current_need_water"].apply(lambda x: f"{x:.1f}")
    # subjects_not_watered = subjects_not_watered.head()

    subjects_not_weighted = subject_data.loc[subject_data["need_weight"], ["subject_fullname", "need_weight"]]
    subjects_not_weighted = subjects_not_weighted.reset_index(drop=True)
    # subjects_not_weighted = subjects_not_weighted.head()

    subjects_not_trained = subject_data.loc[subject_data["training_status"] == 1, ["subject_fullname", "scheduled_rig"]]
    subjects_not_trained = subjects_not_trained.reset_index(drop=True)
    # subjects_not_trained = subjects_not_trained.head()

    subject_not_returned = find_unreturned_subjects()

    slack_json_message = slack_alert_message_format_weight_water(
        subjects_not_watered, subjects_not_weighted, subjects_not_trained,
        missing_transport=subject_not_returned
    )

    webhooks_list = su.get_webhook_list(slack_configuration_dictionary, lab)

    # Send alert
    for this_webhook in webhooks_list:
        su.send_slack_notification(this_webhook, slack_json_message)
        time.sleep(1)
