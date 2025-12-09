import datetime
import time

import datajoint as dj
import pandas as pd

import u19_pipeline.utils.slack_utils as su
import u19_pipeline.lab as lab

# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['alvaro_luna', 'christian_tabedzki']
}


def main_locked_tables_alert():

    locked_tables_query = 'show open tables where in_use > 0' 
    conn = dj.conn()
    locked_tables_df = pd.DataFrame(conn.query(locked_tables_query, as_dict=True).fetchall())

    if locked_tables_df.shape[0] == 0:
        return
    else:
        locked_tables_df = locked_tables_df.head()
        locked_tables_df = locked_tables_df.drop('Name_locked',axis=1)
        locked_tables_df = su.format_df_for_slack_message(locked_tables_df)
        slack_json_message = slack_alert_message_format_locked_tables(locked_tables_df)
    
        webhooks_list = su.get_webhook_list(slack_configuration_dictionary, lab)
        # Send alert
        for this_webhook in webhooks_list:
            su.send_slack_notification(this_webhook, slack_json_message)
            time.sleep(1)


def slack_alert_message_format_locked_tables(locked_tables_df):
    now = datetime.datetime.now()
    datestr = now.strftime("%d-%b-%Y %H:%M:%S")

    msep = dict()
    msep["type"] = "divider"

    # Title#
    m1 = dict()
    m1["type"] = "section"
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ":rotating_light: *Locked Tables Alert *"
    m1["text"] = m1_1

    # Info for subjects missing water
    m2 = dict()
    m2["type"] = "section"
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    m2_1["text"] = "*Locked tables:*" + "\n"
    m2_1["text"] += locked_tables_df
    m2["text"] = m2_1

    message = dict()
    message["blocks"] = [m1, msep, m2, msep]
    message["text"] = "Locked Tables Alert"

    return message
