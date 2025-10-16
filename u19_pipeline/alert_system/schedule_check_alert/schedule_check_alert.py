import datetime
import time

import datajoint as dj
import pandas as pd

import u19_pipeline.utils.slack_utils as su
import u19_pipeline.lab as lab


# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['rigs_issues_and_troubleshooting'],
    "slack_users_channel": ["alvaros"]
}

def get_schedule_query():

    scheduler = dj.create_virtual_module("scheduler", "u19_scheduler")
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    today = (datetime.date.today()).strftime('%Y-%m-%d')
    schedule_query = 'date >= "' + today + '" and date <= "' + tomorrow + '"'
    schedule_df = pd.DataFrame((scheduler.Schedule & schedule_query).fetch('date', 'location', 'subject_fullname', as_dict=True))

    return schedule_df


def main_schedule_check_alert():

    alert = 0
    schedule_df = get_schedule_query()
    tomorrow_schedule = schedule_df.loc[schedule_df['date'] == (datetime.date.today() + datetime.timedelta(days=1)),:]

    if tomorrow_schedule.shape[0] == 0:
        alert = 1
        slack_json_message = slack_alert_empty_schedule()
    else:
        schedule_df = schedule_df.groupby(['date', 'location']).agg({'subject_fullname': [('#subj', 'count')]})
        schedule_df.columns = schedule_df.columns.droplevel()
        schedule_df = schedule_df.reset_index()

        todays_summary_df = schedule_df.loc[schedule_df['date'] == datetime.date.today(),
                                            ['location', '#subj']].copy()
        tomorrow_summary_df = schedule_df.loc[schedule_df['date'] == (datetime.date.today() + datetime.timedelta(days=1)),
                                               ['location', '#subj']].copy()
        
        schedule_comp = pd.merge(todays_summary_df, tomorrow_summary_df, on=['location'], suffixes=['_today', '_tomorrow'])
        schedule_comp

        schedule_comp['diff_subjects'] = schedule_comp['#subj_tomorrow'] - schedule_comp['#subj_today']
        schedule_comp['rig_less_subjects'] = schedule_comp['diff_subjects'] < -2

        subjects_today = schedule_comp['#subj_today'].sum()
        subjects_tomorrow = schedule_comp['#subj_tomorrow'].sum()
        total_rigs_less_subjects = schedule_comp['rig_less_subjects'].sum()

        if subjects_tomorrow/subjects_today < 0.7:
            alert = 1

        if total_rigs_less_subjects >= 3:
            alert = 1

        if alert == 1:
            schedule_comp['location'] = '*'+schedule_comp['location']+'*~'
            schedule_comp = schedule_comp.drop(['diff_subjects', 'rig_less_subjects'], axis=1)
            schedule_comp_df_string = su.format_df_for_slack_message(schedule_comp)
            slack_json_message = slack_alert_message_format_schedule(schedule_comp_df_string)


    if alert == 0:
        return
    else:
        webhooks_list = su.get_webhook_list(slack_configuration_dictionary, lab)
        # Send alert
        for this_webhook in webhooks_list:
            su.send_slack_notification(this_webhook, slack_json_message)
            time.sleep(1)


def get_webhook_list(lab):
    # Get webhook lists
    #slack_configuration_dictionary = {"slack_notification_channel": ["rigs_issues_and_troubleshooting"]}
    slack_configuration_dictionary = {"slack_users_channel": ["alvaro_luna"]}
    webhooks_list = []
    query_slack_webhooks = [{"webhook_name": x} for x in slack_configuration_dictionary["slack_notification_channel"]]
    webhooks_list += (lab.SlackWebhooks & query_slack_webhooks).fetch("webhook_url").tolist()
    return webhooks_list


def slack_alert_message_format_schedule(schedule_df_string):
    now = datetime.datetime.now()
    datestr = now.strftime("%d-%b-%Y %H:%M:%S")

    msep = dict()
    msep["type"] = "divider"

    # Title#
    m1 = dict()
    m1["type"] = "section"
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ":rotating_light: *Schedule Alert *"
    m1["text"] = m1_1

    # Info for subjects missing water
    m2 = dict()
    m2["type"] = "section"
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    m2_1["text"] = "Significantly less subjects scheduled tomorrow\nSchedule per rig:" + "\n\n"
    m2_1["text"] += schedule_df_string
    m2["text"] = m2_1

    message = dict()
    message["blocks"] = [m1, msep, m2, msep]
    message["text"] = "Suspicious Schedule Alert"

    return message

def slack_alert_empty_schedule():
    now = datetime.datetime.now()
    datestr = now.strftime("%d-%b-%Y %H:%M:%S")

    msep = dict()
    msep["type"] = "divider"

    # Title#
    m1 = dict()
    m1["type"] = "section"
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ":rotating_light: *Schedule Alert *"
    m1["text"] = m1_1

    # Info for subjects missing water
    m2 = dict()
    m2["type"] = "section"
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    m2_1["text"] = "*No Schedule found for tomorrow:*" + "\n"
    m2["text"] = m2_1

    message = dict()
    message["blocks"] = [m1, msep, m2, msep]
    message["text"] = "Schedule Empty Alert"

    return message