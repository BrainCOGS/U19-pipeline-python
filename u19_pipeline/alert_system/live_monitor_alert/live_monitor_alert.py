import datajoint as dj
import pandas as pd
import time
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

import u19_pipeline.utils.slack_utils as su

MINUTES_ALERT = 3

def get_webhook_list(lab):
    #Get webhook lists
    slack_configuration_dictionary = {
        'slack_notification_channel': ['rig_training_error_notification']
    }
    webhooks_list = []
    query_slack_webhooks = [{'webhook_name' : x} for x in slack_configuration_dictionary['slack_notification_channel']]
    webhooks_list += (lab.SlackWebhooks & query_slack_webhooks).fetch('webhook_url').tolist()
    return webhooks_list

def slack_alert_message_format_live_stats(alert_dictionary1, alert_dictionary2, time_no_response):
    'Format dictionaries for live monitor slack alert json'

    now = datetime.now()
    datestr = now.strftime('%d-%b-%Y %H:%M:%S')

    msep = dict()
    msep['type'] = "divider"

    #Title#
    m1 = dict()
    m1['type'] = 'section'
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ':rotating_light: * Live Monitor Alert* on ' + datestr + '\n' +\
    'More than ' + str(time_no_response) + ' min without new trial' + '\n'
    m1['text'] = m1_1

    #Info#
    m2 = dict()
    m2['type'] = 'section'
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    m2_1["text"] = '*Session Reported:*' + '\n'
    for key in alert_dictionary1.keys():
        m2_1["text"] += '*' + key + '* : ' + str(alert_dictionary1[key]) + '\n'
    m2_1["text"] += '\n'
    m2['text'] = m2_1

    m4 = dict()
    m4['type'] = 'section'
    m4_1 = dict()
    m4_1["type"] = "mrkdwn"

    m4_1["text"] = '*Last Stats Reported*:' + '\n'
    for key in alert_dictionary2.keys():
        m4_1["text"] += '*' + key + '* : ' + str(alert_dictionary2[key]) + '\n'
    m4_1["text"] += '\n'
    m4['text'] = m4_1

    message = dict()
    message['blocks'] = [m1,msep,m2,msep,m4,msep]
    message['text'] = 'Live Monitor Alert'

    return message


def main_live_monitor_alert():

    # Connect to DB and create virtual modules
    dj.conn()
    acquisition = dj.create_virtual_module('acquisition', 'u19_acquisition')
    lab = dj.create_virtual_module('lab', 'u19_lab')
    subject = dj.create_virtual_module('subject', 'u19_subject')

    # Query today's started sessions that are not finished
    query = {}
    query['session_date'] = datetime.today().strftime('%Y-%m-%d')
    query['is_finished'] = 0

    #Only look for sessions started in the last 1:30
    last_time_start = datetime.now(tz=ZoneInfo('America/New_York')) - timedelta(hours=1,minutes=30)
    last_time_start = last_time_start.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')

    query_started_recently = "session_start_time > '" + last_time_start + "'" 
    sessions = pd.DataFrame((acquisition.SessionStarted & query & query_started_recently).fetch('KEY','session_location','session_start_time',as_dict=True))
    sessions = sessions.loc[~sessions['subject_fullname'].str.startswith('testuser'),:]

    if sessions.shape[0] > 0:

        #If more than one "not finished" session in same rig, grab the last one started
        sessions2 = sessions.groupby('session_location').agg({'session_start_time': [('session_start_time', 'max')]})
        sessions2.columns = sessions2.columns.droplevel()
        sessions2 = sessions2.reset_index()
        sessions = pd.merge(sessions, sessions2, on=['session_location', 'session_start_time'])
        sessions = sessions.drop(columns=['session_location', 'session_start_time'])
        sessions = sessions.reset_index(drop=True)

        #Only analyze sessions that have not been reported
        query_reported  = {} 
        query_reported['session_date'] = datetime.today().strftime('%Y-%m-%d')
        sessions_reported = pd.DataFrame((acquisition.ReportedLiveSessionStats  & query_reported).fetch('KEY', as_dict = True))

        if sessions_reported.shape[0] > 0:

            sessions = pd.merge(sessions,sessions_reported, how='left', indicator=True)
            sessions = sessions.loc[sessions['_merge'] == 'left_only']
            sessions = sessions.drop(columns='_merge')
            sessions = sessions.reset_index(drop=True)

        # print('Sessions not reported\n', sessions)

    if sessions.shape[0] > 0:

        # Query last live stat from the started sessions
        query_live_stats = sessions.to_dict('records')
        lss = acquisition.SessionStarted.aggr(acquisition.LiveSessionStats.proj('current_datetime'), current_datetime="max(current_datetime)")
        live_stats = pd.DataFrame((lss & query_live_stats).fetch(as_dict=True))

        # If there are any sessions with live stats
        if live_stats.shape[0] > 0:

            # Filter sessions whose last trial info is greater than 300s
            right_now_est = datetime.now(tz=ZoneInfo('America/New_York'))
            right_now_est = right_now_est.replace(tzinfo=None)
            live_stats['minutes_elapsed_last_stat'] = (right_now_est- live_stats['current_datetime']).dt.total_seconds()/60
            live_stats['alert'] = live_stats['minutes_elapsed_last_stat'] > MINUTES_ALERT    
            live_stats = live_stats.loc[live_stats['alert']==True,:]


            #If there are any sessions to alert (more then 300s)
            if live_stats.shape[0] > 0:

                #get_session_info to alert (plus slack researcher)
                query_live_stats_sessions = live_stats[['subject_fullname', 'session_date', 'session_number']].to_dict('records')

                session_data_df = pd.DataFrame(((lab.User.proj('slack') * subject.Subject.proj('user_id') *\
                                            acquisition.SessionStarted.proj('session_location')) & query_live_stats_sessions).fetch(as_dict=True))
                
                session_data_df = session_data_df.rename({'slack': 'researcher'}, axis=1)
                session_data_df['researcher'] = '<@'+ session_data_df['researcher'] + '>'
                session_data_df = session_data_df[['researcher', 'subject_fullname', 'session_date', 'session_number']]

                #Query full live stat table
                session_stats = live_stats.copy()
                session_stats = session_stats.rename({'current_datetime': 'last_live_stat'}, axis=1)
                query_live_stats = live_stats[['subject_fullname', 'session_date', 'session_number', 'current_datetime']].to_dict('records')
                live_stats = live_stats.drop(columns=['current_datetime', 'alert'])
                ls_full_df = pd.DataFrame((acquisition.LiveSessionStats & query_live_stats).fetch('KEY', 'current_datetime', 'level',\
                            'sublevel', 'performance', 'bias', 'mean_duration_trial', 'median_duration_trial', as_dict=True))
                ls_full_df = pd.merge(ls_full_df, live_stats, on=['subject_fullname', 'session_date', 'session_number'])
                ls_full_df = ls_full_df.drop(columns=['subject_fullname', 'session_date', 'session_number', 'block'])
                ls_full_df = ls_full_df.rename({'current_datetime': 'last_stat_time'}, axis=1)
                ls_full_df['minutes_elapsed_last_stat'] = ls_full_df['minutes_elapsed_last_stat'].astype(int)

                mid = ls_full_df['minutes_elapsed_last_stat']
                ls_full_df = ls_full_df.drop(columns=['minutes_elapsed_last_stat'])
                ls_full_df.insert(0, 'minutes_elapsed_last_stat', mid)

                mid = ls_full_df['last_stat_time']
                ls_full_df = ls_full_df.drop(columns=['last_stat_time'])
                ls_full_df.insert(0, 'last_stat_time', mid)

                double_cols = ls_full_df.select_dtypes(include=['float64']).columns
                ls_full_df[double_cols] = ls_full_df[double_cols].applymap('{:.2f}'.format)

                ls_full_dict = ls_full_df.to_dict('records')

                #print(ls_full_df)

                webhooks_list = get_webhook_list(lab)

                # Send one alert per session found
                idx_alert = 0
                for this_alert_record in ls_full_dict:

                    #Format message for session and live stat dictionary
                    this_session_stats = session_data_df.iloc[idx_alert,:]
                    slack_json_message = slack_alert_message_format_live_stats(this_session_stats.to_dict(), this_alert_record, this_alert_record['minutes_elapsed_last_stat'])

                    #Send alert
                    for this_webhook in webhooks_list:
                        #print(this_webhook)
                        su.send_slack_notification(this_webhook, slack_json_message)
                        time.sleep(0.5)

                    reported_session = this_session_stats[['subject_fullname', 'session_date', 'session_number']].copy()
                    reported_session['report_datetime'] = right_now_est

                    acquisition.ReportedLiveSessionStats.insert1(reported_session.to_dict())
                    idx_alert += 1
