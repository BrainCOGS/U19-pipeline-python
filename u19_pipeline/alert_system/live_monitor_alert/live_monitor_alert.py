import datajoint as dj
import numpy as np
import pandas as pd
import time
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

import u19_pipeline.utils.slack_utils as su

MINUTES_ALERT = 1
SECONDS_ALERT = MINUTES_ALERT*60
MIN_SESSIONS_COMPLETED = 3

slack_configuration_dictionary = {
    'slack_notification_channel': ['alvaro_luna']
}

'''
slack_configuration_dictionary = {
    'slack_notification_channel': ['rig_training_error_notification']
}
'''

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
    m1_1["text"] = ':rotating_light: *Live Monitor Alert* on ' + datestr + '\n' +\
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
    last_time_start = datetime.now(tz=ZoneInfo('America/New_York')) - timedelta(hours=2,minutes=30)
    last_time_start = last_time_start.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')

    query_started_recently = "session_start_time > '" + last_time_start + "'" 
    sessions = pd.DataFrame((acquisition.SessionStarted & query & query_started_recently).fetch('KEY','session_location','session_start_time',as_dict=True))
    #sessions = sessions.loc[~sessions['subject_fullname'].str.startswith('testuser'),:]

    print('sessions STARTED RECENTLY\n', sessions)

    if sessions.shape[0] > 0:

        #If more than one "not finished" session in same rig, grab the last one started
        sessions2 = sessions.groupby('session_location').agg({'session_start_time': [('session_start_time', 'max')]})
        sessions2.columns = sessions2.columns.droplevel()
        sessions2 = sessions2.reset_index()
        sessions = pd.merge(sessions, sessions2, on=['session_location', 'session_start_time'])
        sessions = sessions.drop(columns=['session_location'])
        sessions = sessions.reset_index(drop=True)

        print('Last session started on rig\n', sessions)

        #Only analyze sessions that have not been reported
        query_reported  = {} 
        query_reported['session_date'] = datetime.today().strftime('%Y-%m-%d')
        sessions_reported = pd.DataFrame((acquisition.ReportedLiveSessionStats  & query_reported).fetch('KEY', as_dict = True))

    if sessions_reported.shape[0] > 0:

        sessions = pd.merge(sessions,sessions_reported, how='left', indicator=True)
        sessions = sessions.loc[sessions['_merge'] == 'left_only']
        sessions = sessions.drop(columns='_merge')
        sessions = sessions.reset_index(drop=True)

    print('Sessions not reported\n', sessions)

    #Only analyze sessions subjects > NUM_SESSIONS_COMPLETED have not been reported
    if sessions.shape[0] > 0:

        query_subjects = "subject_fullname in ('"+ "', '".join(sessions['subject_fullname']) + "')"

        count_sessions_table = (subject.Subject).aggr((acquisition.Session & query_subjects), num_sessions="count(subject_fullname)")
        count_sessions_df = pd.DataFrame(count_sessions_table.fetch(as_dict=True))

        sessions = pd.merge(sessions,count_sessions_df, how='left')
        sessions['num_sessions'] = sessions['num_sessions'].fillna(0)

        sessions = sessions.loc[sessions['num_sessions']>= MIN_SESSIONS_COMPLETED, :]

    print('Subjects with  completed min_num_sessions\n', sessions)

    if sessions.shape[0] > 0:

        # Query last live stat from the started sessions
        query_live_stats = sessions.to_dict('records')
        
        #Last non violation trial in sessions
        query_no_violation_trial = dict()
        query_no_violation_trial['violation_trial'] = 0
        lss_nvio = acquisition.SessionStarted.aggr((acquisition.LiveSessionStats & query_no_violation_trial).proj('current_datetime'), current_datetime="max(current_datetime)")
        live_stats_nvio = pd.DataFrame((lss_nvio & query_live_stats).fetch(as_dict=True))
        live_stats_nvio = live_stats_nvio.rename({'current_datetime': 'last_non_violation_trial'}, axis=1)
        
        #Last violation trial in sessions
        query_violation_trial = dict()
        query_violation_trial['violation_trial'] = 1
        lss_vio = acquisition.SessionStarted.aggr((acquisition.LiveSessionStats & query_violation_trial).proj('current_datetime'), current_datetime="max(current_datetime)")
        live_stats_vio = pd.DataFrame((lss_vio & query_live_stats & query_violation_trial).fetch(as_dict=True))
        live_stats_vio = live_stats_vio.rename({'current_datetime': 'last_violation_trial'}, axis=1)


        # Merge last violation trial and last non violation Trials from sessions
        if live_stats_nvio.shape[0] > 0 and live_stats_vio.shape[0] > 0:
            live_stats = pd.merge(live_stats_nvio,live_stats_vio, how='outer')
        elif live_stats_nvio.shape[0] > 0:
            live_stats = live_stats_nvio.copy()
            live_stats['last_violation_trial'] = pd.NaT
        elif live_stats_vio.shape[0] > 0:
            live_stats = live_stats_vio.copy()
            live_stats['last_non_violation_trial'] = pd.NaT
        else:
            live_stats = pd.DataFrame()


        # If there are any sessions with live stats
        if live_stats.shape[0] > 0:

            live_stats = pd.merge(sessions,live_stats, how='inner')

            print('live_stats')
            print(live_stats)

            fake_date = pd.Timestamp('1900-01-01')

            # Filter sessions whose last trial info is greater than 300s
            right_now_est = datetime.now(tz=ZoneInfo('America/New_York'))
            right_now_est = right_now_est.replace(tzinfo=None)

            print('SECONDS_ALERT')
            print(SECONDS_ALERT)

            live_stats['seconds_elapsed_last_stat_nvio'] = (right_now_est- live_stats['last_non_violation_trial']).dt.total_seconds()
            live_stats['alert_nvio'] = live_stats['seconds_elapsed_last_stat_nvio'] > SECONDS_ALERT

            live_stats['seconds_elapsed_session_started'] = (right_now_est- live_stats['session_start_time']).dt.total_seconds()
            live_stats['alert_vio'] = live_stats['seconds_elapsed_session_started'] > SECONDS_ALERT

            print(live_stats.T)

            live_stats['alert_vio'] = live_stats['alert_vio'] & (pd.isna(live_stats['last_non_violation_trial']))
            
            print(live_stats.T)

            live_stats['alert_vio'] = live_stats['alert_vio'] & (~pd.isna(live_stats['last_violation_trial']))
             

            print("pd.isna(live_stats['last_non_violation_trial']")
            print(pd.isna(live_stats['last_non_violation_trial']))

            print("(~pd.isna(live_stats['last_violation_trial']))")
            print((~pd.isna(live_stats['last_violation_trial'])))

            print(live_stats.T)

            live_stats = live_stats.loc[(live_stats['alert_nvio']==True) | (live_stats['alert_vio']==True),:]

            print('live_stats filtered')
            print(live_stats)

            #If there are any sessions to alert (more then SECONDS_ALERT)
            if live_stats.shape[0] > 0:

                live_stats['current_datetime'] = fake_date
                live_stats['current_datetime'] = live_stats.loc[live_stats['alert_vio']==True, 'last_violation_trial']
                live_stats['current_datetime'] = live_stats.loc[live_stats['alert_nvio']==True, 'last_non_violation_trial']

                live_stats['seconds_elapsed_last_valid_stat'] = 0
                live_stats['seconds_elapsed_last_valid_stat'] = live_stats.loc[live_stats['alert_vio']==True, 'seconds_elapsed_session_started']
                live_stats['seconds_elapsed_last_valid_stat'] = live_stats.loc[live_stats['alert_nvio']==True, 'seconds_elapsed_last_stat_nvio']

                #get_session_info to alert (plus slack researcher)
                query_live_stats_sessions = live_stats[['subject_fullname', 'session_date', 'session_number']].to_dict('records')

                session_data_df = pd.DataFrame(((lab.User.proj('slack') * subject.Subject.proj('user_id') *\
                                            acquisition.SessionStarted.proj('session_location')) & query_live_stats_sessions).fetch(as_dict=True))
                
                session_data_df = session_data_df.rename({'slack': 'researcher'}, axis=1)
                session_data_df['researcher'] = '<@'+ session_data_df['researcher'] + '>'
                session_data_df = session_data_df[['researcher', 'subject_fullname', 'session_date', 'session_number']]

                #Query full live stat table
                #session_stats = live_stats.copy()
                #session_stats = session_stats.rename({'current_datetime': 'last_live_stat'}, axis=1)
                query_live_stats = live_stats[['subject_fullname', 'session_date', 'session_number', 'current_datetime']].to_dict('records')
                live_stats_mini = live_stats[['subject_fullname', 'session_date', 'session_number', 'seconds_elapsed_last_valid_stat']].copy()

                print('query_live_stats')
                print(query_live_stats)
                
                print('live_stats_mini')
                print(live_stats_mini)


                ls_full_df = pd.DataFrame((acquisition.LiveSessionStats & query_live_stats).fetch(as_dict=True))

                print('ls_full_df')
                print(ls_full_df)

                ls_full_df = pd.merge(ls_full_df, live_stats_mini, on=['subject_fullname', 'session_date', 'session_number'])
                ls_full_df = ls_full_df.drop(columns=['subject_fullname', 'session_date', 'session_number'])
                ls_full_df = ls_full_df.rename({'current_datetime': 'last_trial_time'}, axis=1)


                mid = ls_full_df['last_trial_time']
                ls_full_df = ls_full_df.drop(columns=['last_trial_time'])
                ls_full_df.insert(0, 'last_trial_time', mid)

                ls_full_dict = ls_full_df.to_dict('records')

                # Send one alert per session found
                idx_alert = 0
                for this_alert_record in ls_full_dict:

                    #Format message for session and live stat dictionary
                    this_session_stats = session_data_df.iloc[idx_alert,:]
                    slack_json_message = slack_alert_message_format_live_stats(this_session_stats.to_dict(), this_alert_record, int(this_alert_record['seconds_elapsed_last_valid_stat']/60))

                    #Send alert
                    webhooks_list = su.get_webhook_list(slack_configuration_dictionary, lab)
                    for this_webhook in webhooks_list:
                        su.send_slack_notification(this_webhook, slack_json_message)
                        time.sleep(1)

                    reported_session = this_session_stats[['subject_fullname', 'session_date', 'session_number']].copy()
                    reported_session['report_datetime'] = right_now_est

                    acquisition.ReportedLiveSessionStats.insert1(reported_session.to_dict())
                    idx_alert += 1
