
import pandas as pd
import datajoint as dj
import datetime
import numpy as np

import u19_pipeline.alert_system.behavior_metrics as bm
import u19_pipeline.alert_system.alert_system_utility as asu


# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['custom_alerts'],
    'slack_users_channel': ['alvaros']
}

zscore_alert = 2
def main():
    '''
    Main function for subject "num trials & bias" alert
    '''
    # Get sessions
    subject_rig_df, _ = asu.get_acquisition_data_alert_system(type='session_location')

    # Get zscores for each session (group by rig)
    subject_rig_df = bm.BehaviorMetrics.get_zscore_metric_session_df(subject_rig_df, 'num_trials', 'session_location')
    
    # Get only todays data 
    today = datetime.date.today() - datetime.timedelta(days=1)
    subject_rig_df = subject_rig_df.loc[subject_rig_df['session_date'] == today, :]

    # Filter if today we got > 3 zscore of trials for a session
    subject_rig_df['abs_z_score_num_trials'] = np.abs(subject_rig_df['z_score_num_trials'])
    num_trials_alert_df = subject_rig_df.loc[subject_rig_df['abs_z_score_num_trials'] >= zscore_alert, :].copy(deep=True)

    # Get sign of zscpre (only group subjects with same deviation)
    num_trials_alert_df['sign_zscore'] = np.sign(num_trials_alert_df['z_score_num_trials'])
    num_trials_alert_df['sign_zscore'] = num_trials_alert_df['sign_zscore'].astype(int)

    #Count how many subjects were biassed by rig
    num_trials_alert_location_df = num_trials_alert_df.groupby(['session_location', 'sign_zscore']).agg({'session_location': [('num_subjects', 'size')],\
    'subject_fullname': [('subject_fullnames', lambda x: ','.join(x))]})
    num_trials_alert_location_df.columns = num_trials_alert_location_df.columns.droplevel()
    num_trials_alert_location_df = num_trials_alert_location_df.reset_index()

    #Filter if there were subjects with a lot of trials and very little trials
    num_trials_alert_location_df2 = num_trials_alert_location_df.groupby(['session_location']).agg({'session_location': [('num_bias_sides', 'size')]})
    num_trials_alert_location_df2.columns = num_trials_alert_location_df2.columns.droplevel()
    num_trials_alert_location_df2 = num_trials_alert_location_df2.reset_index()
    num_trials_alert_location_df2 = num_trials_alert_location_df2.loc[num_trials_alert_location_df2['num_bias_sides'] == 1, :]
    num_trials_alert_location_df = num_trials_alert_location_df.merge(num_trials_alert_location_df2, on='session_location')

    # Only alert if more than 1 subject was biased today
    num_trials_alert_location_df = num_trials_alert_location_df.loc[num_trials_alert_location_df['num_subjects'] > 1, :]

    # Filter columns and set message
    columns_alert = ['session_location', 'sign_zscore', 'num_subjects', 'subject_fullnames']
    num_trials_alert_location_df = num_trials_alert_location_df[columns_alert]
    num_trials_alert_location_df['alert_message'] = 'Multiple subjects had abnormal number of trials in this rig'

    return num_trials_alert_location_df


