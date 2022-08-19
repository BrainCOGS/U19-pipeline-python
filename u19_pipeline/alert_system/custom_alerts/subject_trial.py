
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

zscore_alert = 1
def main():
    '''
    Main function for subject "num trials & bias" alert
    '''

    print('aqui subject trial')
    # Get sessions
    subject_session_df, _ = asu.get_acquisition_data_alert_system(type='subject_fullname',\
         data_days=5, min_sessions=1)

    # Get zscores for num_trials
    subject_session_df = bm.BehaviorMetrics.get_zscore_metric_session_df(subject_session_df, 'num_trials', 'subject_fullname')

    # Filter df for todays alert
    today = datetime.date.today() - datetime.timedelta(days=1)
    subject_session_df.loc[subject_session_df['session_date'] == today, :]
    subject_session_df['abs_z_score_num_trials'] = np.abs(subject_session_df['z_score_num_trials'])

    # Filter if today we got > 3 zscore of trials for a session
    alert_subjtect_trial_df = subject_session_df.loc[subject_session_df['abs_z_score_num_trials'] >= zscore_alert, :]


    return alert_subjtect_trial_df


