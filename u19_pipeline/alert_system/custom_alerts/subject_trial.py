
import datetime

import numpy as np

import u19_pipeline.alert_system.alert_system_utility as asu
import u19_pipeline.alert_system.behavior_metrics as bm

# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['custom_alerts']
}

zscore_alert = 2
def main():
    '''
    Main function for subject "num trials & bias" alert
    '''

    # Get sessions
    subject_session_df, _ = asu.get_acquisition_data_alert_system(type='subject_fullname')

    # Get zscores for num_trials
    subject_session_df = bm.BehaviorMetrics.get_zscore_metric_session_df(subject_session_df, 'num_trials', 'subject_fullname')

    # Filter df for todays alert
    today = datetime.date.today() - datetime.timedelta(days=1)
    subject_session_df = subject_session_df.loc[subject_session_df['session_date'] == today, :]
    subject_session_df['abs_z_score_num_trials'] = np.abs(subject_session_df['z_score_num_trials'])

    # Filter if today we got > 3 zscore of trials for a session
    alert_subjtect_trial_df = subject_session_df.loc[subject_session_df['abs_z_score_num_trials'] >= zscore_alert, :]

    columns_alert = ['subject_fullname', 'session_date', 'session_number', 'session_location', 'avg_num_trials', 'num_trials', 'z_score_num_trials']
    alert_subjtect_trial_df = alert_subjtect_trial_df[columns_alert]
    alert_subjtect_trial_df['alert_message'] = 'Session had abnormal number of trials'

    return alert_subjtect_trial_df


