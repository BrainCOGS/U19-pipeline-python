
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
    _, subject_session_key_list = asu.get_acquisition_data_alert_system(type='subject_fullname')

    # Get trials
    behavior = dj.create_virtual_module('behavior', dj.config['custom']['database.prefix']+'behavior')
    acquisition = dj.create_virtual_module('acquisition', dj.config['custom']['database.prefix']+'acquisition')

    subject_trial_df = pd.DataFrame((behavior.TowersBlock.Trial * acquisition.SessionStarted & subject_session_key_list).fetch('KEY', 'trial_type', 'choice', 'session_location', as_dict=True))

    # Get zscores for bias
    bias_df = bm.BehaviorMetrics.get_bias_from_trial_df(subject_trial_df)
    bias_df = bm.BehaviorMetrics.get_zscore_metric_session_df(bias_df, 'bias', 'subject_fullname')

    # Filter df for todays alert
    today = datetime.date.today() - datetime.timedelta(days=1)
    bias_df = bias_df.loc[bias_df['session_date'] == today, :]
    bias_df['abs_z_score_bias'] = np.abs(bias_df['z_score_bias'])

    # Filter if today we got > 3 zscore of trials for a session
    alert_bias_df = bias_df.loc[bias_df['abs_z_score_bias'] >= zscore_alert, :]

    columns_alert = ['subject_fullname', 'session_date', 'session_number', 'session_location', 'avg_bias', 'bias', 'z_score_bias']
    alert_bias_df = alert_bias_df[columns_alert]
    alert_bias_df['alert_message'] = 'Session had abnormal bias'

    return alert_bias_df


