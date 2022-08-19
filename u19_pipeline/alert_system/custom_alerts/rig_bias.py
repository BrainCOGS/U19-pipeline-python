
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

    print('aqui rig_bias')
    # Get sessions
    _, rig_session_key_list = asu.get_acquisition_data_alert_system(type='session_location',\
         data_days=5, min_sessions=1)

    # Get trials
    behavior = dj.create_virtual_module('behavior', 'u19_behavior')
    rig_trial_df = pd.DataFrame((behavior.TowersBlock.Trial & rig_session_key_list).fetch('KEY', 'trial_type', 'choice', as_dict=True))

    # Get zscores for bias
    bias_df = bm.BehaviorMetrics.get_bias_from_trial_df(rig_trial_df)
    bias_df = bm.BehaviorMetrics.get_zscore_metric_session_df(bias_df, 'bias', 'session_location')

    # Filter df for todays alert
    today = datetime.date.today() - datetime.timedelta(days=1)
    bias_df.loc[bias_df['session_date'] == today, :]
    bias_df['abs_z_score_bias'] = np.abs(bias_df['z_score_bias'])

    # Filter if today we got > 3 zscore of trials for a session
    alert_bias_df = bias_df.loc[bias_df['abs_z_score_bias'] >= zscore_alert, :]

    return alert_bias_df


