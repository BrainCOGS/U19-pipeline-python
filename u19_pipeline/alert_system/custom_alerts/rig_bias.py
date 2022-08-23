
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
    _, rig_session_key_list = asu.get_acquisition_data_alert_system(type='session_location')

    # Get trials
    behavior = dj.create_virtual_module('behavior', dj.config['custom']['database.prefix']+'behavior')
    acquisition = dj.create_virtual_module('acquisition', dj.config['custom']['database.prefix']+'acquisition')
    rig_trial_df = pd.DataFrame((behavior.TowersBlock.Trial * acquisition.SessionStarted.proj('session_location') \
        & rig_session_key_list).fetch('KEY', 'trial_type', 'choice', 'session_location', as_dict=True))

    # Get zscores for bias
    bias_df = bm.BehaviorMetrics.get_bias_from_trial_df(rig_trial_df)
    bias_df = bm.BehaviorMetrics.get_zscore_metric_session_df(bias_df, 'bias', 'subject_fullname')

    # Filter df for today
    today = datetime.date.today() - datetime.timedelta(days=1)
    bias_df = bias_df.loc[bias_df['session_date'] == today, :]
    bias_df['abs_z_score_bias'] = np.abs(bias_df['z_score_bias'])
    bias_df = bias_df.reset_index(drop=True)

    # Filter if today we got > 3 zscore of trials for a session
    bias_df = bias_df.loc[bias_df['abs_z_score_bias'] >= zscore_alert, :]

    # Get sign of bias (only group subjects with bias to same side)
    bias_df['sign_bias'] = np.sign(bias_df['z_score_bias'])
    bias_df['sign_bias'] = bias_df['sign_bias'].astype(int)

    #Count how many subjects were biassed by rig
    bias_location = bias_df.groupby(['session_location', 'sign_bias']).agg({'session_location': [('num_subjects', 'size')],\
    'subject_fullname': [('subject_fullnames', lambda x: ','.join(x))]})
    bias_location.columns = bias_location.columns.droplevel()
    bias_location = bias_location.reset_index()

    #Filter if there were subjects biased to different sides
    bias_location2 = bias_location.groupby(['session_location']).agg({'session_location': [('num_bias_sides', 'size')]})
    bias_location2.columns = bias_location2.columns.droplevel()
    bias_location2 = bias_location2.reset_index()
    bias_location2 = bias_location2.loc[bias_location2['num_bias_sides'] == 1, :]
    bias_location = bias_location.merge(bias_location2, on='session_location')

    # Only alert if more than 1 subject was biased today
    bias_location = bias_location.loc[bias_location['num_subjects'] > 1, :]

    # Filter columns and set message
    columns_alert = ['session_location', 'sign_bias', 'num_subjects', 'subject_fullnames']
    bias_location = bias_location[columns_alert]
    bias_location['alert_message'] = 'Multiple subjects were biased in this rig'

    return bias_location


