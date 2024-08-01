



# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['custom_alerts'],
    'slack_users_channel': ['alvaros']
}

def main():
    '''
    Main function for subject "new_customalert" alert
    This function should return a pandas DataFrame where each row will be a slack alert message on configured channels.
    You can use datajoint to get data for the alert (e.g. custom_alerts/rig_bias.py) or simply call os scripts (e.g. custom_alerts/braininit_storage.py)
    All columns of the dataframe will be included in the alert. (Don't add too many !!)
    You can check examples of some alers in the u19_pipeline/alert_system/custom_alerts directory
    '''



