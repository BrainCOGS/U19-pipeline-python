
import pandas as pd

import u19_pipeline.lab as lab

import u19_pipeline.alert_system.behavior_metrics as bm
import u19_pipeline.alert_system.alert_system_utility as asu
import os
import subprocess

# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['custom_alerts'],
    'slack_users_channel': []
}

def main():

    braininit_path = lab.Path().get_local_path2('braininit/Data').as_posix()
    command = "df "+ braininit_path + " | tail -1 | awk '{print $4}'"
    # a = os.popen(command).read()

    storage_left = subprocess.check_output(command, shell=True)
    storage_left = storage_left.decode().strip()
    storage_left_kb = int(storage_left)

    print(storage_left_kb)

    storage_left_tb = storage_left_kb/1024/1024/1024

    print(storage_left_tb)

    if storage_left_tb < 16:
        data = {'alert_message': ['Very little space left in braininit'], 'space(tb)': [storage_left/1000]}
        alert_df = pd.DataFrame.from_dict(data)
    else:
        alert_df = pd.DataFrame([])

    return alert_df


