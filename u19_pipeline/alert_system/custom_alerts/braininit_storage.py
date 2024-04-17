
import pandas as pd
import datajoint as dj

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

    conf = dj.config
    braininit_path = conf['custom']['root_data_dir'][0]

    #braininit_path = lab.Path().get_local_path2('braininit/Data').as_posix()
    command = "df "+ braininit_path + " | tail -1 | awk '{print $4}'"
    # a = os.popen(command).read()

    print(command)

    storage_left = subprocess.check_output(command, shell=True)

    print(storage_left)

    storage_left = storage_left.decode().strip()

    print(storage_left)

    storage_left_kb = int(storage_left)

    print(storage_left_kb)

    storage_left_tb = storage_left_kb/1024/1024/1024

    print(storage_left_tb)

    if storage_left_tb < 4:
        data = {'alert_message': ['Very little space left in braininit'], 'space(tb)': [str(storage_left_tb)]}
        alert_df = pd.DataFrame.from_dict(data)
    else:
        alert_df = pd.DataFrame([])

    return alert_df


