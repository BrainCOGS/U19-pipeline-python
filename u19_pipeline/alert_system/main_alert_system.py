
import pandas as pd
import datajoint as dj
import pkgutil
import importlib

import u19_pipeline.alert_system.custom_alerts as ca

def main_alert_system():
    'Call main function of all alerts defined in "custom_alerts'

    all_alert_submodules = pkgutil.iter_modules(ca.__path__)

    for i in all_alert_submodules:

        my_alert_module = importlib.import_module('u19_pipeline.alert_system.custom_alerts.'+i.name)
        my_alert_module.main()

        print(my_alert_module.slack_configuration_dictionary)

        del my_alert_module