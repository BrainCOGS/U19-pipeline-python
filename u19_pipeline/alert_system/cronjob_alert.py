
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(1)

import datajoint as dj
import u19_pipeline.alert_system.main_alert_system as mas
import u19_pipeline.alert_system.alert_system_utility as asu
import u19_pipeline.alert_system.log_deletion.old_log_deletion as old
import u19_pipeline.alert_system.live_session_stats_deletion.live_session_stats_deletion as lssd
import u19_pipeline.alert_system.noDB_backup_creation.noDB_backup_creation_script as noDBbcs
import u19_pipeline.alert_system.custom_alerts.disk_space_alert as dsa
import u19_pipeline.lab as lab
import u19_pipeline.utils.slack_utils as su

# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['dev_notifications']
}


#mas.main_alert_system()
asu.run_cronjob_alert_job('old_log_deletion', old.main_old_log_deletion, lab, su, slack_configuration_dictionary)
asu.run_cronjob_alert_job('live_session_stats_deletion', lssd.main_live_session_stats_deletion, lab, su, slack_configuration_dictionary)
asu.run_cronjob_alert_job('noDB_backup', noDBbcs.main_noDB_backup, lab, su, slack_configuration_dictionary)
asu.run_cronjob_alert_job('disk_space_alert', dsa.main_disk_space_alert, lab, su, slack_configuration_dictionary)
