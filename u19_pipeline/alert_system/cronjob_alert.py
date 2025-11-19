
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(1)

import datajoint as dj
import u19_pipeline.alert_system.main_alert_system as mas
import u19_pipeline.alert_system.log_deletion.old_log_deletion as old
import u19_pipeline.alert_system.live_session_stats_deletion.live_session_stats_deletion as lssd


mas.main_alert_system()
old.main_old_log_deletion()
lssd.main_live_session_stats_deletion()

