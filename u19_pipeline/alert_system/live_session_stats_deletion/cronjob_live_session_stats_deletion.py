
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.live_session_stats_deletion.live_session_stats_deletion as lssd

lssd.main_live_session_stats_deletion()

