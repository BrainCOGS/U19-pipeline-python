
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.live_monitor_alert.live_monitor_alert as lma

lma.main_live_monitor_alert()

