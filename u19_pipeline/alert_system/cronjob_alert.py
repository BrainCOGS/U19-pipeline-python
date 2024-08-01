
import time

from scripts.conf_file_finding import try_find_conf_file

try_find_conf_file()

time.sleep(1)

import u19_pipeline.alert_system.main_alert_system as mas

mas.main_alert_system()

