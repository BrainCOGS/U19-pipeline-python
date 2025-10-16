
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.schedule_check_alert.schedule_check_alert as sca

sca.main_schedule_check_alert()

