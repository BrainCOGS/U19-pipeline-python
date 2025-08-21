
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.water_weigh_alert.water_weigh_alert as wwa

wwa.main_water_weigh_alert()

