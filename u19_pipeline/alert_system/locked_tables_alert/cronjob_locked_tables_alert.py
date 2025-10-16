
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.locked_tables_alert.locked_tables_alert as lta

lta.main_locked_tables_alert()

