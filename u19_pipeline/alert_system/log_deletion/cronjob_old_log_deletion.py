
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.log_deletion.old_log_deletion as old

old.main_old_log_deletion()

