import time

from scripts.conf_file_finding import try_find_conf_file

# The DataJoint config must be loaded before u19_pipeline modules are imported.
try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.schedule_rules.materialize_schedule_job as msj  # noqa: E402

msj.main_materialize_schedule()
