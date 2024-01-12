
import time

from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()
time.sleep(1)

import datajoint as dj
import u19_pipeline.automatic_job.pupillometry_handler as ph

ph.PupillometryProcessingHandler.check_pupillometry_sessions_queue()
