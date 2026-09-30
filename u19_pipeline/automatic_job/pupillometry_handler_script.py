
import time

from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

# pandas 3: stop on chained assignment instead of silently skipping the write
from u19_pipeline.utils.pandas_guard import raise_on_chained_assignment
raise_on_chained_assignment()
time.sleep(1)

import datajoint as dj
import u19_pipeline.automatic_job.pupillometry_handler as ph

ph.PupillometryProcessingHandler.check_pupillometry_sessions_queue()
