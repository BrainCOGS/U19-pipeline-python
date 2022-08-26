
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(1)

import u19_pipeline.automatic_job.recording_handler as rec_handler
import u19_pipeline.automatic_job.recording_process_handler as rec_process_handler

# Check recordings and then jobs
rec_handler.RecordingHandler.pipeline_handler_main()
time.sleep(5)
rec_process_handler.RecProcessHandler.pipeline_handler_main()
time.sleep(5)
rec_process_handler.RecProcessHandler.pipeline_handler_main()

time.sleep(5)
#Check if we need to delete empty directories
rec_process_handler.RecProcessHandler.check_job_process_deletion()


