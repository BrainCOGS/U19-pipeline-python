"""
Cronjob script for NWB export processing.

This script runs continuously and processes NWB export jobs through the pipeline.
It should be run as a background service or cron job.
"""

import time

from scripts.conf_file_finding import try_find_conf_file

# Find and load configuration file
try_find_conf_file()

time.sleep(1)

import u19_pipeline.automatic_job.nwb_export_handler as nwb_handler

print("Starting NWB export cronjob processor...")

# Main processing loop
while True:
    try:
        nwb_handler.NwbExportHandler.pipeline_handler_main()
    except Exception as e:
        print(f"Error in NWB export handler: {e}")
        import traceback

        traceback.print_exc()

    # Check every 5 seconds
    time.sleep(5)
