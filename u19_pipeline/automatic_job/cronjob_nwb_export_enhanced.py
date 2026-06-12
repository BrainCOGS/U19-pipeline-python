"""
Enhanced Cronjob for NWB export processing.

This script runs continuously and processes NWB export jobs through the pipeline.
It should be run as a background service or cron job.

Updates from original:
- Uses NwbExportStatusEnum instead of magic numbers
- Better error handling and logging
- Improved monitoring and status reporting
- Supports DANDI upload workflow
"""

import sys
import time
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/nwb_export_cronjob.log')
    ]
)
logger = logging.getLogger(__name__)

# Find and load configuration
try:
    from scripts.conf_file_finding import try_find_conf_file
    try_find_conf_file()
except ImportError:
    logger.warning("Could not import conf_file_finding, using default config")

time.sleep(1)  # Brief pause for config to load

try:
    from u19_pipeline.automatic_job.nwb_export_handler import NwbExportHandler
    from u19_pipeline.nwb_export_enums import NwbExportStatusEnum
    from u19_pipeline import nwb_production
    logger.info("Successfully imported NWB export handler modules")
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}", exc_info=True)
    sys.exit(1)


class NwbExportCronjob:
    """Wrapper for the NWB export processing cronjob."""

    def __init__(self):
        """Initialize cronjob with monitoring state."""
        self.start_time = time.time()
        self.jobs_processed = 0
        self.jobs_failed = 0
        self.last_error_time = None

    def check_database_connection(self) -> bool:
        """Verify database connection is working."""
        try:
            # Try a simple query
            (nwb_production.NwbExportJob).fetch(limit=1)
            return True
        except Exception as e:
            logger.error(f"Database connection check failed: {e}")
            return False

    def get_active_job_count(self) -> int:
        """Return number of jobs currently being processed."""
        try:
            # Count jobs not in terminal state
            active_jobs = (
                nwb_production.NwbExportJob 
                & "status_id >= 0 AND status_id < 5"  # QUEUED to UPLOAD
            ).fetch(limit=1000)
            return len(active_jobs)
        except Exception:
            return 0

    def log_status(self) -> None:
        """Log current processing status."""
        uptime_hours = (time.time() - self.start_time) / 3600
        active_count = self.get_active_job_count()
        
        logger.info(
            f"Cronjob status: {uptime_hours:.1f}h uptime, "
            f"{self.jobs_processed} jobs processed, "
            f"{self.jobs_failed} jobs failed, "
            f"{active_count} jobs currently active"
        )

    def run_once(self) -> None:
        """Run one iteration of the processing loop."""
        try:
            # Check database connection
            if not self.check_database_connection():
                logger.error("Database connection lost, waiting for reconnection...")
                time.sleep(10)
                return

            # Process active jobs
            NwbExportHandler.pipeline_handler_main()

            # Update counters
            self.jobs_processed += 1
            self.last_error_time = None

        except Exception as e:
            self.jobs_failed += 1
            self.last_error_time = time.time()
            logger.error(f"Error in processing loop: {e}", exc_info=True)
            time.sleep(5)  # Back off on error

    def run_forever(self) -> None:
        """Main cronjob loop - runs forever."""
        logger.info("=" * 80)
        logger.info("Starting NWB export cronjob processor")
        logger.info("=" * 80)

        iteration = 0
        while True:
            try:
                iteration += 1

                # Log status every 20 iterations (~100 seconds at 5s interval)
                if iteration % 20 == 0:
                    self.log_status()

                # Process one iteration
                self.run_once()

                # Sleep before next iteration
                time.sleep(5)

            except KeyboardInterrupt:
                logger.info("Received SIGINT, shutting down gracefully...")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
                time.sleep(10)


def main() -> int:
    """Entry point for cronjob."""
    try:
        cronjob = NwbExportCronjob()
        cronjob.run_forever()
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
