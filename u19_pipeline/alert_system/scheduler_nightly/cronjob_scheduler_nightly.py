"""Run the nightly scheduler jobs by hand, e.g. ``--dry-run`` to compare with MATLAB.

The nightly run itself happens from ``alert_system/cronjob_alert.py``.
"""

import argparse
import datetime
import time

from scripts.conf_file_finding import try_find_conf_file

try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.scheduler_nightly.scheduler_nightly as sn

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--dry-run", action="store_true", help="report what would change without writing"
)
parser.add_argument(
    "--date",
    type=datetime.date.fromisoformat,
    help="run as if today were DATE (YYYY-MM-DD)",
)
args = parser.parse_args()

sn.main_reset_reweight_subjects(dry_run=args.dry_run)
sn.main_populate_schedule_for_tomorrow(today=args.date, dry_run=args.dry_run)
sn.main_populate_technician_schedule(today=args.date, dry_run=args.dry_run)
