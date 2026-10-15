import argparse
import time
from scripts.conf_file_finding import try_find_conf_file

try_find_conf_file()

time.sleep(0.1)

import u19_pipeline.alert_system.water_weigh_alert.water_weigh_alert as wwa
from u19_pipeline.alert_system.water_weigh_alert import alert_tiers

parser = argparse.ArgumentParser()
parser.add_argument(
    "--tier",
    type=alert_tiers.AlertTier,
    choices=list(alert_tiers.AlertTier),
    default=None,
    help="Subjects to report. Defaults to 'early' before 8 PM ET, 'all' from 8 PM on.",
)
args = parser.parse_args()

wwa.main_water_weigh_alert(tier=args.tier)
