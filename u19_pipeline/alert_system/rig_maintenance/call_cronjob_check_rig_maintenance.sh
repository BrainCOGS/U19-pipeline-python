#!/bin/bash

echo $(pwd)

# Sources u19prod's ~/.bashrc and ~/.bash_profile, activates conda (default)
# or checks the uv .venv (U19_CRON_ENV=uv), and cds into the checkout.
source "$(dirname "${BASH_SOURCE[0]}")/../../cron_env.sh"

# Run the checker via python module to use package imports
u19_cron_python -m u19_pipeline.alert_system.rig_maintenance.check_rig_maintenance
