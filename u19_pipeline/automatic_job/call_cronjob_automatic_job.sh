#!/bin/bash

echo $(pwd)

# Sources u19prod's ~/.bashrc and ~/.bash_profile, activates conda (default)
# or checks the uv .venv (U19_CRON_ENV=uv), and cds into the checkout.
source "$(dirname "${BASH_SOURCE[0]}")/../cron_env.sh"
git pull --autostash
u19_cron_python ./u19_pipeline/automatic_job/cronjob_automatic_job.py
