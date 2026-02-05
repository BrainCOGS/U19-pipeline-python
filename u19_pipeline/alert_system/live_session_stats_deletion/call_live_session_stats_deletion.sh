
#!/bin/bash

echo $(pwd)

source /home/u19prod@pu.win.princeton.edu/.bashrc
source /home/u19prod@pu.win.princeton.edu/.bash_profile

cd "/home/u19prod@pu.win.princeton.edu/Datajoint_projs/U19-pipeline_python/"
source .venv/bin/activate
python ./u19_pipeline/alert_system/live_session_stats_deletion/cronjob_live_session_stats_deletion.py
