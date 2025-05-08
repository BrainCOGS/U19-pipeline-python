
#!/bin/bash

echo $(pwd)

source /home/u19prod@pu.win.princeton.edu/.bashrc
source /home/u19prod@pu.win.princeton.edu/.bash_profile

conda activate U19-pipeline_python_env3
cd "/home/u19prod@pu.win.princeton.edu/Datajoint_projs/U19-pipeline_python/"
python ./u19_pipeline/alert_system/live_monitor_alert/cronjob_live_monitor_alert.py
