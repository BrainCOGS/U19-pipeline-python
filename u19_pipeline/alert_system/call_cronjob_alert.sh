
#!/bin/bash

echo $(pwd)

source /home/u19prod@pu.win.princeton.edu/.bashrc
source /home/u19prod@pu.win.princeton.edu/.bash_profile

conda activate U19-pipeline_python_env2
cd "/home/u19prod@pu.win.princeton.edu/Datajoint_projs/U19-pipeline_python/"
git pull
python ./u19_pipeline/alert_system/cronjob_alert.py
