#!/bin/bash

echo $(pwd)

source /home/u19prod@pu.win.princeton.edu/.bashrc
source /home/u19prod@pu.win.princeton.edu/.bash_profile

cd "/home/u19prod@pu.win.princeton.edu/Datajoint_projs/U19-pipeline_python/"
source .venv/bin/activate

# Run the checker via python module to use package imports
python -m u19_pipeline.alert_system.rig_maintenance.check_rig_maintenance