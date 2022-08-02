
#!/bin/bash

conda activate U19-pipeline_python_env
cd ~/Datajoint_Projs/U19-pipeline_python/
git pull
python u19_pipeline/cronjob_script.py