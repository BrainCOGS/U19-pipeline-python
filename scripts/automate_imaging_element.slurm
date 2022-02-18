#!/bin/bash
#SBATCH --job-name=test_sessid_x
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1
#SBATCH --time=00:01:00
#SBATCH --mem=1G
#SBATCH --mail-user=<email>
#SBATCH --mail-type=end
#SBATCH --mail-type=fail
#SBATCH --error=logs/JobName.%J.err
#SBATCH --output=logs/JobName.%J.log

# sbatch --export=sessid=29 ~/U19-pipeline_python/scripts/imaging_element.slurm

echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
echo "SESSID: ${sessid}"

module load anacondapy/2021.11
conda activate U19-pipeline_python
cd ~/U19-pipeline_python
python <script.py>