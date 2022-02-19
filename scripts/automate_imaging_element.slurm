#!/bin/bash
#SBATCH --job-name=test_sessid_x
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1
#SBATCH --time=01:00:00
#SBATCH --mail-user=<email>
#SBATCH --mail-type=end
#SBATCH --mail-type=fail
#SBATCH --error=logs/JobName.%J.err
#SBATCH --output=logs/JobName.%J.log

# sbatch --export=recording_id=13 --mem=24G ~/U19-pipeline_python/scripts/automate_imaging_element.slurm

echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
echo "recording_id: ${recording_id}"

module load anacondapy/2021.11
conda activate U19-pipeline_python
python ~/U19-pipeline_python/scripts/automate_imaging_element.py 