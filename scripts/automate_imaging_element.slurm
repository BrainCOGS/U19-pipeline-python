#!/bin/bash
#SBATCH --job-name=imaging
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1
#SBATCH --time=05:00:00
#SBATCH --mail-type=end
#SBATCH --mail-type=fail
#SBATCH --error=logs/JobName.%J.err
#SBATCH --output=logs/JobName.%J.log

# sbatch --export=recording_id=13,process_method='suite2p',paramset_idx=0 --mem=24G ./scripts/automate_imaging_element.slurm

# sbatch --export=recording_id=13,process_method='caiman',paramset_idx=1 --mem=24G ./scripts/automate_imaging_element.slurm

echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
echo "recording_id: ${recording_id}"

module load anacondapy/2021.11
conda activate U19-pipeline_python
python ./scripts/automate_imaging_element.py 