#!/bin/bash
#SBATCH --job-name=test_sessid_x
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1
#SBATCH --time=01:00:00
##SBATCH --mem=15G
#SBATCH --mail-user=geetika@datajoint.com
#SBATCH --mail-type=end
#SBATCH --mail-type=fail
#SBATCH --error=logs/JobName.%J.err
#SBATCH --output=logs/JobName.%J.log

# sbatch --export=recording_id=13,process_method='suite2p',paramset_idx=0 --mem=24G ~/U19-pipeline_python/scripts/automate_imaging_element.slurm

# sbatch --export=recording_id=13,process_method='caiman',paramset_idx=1 --mem=24G ~/U19-pipeline_python/scripts/automate_imaging_element.slurm

echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
echo "Recording_id: ${recording_id}"
# echo "subject_fullname: ${subject_fullname}"
# echo "session_date: ${session_date}"
# echo "session_number: ${session_number}"


module load anacondapy/2021.11
conda activate U19-pipeline_python
python ~/U19-pipeline_python/scripts/automate_imaging_element.py


# squeue -u gs6614
# scontrol show slurmd
# scontrol: error: slurm_slurmd_info: Connection refused