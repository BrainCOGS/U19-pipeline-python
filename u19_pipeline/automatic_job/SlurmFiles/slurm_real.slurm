#!/bin/bash
#SBATCH --job-name=recording_process_id_105
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=5:00:00
#SBATCH --mem=200G
#SBATCH --gres=gpu:1
#SBATCH --mail-user=alvaros@princeton.edu
#SBATCH --mail-type=END
#SBATCH --output=/scratch/gpfs/BRAINCOGS/OutputLog/recording_process_id_105.log

    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anaconda3/5.3.1
    module load matlab/R2020a

    conda activate /home/alvaros/.conda/envs/BrainCogsEphysSorters_env

    cd ${repository_dir}
    python ${process_script_path}
    