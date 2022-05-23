#!/bin/bash
#SBATCH --job-name=rec9
#SBATCH --nodes=2
#SBATCH --ntasks=2
#SBATCH --time=5:00:00
#SBATCH --mem=200G
#SBATCH --gres=gpu:2
#SBATCH --mail-user=alvaros@princeton.edu
#SBATCH --mail-type=END
#SBATCH --output=/scratch/gpfs/BRAINCOGS/OutputLog/rec9.log

    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"
    echo "MODEL_PATH: ${model_path}"

    module load anaconda3/5.3.1
    conda activate /home/alvaros/.conda/envs/u19_datajoint_py39_env

    python ${process_script_path} ${raw_data_directory} ${model_path} ${processed_data_directory}
    