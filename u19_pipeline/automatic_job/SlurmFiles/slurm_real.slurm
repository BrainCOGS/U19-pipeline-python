#!/bin/bash
#SBATCH --job-name=job_id_12
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --time=5:00:00
#SBATCH --mem=24G
#SBATCH --mail-user=<email>
#SBATCH --mail-type=END
#SBATCH --output=/scratch/gpfs/BRAINCOGS/OutputLog/rec9.log

    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anacondapy/2021.11
    conda activate u19_pipeline_python_env

    python ${process_script_path} ${raw_data_directory} ${model_path} ${processed_data_directory}
    