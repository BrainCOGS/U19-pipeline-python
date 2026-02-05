#!/bin/bash
#SBATCH --job-name=rec5
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --time=5:00:00
#SBATCH --mem=24G
#SBATCH --mail-user=alvaros@princeton.edu
#SBATCH --mail-type=END
#SBATCH --output=/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline_python/u19_pipeline/automatic_job/OutputLog/rec5.log

    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"
    echo "MODEL_PATH: ${model_path}"

    
    cd ${repository_dir}
    source .venv/bin/activate

    python ${process_script_path} ${raw_data_directory} ${model_path} ${processed_data_directory}
    