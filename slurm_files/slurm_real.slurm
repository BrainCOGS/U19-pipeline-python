#!/bin/bash
#SBATCH --job-name=recording_process_id_26
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --time=02:00:00
#SBATCH --mem=24G
#SBATCH --mail-user=alvaros@princeton.edu
#SBATCH --mail-type=END
#SBATCH --output=/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline_python/job_log/recording_process_id_26.log

    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anacondapy/2021.11

    conda activate u19_pipeline_python_env

    cd ${repository_dir}
    python ${process_script_path}
    #python ${process_script_path} ${recording_process_id}
    