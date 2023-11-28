#!/bin/bash
#SBATCH --job-name=job_id_efonseca_ef111_act111_20231010_g0
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --time=10:00:00
#SBATCH --mem=50G
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL
#SBATCH --output=/mnt/cup/braininit/Shared/repos/U19-pipeline_python/u19_pipeline/automatic_job/OutputLogjob_id_efonseca_ef111_act111_20231010_g0.log
#SBATCH --error=/mnt/cup/braininit/Shared/repos/U19-pipeline_python/u19_pipeline/automatic_job/ErrorLogjob_id_efonseca_ef111_act111_20231010_g0.log

        echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
        echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
        echo "VIDEO_DIR: ${video_dir}"
        echo "MODEL_DIR: ${model_dir}"
        echo "REPOSITORY_DIR: ${repository_dir}"
        echo "OUTPUT_DIR: ${output_dir}"

        module load anacondapy/2021.11

        conda activate u19_pipeline_python_env

        cd ${repository_dir}
        python ${process_script_path} ${video_dir} ${model_dir} ${output_dir}
        #python ${process_script_path} ${recording_process_id}
        