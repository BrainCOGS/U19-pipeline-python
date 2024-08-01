

import copy
import pathlib
import re
import subprocess

import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
import u19_pipeline.automatic_job.params_config as config
from u19_pipeline.utility import is_this_spock
from u19_pipeline.utils.file_utils import write_file

# Functions to create slurm jobs

slurms_filepath = 'u19_pipeline/automatic_job/SlurmFiles'
default_slurm_filename = 'slurm_real.slurm'

default_process_script_path = "scripts/automate_imaging_element.py"

default_preprocessing_tool = 'kilosort2'
default_matlab_ver = 'R2020b'

def generate_slurm_file(job_id, program_selection_params):
    '''
    Generate and send slurm file to be queued in processing cluster
    '''

    #Get all associated directories given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(program_selection_params['process_cluster'])

    # Start with default values
    slurm_dict = copy.deepcopy(cluster_vars['slurm_default'])
    label_rec_process = 'job_id_'+str(job_id)
    slurm_dict['job-name'] = label_rec_process

    slurm_dict['output'] = str(pathlib.Path(cluster_vars['log_files_dir'],label_rec_process+ '.log'))
    slurm_dict['error'] = str(pathlib.Path(cluster_vars['error_files_dir'],label_rec_process+ '.log'))

    print('slurm_dict', slurm_dict)

    if program_selection_params['process_cluster'] == 'spock':
        slurm_text = generate_slurm_spock(slurm_dict)
    else:
        slurm_text = generate_slurm_tiger(slurm_dict)

    slurm_file_name = default_slurm_filename
    slurm_file_local_path = str(pathlib.Path(slurms_filepath,slurm_file_name))

    print(slurm_file_local_path)
    print(cluster_vars['slurm_files_dir'])
    print(slurm_file_name)

    write_file(slurm_file_local_path, slurm_text)

    if program_selection_params['process_cluster'] == 'spock' and is_this_spock():
        status = config.system_process['SUCCESS']
        slurm_destination = slurm_file_local_path
    else:
        slurm_destination = pathlib.Path(cluster_vars['slurm_files_dir'], slurm_file_name).as_posix()
        status = transfer_slurm_file(slurm_file_local_path, slurm_destination, cluster_vars)

    print(status)
    print(slurm_destination)
    print(cluster_vars)
    
    return status, slurm_destination


def queue_slurm_file(job_id, program_selection_params, raw_directory, proc_directory, modality, slurm_location):

    id_slurm_job = -1
    job_id = str(job_id)
    
    #Get all associated variables given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(program_selection_params['process_cluster'])

    print('queue_slurm_file **********************************')

    
    processing_repository = program_selection_params['process_repository']
    repository_dir = pathlib.Path(cluster_vars[modality+'_process_dir'],processing_repository).as_posix()

    command = ['ssh', cluster_vars['user']+"@"+cluster_vars['hostname'], 'sbatch', 
    "--export=recording_process_id="+job_id+
    ",raw_data_directory='"+raw_directory+
    "',processed_data_directory='"+proc_directory+
    "',repository_dir='"+repository_dir+
    "',process_script_path='"+program_selection_params['process_script']+"'"
    , slurm_location
    ]

    if program_selection_params['process_cluster'] == 'spock' and is_this_spock():
        command = command[2:]

    print(command)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #p = os.popen(command_new).read()
    p.wait()
    stdout, stderr = p.communicate()

    print(stdout)
    print(stderr)

    if p.returncode == config.system_process['SUCCESS']:
        error_message = ''
        batch_job_sentence = stdout.decode('UTF-8')
        print('batch_job_sentence', batch_job_sentence)
        id_slurm_job   = batch_job_sentence.replace("Submitted batch job ","")
        id_slurm_job   = re.sub(r"[\n\t\s]*", "", id_slurm_job)
    else:
        error_message = stderr.decode('UTF-8')

    return p.returncode, id_slurm_job, error_message


def check_slurm_job(ssh_user, host, jobid, local_user=False):
    
    if local_user:
        command = ['sacct', '--job', jobid, '--format=state']
    else:
        command = ['ssh', ssh_user+'@'+host, 'sacct', '--job', jobid, '--format=state']

    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    stdout, stderr = p.communicate()
    stdout = stdout.decode('UTF-8')

    if p.returncode == config.system_process['SUCCESS']:
        print('job state ....')
        print(stdout)
        state_slurm_job = stdout.split("\n")[2].strip()

        state_pipeline = config.slurm_states[state_slurm_job]['pipeline_status']
        error_message  = config.slurm_states[state_slurm_job]['message']

        print('state_pipeline ....', state_pipeline)
        print('error_message', error_message)

    else:
        state_pipeline = config.status_update_idx['ERROR_STATUS']
        error_message  = 'Failed to retrieve slurm job status'
        
    return state_pipeline, error_message


def transfer_slurm_file(slurm_file_local_path, slurm_destination, cluster_vars):
    '''
    Create scp command from cluster directories and local slurm file
    '''

    user_host = cluster_vars['user']+'@'+cluster_vars['hostname']
    slurm_destination = user_host+':'+slurm_destination
    status = ft.scp_file_transfer(slurm_file_local_path, slurm_destination)

    return status


def create_slurm_params_file(slurm_dict):

    text_dict = ''
    for slurm_param in slurm_dict:

        if isinstance(slurm_dict[slurm_param], list):
            for list_param in slurm_dict[slurm_param]:
                text_dict += '#SBATCH --' + str(slurm_param) + '=' + str(list_param) + '\n'
        else:
            text_dict += '#SBATCH --' + str(slurm_param) + '=' + str(slurm_dict[slurm_param]) + '\n'

    return text_dict

def generate_slurm_spock(slurm_dict):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += '''
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anacondapy/2023.07-cuda -s
    module load matlab/R2024a -s

    conda activate u19_pipeline_python_env2

    cd ${repository_dir}
    python -u ${process_script_path}
    #python ${process_script_path} ${recording_process_id}
    '''
    
    return slurm_text   

def generate_slurm_spockmk2_ephys(slurm_dict):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += '''
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anacondapy/2023.07-cuda -s
    module load matlab/R2024a -s

    conda activate BraincogsEphysSorters_Env

    cd ${repository_dir}
    python -u ${process_script_path}
    #python ${process_script_path} ${recording_process_id}
    '''
    
    return slurm_text   


def generate_slurm_tiger(slurm_dict):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += '''
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RECORDING_PROCESS_ID: ${recording_process_id}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anaconda3/2022.5 -s
    module load matlab/R2024a -s

    conda activate BrainCogsEphysSorters_env

    cd ${repository_dir}
    python -u ${process_script_path}
    '''

    return slurm_text


def generate_slurm_dlc(slurm_dict):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += '''
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"
    echo "MODEL_PATH: ${model_path}"

    module load anacondapy/2021.11 -s
    conda activate /usr/people/alvaros/.conda/envs/u19_datajoint_py39_env

    python -u ${process_script_path} ${raw_data_directory} ${model_path} ${processed_data_directory}
    '''

    return slurm_text 


def generate_slurm_dlc2(slurm_dict):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += '''
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "RAW_DATA_DIRECTORY: ${raw_data_directory}"
    echo "PROCESSED_DATA_DIRECTORY: ${processed_data_directory}"
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"
    echo "MODEL_PATH: ${model_path}"

    module load anaconda3/5.3.1 -s
    conda activate /home/alvaros/.conda/envs/u19_datajoint_py39_env

    python -u ${process_script_path} ${raw_data_directory} ${model_path} ${processed_data_directory}
    '''

    return slurm_text 

