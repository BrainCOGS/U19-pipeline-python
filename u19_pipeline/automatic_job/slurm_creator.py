

import os
#import pathlib
import subprocess
import pathlib
import json
import re
import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
from u19_pipeline.utility import create_str_from_dict, is_this_spock
import u19_pipeline.automatic_job.params_config as config 
from u19_pipeline.utils.file_utils import write_file

# Functions to create slurm jobs

slurm_states = {
    'SUCCESS': 'COMPLETED'
}

slurms_filepath = 'u19_pipeline/automatic_job/SlurmFiles'
default_slurm_filename = 'slurm_real.slurm'

default_process_script_path = "scripts/automate_imaging_element.py"

default_preprocessing_tool = 'kilosort2'
default_matlab_ver = 'R2020b'

def generate_slurm_file(record_process_series):
    '''
    Generate and send slurm file to be queued in processing cluster
    '''

    #get preprocess params (some of these will change slurm creation)
    preprocess_params = record_process_series['preprocess_paramset']

    #relative Path where data will be located
    rel_path = record_process_series['recording_process_pre_path']

    #get processing_id string
    key = record_process_series['query_key']
    str_key = create_str_from_dict(key)

    #Get all associated directories given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(preprocess_params['process_cluster'])

    # Start with default values
    slurm_dict = cluster_vars['slurm_default'].copy()
    label_rec_process = 'recording_process_id_'+str(record_process_series['recording_process_id'])
    slurm_dict['job-name'] = label_rec_process

    slurm_dict['output'] = str(pathlib.Path(cluster_vars['log_files_dir'],label_rec_process+ '.log'))

    if preprocess_params['process_cluster'] == 'spock':
        slurm_text = generate_slurm_spock(slurm_dict)
    else:
        slurm_text = generate_slurm_tiger(slurm_dict)
    slurm_file_name = default_slurm_filename
    slurm_file_local_path = str(pathlib.Path(slurms_filepath,slurm_file_name))

    write_file(slurm_file_local_path, slurm_text)

    if preprocess_params['process_cluster'] == 'spock' and is_this_spock():
        #status, slurm_destination = copy_spock_local_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars)
        status = config.system_process['SUCCESS']
        slurm_destination = slurm_file_local_path
    else:
        slurm_destination = pathlib.Path(cluster_vars['slurm_files_dir'], slurm_file_name).as_posix()
        status = transfer_slurm_file(slurm_file_local_path, slurm_destination, cluster_vars)
    
    return status, slurm_destination

def transfer_slurm_file(slurm_file_local_path, slurm_destination, cluster_vars):
    '''
    Create scp command from cluster directories and local slurm file
    '''

    user_host = cluster_vars['user']+'@'+cluster_vars['hostname']
    slurm_destination = user_host+':'+slurm_destination
    status = ft.scp_file_transfer(slurm_file_local_path, slurm_destination)

    return status

def copy_spock_local_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars):
    '''
    Copy local spock slurm file
    '''
    slurm_destination = str(pathlib.Path(cluster_vars['slurm_files_dir'], slurm_file_name))
    p = subprocess.Popen(["cp", slurm_file_local_path, slurm_destination])
    status = p.wait()

    return status, slurm_destination

def create_slurm_params_file(slurm_dict):

    text_dict = ''
    for slurm_param in slurm_dict.keys():

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
    echo "REPOSITORY_DIR: ${repository_dir}"
    echo "PROCESS_SCRIPT_PATH: ${process_script_path}"

    module load anacondapy/2021.11

    conda activate u19_pipeline_python_env

    cd ${repository_dir}
    python ${process_script_path}
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

    module load anaconda3/5.3.1
    module load matlab/R2020a

    conda activate /home/alvaros/.conda/envs/BrainCogsEphysSorters_env

    cd ${repository_dir}
    python ${process_script_path}
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

    module load anacondapy/2021.11
    conda activate /usr/people/alvaros/.conda/envs/u19_datajoint_py39_env

    python ${process_script_path} ${raw_data_directory} ${model_path} ${processed_data_directory}
    '''

    return slurm_text 


def queue_slurm_file(record_process_series, slurm_location):

    id_slurm_job = -1

    #get preprocess params (some of these will change slurm creation)
    preprocess_params = record_process_series['preprocess_paramset']
    recording_process_id = str(record_process_series['recording_process_id'])
    
    #Get all associated variables given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(preprocess_params['process_cluster'])
    
    processed_data_directory = pathlib.Path(record_process_series['recording_process_pre_path'], "recording_process_id_"+recording_process_id).as_posix()
    modality = record_process_series['recording_modality']
    processing_repository = preprocess_params['process_repository']
    repository_dir = pathlib.Path(cluster_vars[modality+'_process_dir'],processing_repository).as_posix()



    command = ['ssh', cluster_vars['user']+"@"+cluster_vars['hostname'], 'sbatch', 
    "--export=recording_process_id="+recording_process_id+
    ",raw_data_directory='"+record_process_series['recording_process_pre_path']+
    "',processed_data_directory='"+processed_data_directory+
    "',repository_dir='"+repository_dir+
    "',process_script_path='"+preprocess_params['process_script']+"'"
    , slurm_location
    ]

    if preprocess_params['process_cluster'] == 'spock' and is_this_spock():
        command = command[2:]

    print(command)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #p = os.popen(command_new).read()
    p.wait()
    stdout, stderr = p.communicate()
    print('stdout', stdout.decode('UTF-8'))
    print('stderr', stderr.decode('UTF-8'))
    print('p.returncode', p.returncode)

    if p.returncode == config.system_process['SUCCESS']:
        batch_job_sentence = stdout.decode('UTF-8')
        print('batch_job_sentence', batch_job_sentence)
        id_slurm_job   = batch_job_sentence.replace("Submitted batch job ","")
        id_slurm_job   = re.sub(r"[\n\t\s]*", "", id_slurm_job)

    return p.returncode, id_slurm_job


def check_slurm_job(ssh_user, jobid, local_user=False):
    
    state_job = 'FAIL'
    if local_user:
        command = ['sacct', '--job', jobid, '--format=state']
    else:
        command = ['ssh', ssh_user, 'sacct', '--job', jobid, '--format=state']

    print(command)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    stdout, stderr = p.communicate()
    if p.returncode == config.system_process['SUCCESS']:
        stdout = stdout.decode('UTF-8')
        state_job = stdout.split("\n")[2].strip()
        print(stdout)

    return state_job

