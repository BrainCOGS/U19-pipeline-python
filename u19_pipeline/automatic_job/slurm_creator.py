

#import os
#import pathlib
import subprocess
import pathlib
import json
import re
import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
from u19_pipeline.utility import create_str_from_dict, is_this_spock
import u19_pipeline.automatic_job.params_config as config 

# Functions to create slurm jobs

#Slurm default values for queue job
slurm_dict_tiger_default = {
    'job-name': 'kilosort2',
    'nodes': 1,
    'ntasks': 1,
    'time': '5:00:00',
    'mem': '200G',
    'gres': 'gpu:1',
    'mail-user': 'alvaros@princeton.edu',
    'mail-type': ['begin', 'END'],
    'output': 'job_log/recording_process_${recording_process_id}".log'
}
slurm_dict_spock_default = {
    'job-name': 'dj_ingestion',
    'nodes': 1,
    'cpus-per-task': 1,
    'time': '00:30:00',
    'mem': '16G',
    'mail-user': 'alvaros@princeton.edu',
    'mail-type': ['begin', 'END'],
    'output': 'job_log/recording_process_${recording_process_id}".log'
}


slurm_states = {
    'SUCCESS': 'COMPLETED'
}

default_slurm_filename = 'slurm_real.slurm'

default_process_script_path = "slurm_files/test.py"

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

    # Start with default values
    slurm_dict = slurm_dict_spock_default.copy()
    slurm_dict['job-name'] = preprocess_params['sorting_algorithm'] + "_" + str_key

    #Get all associated directories given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(preprocess_params['process_cluster'])
    slurm_dict['output'] = str(pathlib.Path(cluster_vars['log_files_dir'],str_key + '.log'))

    if preprocess_params['process_cluster'] == 'spock':
        slurm_text = generate_slurm_spock(slurm_dict)
    else:
        slurm_text = generate_slurm_tigger(slurm_dict,)
    slurm_file_name = default_slurm_filename
    slurm_file_local_path = str(pathlib.Path("slurm_files",slurm_file_name))

    write_slurm_file(slurm_file_local_path, slurm_text)

    if preprocess_params['process_cluster'] == 'spock' and is_this_spock():
        #status, slurm_destination = copy_spock_local_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars)
        status = config.system_process['SUCCESS']
        slurm_destination = slurm_file_local_path
    else:
        status, slurm_destination = transfer_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars)
    
    return status, slurm_destination

def transfer_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars):
    '''
    Create scp command from cluster directories and local slurm file
    '''

    user_host = cluster_vars['user']+'@'+cluster_vars['hostname']
    slurm_destination = user_host+':'+str(pathlib.Path(cluster_vars['slurm_files_dir'], slurm_file_name))
    status = ft.scp_file_transfer(slurm_file_local_path, slurm_destination)

    return status, slurm_destination

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


def write_slurm_file(slurm_path, slurm_text):

    f_slurm = open(slurm_path, "w")
    f_slurm.write(slurm_text)
    f_slurm.close()

# ALS revisit, slurm for element ingestion vs out processing
# Spock vs tiger only send export variable initial directory

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
    python ${process_script_path} ${recording_process_id}
    '''
    
    return slurm_text   


def generate_slurm_tiggert(slurm_dict, matlab_ver, user_run, raw_file_path):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += '''
    module load matlab/R2020b\n
    cd /tigress/alvaros
    matlab -singleCompThread -nodisplay -nosplash -r "pause(1); disp('aqui la chides'); exit"
    '''

    return slurm_text


def queue_slurm_file(record_process_series, slurm_location):

    id_slurm_job = -1

    #get preprocess params (some of these will change slurm creation)
    preprocess_params = record_process_series['preprocess_paramset']
    recording_id = str(record_process_series['recording_process_id'])

    #Get all associated variables given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(preprocess_params['process_cluster'])

    command = ['ssh', cluster_vars['user'], 'sbatch', 
    "--export=recording_process_id="+recording_id+
    ",repository_dir="+cluster_vars['home_dir']+
    ",process_script_path="+str(pathlib.Path(cluster_vars['home_dir'],default_process_script_path)), slurm_location]

    if preprocess_params['process_cluster'] == 'spock' and is_this_spock():
        command = command[2:]

    print(command)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    print('aftercommand before comm')
    stdout, stderr = p.communicate()
    print('aftercommand after comm')
    print(stdout.decode('UTF-8'))
    print(stderr.decode('UTF-8'))
    if p.returncode == config.system_process['SUCCESS']:
        batch_job_sentence = stdout.decode('UTF-8')
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

