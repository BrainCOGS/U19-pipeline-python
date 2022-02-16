

#import os
#import pathlib
import subprocess
import pathlib
import json
import re
import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
from u19_pipeline.utility import create_str_from_dict, is_this_spock

# Functions to create slurm jobs

#Slurm default values for queue job
slurm_dict_default = {
    'job-name': 'kilosort2',
    'nodes': 1,
    'ntasks': 1,
    'time': '5:00:00',
    'mem': '200G',
    'gres': 'gpu:1',
    'mail-user': 'alvaros@princeton.edu',
    'mail-type': ['begin', 'END'],
    'output': 'job_log/kilojob.log'
}

slurm_states = {
    'SUCCESS': 'COMPLETED'
}


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
    slurm_dict = slurm_dict_default.copy()
    print(preprocess_params['sorting_algorithm'])
    print(str_key)
    print(type(str_key))
    print(type(preprocess_params['sorting_algorithm']))
    slurm_dict['job-name'] = preprocess_params['sorting_algorithm'] + "_" + str_key

    #Get all associated directories given the selected processing cluster
    cluster_vars = ft.get_cluster_vars(preprocess_params['process_cluster'])
    slurm_dict['output'] = str(pathlib.Path(cluster_vars['log_files_dir'],str_key + '.log'))

    if preprocess_params['process_cluster'] == 'spock' and preprocess_params["dj_element_processing"] == "trigger":
        slurm_text = generate_slurm_dj_trigger(slurm_dict)
    else:
        slurm_text = generate_slurm_kilosort_text(slurm_dict, default_matlab_ver, ft.default_user, rel_path)
    slurm_file_name = 'slurm_' + str_key +  '.slurm'
    slurm_file_local_path = str(pathlib.Path("slurm_files",slurm_file_name))

    write_slurm_file(slurm_file_local_path, slurm_text)

    if preprocess_params['process_cluster'] == 'spock' and is_this_spock():
        status = copy_spock_local_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars)
    else:
        status = transfer_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars)
    
    return status

def transfer_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars):
    '''
    Create scp command from cluster directories and local slurm file
    '''

    user_host = cluster_vars['user']+'@'+cluster_vars['hostname']
    slurm_destination = user_host+':'+str(pathlib.Path(cluster_vars['slurm_files_dir'], slurm_file_name))
    status = ft.scp_file_transfer(slurm_file_local_path, slurm_destination)

    return status

def copy_spock_local_slurm_file(slurm_file_local_path, slurm_file_name, cluster_vars):
    '''
    Copy local spock slurm file
    '''
    slurm_destination = str(pathlib.Path(cluster_vars['slurm_files_dir'], slurm_file_name))
    p = subprocess.Popen(["cp", slurm_file_local_path, slurm_destination])
    status = p.wait()

    return status

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


def generate_slurm_kilosort_text(slurm_dict, matlab_ver, user_run, raw_file_path):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += 'module load matlab/' + matlab_ver + '\n'
    slurm_text += 'cd /tigress/' + user_run + '\n'
    slurm_text += 'matlab -singleCompThread -nodisplay -nosplash -r "pause(1); ' + "disp('aqui la chides'); exit" + '"'
    #slurm_text += 'matlab -singleCompThread -nodisplay -nosplash -r "addpath(''/tigress/' + user_run +  "/run_kilosort/spikesorters/'); "
    #slurm_text += "run_ks2('/tigress/" + user_run + "/ephys_raw" + raw_file_path + "','/tigress/" + user_run + "/run_kilosort/tmp/'); exit" + '"' 

    return slurm_text


def generate_slurm_text(slurm_dict, matlab_ver, user_run, raw_file_path):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += 'module load matlab/' + matlab_ver + '\n'
    slurm_text += 'cd /tigress/' + user_run + '\n'
    slurm_text += 'matlab -singleCompThread -nodisplay -nosplash -r "pause(1); ' + "disp('aqui la chides'); exit" + '"'
    #slurm_text += 'matlab -singleCompThread -nodisplay -nosplash -r "addpath(''/tigress/' + user_run +  "/run_kilosort/spikesorters/'); "
    #slurm_text += "run_ks2('/tigress/" + user_run + "/ephys_raw" + raw_file_path + "','/tigress/" + user_run + "/run_kilosort/tmp/'); exit" + '"' 

    return slurm_text   


def generate_slurm_dj_trigger(slurm_dict):

    slurm_text = '#!/bin/bash\n'
    slurm_text += create_slurm_params_file(slurm_dict)
    slurm_text += '''
    echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
    echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
    echo "SESSID: ${sessid}"

    module load anacondapy/2021.11

    conda activate U19-pipeline_python

    cd /usr/people/kg7524/U19-pipeline_python

    python /usr/people/kg7524/slurm/test.py   
    '''
    
    return slurm_text   


def queue_slurm_file(ssh_user, slurm_file):

    id_slurm_job = -1
    command = ['ssh', ssh_user, 'sbatch', slurm_file]
    print(command)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    print('aftercommand before comm')
    stdout, stderr = p.communicate()
    print('aftercommand after comm')
    print(stdout.decode('UTF-8'))
    print(stderr.decode('UTF-8'))
    if p.returncode == system_process['SUCCESS']:
        batch_job_sentence = stdout.decode('UTF-8')
        id_slurm_job   = batch_job_sentence.replace("Submitted batch job ","")
        id_slurm_job   = re.sub(r"[\n\t\s]*", "", id_slurm_job)

    return p.returncode, id_slurm_job


def check_slurm_job(ssh_user, jobid):
    
    state_job = 'FAIL'
    command = ['ssh', ssh_user, 'sacct', '--job', jobid, '--format=state']
    print(command)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    stdout, stderr = p.communicate()
    if p.returncode == system_process['SUCCESS']:
        stdout = stdout.decode('UTF-8')
        state_job = stdout.split("\n")[2].strip()
        print(stdout)

    return state_job

