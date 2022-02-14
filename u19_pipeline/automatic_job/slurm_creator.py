

#import os
#import pathlib
import subprocess
import json
import re

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


    rel_path = record_process_series['acquisition_raw_rel_path']
    key = record_process_series['query_key']
    
    slurm_dict = slurm_dict_default.copy()
    slurm_dict['job-name'] = default_preprocessing_tool + "_" + create_str_from_dict(key)
    slurm_dict['output'] = str(pathlib.Path(tiger_log_files_dir,default_preprocessing_tool + create_str_from_dict(key) + '.log'))

    slurm_text = generate_slurm_kilosort_text(slurm_dict, default_matlab_ver, default_user, raw_rel_path)
    slurm_file_name = 'slurm_' + create_str_from_dict(key) +  '.slurm'
    slurm_file_path = str(pathlib.Path("slurm_files",slurm_file_name))

    write_slurm_file(slurm_file_path, slurm_text)
    
    tiger_slurm_user = default_user+'@'+tiger_gpu_host
    tiger_slurm_location = tiger_slurm_user+':'+tiger_slurm_files_dir+slurm_file_name
    transfer_request = scp_file_transfer(slurm_file_path, tiger_slurm_location)


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

