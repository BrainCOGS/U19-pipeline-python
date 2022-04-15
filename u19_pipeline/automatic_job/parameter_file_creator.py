

#import os
#import pathlib
import subprocess
import pathlib
import json
import re
import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
import u19_pipeline.automatic_job.params_config as config 
from u19_pipeline.utils.file_utils import write_file

# Functions to create parameter files and send them



slurm_states = {
    'SUCCESS': 'COMPLETED'
}


parameter_files_filepath = 'u19_pipeline/automatic_job/ParameterFiles'
default_preprocess_filename = 'preprocess_paramset_%s.json'
default_process_filename = 'process_paramset_%s.json'

default_process_script_path = "scripts/automate_imaging_element.py"


def generate_parameter_file(record_process_series):
    '''
    Generate and send parameter files for processing
    '''

    preprocess_params = record_process_series['preprocess_paramset']
    recording_process_id = record_process_series['recording_process_id']

    cluster_vars = ft.get_cluster_vars(preprocess_params['process_cluster'])
    params_file_cluster_path = cluster_vars['params_files_dir']
    user_host = cluster_vars['user']+'@'+cluster_vars['hostname']

    #Write preprocessing parameter file
    write_parameter_file(preprocess_params, recording_process_id, default_preprocess_filename)

    status = transfer_parameter_file(recording_process_id, default_preprocess_filename, params_file_cluster_path, user_host)

    if status == config.system_process['SUCCESS']:
        #Write processing parameter file
        write_parameter_file(record_process_series['process_paramset'], recording_process_id, default_process_filename)

        status = transfer_parameter_file(recording_process_id, default_process_filename, params_file_cluster_path, user_host)

    return status



def write_parameter_file(params, recording_process_id, default_param_filename):
    '''
    Write local parameter file to send
    '''
    str_params = json.dumps(params)

    param_filename = default_param_filename % (recording_process_id)
    params_file_local_path = str(pathlib.Path(parameter_files_filepath,param_filename))

    write_file(params_file_local_path, str_params)

def transfer_parameter_file(recording_process_id, default_param_filename, cluster_param_dir, user_host):
    '''
    Transfer parameter file to processing cluster
    '''

    param_filename = default_param_filename % (recording_process_id)
    params_file_local_path = str(pathlib.Path(parameter_files_filepath,param_filename))
    params_file_cluster_path = str(pathlib.Path(cluster_param_dir,param_filename))
    param_file_full_path = user_host+':'+params_file_cluster_path

    status = ft.scp_file_transfer(params_file_local_path, param_file_full_path)

    return status
