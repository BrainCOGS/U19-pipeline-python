

#import os
#import pathlib
import json
import pathlib

from scipy.io import savemat

import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
import u19_pipeline.automatic_job.params_config as config
from u19_pipeline.utils.file_utils import write_file

# Functions to create parameter files and send them


#parameter_files_filepath = 'u19_pipeline/automatic_job/ParameterFiles'
#default_preprocess_filename = 'preprocess_paramset_%s.json'
#default_process_filename = 'process_paramset_%s.json'

#chanmap_files_filepath = 'u19_pipeline/automatic_job/ChanMapFiles'
#default_chanmap_filename = 'chanmap_%s.mat'

default_process_script_path = "scripts/automate_imaging_element.py"


def generate_parameter_file(recording_process_id, params, type_params, program_selection_params):
    '''
    Generate and send parameter files for processing
    '''
    if program_selection_params['local_or_cluster'] == 'cluster':
        cluster_vars = ft.get_cluster_vars(program_selection_params['process_cluster'])
        params_file_cluster_path = cluster_vars['params_files_dir']
        user_host = cluster_vars['user']+'@'+cluster_vars['hostname']

    #Write preprocessing parameter file
    if type_params == 'preparams':
        write_parameter_file(params, recording_process_id, config.default_preprocess_filename)
        if program_selection_params['local_or_cluster'] == 'cluster':
            status = transfer_parameter_file(recording_process_id, config.default_preprocess_filename, params_file_cluster_path, user_host)
        else:
            status = config.system_process['SUCCESS']
    else:
        write_parameter_file(params, recording_process_id, config.default_process_filename)
        if program_selection_params['local_or_cluster'] == 'cluster':
            status = transfer_parameter_file(recording_process_id, config.default_process_filename, params_file_cluster_path, user_host)
        else:
            status = config.system_process['SUCCESS']

    return status


def write_parameter_file(params, recording_process_id, default_param_filename):
    '''
    Write local parameter file to send
    '''
    str_params = json.dumps(params)

    param_filename = default_param_filename % (recording_process_id)
    params_file_local_path = str(pathlib.Path(config.parameter_files_filepath,param_filename))

    write_file(params_file_local_path, str_params)


def transfer_parameter_file(recording_process_id, default_param_filename, cluster_param_dir, user_host):
    '''
    Transfer parameter file to processing cluster
    '''

    param_filename = default_param_filename % (recording_process_id)
    params_file_local_path = str(pathlib.Path(config.parameter_files_filepath,param_filename))
    params_file_cluster_path = str(pathlib.Path(cluster_param_dir,param_filename))
    param_file_full_path = user_host+':'+params_file_cluster_path

    print('transfer_parameter_file', params_file_local_path, param_file_full_path)

    status = ft.scp_file_transfer(params_file_local_path, param_file_full_path)

    return status


def generate_chanmap_file(recording_process_id, program_selection_params):
    '''
    Generate and send chanmap files for processing
    '''
    
    cluster_vars = ft.get_cluster_vars(program_selection_params['process_cluster'])
    params_file_cluster_path = cluster_vars['chanmap_files_dir']
    user_host = cluster_vars['user']+'@'+cluster_vars['hostname']

    #Write chanmap file
    # write_chanmap_file(chanmap_df, recording_process_id, config.default_chanmap_filename)
    status = transfer_chanmap_file(recording_process_id, config.default_chanmap_filename, params_file_cluster_path, user_host)

    return status

def write_chanmap_file(chanmap_dict, recording_process_id, default_chanmap_filename):
    '''
    Write local chanmap file to send
    '''
    chanmap_filename = default_chanmap_filename % (recording_process_id)
    chanmap_file_local_path = str(pathlib.Path(config.chanmap_files_filepath,chanmap_filename))

    savemat(chanmap_file_local_path, chanmap_dict)


def transfer_chanmap_file(recording_process_id, default_chanmap_filename, cluster_chanmap_dir, user_host):
    '''
    Transfer chanmap file to processing cluster
    '''

    chanmap_filename = default_chanmap_filename % (recording_process_id)
    chanmap_file_local_path = str(pathlib.Path(config.chanmap_files_filepath,chanmap_filename))
    chanmap_file_cluster_path = str(pathlib.Path(cluster_chanmap_dir,chanmap_filename))
    chanmap_file_full_path = user_host+':'+chanmap_file_cluster_path

    status = ft.scp_file_transfer(chanmap_file_local_path, chanmap_file_full_path)

    return status

