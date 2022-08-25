import datajoint as dj
import pathlib
import subprocess
import json
import re
import os
import time

from datetime import datetime
from element_interface.utils import dict_to_uuid

import u19_pipeline.automatic_job.params_config as config
import u19_pipeline.automatic_job.machine_variables as mv
#Functions to transfer files (globus, scp, smbclient)

#FOR PNI endpoint
#pni_ep_id = '6ce834d6-ff8a-11e6-bad1-22000b9a448b'
pni_ep_id = '005329dc-f31c-11ec-b3c1-15403b7b75ed'  # pni BRAINCOGS ep points to /braininit/Data/
pni_data_dir   = ''         

#For tiger endpoint
default_user   = 'alvaros'                               # This will change to our automatic client for globus transfers
tiger_gpu_host = 'tigergpu.princeton.edu'
#tiger_ep_dir  = 'a9df83d2-42f0-11e6-80cf-22000b1701d1'  # tiger ep
tiger_ep_dir   = 'ef3a4e74-e742-11ec-9912-3b4cfda38030'  # tiger BRAINCOGS ep points to /scratch/gpfs/BRAINCOGS/
tiger_home_dir_globus = '/Data'   

#Slurm default values for queue job
slurm_dict_tiger_default = {
    'job-name': 'kilosort2',
    'nodes': 1,
    'ntasks': 1,
    'time': '10:00:00',
    'mem': '50G',
    'gres': 'gpu:1',
    'mail-user': 'alvaros@princeton.edu',
    'mail-type': ['END'],
    'output': 'OutputLog/job_id_${job_id}".log',
    'error':  'ErrorLog/job_id_${job_id}".log'
}
slurm_dict_spock_default = {
    'job-name': 'dj_ingestion',
    'nodes': 1,
    'cpus-per-task': 1,
    'time': '10:00:00',
    'mem': '50G',
    'mail-type': ['END', 'FAIL'],
    'output': 'OutputLog/job_id_${job_id}".log',
    'error': 'ErrorLog/job_id_${job_id}".log'
}


#PNI directories
pni_root_data_dir   = dj.config['custom']['root_data_dir']

tiger_home_dir = '/scratch/gpfs/BRAINCOGS'    
spock_home_dir = '/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline_python'

#Cluster directories
cluster_vars = {
    "tiger": {
        "home_dir":                      tiger_home_dir, 
        "root_data_dir_globus":          tiger_home_dir_globus + "/Raw", 
        "processed_data_dir_globus":     tiger_home_dir_globus + "/Processed", 
        "root_data_dir":                 tiger_home_dir + "/Data/Raw", 
        "processed_data_dir":            tiger_home_dir + "/Data/Processed", 
        "slurm_files_dir":               tiger_home_dir + "/SlurmFiles", 
        "params_files_dir":              tiger_home_dir + "/ParameterFiles", 
        "chanmap_files_dir":             tiger_home_dir + "/ChanMapFiles", 
        "electrophysiology_process_dir": tiger_home_dir + "/electorphysiology_processing", 
        "imaging_process_dir":           tiger_home_dir + "/imaging_processing", 
        "log_files_dir":                 tiger_home_dir + "/OutputLog", 
        "error_files_dir":               tiger_home_dir + "/ErrorLog", 
        "user":                          default_user, 
        "slurm_default":                 slurm_dict_tiger_default, 
        "hostname":                      "tigergpu.princeton.edu",
        "script_path":                   "",
        "conda_env":                     '/home/alvaros/.conda/envs/BrainCogsEphysSorters_env' 
    },
    "spock": {
        "home_dir":                      spock_home_dir, 
        "root_data_dir_globus":          pni_data_dir   + "/Raw",
        "processed_data_dir_globus":     pni_data_dir   + "/Processed",
        "root_data_dir":                 spock_home_dir + "/Raw",
        "processed_data_dir":            spock_home_dir + "/Processed",
        "slurm_files_dir":               spock_home_dir + "/SlurmFiles", 
        "params_files_dir":              spock_home_dir + "/ParameterFiles",
        "chanmap_files_dir":             spock_home_dir + "/ChanMapFiles", 
        "electrophysiology_process_dir": spock_home_dir + "/electorphysiology_processing", 
        "imaging_process_dir":           spock_home_dir + "/imaging_processing",  
        "log_files_dir":                 spock_home_dir + "/u19_pipeline/automatic_job/OutputLog", 
        "error_files_dir":               spock_home_dir + "/u19_pipeline/automatic_job/ErrorLog", 
        "user":                          default_user,
        "slurm_default":                 slurm_dict_spock_default, 
        "hostname":                      "spock.princeton.edu",
        "script_path":                   "",
        "conda_env":                     'u19_pipeline_python_env'
    }
}


def get_cluster_vars(cluster):

    if cluster in cluster_vars:
        return cluster_vars[cluster]
    else:
        raise('Non existing cluster')


def scp_file_transfer(source, dest):

    print("scp", source, dest)
    p = subprocess.Popen(["scp", "-i", mv.public_key_location2, source, dest])
    transfer_status = p.wait()
    return transfer_status


def cp_file_transfer(source, dest):

    print("cp", source, dest)
    p = subprocess.Popen(["cp", source, dest])
    transfer_status = p.wait()
    return transfer_status

def request_globus_transfer(job_id_str, source_ep, dest_ep, source_filepath, dest_filepath):

    source_fullpath = source_ep+ ":" + source_filepath
    dest_fullpath   = dest_ep  + ":" + dest_filepath

    globus_command = ["globus", "transfer", source_fullpath, dest_fullpath, '--label', job_id_str, '--recursive', '--format', 'json']
    p = subprocess.run(globus_command, capture_output=True)

    transfer_request = dict()

    if len(p.stderr) == 0:
        dict_output = json.loads(p.stdout.decode('UTF-8'))
        #dict_output = translate_globus_output(p.stdout)
        transfer_request['status'] = config.system_process['SUCCESS']
        transfer_request['task_id'] = dict_output['task_id']
    else:
        transfer_request['status'] = config.system_process['ERROR']
        transfer_request['error_info'] = p.stderr.decode('UTF-8')
        
    return transfer_request


def request_globus_transfer_status(job_id):

    globus_command = ["globus", "task", "show", job_id, '--format', 'json']
    s = subprocess.run(globus_command, capture_output=True)
    task_output = json.loads(s.stdout.decode('UTF-8'))

    transfer_request = dict()
    if task_output['status'] == 'SUCCEEDED':
        transfer_request['status'] = config.system_process['COMPLETED']
    elif task_output['status'] in ['PENDING','RETRYING', 'ACTIVE']:
        transfer_request['status'] = config.system_process['SUCCESS']
    else:
        transfer_request['status'] = config.system_process['ERROR']

    return transfer_request


def globus_transfer_to_tiger(job_id, raw_rel_path, modality):

    job_id_str = "job_id_"+str(job_id)+"_raw_transfer"
    source_ep = pni_ep_id
    dest_ep   = tiger_ep_dir

    source_filepath = pathlib.Path(cluster_vars['spock']['root_data_dir_globus'], modality, raw_rel_path).as_posix()
    dest_filepath = pathlib.Path(cluster_vars['tiger']['root_data_dir_globus'], modality, raw_rel_path).as_posix()

    transfer_request = request_globus_transfer(job_id_str, source_ep, dest_ep, source_filepath, dest_filepath)

    return transfer_request

def globus_transfer_to_pni(job_id, processed_rel_path, modality):

    job_id_str = "job_id_"+str(job_id)+"_processed_transfer"
    source_ep = tiger_ep_dir
    dest_ep   = pni_ep_id

    dest_filepath = pathlib.Path(cluster_vars['spock']['processed_data_dir_globus'], modality, processed_rel_path).as_posix()
    source_filepath = pathlib.Path(cluster_vars['tiger']['processed_data_dir_globus'], modality, processed_rel_path).as_posix()

    transfer_request = request_globus_transfer(job_id_str, source_ep, dest_ep, source_filepath, dest_filepath)
    
    return transfer_request

def translate_globus_output(stdout_process):

    u = stdout_process.decode('UTF-8')

    n = u.split(sep='\n')
    n2 = [x.split(sep=':', maxsplit=1) for x in n]

    flat_list = [item for l in n2 for item in l]
    flat_list2 = [x.strip() for x in flat_list]

    d1 = dict(zip(flat_list2[::2], flat_list2[1::2]))
    return d1


def transfer_log_file(recording_process_id, program_selection_params, user_host, log_type='ERROR'):
    '''
    Transfer and send parameter files for processing
    '''
    this_cluster_vars = get_cluster_vars(program_selection_params['process_cluster'])
    if log_type == 'ERROR':
        cluster_log_file_dir = this_cluster_vars['error_files_dir']
        local_log_file_dir = dj.config['custom']['error_logs_dir']
    else:
        cluster_log_file_dir = this_cluster_vars['log_files_dir']
        local_log_file_dir = dj.config['custom']['output_logs_dir']

    user_host = this_cluster_vars['user']+'@'+this_cluster_vars['hostname']

    default_log_filename = 'job_id_%s.log'

    log_filename = default_log_filename % (recording_process_id)
    log_file_local_path = pathlib.Path(local_log_file_dir,log_filename).as_posix()
    log_file_cluster_path = pathlib.Path(cluster_log_file_dir,log_filename).as_posix()
    chanmap_file_full_path = user_host+':'+log_file_cluster_path

    status = scp_file_transfer(chanmap_file_full_path, log_file_local_path)

    return status


def get_error_log_str(recording_process_id):

    error_log_data = ''
    default_log_filename = 'job_id_%s.log'

    local_log_file_dir = dj.config['custom']['error_logs_dir']
    log_filename = default_log_filename % (recording_process_id)
    log_file_local_path = pathlib.Path(local_log_file_dir,log_filename).as_posix()

    if os.path.exists(log_file_local_path):
        with open(log_file_local_path, 'r') as error_log_file:
            error_log_data = ' '.join(error_log_file.readlines())
    
    return error_log_data


def check_directory_exists_cluster(directory, cluster, modality, type='raw'):
    '''
    Check if directory exists in cluster, runs check_directory_script (check_directory.sh) in cluster machine
    Output
        dir_exists 1 if directory exists / 0 otherwise
    '''

    this_cluster_vars = get_cluster_vars(cluster)

    if type=='raw':
        final_directory = pathlib.Path(cluster_vars[cluster]['root_data_dir'], modality, directory).as_posix()
    else:
        final_directory = pathlib.Path(cluster_vars[cluster]['processed_data_dir'], modality, directory).as_posix()

    base_command = "ssh " + this_cluster_vars['user']+'@'+this_cluster_vars['hostname'] + " "

    command = "'if [ -d " +  final_directory + " ]; "
    post_command = """then
                   echo "1"  
                   else echo "0"
                   fi'"""

    command = base_command + command + post_command

    dir_exists = int(subprocess.check_output(command, shell=True).decode().strip())

    if dir_exists:
        return final_directory
    else:
        return 0


def delete_directory_cluster(directory, cluster):
    '''
    Delete directory in cluster
    '''

    this_cluster_vars = get_cluster_vars(cluster)

    command = "ssh " + this_cluster_vars['user']+'@'+this_cluster_vars['hostname']\
        + " 'rm -R " + directory + " '"

    p = subprocess.run(command, shell=True)
    output = p.returncode

    if output != 0:
        output = -1

    return output


def delete_directory_tiger_globus(modality, raw_rel_path):
    '''
    Delete directory in cluster with globus command (for Raw directory globus)
    '''

    source_ep = tiger_ep_dir
    source_filepath = pathlib.Path(cluster_vars['tiger']['root_data_dir_globus'], modality, raw_rel_path).as_posix()

    source_fullpath = source_ep+ ":" + source_filepath

    globus_command = ["globus", "delete", source_fullpath, '--recursive']
    p = subprocess.run(globus_command, capture_output=True)
    output = p.returncode

    return output


def delete_empty_data_directory_cluster(cluster, type='raw'):
    """
    Check if directory (or its childs) contains files and if not delete them
    """

    max_deletion = 10
    this_cluster_vars = get_cluster_vars(cluster)
    base_command = "ssh " + this_cluster_vars['user']+'@'+this_cluster_vars['hostname'] + ' '

    # Check base directory to delete
    if type == 'raw':
        filepath = this_cluster_vars['root_data_dir']
    else:
        filepath = this_cluster_vars['processed_data_dir']

    # Repeat always if we find a directory to delete
    total_deletion = 0
    while 1:
        deleted_dirs = 0

        # List all directories on base (raw/processed) directory
        command_list_dir = base_command + 'find ' + filepath + ' -type d  -print'
        p = subprocess.run(command_list_dir, shell=True, capture_output=True)

        list_dir = p.stdout.decode()
        list_dir = list_dir.split('\n')
        if '.' in list_dir:
            list_dir.remove('.')
        if '' in list_dir:
            list_dir.remove('')

        for dir in list_dir:

            # Check if directory has no files in it (empty)
            command = base_command + 'find ' + dir + ' -type f | wc -l'
            num_files = subprocess.check_output(command, shell=True)
            num_files = int(num_files.decode().strip())

            #If directory empty, delete it
            if num_files == 0:
                if type == 'raw':

                    # For raw directories, delete with globus
                    # Translate local directory to globus dir
                    dir_globus = dir.replace(this_cluster_vars['root_data_dir'], '')
                    dir_globus = dir_globus[1:]

                    index_sep = dir_globus.index("/")
                    modality = dir_globus[0:index_sep]
                    dir_globus = dir_globus[index_sep+1:]

                    #Delete with globus
                    status = delete_directory_tiger_globus(modality, dir_globus)
                    time.sleep(2)
                    if status == config.system_process['SUCCESS']:
                        deleted_dirs = 1
                        break
                    total_deletion +=1
                else:
                    #Delete processed files with "normal" ssh 
                    status = delete_directory_cluster(dir, cluster)
                    if status == config.system_process['SUCCESS']:
                        deleted_dirs = 1
                        break
                    total_deletion +=1
                
        # If in this round no directories were deleted we are done
        if deleted_dirs == 0 or total_deletion >= max_deletion:
            break

