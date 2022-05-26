import datajoint as dj
import pathlib
import subprocess
import json
import re
import os

from datetime import datetime
from element_interface.utils import dict_to_uuid

import u19_pipeline.automatic_job.params_config as config
#Functions to transfer files (globus, scp, smbclient)

#FOR PNI endpoint
pni_ep_id = '6ce834d6-ff8a-11e6-bad1-22000b9a448b'
#pni_ephys_sorted_data_dir = '/mnt/cup/labs/brody/RATTER/PhysData/Test_ephys_pipeline_NP_sorted/'

#PNI directories
pni_root_data_dir   = dj.config['custom']['root_data_dir']

#For tiger endpoint
default_user     = 'alvaros'                         # This will change to our automatic client for globus transfers
tiger_gpu_host = 'tigergpu.princeton.edu'
tiger_ep_dir = 'a9df83d2-42f0-11e6-80cf-22000b1701d1'

#Slurm default values for queue job
slurm_dict_tiger_default = {
    'job-name': 'kilosort2',
    'nodes': 1,
    'ntasks': 1,
    'time': '5:00:00',
    'mem': '200G',
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
    'time': '5:00:00',
    'mem': '24G',
    'mail-type': ['END', 'FAIL'],
    'output': 'OutputLog/job_id_${job_id}".log',
    'error': 'ErrorLog/job_id_${job_id}".log'
}


tiger_home_dir_globus = '/tiger/scratch/gpfs/BRAINCOGS'    
tiger_home_dir = '/scratch/gpfs/BRAINCOGS'    
spock_home_dir = '/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline_python'
pni_data_dir   = '/mnt/cup/braininit/Data'
#Cluster directories
cluster_vars = {
    "tiger": {
        "home_dir":                      tiger_home_dir, 
        "root_data_dir":                 tiger_home_dir_globus + "/Data/Raw", 
        "processed_data_dir":            tiger_home_dir_globus + "/Data/Processed", 
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
        "script_path":                   ""
    },
    "spock": {
        "home_dir":                      spock_home_dir, 
        "root_data_dir":                 pni_data_dir   + "/Raw",
        "processed_data_dir":            pni_data_dir   + "/Processed",
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
        "script_path":                   ""
    }
}


def get_cluster_vars(cluster):

    if cluster in cluster_vars:
        return cluster_vars[cluster]
    else:
        raise('Non existing cluster')


def scp_file_transfer(source, dest):

    print("scp", source, dest)
    p = subprocess.Popen(["scp", "-i", "~/.ssh/id_rsa_alvaros_tiger.pub", source, dest])
    transfer_status = p.wait()
    print(transfer_status)
    return transfer_status


def cp_file_transfer(source, dest):

    print("cp", source, dest)
    p = subprocess.Popen(["cp", source, dest])
    transfer_status = p.wait()
    print(transfer_status)
    return transfer_status


def request_globus_transfer_status(job_id):

    transfer_request = dict()
    globus_job_command = ["globus-timer","job","status",job_id,"--verbose"]
    s = subprocess.run(globus_job_command, capture_output=True)

    if s.stderr:
        print(s.stderr)

    job_output = json.loads(s.stdout.decode('UTF-8'))

    task_id = job_output['results']['data'][0]['data']['details']['task_id']

    globus_task_command = ["globus","task","show",task_id,"--format","json"]
    s = subprocess.run(globus_task_command, capture_output=True)
    task_output = json.loads(s.stdout.decode('UTF-8'))

    if task_output['status'] == 'SUCCEEDED':
        transfer_request['status'] = config.system_process['COMPLETED']
    elif task_output['status'] in ['PENDING','RETRYING', 'ACTIVE']:
        transfer_request['status'] = config.system_process['SUCCESS']
    else:
        transfer_request['status'] = config.system_process['ERROR']

    return transfer_request


def request_globus_transfer(job_id_str, source_ep, dest_ep, source_filepath, dest_filepath):

    transfer_request = dict()

    now = datetime.now()
    date_time = now.strftime("%Y-%m-%dT23:59:59")

    globus_command = ["globus-timer", "job", "transfer", 
    "--name",   job_id_str,
    "--label",  job_id_str,
    "--interval", "72h",
    "--stop-after-date", date_time,
    "--source-endpoint", source_ep,
    "--dest-endpoint", dest_ep,
    "--item",  source_filepath, dest_filepath, "true",
    "--verbose"]
        
    p = subprocess.run(globus_command, capture_output=True)

    if len(p.stderr) == 0:
        dict_output = json.loads(p.stdout.decode('UTF-8'))
        #dict_output = translate_globus_output(p.stdout)
        transfer_request['status'] = config.system_process['SUCCESS']
        transfer_request['task_id'] = dict_output['job_id']
    else:
        transfer_request['status'] = config.system_process['ERROR']
        transfer_request['error_info'] = p.stderr.decode('UTF-8')
        
    return transfer_request


def globus_transfer_to_tiger(job_id, raw_rel_path, modality):

    job_id_str = "job_id_"+str(job_id)+"_raw_transfer"
    source_ep = pni_ep_id
    dest_ep   = tiger_ep_dir

    source_filepath = pathlib.Path(cluster_vars['spock']['root_data_dir'], modality, raw_rel_path).as_posix()
    dest_filepath = pathlib.Path(cluster_vars['tiger']['root_data_dir'], modality, raw_rel_path).as_posix()

    transfer_request = request_globus_transfer(job_id_str, source_ep, dest_ep, source_filepath, dest_filepath)

    return transfer_request

def globus_transfer_to_pni(job_id, processed_rel_path, modality):

    job_id_str = "job_id_"+str(job_id)+"_processed_transfer"
    source_ep = tiger_ep_dir
    dest_ep   = pni_ep_id

    dest_filepath = pathlib.Path(cluster_vars['spock']['processed_data_dir'], modality, processed_rel_path).as_posix()
    source_filepath = pathlib.Path(cluster_vars['tiger']['processed_data_dir'], modality, processed_rel_path).as_posix()

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