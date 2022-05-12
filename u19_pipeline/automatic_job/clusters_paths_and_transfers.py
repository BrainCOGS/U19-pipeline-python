import datajoint as dj
import pathlib
import subprocess
import json
import re

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
    'nodes': 2,
    'ntasks': 2,
    'time': '5:00:00',
    'mem': '200G',
    'gres': 'gpu:2',
    'mail-user': 'alvaros@princeton.edu',
    'mail-type': ['END'],
    'output': 'OutputLog/recording_process_${recording_process_id}".log'
}
slurm_dict_spock_default = {
    'job-name': 'dj_ingestion',
    'nodes': 1,
    'cpus-per-task': 1,
    'time': '5:00:00',
    'mem': '24G',
    'mail-user': 'alvaros@princeton.edu',
    'mail-type': ['END'],
    'output': 'OutputLog/recording_process_${recording_process_id}".log'
}


tiger_home_dir = '/tiger/scratch/gpfs/BRAINCOGS'    
spock_home_dir = '/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline_python'
pni_data_dir   = '/mnt/cup/braininit/Data'
#Cluster directories
cluster_vars = {
    "tiger": {
        "home_dir":                      tiger_home_dir, 
        "root_data_dir":                 tiger_home_dir + "/Data/Raw", 
        "processed_data_dir":            tiger_home_dir + "/Data/Processed", 
        "slurm_files_dir":               tiger_home_dir + "/SlurmFiles", 
        "params_files_dir":              tiger_home_dir + "/ParameterFiles", 
        "electrophysiology_process_dir": tiger_home_dir + "/electorphysiology_processing", 
        "imaging_process_dir":           tiger_home_dir + "/imaging_processing", 
        "log_files_dir":                 tiger_home_dir + "/OutputLog", 
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
        "electrophysiology_process_dir": spock_home_dir + "/electorphysiology_processing", 
        "imaging_process_dir":           spock_home_dir + "/imaging_processing",  
        "log_files_dir":                 spock_home_dir + "/u19_pipeline/automatic_job/OutputLog", 
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


def request_globus_transfer2(source, destination):

    globus_command = ["globus", "transfer", source, destination, '--recursive', '--format', 'json']
    #print(globus_command)
    #s = subprocess.run(globus_command, capture_output=True)
    #transfer_request = json.loads(s.stdout.decode('UTF-8'))
    transfer_request = dict()
    transfer_request['code'] = 'Accepted'
    transfer_request['task_id'] = dict_to_uuid({'test':1})
    return transfer_request


def request_globus_transfer_status(job_id):

    globus_job_command = ["globus-timer","job","status",job_id,"--verbose"]
    s = subprocess.run(globus_job_command, capture_output=True)
    job_output = json.load(s.stdout.decode('UTF-8'))

    task_id = job_output['results']['data'][0]['data']['details']['task_id']

    globus_task_command = ["globus","task","show",task_id,"--format","json"]
    s = subprocess.run(globus_task_command, capture_output=True)
    task_output = json.load(s.stdout.decode('UTF-8'))

    if task_output['status'] == 'SUCCEEDED':
        return 1
    elif task_output['status'] in ['PENDING','RETRYING']:
        return 0
    else:
        return -1


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
    
    print(globus_command)
    
    p = subprocess.run(globus_command, capture_output=True)

    if len(p.stderr) == 0:
        print("p.stdout", p.stdout)
        dict_output = translate_globus_output(p.stdout)
        print(dict_output)
        transfer_request['code'] = config.system_process['SUCCESS']
        transfer_request['task_id'] = dict_output['Job ID']

    else:
         transfer_request['code'] = config.system_process['ERROR']
        

    return transfer_request

def translate_globus_output(stdout_process):

    u = stdout_process.decode('UTF-8')

    n = u.split(sep='\n')
    n2 = [x.split(sep=':', maxsplit=1) for x in n]

    flat_list = [item for l in n2 for item in l]
    flat_list2 = [x.strip() for x in flat_list]

    d1 = dict(zip(flat_list2[::2], flat_list2[1::2]))
    return d1
