import datajoint as dj
import pathlib
import subprocess
import json
import re
from element_interface.utils import dict_to_uuid

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




tiger_home_dir = '/scratch/gpfs/BRAINCOGS'    
spock_home_dir = '/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline_python'
#Cluster directories
cluster_vars = {
    "tiger": {
        "home_dir":        tiger_home_dir, 
        "root_data_dir":   tiger_home_dir + "/Data", 
        "sorted_data_dir": tiger_home_dir + "/DataSorted", 
        "slurm_files_dir": tiger_home_dir + "/slurm_files", 
        "log_files_dir":   tiger_home_dir + "/job_log", 
        "user":            default_user, 
        "hostname":        "tigergpu.princeton.edu",
        "script_path":      ""
    },
    "spock": {
        "home_dir":        spock_home_dir, 
        "root_data_dir":   dj.config['custom']['root_data_dir'], 
        "slurm_files_dir": spock_home_dir + "/slurm_files", 
        "log_files_dir":   spock_home_dir + "/job_log", 
        "user":            default_user,
        "hostname":        "spock.princeton.edu",
        "script_path":      "scripts/automate_imaging_element.py"
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
    return transfer_status


def request_globus_transfer(source, destination):

    globus_command = ["globus", "transfer", source, destination, '--recursive', '--format', 'json']
    #print(globus_command)
    #s = subprocess.run(globus_command, capture_output=True)
    #transfer_request = json.loads(s.stdout.decode('UTF-8'))
    transfer_request = dict()
    transfer_request['code'] = 'Accepted'
    transfer_request['task_id'] = dict_to_uuid({'test':1})
    return transfer_request


def request_globus_transfer_status(id_task):

    globus_command = ["globus", "task", "show", id_task, '--format', 'json']
    #print(globus_command)
    #s = subprocess.run(globus_command, capture_output=True)
    #transfer_request = json.loads(s.stdout.decode('UTF-8'))
    transfer_request = dict()
    transfer_request['status'] = 'SUCCEEDED'
    return transfer_request


def globus_transfer_to_tiger(raw_rel_path):

    source_ep = pni_ep_id + ':' + pni_root_data_dir + raw_rel_path
    dest_ep   = tiger_ep_dir + ':' + cluster_vars['tiger']['root_data_dir'] + raw_rel_path
    transfer_request = request_globus_transfer(source_ep, dest_ep)
    return transfer_request


def globus_transfer_to_pni(sorted_rel_path):

    source_ep = tiger_ep_dir + ':' + cluster_vars['tiger']['sorted_data_dir'] + sorted_rel_path
    dest_ep   = pni_ep_id  + ':' + pni_root_data_dir + sorted_rel_path
    transfer_request = request_globus_transfer(source_ep, dest_ep)
    return transfer_request
