
import pathlib
import subprocess
import json
import re


#Functions to transfer files (globus, scp, smbclient)


#FOR PNI endpoint
pni_ep_id = '6ce834d6-ff8a-11e6-bad1-22000b9a448b'
#pni_ephys_sorted_data_dir = '/mnt/cup/labs/brody/RATTER/PhysData/Test_ephys_pipeline_NP_sorted/'

#PNI directories
pni_root_data_dir   = '/braininit/Data/'

#For tiger endpoint
default_user     = 'alvaros'                         # This will change to our automatic client for globus transfers
tiger_gpu_host = 'tigergpu.princeton.edu'
tiger_ep_dir = 'a9df83d2-42f0-11e6-80cf-22000b1701d1'

# Tiger directories
tiger_home_dir   = '/tigress/alvaros'                # This will be changed to /scratch/gpfs/BRAINCOGS when permissions are granted
tiger_raw_root_data_dir = tiger_home_dir + '/DataRaw'
tiger_sorted_root_data_dir = tiger_home_dir + '/DataProcessed'
tiger_slurm_files_dir = tiger_home_dir + '/slurm_files/'
tiger_log_files_dir = tiger_home_dir + '/job_log/'


def scp_file_transfer(source, dest):

    p = subprocess.Popen(["scp", source, dest])
    transfer_status = p.wait()
    return transfer_status


def request_globus_transfer(source, destination):

    globus_command = ["globus", "transfer", source, destination, '--recursive', '--format', 'json']
    print(globus_command)
    s = subprocess.run(globus_command, capture_output=True)
    transfer_request = json.loads(s.stdout.decode('UTF-8'))
    return transfer_request


def request_globus_transfer_status(id_task):

    globus_command = ["globus", "task", "show", id_task, '--format', 'json']
    print(globus_command)
    s = subprocess.run(globus_command, capture_output=True)
    transfer_request = json.loads(s.stdout.decode('UTF-8'))
    return transfer_request


def globus_transfer_to_tiger(raw_rel_path):

    source_ep = pni_ep_id + ':' + pni_root_data_dir + raw_rel_path
    dest_ep   = tiger_ep_dir + ':' + tiger_raw_root_data_dir + raw_rel_path
    transfer_request = request_globus_transfer(source_ep, dest_ep)
    return transfer_request


def globus_transfer_to_pni(sorted_rel_path):

    source_ep = tiger_ep_dir + ':' + tiger_sorted_root_data_dir + sorted_rel_path
    dest_ep   = pni_ep_id  + ':' + pni_root_data_dir + sorted_rel_path
    transfer_request = request_globus_transfer(source_ep, dest_ep)
    return transfer_request