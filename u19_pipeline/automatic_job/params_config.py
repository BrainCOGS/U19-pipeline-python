
import os
import pathlib

import pandas as pd

import u19_pipeline.lab as lab
from scripts.conf_file_finding import get_root_directory

#Dictionary with main configuration for each modality (ephys or imaging)
recording_modality_dict = [
    {
        'recording_modality': 'electrophysiology',
        'local_or_cluster': 'cluster', # Where processing will happen, locally or cluster
        'process_repository': 'BrainCogsEphysSorters', # Which repositroy will be used to process
        'process_cluster': 'spock', # Which cluster will be used to process (check clusters_paths_and_transfers.py file)
        'process_script': 'main_script.py' # Script in process_repository to 
    },
    {
        'recording_modality': 'imaging',
        'local_or_cluster': 'local',
        'process_repository': 'element-calcium-imaging',
        'process_cluster': 'spock', # Which cluster will be used to process (check clusters_paths_and_transfers.py file)
        'process_script': 'none'
    },
]

recording_modality_list = [list(i.values()) for i in recording_modality_dict]
recording_modality_df = pd.DataFrame(recording_modality_dict)

#Dictionary with status configuration for recordings
recording_status_dict = [
    {
        'Value': -1,     # Status id
        'Key': 'ERROR',  # Status "nickname"
        'Label': 'Error in recording handling', # Status description
        'UpdateField': None,       # Which field in the u19_recording.recording table will be updated
        'ProcessFunction': None,   # Which function to execute in workflow for this status
        'FunctionField': None,     # Which field of u19_recording.recording will be used in status function
        'SlackMessage': None       # Slack notification message for this status
    },
    {
        'Value': 0,
        'Key': 'NEW_RECORDING',
        'Label': 'New recording',
        'UpdateField': None,
        'ProcessFunction': None,
        'FunctionField': None,
        'SlackMessage': None
    },
    {
        'Value': 1,
        'Key': 'PNI_DRIVE_TRANSFER_REQUEST',
        'Label': 'Recording directory transfer to PNI requested',
        'UpdateField': 'task_copy_id_pni',
        'ProcessFunction': 'local_transfer_request',
        'FunctionField': 'recording_process_pre_path',
        'SlackMessage': None
    },
    {
        'Value': 2,
        'Key': 'PNI_DRIVE_TRANSFER_END',
        'Label': 'Recording directory transferred to PNI',
        'UpdateField': None,
        'ProcessFunction': 'local_transfer_check',
        'FunctionField': 'task_copy_id_pni',
        'SlackMessage': 'Recording was transferred to braininit (cup) drive' 
    },
    {
        'Value': 3,
        'Key': 'MODALITY_PREINGESTION',
        'Label': 'modality ingestion & Syncing jobs done',
        'UpdateField': None,
        'ProcessFunction': 'modality_preingestion',
        'FunctionField': None,
        'SlackMessage': None 
    },
  
]

recording_status_list = [[i['Value'], i['Label']] for i in recording_status_dict]
recording_status_df = pd.DataFrame(recording_status_dict)
RECORDING_STATUS_ERROR_ID = recording_status_df.loc[recording_status_df['Key'] == 'ERROR', 'Value'].values[0]

#Dictionary with status configuration for recording process (units of each recording)
recording_process_status_dict = [
    {
        'Value': -2,
        'Key': 'ERROR_DELETED',
        'Label': 'Error in recording process / Deleted cluster files',
        'UpdateField': None,
        'ProcessFunction': None,
        'FunctionField': None,
        'SlackMessage': None
    },
    {
        'Value': -1,
        'Key': 'ERROR',
        'Label': 'Error in recording process',
        'UpdateField': None,
        'ProcessFunction': None,
        'FunctionField': None,
        'SlackMessage': None
    },
    {
        'Value': 0,
        'Key': 'NEW_RECORDING_PROCESS',
        'Label': 'New recording process',
        'UpdateField': None,
        'ProcessFunction': None,
        'FunctionField': None,
        'SlackMessage': None
    },
    {
        'Value': 1,
        'Key': 'RAW_FILE_TRANSFER_REQUEST',
        'Label': 'Raw file transfer requested',
        'UpdateField': 'task_copy_id_pre',
        'ProcessFunction': 'transfer_request',
        'FunctionField': 'recording_process_pre_path',
        'SlackMessage': None
    },
    {
        'Value': 2,
        'Key': 'RAW_FILE_TRANSFER_END',
        'Label': 'Raw file transferred to cluster',
        'UpdateField': None,
        'ProcessFunction': 'transfer_check',
        'FunctionField': 'task_copy_id_pre',
        'SlackMessage': None
    },
    {
        'Value': 3,
        'Key': 'JOB_QUEUE',
        'Label': 'Processing job in queue',
        'UpdateField': 'slurm_id',
        'ProcessFunction': 'slurm_job_queue',
        'FunctionField': None,
        'SlackMessage': 'Job was queued to be processed in cluster' 
    },
    {
        'Value': 4,
        'Key': 'JOB_FINISHED',
        'Label': 'Processing job finished',
        'UpdateField': None,
        'ProcessFunction': 'slurm_job_check',
        'FunctionField': 'slurm_id',
        'SlackMessage': None
    },
    {
        'Value': 5,
        'Key': 'PROC_FILE_TRANSFER_REQUEST',
        'Label': 'Processed file transfer requested',
        'UpdateField': 'task_copy_id_post',
        'ProcessFunction': 'transfer_request',
        'FunctionField': 'recording_process_post_path',
        'SlackMessage': None
    },
    {
        'Value': 6,
        'Key': 'PROC_FILE_TRANSFER_END',
        'Label': 'Processed file transferred to PNI',
        'UpdateField': None,
        'ProcessFunction': 'transfer_check',
        'FunctionField': 'task_copy_id_post',
        'SlackMessage': 'Processed data was transferred to cup. Available on the GUI'
    },
    {
        'Value': 7,
        'Key': 'JOB_FINISHED_ELEMENT_WORKFLOW',
        'Label': 'Data in element, Finished !!',
        'UpdateField': None,
        'ProcessFunction': 'populate_element',
        'FunctionField': None,
        'SlackMessage': 'Job was successfully processed. Data in element DB' 
    },
    {
        'Value': 8,
        'Key': 'JOB_DATA_DELETED_CLUSTER',
        'Label': 'Data in element, Finished !!',
        'UpdateField': None,
        'ProcessFunction': 'populate_element',
        'FunctionField': None,
        'SlackMessage': None
    },
]

recording_process_status_list = [[i['Value'], i['Label']] for i in recording_process_status_dict]
recording_process_status_df = pd.DataFrame(recording_process_status_dict)

# All error and success status values
JOB_STATUS_ERROR_ID = recording_process_status_df.loc[recording_process_status_df['Key'] == 'ERROR', 'Value'].values[0]
JOB_STATUS_PROCESSED = recording_process_status_df.loc[recording_process_status_df['Key'] == 'JOB_FINISHED_ELEMENT_WORKFLOW', 'Value'].values[0]
JOB_STATUS_POST_PROCESSED = recording_process_status_df.loc[recording_process_status_df['Key'] == 'JOB_DATA_DELETED_CLUSTER', 'Value'].values[0]
JOB_STATUS_ERROR_DELETED = recording_process_status_df.loc[recording_process_status_df['Key'] == 'ERROR_DELETED', 'Value'].values[0]

# Return code for functions for errors, next state or no status change
status_update_idx = {
    'NEXT_STATUS': 1,
    'NO_CHANGE': 0,
    'ERROR_STATUS':-1
}

# Degault Dictionary "skeleton" that is returned in all functions of workflow
default_update_value_dict ={
    'value_update': None,
    'error_info': {
        'error_message': None,
        'error_exception': None,
    },
}

# Return Codes from 
system_process = {
    'COMPLETED': 1,
    'SUCCESS':   0,
    'ERROR':    -1
}

# Possible states from slurm queued jobs results and result in pipeline status (next state, no change or error)
slurm_states = {
    'COMPLETED': {
        'pipeline_status': status_update_idx['NEXT_STATUS'],
        'message':         ''
    },
    'PENDING':   {
        'pipeline_status': status_update_idx['NO_CHANGE'],
        'message':         ''
    },
    'RUNNING':   {
        'pipeline_status': status_update_idx['NO_CHANGE'],
        'message':         ''
    },
    'FAILED':    {
        'pipeline_status': status_update_idx['ERROR_STATUS'],
        'message':         ''
    },
    'TIMEOUT':
    {
        'pipeline_status': status_update_idx['ERROR_STATUS'],
        'message':         'Timeout for job has expired'
    },
    'CANCELLED+':
        {
        'pipeline_status': status_update_idx['ERROR_STATUS'],
        'message':         'Job was cancelled'
        },
    'CANCELLED':
        {
        'pipeline_status': status_update_idx['ERROR_STATUS'],
        'message':         'Job was cancelled'
        }
}


# Look for u19_matlab_dir (Should be present on same directory as U19-Pipeline_Python)
_, u19_pipeline_python_dir = get_root_directory()
datajoint_proj_dir = u19_pipeline_python_dir.parent
u19_matlab_dir = pathlib.Path(datajoint_proj_dir, 'U19-pipeline-matlab')
startup_pipeline_matlab_dir = pathlib.Path(u19_matlab_dir, 'scripts').as_posix()

this_dir = os.path.dirname(__file__)
ingest_scaninfo_script = pathlib.Path(this_dir, 'ingest_scaninfo_shell.sh').as_posix()

# Look for CATGT directory (Should be present on same directory as U19-Pipeline_Python)
catgt_dir = pathlib.Path(datajoint_proj_dir, 'CatGT-linux')
catgt_script = pathlib.Path(catgt_dir, 'runit.sh').as_posix()

# For parameter & channmap storing
parameter_files_filepath = pathlib.Path(this_dir, 'ParameterFiles').as_posix()
default_preprocess_filename = 'preprocess_paramset_%s.json'
default_process_filename = 'process_paramset_%s.json'

# default xyzPicksFiles
xyz_picks_files_dir = pathlib.Path(this_dir, 'xyzPicksFiles')
xyz_picks_0 = pathlib.Path(xyz_picks_files_dir, 'xyz_picks.json').as_posix()
xyz_picks_n = pathlib.Path(xyz_picks_files_dir, 'xyz_picks_shank%d.json').as_posix()

#chanmap files
chanmap_files_filepath = pathlib.Path(this_dir, 'ChanMapFiles').as_posix()
default_chanmap_filename = 'chanmap_%s.mat'


#Slack notification channels
slack_webhooks = lab.SlackWebhooks.fetch()

slack_webhooks_dict = dict()
for i in range(slack_webhooks.shape[0]):
    slack_webhooks_dict[slack_webhooks[i][0]] = slack_webhooks[i][1]