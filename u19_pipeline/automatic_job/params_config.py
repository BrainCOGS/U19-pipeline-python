
import pandas as pd
import numpy as np
import os
import pathlib

recording_modality_dict = [
    {
        'RecordingModality': 'electrophysiology',
        'Description': '',
        'RootDirectorty': '/braininit/Data/electrophysiology',
        'FileExtensions': np.asarray(['ap.bin', 'ap.meta']),
        'RecordingFilePattern': np.asarray(['/*g[0-9]/*imec[0-9]']),
        'ProcessUnitFilePattern': np.asarray(['/*imec[0-9]/']),
        'ProcessUnitDirectoryField': 'probe_directory',
        'ProcessUnitField': 'probe',
        'ProcessingRepository': 'BrainCogsEphysSorters',
    },
    {
        'RecordingModality': 'imaging',
        'Description': '',
        'RootDirectorty': '/braininit/Data/imaging',
        'FileExtensions': np.asarray(['.avi', '.tiff','.tif']),
        'RecordingFilePattern': np.asarray(['']),
        'ProcessUnitFilePattern': np.asarray(['']),
        'ProcessUnitDirectoryField': 'fov_directory',
        'ProcessUnitField': 'fov',
        'ProcessingRepository': 'BrainCogsImagingSegmentation',
    },
    {
        'RecordingModality': 'video_acquisition',
        'Description': '',
        'RootDirectorty': '/braininit/Data/imaging',
        'FileExtensions': np.asarray(['.avi', '.mp4']),
        'RecordingFilePattern': np.asarray(['']),
        'ProcessUnitFilePattern': np.asarray(['']),
        'ProcessUnitDirectoryField': 'video_directory',
        'ProcessUnitField': '',
        'ProcessingRepository': 'None',
    },
]

recording_modality_list = [list(i.values()) for i in recording_modality_dict]
recording_modality_df = pd.DataFrame(recording_modality_dict)

recording_status_dict = [
    {
        'Value': -1,
        'Key': 'ERROR',
        'Label': 'Error in recording handling',
        'UpdateField': None,
        'ProcessFunction': None,
        'FunctionField': None,
    },
    {
        'Value': 0,
        'Key': 'NEW_RECORDING',
        'Label': 'New recording',
        'UpdateField': None,
        'ProcessFunction': None,
        'FunctionField': None,
    },
    {
        'Value': 1,
        'Key': 'PNI_DRIVE_TRANSFER_REQUEST',
        'Label': 'Recording directory transfer to PNI requested',
        'UpdateField': 'task_copy_id_pni',
        'ProcessFunction': 'local_transfer_request',
        'FunctionField': 'recording_process_pre_path',
    },
    {
        'Value': 2,
        'Key': 'PNI_DRIVE_TRANSFER_END',
        'Label': 'Recording directory transferred to PNI',
        'UpdateField': None,
        'ProcessFunction': 'local_transfer_check',
        'FunctionField': 'task_copy_id_pni',
    },
    {
        'Value': 3,
        'Key': 'MODALITY_PREINGESTION',
        'Label': 'modality ingestion & Syncing jobs done',
        'UpdateField': None,
        'ProcessFunction': 'modality_preingestion',
        'FunctionField': None,
    },
  
]

recording_status_list = [[i['Value'], i['Label']] for i in recording_status_dict]
recording_status_df = pd.DataFrame(recording_status_dict)
RECORDING_STATUS_ERROR_ID = recording_status_df.loc[recording_status_df['Key'] == 'ERROR', 'Value'].values[0]

status_update_idx = {
    'NEXT_STATUS': 1,
    'NO_CHANGE': 0,
    'ERROR_STATUS':-1
}

default_update_value_dict ={
    'value_update': None,
    'error_info': {
        'error_message': None,
        'error_exception': None,
    },
}

recording_process_status_dict = [
    {
        'Value': -1,
        'Key': 'ERROR',
        'Label': 'Error in recording process',
        'UpdateField': None,
        'ProcessFunction': None,
        'FunctionField': None,
    },
    {
        'Value': 0,
        'Key': 'NEW_RECORDING_PROCESS',
        'Label': 'New recording process',
        'UpdateField': None,
        'ProcessFunction': None,
        'FunctionField': None,
    },
    {
        'Value': 1,
        'Key': 'RAW_FILE_TRANSFER_REQUEST',
        'Label': 'Raw file transfer requested',
        'UpdateField': 'task_copy_id_pre',
        'ProcessFunction': 'transfer_request',
        'FunctionField': 'recording_process_pre_path',
    },
    {
        'Value': 2,
        'Key': 'RAW_FILE_TRANSFER_END',
        'Label': 'Raw file transferred to cluster',
        'UpdateField': None,
        'ProcessFunction': 'transfer_check',
        'FunctionField': 'task_copy_id_pre', 
    },
    {
        'Value': 3,
        'Key': 'JOB_QUEUE',
        'Label': 'Processing job in queue',
        'UpdateField': 'slurm_id',
        'ProcessFunction': 'slurm_job_queue',
        'FunctionField': None,
    },
    {
        'Value': 4,
        'Key': 'JOB_FINISHED',
        'Label': 'Processing job finished',
        'UpdateField': None,
        'ProcessFunction': 'slurm_job_check',
        'FunctionField': 'slurm_id',
    },
    {
        'Value': 5,
        'Key': 'PROC_FILE_TRANSFER_REQUEST',
        'Label': 'Processed file transfer requested',
        'UpdateField': 'task_copy_id_post',
        'ProcessFunction': 'transfer_request',
        'FunctionField': 'recording_process_post_path',
    },
    {
        'Value': 6,
        'Key': 'PROC_FILE_TRANSFER_END',
        'Label': 'Processed file transferred to PNI',
        'UpdateField': None,
        'ProcessFunction': 'transfer_check',
        'FunctionField': 'task_copy_id_post',
    },
    {
        'Value': 7,
        'Key': 'JOB_FINSISHED_ELEMENT_WORKFLOW',
        'Label': 'Element ingested, finished',
        'UpdateField': None,
        'ProcessFunction': 'populate_element',
        'FunctionField': None,
    },
]

all_preprocess_params = {
"process_cluster": [
    "tiger",
    "spock"],
"dj_element_processing":[ 
    "trigger",
    "load",
],  
"processing_algorithm": [
    "kilosort2",
    "suite2p",
]
}

recording_process_status_list = [[i['Value'], i['Label']] for i in recording_process_status_dict]
recording_process_status_df = pd.DataFrame(recording_process_status_dict)
RECORDING_PROCESS_STATUS_ERROR_ID = recording_status_df.loc[recording_status_df['Key'] == 'ERROR', 'Value'].values[0]

system_process = {
    'COMPLETED': 1,
    'SUCCESS':   0,
    'ERROR':    -1
}

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


program_selection_params = {
    'process_cluster': 'tiger',
    'process_repository': 'BrainCogsEphysSorters',
    'process_script': 'main_script.py'
}


startup_pipeline_matlab_dir = '/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline-matlab/scripts'
ingest_scaninfo_script = '/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline_python/u19_pipeline/automatic_job/ingest_scaninfo_shell.sh'

# For parameter & channmap storing
this_dir = os.path.dirname(__file__)

parameter_files_filepath = pathlib.Path(this_dir, 'ParameterFiles').as_posix()
default_preprocess_filename = 'preprocess_paramset_%s.json'
default_process_filename = 'process_paramset_%s.json'


chanmap_files_filepath = pathlib.Path(this_dir, 'ChanMapFiles').as_posix()
default_chanmap_filename = 'chanmap_%s.mat'