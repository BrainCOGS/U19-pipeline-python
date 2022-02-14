import pandas as pd
import numpy as np

recording_modality_dict = [
    {
        'RecordingModality': 'electrophysiology',
        'Description': '',
        'RootDirectorty': '/braininit/Data/electrophysiology',
        'FileExtensions': np.asarray(['ap.bin', 'ap.meta']),
        'RecordingFilePattern': np.asarray(['/*g[0-9]/*imec[0-9]']),
        'ProcessUnitFilePattern': np.asarray(['/*imec[0-9]/']),
        'ProcessUnitDirectoryField': 'probe_directory',
    },
    {
        'RecordingModality': 'imaging',
        'Description': '',
        'RootDirectorty': '/braininit/Data/imaging',
        'FileExtensions': np.asarray(['.avi', '.tiff','.tif']),
        'RecordingFilePattern': np.asarray(['']),
        'ProcessUnitFilePattern': np.asarray(['']),
        'ProcessUnitDirectoryField': 'fov_directory',
    },
    {
        'RecordingModality': 'video_acquisition',
        'Description': '',
        'RootDirectorty': '/braininit/Data/imaging',
        'FileExtensions': np.asarray(['.avi', '.mp4']),
        'RecordingFilePattern': np.asarray(['']),
        'ProcessUnitFilePattern': np.asarray(['']),
        'ProcessUnitDirectoryField': 'video_directory'
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
        'UpdateField': 'task_copy_id_pre_path',
        'ProcessFunction': 'transfer_request',
        'FunctionField': 'recording_process_pre_path',
    },
    {
        'Value': 2,
        'Key': 'RAW_FILE_TRANSFER_END',
        'Label': 'Raw file transferred to cluster',
        'UpdateField': None,
        'ProcessFunction': 'transfer_check',
        'FunctionField': 'task_copy_id_pre_path',
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
        'Key': 'JOB_QUEUE_ELEMENT_WORKFLOW',
        'Label': 'Job Queue Element Workflow ingestion',
        'UpdateField': None,
        'ProcessFunction': None,
        #'ProcessFunction': RecProcessHandler.slurm_job_element,
        'FunctionField': None,
    },
    {
        'Value': 8,
        'Key': 'JOB_FINSISHED_ELEMENT_WORKFLOW',
        'Label': 'Process finished',
        'UpdateField': None,
        'ProcessFunction': 'slurm_job_check',
        'FunctionField': None,
    }
]

recording_process_status_list = [[i['Value'], i['Label']] for i in recording_process_status_dict]

system_process = {
    'SUCCESS': 0
}


startup_pipeline_matlab_dir = '/usr/people/alvaros/BrainCogsProjects/Datajoint_projs/U19-pipeline-matlab/scripts'