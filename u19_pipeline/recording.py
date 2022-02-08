import datajoint as dj
from u19_pipeline import lab, task, subject, acquisition

schema = dj.schema(dj.config['custom']['database.prefix'] + 'recording')



@schema
class RecordingModality(dj.Lookup):
     definition = """
     recording_modality:        varchar(64)          # modalities for recording (ephys, imaging, video_recording, etc.) 
     ---
     modality_description:      varchar(255)         # description for the modality
     root_directory:             varchar(255)         # root directory where that modality is stored
     recording_file_extensions: blob                 # file extensions specific for this modality
     """
     contents = [
<<<<<<< Updated upstream
        ['ephys',             '', '/braininit/Data/electrophysiology', ['ap.bin', 'ap.meta']],
        ['imaging',           '', '/braininit/Data/imaging', ['.avi', '.tiff','.tif']],
=======
        ['electrophysiology', '', '/braininit/Data/eletrophysiology', ['ap.bin', 'ap.meta']],
        ['imaging',           '', '/braininit/Data/eletrophysiology', ['.avi', '.tiff','.tif']],
>>>>>>> Stashed changes
        ['video_acquisition', '', '/braininit/Data/video_acquisition', ['.avi', '.mp4']]
     ]


@schema
class PreprocessingParamSet(dj.Lookup):
    definition = """
    # Parameter set to be used in the preprocessing steps
    preprocessing_paramset_idx:  int(11) AUTO_INCREMENT
    ---
    -> RecordingModality    
    preprocessing_paramset_desc: varchar(128)
    preprocessing_paramset_hash: uuid
    unique index (preprocessing_paramset_hash)
    preprocessing_params: longblob  # dictionary of all applicable parameters
    """

    @classmethod
    def insert_new_params(cls, recording_modality: str, paramset_idx: int,
                          paramset_desc: str, params: dict):
        param_dict = {'recording_modality': recording_modality,
                      'preprocessing_paramset_idx': paramset_idx,
                      'preprocessing_paramset_desc': paramset_desc,
                      'preprocessing_params': params,
                      'preprocessing_paramset_hash':  dict_to_uuid(params)}
        param_query = cls & {'preprocessing_paramset_hash': param_dict['preprocessing_paramset_hash']}

        if param_query:  # If the specified param-set already exists
            existing_paramset_idx = param_query.fetch1('preprocessing_paramset_idx')
            if existing_paramset_idx == paramset_idx:  # If the existing set has the same paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
                raise dj.DataJointError(
                    'The specified param-set'
                    ' already exists - preprocess_paramset_idx: {}'.format(existing_paramset_idx))
        else:
            cls.insert1(paramset_dict)


@schema
class ProcessingParamSet(dj.Lookup):
    definition = """
    # Parameter set to be used in the preprocessing steps
    processing_paramset_idx:  int(11) AUTO_INCREMENT
    ---
    -> RecordingModality    
    processing_paramset_desc: varchar(128)
    processing_paramset_hash: uuid
    unique index (processing_paramset_hash)
    processing_params: longblob  # dictionary of all applicable parameters
    """

    @classmethod
    def insert_new_params(cls, recording_modality: str, paramset_idx: int,
                          paramset_desc: str, params: dict):
        param_dict = {'recording_modality': recording_modality,
                      'processing_paramset_idx': paramset_idx,
                      'processing_paramset_desc': paramset_desc,
                      'processing_params': params,
                      'processing_paramset_hash':  dict_to_uuid(params)}
        param_query = cls & {'processing_paramset_hash': param_dict['processing_paramset_hash']}

        if param_query:  # If the specified param-set already exists
            existing_paramset_idx = param_query.fetch1('processing_paramset_idx')
            if existing_paramset_idx == paramset_idx:  # If the existing set has the same paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
                raise dj.DataJointError(
                    'The specified param-set'
                    ' already exists - paramset_idx: {}'.format(existing_paramset_idx))
        else:
            cls.insert1(param_dict)

@schema
class Recording(dj.Manual):
     definition = """
     recording_id:                      INT(11) AUTO_INCREMENT                  # Unique number assigned to each recording   
     -----
     -> [nullable] acquisition.Session                                          # acquisition Session key
     -> RecordingModality                         
<<<<<<< Updated upstream
     recording_directory:               varchar(255)                            # relative directory where the data for this session will be stored on cup
=======
     -> lab.Location
     recording_directory:               varchar(255)                            # the relative directory where the ephys data for this session will be stored in braininit drive
     local_directory:                   varchar(255)                            # local directory where this file is stored on the recording system
     -> [nullable] subject.Subject.proj(acquisition_subject='subject_fullname') # Recording subject when no behavior Session present
     recording_datetime=null:           datetime                                # Recording datetime when no behavior Session present
>>>>>>> Stashed changes
     """    


status_pipeline_dict = {
    'ERROR':             {'Value': -1,
                         'Label': 'Error in process',
                         'Task_Field': None},
    'NEW_SESSION':       {'Value': 0,
                         'Label': 'New session',
                         'Task_Field': None},
    'PNI_DRIVE_TRANSFER_REQUEST':       {'Value': 1,
                                        'Label': 'Recording directory transfer to PNI requested',
                         '               Task_Field': 'task_copy_id_pni'},
    'PNI_DRIVE_TRANSFER_END':           {'Value': 2,
                                        'Label': 'Recording directory transferred to PNI',
                         '               Task_Field': None},
    'MODALITY_PREINGESTION':           {'Value': 3,
                                        'Label': 'Preprocessing & Syncing jobs',
                         '               Task_Field': None},
    'RAW_FILE_REQUEST':  {'Value': 4,
                          'Label': 'Raw file transfer requested',
                          'Task_Field': 'task_copy_id_pre_path'},
    'RAW_FILE_CLUSTER':  {'Value': 5,
                         'Label': 'Raw file transferred to cluster',
                         'Task_Field': None},
    'JOB_QUEUE':         {'Value': 6,
                         'Label': 'Processing job in queue',
                         'Task_Field': 'slurm_id_sorting'},
    'JOB_FINISHED':      {'Value': 7,
                         'Label': 'Processing job finished',
                         'Task_Field': None},
    'PROC_FILE_REQUEST': {'Value': 8,
                         'Label': 'Processed file transfer requested',
                         'Task_Field': 'task_copy_id_pos_path'},
    'PROC_FILE_HOME':    {'Value': 9,
                         'Label': 'Processed file transferred to PNI',
                         'Task_Field': None},
    'CANONICAL_PIPELINE': {'Value': 10,
                         'Label': 'Processed with Canonical pipeline',
                         'Task_Field': None},
}

def get_content_list_from_status_dict():
    return [[status_pipeline_dict[i]['Value'], status_pipeline_dict[i]['Label']] for i in status_pipeline_dict.keys()]


@schema
class StatusProcessDefinition(dj.Lookup):
     definition = """
     status_pipeline_idx:                    TINYINT(1)      # status in the automate process pipeline
     ---
     status_definition:                  VARCHAR(256)    # Status definition 
     """
     contents = get_content_list_from_status_dict()


@schema
class RecordingProcess(dj.Manual):
     definition = """
     recording_process_id:              INT(11) AUTO_INCREMENT    # Unique number assigned to each processing job for a recording 
     -----
     -> Recording
     -> StatusProcessDefinition                                   # current status in the pipeline
     -> PreprocessingParamSet                                     # reference to params to preprocess recording
     -> ProcessingParamSet                                        # reference to params to process recording
     recording_process_path=null:       VARCHAR(200)              # relative path for processed recording
     task_copy_id_pni=null:             UUID                      # id for globus transfer task raw file local->cup
     task_copy_id_pre=null:             UUID                      # id for globus transfer task raw file cup->tiger  
     task_copy_id_pos=null:             UUID                      # id for globus transfer task sorted file tiger->cup
     slurm_id=null:                     VARCHAR(16)               # id for slurm process in tiger
     """    


@schema
class RecordingProcessStatus(dj.Manual):
     definition = """
     -> RecordingProcess
     -----
     -> StatusProcessDefinition.proj(status_pipeline_old='status_pipeline') # old status in the pipeline
     -> StatusProcessDefinition.proj(status_pipeline_new='status_pipeline') # current status in the pipeline
     status_timestamp:                  DATETIME        # timestamp when status change ocurred
     error_message=null:                VARCHAR(4096)   # Error message if status now is failed
     error_exception=null:              BLOB            # Error exception if status now is failed
     """
