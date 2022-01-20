import datajoint as dj
from u19_pipeline import lab, task, subject

schema = dj.schema('u19_recording')

from u19_pipeline import acquisition, subject


@schema
class RecordingModality(dj.Lookup):
     definition = """
     recording_modality:   varchar(64)                  # modalities for recording (ephys, imaging, video_recording, etc.) 
     ---
     modality_description: varchar(255)                 # description for the modality
     root_direcory:        varchar(255)                 # root directory where that modality is stored (e.g. ephys = /braininit/Data/eletrophysiology)
     """
     contents = [
        ['ephys',             '', '/braininit/Data/eletrophysiology'],
        ['imaging',           '', '/braininit/Data/eletrophysiology'],
        ['video_acquisition', '', '/braininit/Data/video_acquisition']
     ]


@schema
class PreprocessingParamSet(dj.Lookup):
    definition = """
    # Parameter set to be used in the preprocessing steps
    preprocess_paramset_idx:  smallint
    ---
    -> RecordingModality    
    preprocess_paramset_desc: varchar(128)
    preprocess_param_set_hash: uuid
    unique index (preprocess_param_set_hash)
    preprocessing_params: longblob  # dictionary of all applicable parameters
    """

    @classmethod
    def insert_new_params(cls, recording_modality: str, paramset_idx: int,
                          paramset_desc: str, params: dict):
        param_dict = {'recording_modality': recording_modality,
                      'preprocess_paramset_idx': paramset_idx,
                      'preprocess_paramset_desc': paramset_desc,
                      'preprocessing_params': params,
                      'preprocess_param_set_hash':  dict_to_uuid(params)}
        param_query = cls & {'preprocess_param_set_hash': param_dict['preprocess_param_set_hash']}

        if param_query:  # If the specified param-set already exists
            existing_paramset_idx = param_query.fetch1('preprocess_paramset_idx')
            if existing_paramset_idx == paramset_idx:  # If the existing set has the same paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
                raise dj.DataJointError(
                    'The specified param-set'
                    ' already exists - paramset_idx: {}'.format(existing_paramset_idx))
        else:
            cls.insert1(param_dict)


@schema
class ProcessingParamSet(dj.Lookup):
    definition = """
    # Parameter set to be used in the preprocessing steps
    processing_paramset_idx:  smallint
    ---
    -> RecordingModality    
    processing_paramset_desc: varchar(128)
    processing_param_set_hash: uuid
    unique index (processing_param_set_hash)
    processing_params: longblob  # dictionary of all applicable parameters
    """

    @classmethod
    def insert_new_params(cls, recording_modality: str, paramset_idx: int,
                          paramset_desc: str, params: dict):
        param_dict = {'recording_modality': recording_modality,
                      'processing_paramset_idx': paramset_idx,
                      'processing_paramset_desc': paramset_desc,
                      'processing_params': params,
                      'processing_param_set_hash':  dict_to_uuid(params)}
        param_query = cls & {'processing_param_set_hash': param_dict['processing_param_set_hash']}

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
     -> [nullable] subject.Subject.proj(acquisition_subject='subject_fullname') # Recording subject when no behavior Session present
     recording_datetime=null:           datetime                                # Recording datetime when no bheavior Session present
     -> RecordingModality                         
     recording_directory:               varchar(255)                            # the relative directory where the ephys data for this session will be stored in bucket
     """    


#Status pipeline dictionary
status_pipeline_dict = {
    'ERROR':             {'Value': -1,
                         'Label': 'Error in process',
                         'Task_Field': None},
    'NEW_SESSION':       {'Value': 0,
                         'Label': 'New session',
                         'Task_Field': None},
    'RAW_FILE_REQUEST':  {'Value': 1,
                          'Label': 'Raw file transfer requested',
                          'Task_Field': 'task_copy_id_pre_path'},
    'RAW_FILE_CLUSTER':  {'Value': 2,
                         'Label': 'Raw file transferred to cluster',
                         'Task_Field': None},
    'JOB_QUEUE':         {'Value': 3,
                         'Label': 'Processing job in queue',
                         'Task_Field': 'slurm_id_sorting'},
    'JOB_FINISHED':      {'Value': 4,
                         'Label': 'Processing job finished',
                         'Task_Field': None},
    'PROC_FILE_REQUEST': {'Value': 5,
                         'Label': 'Processed file transfer requested',
                         'Task_Field': 'task_copy_id_pos_path'},
    'PROC_FILE_HOME':    {'Value': 6,
                         'Label': 'Processed file transferred to PNI',
                         'Task_Field': None},
    'CANONICAL_PIPELINE': {'Value': 7,
                         'Label': 'Processed with Canonical pipeline',
                         'Task_Field': None},
}

def get_content_list_from_status_dict():
    contents = list()
    for i in status_pipeline_dict.keys():    
        contents.append([status_pipeline_dict[i]['Value'], status_pipeline_dict[i]['Label']])
    return contents


@schema
class StatusProcessDefinition(dj.Lookup):
     definition = """
     status_pipeline:                    TINYINT(1)      # status in the automate process pipeline
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
     task_copy_id_pre=null:             UUID                      # id for globus transfer task raw file cup->tiger  
     task_copy_id_pos=null:             UUID                      # id for globus transfer task sorted file tiger->cup

     slurm_id=null:                     VARCHAR(16)               # id for slurm process in tiger
     """    


@schema
class RecordingProcessStatus(dj.Manual):
     definition = """
     ->RecordingProcess
     -----
     -> StatusProcessDefinition.proj(status_pipeline_old='status_pipeline') # old status in the pipeline
     -> StatusProcessDefinition.proj(status_pipeline_new='status_pipeline') # current status in the pipeline
     status_timestamp:                  DATETIME        # timestamp when status change ocurred
     error_message=null:                VARCHAR(4096)   # Error message if status now is failed
     error_exception=null:              BLOB            # Error exception if status now is failed
     """
