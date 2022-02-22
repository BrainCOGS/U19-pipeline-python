import datajoint as dj
import numpy as np
from u19_pipeline import lab, task, subject, acquisition
import u19_pipeline.automatic_job.params_config as config
from element_interface.utils import dict_to_uuid
from u19_pipeline.utility import numpy_array_to_dict

schema = dj.schema(dj.config['custom']['database.prefix'] + 'recording')

@schema
class RecordingModality(dj.Lookup):
     definition = """
     recording_modality:         varchar(64)          # modalities for recording (ephys, imaging, video_recording, etc.) 
     ---
     modality_description:       varchar(255)         # description for the modality
     root_direcory:              varchar(255)         # root directory where that modality is stored
     recording_file_extensions:  blob                 # file extensions specific for this modality
     recording_file_pattern:     blob                 # directory pattern to find recordings in path
     process_unit_file_pattern:  blob                 # process "unit" pattern to find in path
     process_unit_dir_fieldname: varchar(64)          # FieldName that stores process unit directory for specific modality
     process_unit_fieldname:     varchar(32)          # FieldName that stores process unit for specific modality (fov, probe, etc)
     process_repository:         varchar(64)          # Name of the repository that handles the processing of these modality
     """
     contents = config.recording_modality_list


@schema
class StatusRecordingDefinition(dj.Lookup):
     definition = """
     status_recording_idx:               TINYINT(1)      # status in the automate process pipeline
     ---
     status_definition:                  VARCHAR(256)    # Status definition 
     """
     contents = config.recording_status_list


@schema
class PreprocessParamSet(dj.Lookup):
    definition = """
    # Parameter set to be used in the preprocessing steps
    preprocess_paramset_idx:  int(11) AUTO_INCREMENT
    ---
    -> RecordingModality
    preprocess_paramset_desc: varchar(128)
    preprocess_paramset_hash: uuid
    unique index (preprocess_paramset_hash)
    preprocess_paramset: longblob  # dictionary of all applicable parameters
    """

    @classmethod
    def insert_new_params(cls, recording_modality: str, preprocess_paramset_idx: int,
                          preprocess_paramset_desc: str, preprocess_paramset: dict):
        paramset_dict = {'recording_modality': recording_modality,
                      'preprocess_paramset_idx': preprocess_paramset_idx,
                      'preprocess_paramset_desc': preprocess_paramset_desc,
                      'preprocess_paramset': preprocess_paramset,
                      'preprocess_paramset_hash':  dict_to_uuid(preprocess_paramset)}
        paramset_query = cls & {'preprocess_paramset_hash': paramset_dict['preprocess_paramset_hash']}
        if paramset_query:  # If the specified param-set already exists
            existing_paramset_idx = paramset_query.fetch1('preprocess_paramset_idx')
            if existing_paramset_idx == preprocess_paramset_idx:  # If the existing set has the same preprocess_paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
                raise dj.DataJointError(
                    'The specified param-set'
                    ' already exists - preprocess_paramset_idx: {}'.format(existing_paramset_idx))
        else:
            cls.insert1(paramset_dict)


    def get_preprocess_params(self, preprocess_param_idx):
        '''
        Get process params for current recording process
        Return:
            preprocess_paramset (dict): preprocess params associated with recording process
        '''

        preprocess_paramset =  (self & preprocess_param_idx).fetch1('preprocess_paramset')

        #If stored in MATLAB this is a numpy array to be converted to dictionary
        if isinstance(preprocess_paramset, np.ndarray):
            preprocess_paramset = numpy_array_to_dict(preprocess_paramset)


        return preprocess_paramset


@schema
class ProcessParamSet(dj.Lookup):
    definition = """
    # Parameter set to be used in the processing steps
    process_paramset_idx:  int(11) AUTO_INCREMENT
    ---
    -> RecordingModality    
    process_paramset_desc: varchar(128)
    process_paramset_hash: uuid
    unique index (process_paramset_hash)
    process_paramset: longblob  # dictionary of all applicable parameters
    """

    @classmethod
    def insert_new_params(cls, recording_modality: str, paramset_idx: int,
                          paramset_desc: str, params: dict):
        paramset_dict = {'recording_modality': recording_modality,
                      'process_paramset_idx': paramset_idx,
                      'process_paramset_desc': paramset_desc,
                      'process_paramset': params,
                      'process_paramset_hash':  dict_to_uuid(params)}
        param_query = cls & {'process_paramset_hash': paramset_dict['process_paramset_hash']}

        if param_query:  # If the specified param-set already exists
            existing_paramset_idx = param_query.fetch1('process_paramset_idx')
            if existing_paramset_idx == paramset_idx:  # If the existing set has the same paramset_idx: job done
                return
            else:  # If not same name: human error, trying to add the same paramset with different name
                raise dj.DataJointError(
                    'The specified param-set'
                    ' already exists - process_paramset_idx: {}'.format(existing_paramset_idx))
        else:
            cls.insert1(paramset_dict)


    def get_process_params(self, process_param_idx):
        '''
        Get process params for current recording process
        Return:
            process_paramset (dict): process params associated with recording process
        '''

        process_paramset =  (self & process_param_idx).fetch1('process_paramset')

        #If stored in MATLAB this is a numpy array to be converted to dictionary
        if isinstance(process_paramset, np.ndarray):
            process_paramset = numpy_array_to_dict(process_paramset)

        return process_paramset


@schema
class Recording(dj.Manual):
     definition = """
     recording_id:                      INT(11) AUTO_INCREMENT                  # Unique number assigned to each recording   
     -----
     -> RecordingModality    
     -> lab.Location                        
     -> StatusRecordingDefinition                                               # current status for recording in the pipeline
     -> PreprocessParamSet.proj(def_preprocess_paramset_idx='preprocess_paramset_idx')  # reference to params to default preprocess recording (possible to inherit to recordigprocess)
     -> ProcessParamSet.proj(def_process_paramset_idx='process_paramset_idx')   # reference to params to default process recording  (possible to inherit to recordigprocess)
     task_copy_id_pni=null:             UUID                                    # id for globus transfer task raw file local->cup
     inherit_params_recording=1:        boolean                                 # all RecordingProcess from a recording will have same paramSets
     recording_directory:               varchar(255)                            # relative directory where the recording will be stored on cup
     local_directory:                   varchar(255)                            # local directory where the recording is stored on system
     """    
    
     class BehaviorSession(dj.Part):
         definition = """
         -> master
         ---
         -> acquisition.Session
         """
    
     class RecordingSession(dj.Part):
         definition = """
         -> master
         ---
         -> subject.Subject
         recording_datetime: datetime
         """


@schema
class StatusProcessDefinition(dj.Lookup):
     definition = """
     status_pipeline_idx:                TINYINT(1)      # status in the automate process pipeline
     ---
     status_definition:                  VARCHAR(256)    # Status definition 
     """
     contents = config.recording_process_status_list


@schema
class RecordingProcess(dj.Manual):
     definition = """
     recording_process_id:              INT(11) AUTO_INCREMENT    # Unique number assigned to each processing job for a recording unit
     -----
     -> Recording
     -> StatusProcessDefinition                                   # current status in the pipeline
     -> PreprocessParamSet                                        # reference to params to preprocess recording
     -> ProcessParamSet                                           # reference to params to process recording
     fragment_number:                   TINYINT(1)                # fov# or probe#, etc. reference from the corresponding modality 
     recording_process_pre_path=null:   VARCHAR(200)              # relative path for raw data recording subdirectory that will be processed (ephys-> probe, imaging->fieldofview)
     recording_process_post_path=null:  VARCHAR(200)              # relative path for processed data recording
     task_copy_id_pre=null:             UUID                      # id for globus transfer task raw file cup->tiger  
     task_copy_id_post=null:            UUID                      # id for globus transfer task sorted file tiger->cup
     slurm_id=null:                     VARCHAR(16)               # id for slurm process in tiger
     """  

     def insert_recording_process(self, recording_key, rec_unit, unit_directory_fieldname, unit_fieldname):
        '''
        #Insert RecordingProcess(es) from recording.
        # For each processing "unit" of a recording add a new recordingProcess (imaging ->field of view, electrophysiology->probe)
        Input:
        recording_key            (dict) = Dictionary with recording record
        rec_unit                 (dict) = Dictionary of recording "unit" to be processed
        unit_directory_fieldname (str)  = Unit directory fieldname to be read (ephys-> probe_directory, imaging->fov_directory)
        unit_fieldname           (str)  = Unit fieldname to be read (ephys-> probe_, imaging->fov)
        '''

        # Get directory fieldname for specific modality (probe_directory, fov_directory, etc.)

        #Append data for the unit to insert
        this_recprocess_key = dict()
        this_recprocess_key['recording_id']               = recording_key['recording_id']
        this_recprocess_key['preprocess_paramset_idx']    = recording_key['preprocess_paramset_idx']
        this_recprocess_key['process_paramset_idx']       = recording_key['process_paramset_idx']
        this_recprocess_key['fragment_number']            = rec_unit[unit_fieldname]
        this_recprocess_key['recording_process_pre_path'] = rec_unit[unit_directory_fieldname]
        this_recprocess_key['status_pipeline_idx'] = 0

        print('this_recprocess_key', this_recprocess_key)

        self.insert1(this_recprocess_key)  


@schema
class RecordingProcessStatus(dj.Manual):
     definition = """
     recording_process_status_id:       INT(11) AUTO_INCREMENT    # Unique number assigned to each change of status for all processing jobs
     -----
     -> RecordingProcess
     -> StatusProcessDefinition.proj(status_pipeline_idx_old='status_pipeline_idx') # old status in the pipeline
     -> StatusProcessDefinition.proj(status_pipeline_idx_new='status_pipeline_idx') # current status in the pipeline
     status_timestamp:                  DATETIME        # timestamp when status change ocurred
     error_message=null:                VARCHAR(4096)   # Error message if status now is failed
     error_exception=null:              BLOB            # Error exception if status now is failed
     """
