import datajoint as dj
from u19_pipeline import lab, subject, acquisition
import u19_pipeline.automatic_job.params_config as config

schema = dj.schema(dj.config['custom']['database.prefix'] + 'recording')


@schema
class RecordingModality(dj.Lookup):
     definition = """
     recording_modality:         varchar(64)  # recording modalities 
                                              # (ephys, imaging, video_recording, etc.) 
     ---
     modality_description:       varchar(255) # description for the modality
     root_directory:              varchar(255) # root directory where modality is stored
     recording_file_extensions:  blob         # file extensions for this modality
     recording_file_pattern:     blob         # directory pattern to find recordings in path
     process_unit_file_pattern:  blob         # process "unit" pattern to find in path
     process_unit_dir_fieldname: varchar(64)  # FieldName that stores process unit 
                                              # directory for specific modality
     process_unit_fieldname:     varchar(32)  # FieldName that stores process unit for 
                                              # specific modality (fov, probe, etc)
     process_repository:         varchar(64)  # Name of the repository that handles the 
                                              # processing of these modality
     """
     contents = config.recording_modality_list


@schema
class RecordingStatusDefinition(dj.Lookup):
     definition = """
     status_recording_id: TINYINT(1)      # Status in the automatic processing pipeline
     ---
     status_definition:   VARCHAR(256)    # Status definition 
     """
     contents = config.recording_status_list


@schema
class Recording(dj.Manual):
     definition = """
     recording_id:  INT(11) AUTO_INCREMENT    # Unique number assigned to recording   
     -----
     -> RecordingModality
     -> lab.Location
     -> RecordingStatusDefinition             # current status for recording in the pipeline
     
     task_copy_id_pni=null:      UUID         # id for globus transfer task raw file local->cup
     
     inherit_params_recording=1: boolean      # all RecordingProcess from a recording will have same paramSets
     
     recording_directory:        varchar(255) # relative directory where the recording will be stored on cup
     
     local_directory:            varchar(255) # local directory where the recording is stored on system
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
class EphysRecordingSession(dj.Computed):
    definition = """
    -> Recording
    """

    @property
    def key_source(self):
        return Recording & {'recording_modality': 'electrophysiology'}

    def make(self, key):
        self.insert1(key)

# --------------------------------------------------------------------------------------
ephys_element_schema = dj.create_virtual_module(dj.config['custom']['database.prefix'] + 'ephys_element',dj.config['custom']['database.prefix'] + 'ephys_element')

# imaging_element_schema = dj.create_virtual_module(dj.config['custom']['database.prefix'] + 'imaging_element',dj.config['custom']['database.prefix'] + 'imaging_element')

@schema
class ProcessingStatusDefinition(dj.Lookup):
     definition = """
     status_processing_id: TINYINT(1)      # Status in the automatic processing pipeline
     ---
     status_definition:    VARCHAR(256)    # Status definition 
     """
     contents = config.recording_process_status_list


@schema
class Processing(dj.Manual):
     definition = """
     job_id:                INT(11) AUTO_INCREMENT
                                                    # Unique number assigned to each 
                                                    # processing job for a recording
                                                    # unit
     ---
     -> Recording
     -> ProcessingStatusDefinition                  # current status in the pipeline
     fragment_number:                  TINYINT(1)   # fov# or probe#, etc. reference 
                                                    # from the corresponding modality 
     recording_process_pre_path=null:  VARCHAR(200) # relative path for raw data 
                                                    # recording subdirectory that will 
                                                    # be processed (ephys-> probe, 
                                                    # imaging->fieldofview)
     recording_process_post_path=null: VARCHAR(200) # relative path of processed 
                                                    # data recording
     task_copy_id_pre=null:            UUID         # id for globus transfer task raw 
                                                    # file cup->tiger
     task_copy_id_post=null:           UUID         # id for globus transfer task 
                                                    # sorted file tiger->cup
     slurm_id=null:                    VARCHAR(16)  # id for slurm process in tiger
     """ 

     class EphysParams(dj.Part):
          definition="""
          -> master
          ---
          -> ephys_element_schema.PreClusterParamList
          -> ephys_element_schema.ClusteringParamSet
          """

     # class ImagingParams(dj.Part):
     #    definition="""
     #    -> master
     #    ---
     #    -> imaging_element.ProcessingParamSet
     #    """  
     
     # This table would control ingestion of PreClusteringTask
     def insert_recording_process(self, recording_key, rec_unit, unit_directory_fieldname, unit_fieldname):
        '''
        # Insert RecordingTask(s) from recording.
        # For each processing "unit" of a recording add a new RecordingTask (imaging ->field of view, electrophysiology->probe)
        Input:
        recording_key            (dict) = Dictionary with recording record
        rec_unit                 (dict) = Dictionary of recording "unit" to be processed
        unit_directory_fieldname (str)  = Unit directory fieldname to be read (ephys-> probe_directory, imaging->fov_directory)
        unit_fieldname           (str)  = Unit fieldname to be read (ephys-> probe_, imaging->fov)
        '''

        # Get directory fieldname for specific modality (probe_directory, fov_directory, etc.)

        # Append data for the unit to insert
        this_recprocess_key = dict()
        this_recprocess_key['recording_id'] = recording_key['recording_id']
        this_recprocess_key['fragment_number'] = rec_unit[unit_fieldname]
        this_recprocess_key['recording_process_pre_path'] = rec_unit[unit_directory_fieldname]
        this_recprocess_key['status_processing_id'] = 0

        print('this_recprocess_key', this_recprocess_key)

        self.insert1(this_recprocess_key)  


@schema
class ProcessingStatus(dj.Manual):
     definition = """
     job_status_id: INT(11) AUTO_INCREMENT
                                              # Unique number assigned to each change 
                                              # of status for all processing jobs
     -----
     -> Processing
     -> ProcessingStatusDefinition.proj(status_processing_id_old='status_processing_id') 
                                              # Previous status in the pipeline
     -> ProcessingStatusDefinition.proj(status_processing_id_new='status_processing_id') 
                                              # Current status in the pipeline
     status_timestamp:        DATETIME        # Timestamp when status change ocurred
     error_message=null:      VARCHAR(4096)   # Error message if status now is failed
     error_exception=null:    BLOB            # Error exception if status now is failed
     """
