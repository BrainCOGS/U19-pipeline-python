import datajoint as dj
from u19_pipeline import lab, subject, acquisition
import u19_pipeline.automatic_job.params_config as config

schema = dj.schema(dj.config['custom']['database.prefix'] + 'recording')

# Declare recording tables -------------------------------------------------------------
@schema
class Modality(dj.Lookup):
     definition = """
     recording_modality:         varchar(64)  # recording modalities 
                                              # (ephys, imaging, video_recording, etc.) 
     ---
     modality_description:       varchar(255) # description for the modality
     root_directory:             varchar(255) # root directory where modality is stored
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
class Status(dj.Lookup):
     definition = """
     status_recording_id: TINYINT(1)      # Status in the automatic processing pipeline
     ---
     status_recording_definition:   VARCHAR(256)    # Status definition 
     """
     contents = config.recording_status_list


@schema
class Recording(dj.Manual):
     definition = """
     recording_id:  INT(11) AUTO_INCREMENT    # Unique number assigned to recording   
     -----
     -> Modality
     -> lab.Location
     -> Status                                # current status for recording
     task_copy_id_pni=null:      UUID         # globus transfer task raw file local->cup
     inherit_params_recording=1: boolean      # all RecordingProcess from a recording will have same paramSets
     recording_directory:        varchar(255) # relative directory on cup
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

