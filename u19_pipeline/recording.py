import datajoint as dj
import copy

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
     task_copy_id_pni=null:      int(11)      # globus transfer task raw file local->cup
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


@schema
class Log(dj.Manual):
     definition = """
     recording_log_id: INT(11) AUTO_INCREMENT # Unique number assigned to each change 
                                              # of status for all recordings
     -----
     -> Recording
     -> Status.proj(status_recording_id_old='status_recording_id') # Previous status
     -> Status.proj(status_recording_id_new='status_recording_id') # Current status
     recording_status_timestamp:  DATETIME          # Timestamp when status change ocurred
     recording_error_message=null: VARCHAR(256)     # Error message if status now is failed
     recording_error_exception=null: VARCHAR(4096)  # Error exception if status now is failed
     """


@schema
class DefaultParams(dj.Manual):
     definition = """
     -> Recording
     fragment_number:                   TINYINT(1)  # probe/field_of_view # if not always the same 
     -----
     default_same_preparams_all=1:      TINYINT(1)  # by default all probes/fields of view have same preparameters
     pre_param_list_id:                 INT(11)     # preparams index for recording (could be imaging/ephys)
     default_same_params_all=1:         TINYINT(1)  # by default all probes/fields of view have same parameters
     paramset_idx:                      INT(11)     # params index for recording (could be imaging/ephys)
     """

     @staticmethod
     def get_default_params_rec_process(recording_processes, default_params_record_df):
          'Get associated params from DefaultParams record and recording processes (jobs) of recording'

          params_rec_process = list()
          for i in recording_processes:

               this_params_rec_process = dict()
               this_params_rec_process['job_id'] = i['job_id']
               this_fragment = i['fragment_number']

               this_params_rec_process['precluster_param_list_id'] = \
                    DefaultParams.get_corresponding_param(default_params_record_df, this_fragment, 'default_same_preparams_all', 'pre_param_list_id')

               this_params_rec_process['paramset_idx'] = \
                    DefaultParams.get_corresponding_param(default_params_record_df, this_fragment, 'default_same_params_all', 'paramset_idx')

               params_rec_process.append(this_params_rec_process)

          return params_rec_process


     @staticmethod
     def get_corresponding_param(default_params_record_df, this_fragment, default_label, param_label):
          'Get corresponding param (pre_param_list_id or paramset_idx) for this fragment'

          'default_label = default_same_preparams_all / default_same_params_all '
          'param_label   = precluster_param_list_id   / paramset_idx '

          if default_params_record_df.loc[0, default_label] == 1:
               this_fragment_pre_param_list_id = default_params_record_df.loc[0, param_label]
          else:
               this_fragment_pre_param_list_id = \
                    default_params_record_df.loc[default_params_record_df['fragment_number'] == this_fragment, param_label]
               #If there is no list id for this specific fragment, get default one
               if this_fragment_pre_param_list_id.shape[0] == 0:
                    this_fragment_pre_param_list_id = default_params_record_df.loc[0, param_label]
               else:
                    this_fragment_pre_param_list_id = this_fragment_pre_param_list_id.values[0]

          return this_fragment_pre_param_list_id