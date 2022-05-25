
import pathlib

import datajoint as dj
import u19_pipeline.automatic_job.params_config as config

schema = dj.schema(dj.config['custom']['database.prefix'] + 'recording_process')

# Declare recording processing tables --------------------------------------------------
@schema
class Status(dj.Lookup):
     definition = """
     status_processing_id: TINYINT(1)      # Status in the automatic processing pipeline
     ---
     status_processing_definition:    VARCHAR(256)    # Status definition 
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
     -> recording.Recording
     -> Status                                      # current status in the pipeline
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
          -> ephys_element.PreClusterParamList
          -> ephys_element.ClusteringParamSet.proj(cluster_paramset_idx='paramset_idx')
          """

     class ImagingParams(dj.Part):
        definition="""
        -> master
        ---
        -> imaging_element.ProcessingParamSet
        """  

     # This table would control ingestion of PreClusteringTask
     def insert_recording_process(self, fragment_keys, fragment_fieldname):
        '''
        # Insert RecordingTask(s) from recording.
        # For each processing "unit" of a recording add a new RecordingTask (imaging ->field of view, electrophysiology->probe)
        Input:
        recording_key            (dict) = Dictionary with recording record
        rec_unit                 (dict) = Dictionary of recording "unit" to be processed
        unit_fieldname           (str)  = Unit fieldname to be read (ephys-> probe_, imaging->fov)
        '''

        # Get directory fieldname for specific modality (probe_directory, fov_directory, etc.)

        # Append data for the unit to insert
        this_recprocess_key = list()
        for idx, fragment_key in enumerate(fragment_keys):
          this_recprocess_key.append(dict())
          this_recprocess_key[idx]['recording_id'] = fragment_key['recording_id']
          this_recprocess_key[idx]['fragment_number'] = fragment_key[fragment_fieldname]
          this_recprocess_key[idx]['status_processing_id'] = 0
          this_recprocess_key[idx]['recording_process_pre_path'] = fragment_key['recording_process_pre_path']

        self.insert(this_recprocess_key)

     def set_recording_process_post_path(self, recprocess_keys):
          '''
          # Update recording_process_post_path from recording_process keys
          Input:
          recprocess_keys          (dict) = Dictionary with job_ids & recording_process_pre_path
          '''

          for rec_process in recprocess_keys:
               this_key_dict = dict()
               this_key_dict['job_id'] = rec_process['job_id']
               this_key_dict['recording_process_post_path'] = \
                    pathlib.Path(rec_process['recording_process_pre_path'], 'job_id_' +  str(rec_process['job_id'])).as_posix()
               self.update1(this_key_dict)


@schema
class Log(dj.Manual):
     definition = """
     log_id: INT(11) AUTO_INCREMENT           # Unique number assigned to each change 
                                              # of status for all processing jobs
     ---
     -> Processing
     -> Status.proj(status_processing_id_old='status_processing_id') # Previous status
     -> Status.proj(status_processing_id_new='status_processing_id') # Current status
     status_timestamp:        DATETIME        # Timestamp when status change ocurred
     error_message=null:      VARCHAR(256)    # Error message if status now is failed
     error_exception=null:    VARCHAR(4096)   # Error exception if status now is failed
     """
