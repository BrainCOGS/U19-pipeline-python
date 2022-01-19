import datajoint as dj
from u19_pipeline import lab, task, subject

schema = dj.schema('u19_acquisition')


@schema
class SessionStarted(dj.Manual):
    definition = """
    # General information of a session
    -> subject.Subject
    session_date         : date                         # date of experiment
    session_number       : int                          # number
    ---
    session_start_time   : datetime                     # start time
    -> lab.Location.proj(session_location="location")
    -> task.Task
    local_path_behavior_file : varchar(255)                 # Path were session file is stored in local computer
    remote_path_behavior_file : varchar(255)                 # Path were session file will be stored in bucket
    is_finished=0        : tinyint                      # Flag that indicates if this session was finished successfully
    """


@schema
class Session(dj.Manual):
    definition = """
    -> SessionStarted
    ---
    session_start_time   : datetime                     # start time
    session_end_time=null : datetime                     # end time
    -> lab.Location.proj(session_location="location")
    -> task.TaskLevelParameterSet
    stimulus_bank=""     : varchar(255)                 # path to the function to generate the stimulus
    stimulus_commit=""   : varchar(64)                  # git hash for the version of the function
    session_performance  : float                        # percentage correct on this session
    num_trials=null      : int                          # Number of trials for the session
    num_trials_try=null  : tinyblob                     # Accumulative number of trials for each try of the session
    session_narrative="" : varchar(512)
    session_protocol=null : varchar(255)                 # function and parameters to generate the stimulus
    session_code_version=null : blob                         # code version of the stimulus, maybe a version number, or a githash
    is_bad_session=null  : tinyint                      # Flag that indicates if this session had any issues
    session_comments=null : varchar(2048)                # Text to indicate some particularity of the session (e.g. state the issues in a bad session)
    """


@schema
class DataDirectory(dj.Computed):
    definition = """
    -> Session
    ---
    data_dir             : varchar(255)                 # data directory for each session
    file_name            : varchar(255)                 # file name
    combined_file_name   : varchar(255)                 # combined filename
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
class StatusDefinition(dj.Lookup):
     definition = """
     status_pipeline:                    TINYINT(1)      # status in the ephys/imaging pipeline
     ---
     status_definition:                  VARCHAR(256)    # Status definition 
     """
     contents = get_content_list_from_status_dict()

@schema
class RecordingModality(dj.Manual):
     definition = """
     recording_modality:                      INT(11) AUTO_INCREMENT        # Unique number assigned to each recording   
     -----
     -> [nullable] acquisition.Session                                          # acquisition Session key
     -> [nullable] subject.Subject.proj(recording_subject='subject_fullname') # Recording subject when no behavior Session present
     -> [nullable] recording_datetime:  datetime                                # Recording datetime when no bheavior Session present
     modality:                          
     recording_directory:                   varchar(255)                            # the relative directory where the ephys data for this session will be stored in bucket
     """    


(dj.Lookup):
    definition = """
    recording_modality:   varchar(64)                  # modalities for recording (ephys, imaging, video_recording, etc.)
    ---
    modality_description: varchar(255)                 # description for the modality
    root_direcory:        varchar(255)                 # root directory where that modality is stored (e.g. ephys = /braininit/Data/eletrophysiology)
    """
    contents = [
        ['ephys', '', '/braininit/Data/eletrophysiology'],
        ['imaging', '', '/braininit/Data/eletrophysiology'],
        ['ephys', '', '/braininit/Data/eletrophysiology']
    ]



@schema
class Recordings(dj.Manual):
     definition = """
     recording_id:                      INT(11) AUTO_INCREMENT        # Unique number assigned to each recording   
     -----
     -> [nullable] acquisition.Session                                          # acquisition Session key
     -> [nullable] subject.Subject.proj(acquisition_subject='subject_fullname') # Recording subject when no behavior Session present
     -> [nullable] recording_datetime:  datetime                                # Recording datetime when no bheavior Session present
     modality:                          
     recording_directory:                   varchar(255)                            # the relative directory where the ephys data for this session will be stored in bucket
     """    



@schema
class AcquisitionSessionsTestAutoPipeline(dj.Manual):
     definition = """
     ->Sessions
     -----
     ->StatusDefinition                                # current status in the ephys pipeline
     session_rat:                       VARCHAR(8)      # ratname inherited from rats table
     session_userid:                    VARCHAR(32)     # rat owner inherited from contacts table
     session_rigid:                     INT(3)          # rig id number inherited from riginfo table
     acquisition_type:                  VARCHAR(32)     # ephys or imaging
     acquisition_raw_rel_path=null:     VARCHAR(200)    # relative path of raw files 
     acquisition_post_rel_path=null:    VARCHAR(200)    # relative path for sorted files
     task_copy_id_pre_path=null:        UUID            # id for globus transfer task raw file cup->tiger  
     task_copy_id_pos_path=null:        UUID            # id for globus transfer task sorted file tiger->cup 
     slurm_id_sorting=null:             VARCHAR(16)     # id for slurm process in tiger
     """    

@schema
class AcquisitionSessionsStatus(dj.Manual):
     definition = """
     ->AcquisitionSessions
     -----
     -> StatusDefinition.proj(status_pipeline_old='status_pipeline') # old status in the ephys pipeline
     -> StatusDefinition.proj(status_pipeline_new='status_pipeline') # current status in the ephys pipeline
     status_timestamp:                  DATETIME        # timestamp when status change ocurred
     error_message=null:                VARCHAR(4096)   # Error message if status now is failed
     error_exception=null:              BLOB            # Error exception if status is failed
     """
