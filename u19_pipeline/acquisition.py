import datajoint as dj
from u19_pipeline import lab, task, subject

schema = dj.schema(dj.config['custom']['database.prefix'] + 'acquisition')


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


@schema
class SessionManipulation(dj.Manual):
    definition = """
    # Relationship between session & videos acquired
    -> acquisition.Session
    -> lab.VideoType
    ---
    local_path_video_file    : varchar(255)                  # absolute path were video file is stored in local computer
    remote_path_video_file   : varchar(255)                  # relative path were video file will be stored in braininit drive
    """


@schema
class SessionVideo(dj.Manual):
    definition = """
    # Relationship between session & videos acquired
    -> acquisition.Session
    -> lab.VideoType
    ---
    local_path_video_file    : varchar(255)                  # absolute path were video file is stored in local computer
    remote_path_video_file   : varchar(255)                  # relative path were video file will be stored in braininit drive
    """
