"""This module defines tables in the schema U19_acquisition"""


import datajoint as dj
from . import lab, subject, task, reference

schema = dj.schema(dj.config['database.prefix'] + 'acquisition')


@schema
class Session(dj.Manual):
    definition = """
    -> subject.Subject
    session_date         : date                         # date of experiment
    session_number       : int                          # number
    ---
    session_start_time   : datetime                     # start time
    session_end_time=null : datetime                     # end time
    -> lab.Location.proj(session_location='location')
    -> task.TaskLevelParameterSet
    stimulus_bank=""     : varchar(255)                 # path to the function to generate the stimulus
    stimulus_commit=""   : varchar(64)                  # git hash for the version of the function
    session_performance  : float                        # percentage correct on this session
    session_narrative="" : varchar(512)
    session_protocol=null       : varchar(255)          # function and parameters to generate the stimulus
    session_code_version=null   : blob                  # code version of the stimulus, maybe a version number, or a githash
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
