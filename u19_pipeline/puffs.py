"""This module defines tables in the schema ahoag_puffs_lab_demo"""

import datajoint as dj
from . import lab, acquisition, task

schema = dj.schema(dj.config['database.prefix'] + 'puffs')

@schema
class PuffsCohort(dj.Manual):
    definition = """
    -> lab.User         
    project_name         : varchar(64)                  # Corresponds to the path on bucket /puffs/netid/project_name/cohortX/ ...
    cohort               : varchar(64)                  # Corresponds to the path on bucket /puffs/netid/project_name/cohortX/ ...
    ---
    """

@schema
class PuffsFileAcquisition(dj.Manual):
    definition = """
    -> PuffsCohort
    rig                  : tinyint                      # an integer that describes which rig was used. 0 corresponds to location = "pni-ltl016-05", 1 corresponds to location = "wang-behavior"
    h5_filename          : varchar(256)                 # The full path name, e.g. "data.h5" or "data_compressed_XX.h5" for the h5 behavior data file
    ---
    ingested             : boolean                      # A flag for whether this file has already been ingested.  
    """

"""This module defines tables in the schema ahoag_puffs_behavior_demo"""


@schema
class PuffsSession(dj.Manual):
    definition = """
    -> acquisition.Session
    ---
    session_params = NULL       : blob                         # The parameters for this session, e.g. phase_durations, whether puffs are on, etc... 
    rig                         : tinyint                      # an integer that describes which rig was used. 0 corresponds to location = "pni-ltl016-05", 1 corresponds to location = "wang-behavior"
    notes = ''                  : varchar(1024)                # notes recorded by experimenter during the session
    stdout = NULL               : blob                         # stdout for the GUI during the session
    stderr = NULL               : blob                         # stderr for the GUI during the session
    sync = NULL                 : blob                         # At the start of the session, the software runs multiple python processes. This column contains the times of these processes in seconds 
    """

    class Trial(dj.Part):
    	definition = """
        -> PuffsSession
        trial_idx            : int                          # trial index, keep the original number in the file
        ---
		-> task.TaskLevelParameterSet.proj(level="level")   # The difficulty level of the trial
        trial_type           : enum('L','R')                # answer of this trial, left or right
        choice               : enum('L','R','nil')          # choice of this trial, left or right
        trial_prior_p_left   : float                        # prior probablity of this trial for left
	    trial_rel_start      : float                        # start time of the trial relative to the beginning of the session [seconds]
	    trial_rel_finish     : float                        # end time of the trial relative to the beginning of the session [seconds]
	    trial_duration       : float                        # duration of the trial [seconds]
	    cue_period           : float                        # duration of cue period [seconds]
        num_puffs_intended_l : tinyint                      # number of puffs intended on the left side
	    num_puffs_received_r : tinyint                      # number of puffs actually received on the right side
	    num_puffs_intended_r : tinyint                      # number of puffs intended on the right side
	    num_puffs_received_l : tinyint                      # number of puffs actually received on the left side
	    reward_rel_start     : float                        # timing of reward relative to the beginning of the session [seconds]
	    reward_scale         : float                        # subject is given 4 microliters * reward_scale as a reward 
	    rule                 : tinyint                      # 
        """
    
    class TrialPhase(dj.Part):
    	definition = """
        -> master.Trial
        phase                : tinyint                      # phase index, 0=intro, 1=stimulus period, ... see Puffs Task Documentation
        ---
	    phase_rel_start      : float                        # start time of the phase relative to the beginning of the session [seconds]
	    phase_rel_finish     : float                        # end time of the phase relative to the beginning of the session [seconds]
        """
    
    class Puff(dj.Part):
    	definition = """
        -> master.Trial
        puff_idx             : tinyint                     # the index of the puff in this particular trial
        side                 : tinyint                     # 0 = left side, 1 = right side
        ---
	    puff_rel_time        : float                       # time of the puff relative to the beginning of the trial [seconds]
        """

