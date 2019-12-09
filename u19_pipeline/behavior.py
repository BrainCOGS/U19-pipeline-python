"""This module defines tables in the schema U19_behavior"""

import datajoint as dj
from . import acquisition, task

schema = dj.schema(dj.config['database.prefix'] + 'behavior')


@schema
class TowersSession(dj.Imported):
    definition = """
    -> acquisition.Session
    ---
    stimulus_set         : tinyint                      # an integer that describes a particular set of stimuli in a trial
    ball_squal           : float                        # quality measure of ball data
    rewarded_side        : blob                         # Left or Right X number trials
    chosen_side          : blob                         # Left or Right X number trials
    maze_id              : blob                         # level X number trials
    num_towers_r         : blob                         # Number of towers shown to the right x number of trials
    num_towers_l         : blob                         # Number of towers shown to the left x tumber of trials
    """


@schema
class TowersBlock(dj.Imported):
    definition = """
    -> TowersSession
    block                : tinyint                      # block number
    ---
    -> task.TaskLevelParameterSet
    n_trials             : int                          # number of trials in this block
    first_trial          : int                          # trial_idx of the first trial in this block
    block_duration       : float                        # in secs, duration of the block
    block_start_time     : datetime                     # absolute start time of the block
    reward_mil           : float                        # in mL, reward volume in this block
    reward_scale         : tinyint                      # scale of the reward in this block
    easy_block           : tinyint                      # true if the difficulty reduces during the session
    block_performance    : float                        # performance in the current block
    """


    class Trial(dj.Part):
        definition = """
        -> TowersBlock
        trial_idx            : int                          # trial index, keep the original number in the file
        ---
        trial_type           : enum('L','R')                # answer of this trial, left or right
        choice               : enum('L','R','nil')          # choice of this trial, left or right
        trial_time           : longblob                     # time series of this trial, start from zero for each trial
        trial_abs_start      : float                        # absolute start time of the trial realtive to the beginning of the session
        collision            : longblob                     # boolean vector indicating whether the subject hit the maze on each time point
        cue_presence_left    : blob                         # boolean vector for the presence of the towers on the left
        cue_presence_right   : blob                         # boolean vector for the presence of the towers on the right
        cue_onset_left=null  : blob                         # onset time of the cues on the left (only for the present ones)
        cue_onset_right=null : blob                         # onset time of the cues on the right (only for the present ones)
        cue_offset_left=null : blob                         # offset time of the cues on the left (only for the present ones)
        cue_offset_right=null : blob                         # offset time of the cues on the right (only for the present ones)
        cue_pos_left=null    : blob                         # position of the cues on the left (only for the present ones)
        cue_pos_right=null   : blob                         # position of the cues on the right (only for the present ones)
        trial_duration       : float                        # duration of the entire trial
        excess_travel        : float
        i_arm_entry          : int                          # the index of the time series when the mouse enters the arm part
        i_blank              : int                          # the index of the time series when the mouse enters the blank zone
        i_cue_entry          : int                          # the index of the time series when the mouse neters the cue zone
        i_mem_entry          : int                          # the index of the time series when the mouse enters the memory zone
        i_turn_entry         : int                          # the index of the time series when the mouse enters turns
        iterations           : int                          # length of the meaningful recording
        position             : longblob                     # 3d recording of the position of the mouse, length equals to interations
        velocity             : longblob                     # 3d recording of the velocity of the mouse, length equals to interations
        sensor_dots          : longblob                     # raw recordings of the ball
        trial_id             : int
        trial_prior_p_left   : float                        # prior probablity of this trial for left
        vi_start             : int
        """
