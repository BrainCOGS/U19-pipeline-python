#!/bin/env python

import datajoint as dj

prefix = dj.config['custom']['database.prefix']

schema = dj.schema(prefix + 'scheduler')

connect_mod = lambda x: dj.VirtualModule(x, prefix  + x)
lab = connect_mod('lab')
subject = connect_mod('subject')

@schema
class BehaviorProfile(dj.Manual):
    definition = """
    behavior_profile_id                    : int auto_increment
    ---
    -> lab.User
    date_created                 : date
    behavior_profile_name                  : varchar(255)          # Profile name
    behavior_profile_description           : varchar(255)          # Profile description
    behavior_profile_variables             : blob                  # Encoded for the variables
    """

# @schema
# class TestProfile(dj.Manual):
#     definition = """
#     recording_profile_id                    : int auto_increment
#     ---
#     -> lab.User
#     date_created                 : date
#     recording_profile_name                  : varchar(255)          # Profile name
#     recording_profile_description           : varchar(255)          # Profile description
#     recording_profile_variables             : blob # Encoded for the variables
#     """

@schema
class RecordingProfile(dj.Manual):
    definition = """
    recording_profile_id                    : int auto_increment
    ---
    -> lab.User
    date_created                 : date
    recording_profile_name                  : varchar(255)          # Profile name
    recording_profile_description           : varchar(255)          # Profile description
    recording_profile_variables             : blob                  # Encoded for the variables
    """

@schema
class InputOutputProfile(dj.Manual):
    definition = """
    # Input/Outuput profile registry table
    input_output_profile_id          : int AUTO_INCREMENT           # numeric_id for Input/Output profile
    ---
    ->lab.User
    input_output_profile_name         : varchar(32)                 # Input/Output profile name
    input_output_profile_description  : varchar(255)                # Input/Output profile description
    input_output_profile_date         : date                        # Input/Output profile creation date
    """

@schema
class Schedule(dj.Manual):
    definition = """
    date                         : date                  # Full date
    -> lab.Location                                      # Full rig name, e.g., 165I-Rig1-T
    timeslot                     : int                   # timeslot by number
    ---
    -> [nullable] subject.Subject                             # subject name
    -> [nullable] BehaviorProfile                         # Reference to `BehaviorProfile`
    -> [nullable] RecordingProfile                        # Reference to `RecordingProfile`
    -> [nullable] InputOutputProfile                      # Reference to `InputOutputProfile`
    """

@schema
class InputOutputRig(dj.Lookup):
    definition = """
    # Which Inputs and Outputs can be installed in rigs (for RigTester purposes)
    input_output_name          : varchar(32)                 # Name and ID for Input/Output
    ---
    description                : varchar(255)                # Input/Output description
    direction                  : enum('Input', 'Output')     # Input/Output direction (for RigTester purposes)
    test_type                  : enum('Automatic', 'Manual') # Manual if technician have to check test (e.g. Speaker) Automatic otherwise
    """
    contents = [
            ['Arduino', '', 'Input', 'Automatic'],
            ['MotionSensor',  '', 'Input', 'Automatic'],
            ['LateralCamera',  '', 'Input', 'Automatic'],
            ['TopCamera',  '', 'Input', 'Automatic'],
            ['Speakers',  '', 'Output', 'Manual'],
            ['Motors',  '', 'Output', 'Manual'],
            ['Reward',  '', 'Output', 'Manual'],
            ['Laser',  '', 'Output', 'Manual'],
            ['LeftPuff',  '', 'Output', 'Manual'],
            ['RightPuff',  '', 'Output', 'Manual'],
            ['LeftReward',  '', 'Output', 'Manual'],
            ['RightReward',  '', 'Output', 'Manual'],
            ['Lickometer',  '', 'Output', 'Manual'],
    ]

@schema
class InputOutputProfileList(dj.Manual):
    definition = """
    # InputOutputProfile full list of InputsOutputs and type of test for each
    -> InputOutputProfile
    input_output_num           : int                         # # Of Input/Output for this profile
    ---
    -> InputOutputRig
    check_type                 : enum('Mandatory','Optional') # Prevent training if missing this input/output
    """