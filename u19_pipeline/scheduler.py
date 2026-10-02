#!/bin/env python

import datajoint as dj

prefix = dj.config["custom"]["database.prefix"]

schema = dj.schema(prefix + "scheduler")


def connect_mod(x):
    return dj.VirtualModule(x, prefix + x)


lab = connect_mod("lab")
subject = connect_mod("subject")


@schema
class TrainingProfile(dj.Manual):
    definition = """
    training_profile_id    : int auto_increment
    ---
    -> lab.User
    date_created                 : date
    date_last_used                 : date
    training_profile_name                  : varchar(255)          # Profile name
    training_profile_description           : varchar(255)          # Profile description
    training_profile_variables             : varchar(16384)        # Encoded for the variables
    deprecated            : int                   # Deprecation  to hide
    """


@schema
class RecordingProfile(dj.Manual):
    definition = """
    recording_profile_id                    : int auto_increment
    ---
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
class ScheduleRule(dj.Manual):
    definition = """
    # Recurring booking of a subject on a rig slot, expanded into Schedule rows
    # by u19_pipeline.utils.schedule_rules.materialize_schedule
    rule_id                      : int auto_increment
    ---
    -> subject.Subject
    -> lab.Location
    timeslot                     : int                   # same numbering as Schedule.timeslot
    -> TrainingProfile
    -> RecordingProfile
    -> InputOutputProfile
    experimenters_instructions = '' : varchar(4096)   # kept well under MySQL's 65535-byte row limit
    level = 0                    : int                   # 0 means derive from past performance
    sublevel = 0                 : int                   # 0 means derive from past performance
    start_date                   : date                  # first date the rule may run, inclusive
    end_date = NULL              : date                  # last date, inclusive; NULL is open-ended
    status = 'active'            : enum('active', 'ended', 'cancelled')
    -> lab.User.proj(created_by='user_id')
    created_at = CURRENT_TIMESTAMP : timestamp
    """

    class Day(dj.Part):
        definition = """
        # Days of the week on which the rule runs
        -> master
        weekday                  : enum('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
        """


@schema
class ScheduleRuleException(dj.Manual):
    definition = """
    # One-off deviation from a ScheduleRule on a single date
    -> ScheduleRule
    date                         : date
    ---
    action                       : enum('skip', 'override')  # skip: no session that day; override: row edited by hand, generator leaves it
    reason = ''                  : varchar(255)
    -> lab.User
    created_at = CURRENT_TIMESTAMP : timestamp
    """


@schema
class LabClosure(dj.Manual):
    definition = """
    # Lab-wide dates on which no ScheduleRule produces sessions (holidays, shutdowns)
    date                         : date
    ---
    reason                       : varchar(255)
    -> lab.User
    """


@schema
class Schedule(dj.Manual):
    # rule_id is added to the existing table by scripts/migrations/add_schedule_rules.py;
    # DataJoint never alters a declared table from this definition.
    definition = """
    date                         : date                  # Full date
    -> lab.Location                                      # Full rig name, e.g., 165I-Rig1-T
    timeslot                     : int                   # timeslot by number
    ---
    -> subject.Subject                             # subject name
    -> TrainingProfile                         # Reference to `TrainingProfile`
    -> RecordingProfile                        # Reference to `RecordingProfile`
    -> InputOutputProfile                      # Reference to `InputOutputProfile`
    experimenters_instructions  :varchar(64532)
    level                       : int
    sublevel                    : int
    -> [nullable] ScheduleRule                 # rule that generated this row; NULL for manual or copy-forward rows
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
        ["Arduino", "", "Input", "Automatic"],
        ["MotionSensor", "", "Input", "Automatic"],
        ["LateralCamera", "", "Input", "Automatic"],
        ["TopCamera", "", "Input", "Automatic"],
        ["Speakers", "", "Output", "Manual"],
        ["Motors", "", "Output", "Manual"],
        ["Reward", "", "Output", "Manual"],
        ["Laser", "", "Output", "Manual"],
        ["LeftPuff", "", "Output", "Manual"],
        ["RightPuff", "", "Output", "Manual"],
        ["LeftReward", "", "Output", "Manual"],
        ["RightReward", "", "Output", "Manual"],
        ["Lickometer", "", "Output", "Manual"],
    ]


@schema
class TechDuties(dj.Lookup):
    definition = """
    # Defines
    tech_duty : varchar(100)                 # Name and ID for Input/Output
    ---
    description                : varchar(255)                # Input/Output description
    """
    contents = [
        ["Off", ""],
        ["Watering Only", ""],
        ["Training", ""],
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

@schema
class RigStatus(dj.Manual):
    definition = """
    # Status for each IO module of the rig
    -> lab.Location
    -> scheduler.InputOutputRig
    ---
    current_status       : enum('OK','Not OK','N/A')    # if module is working or not
    -> [nullable] scheduler.RigIOTechReport
    last_status_update   : datetime                     # at what time status changed
    """

@schema
class Shift(dj.Lookup):
    definition = """
    # Defines
    shift: varchar(100)                 # Name and ID for Input/Output
    ---
    start_time: time        # Date agnostic time; make sure to add the datetime when using it with tech_scheduler
    end_time: time          # Date agnostic time; make sure to add the datetime when using it with tech_scheduler
    """
    contents = [
        ["Day", "09:00:00", "17:00:00"],
        ["Evening", "17:00:00", "01:00:00"],
        ["Night", "01:00:00", "09:00:00"],
    ]


@schema
class TechSchedule(dj.Manual):
    definition = """
    shift_index: int auto_increment
    ---
    date                 : date
    -> Shift
    -> lab.User
    -> TechDuties
    start_time : datetime          # Datetime of when the shift ends
    end_time : datetime          # Datetime of when the shift ends
    """


@schema
class SchedulingNotes(dj.Manual):
    definition = """
    notes_index: int auto_increment
    ---
    date                 : date
    message : varchar(32768)
    -> lab.User
    """
