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
    profile_id                    : int
    ---
    -> lab.User
    date_created                 : date
    profile_description           : varchar(255)          # Profile description
    profile_variables             : longblob                  # Encoded for the variables
    """

@schema
class RecordingProfile(dj.Manual):
    definition = """
    profile_id                    : int
    ---
    -> lab.User
    date_created                 : date
    profile_description           : varchar(255)          # Profile description
    profile_variables             : longblob                  # Encoded for the variables
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


