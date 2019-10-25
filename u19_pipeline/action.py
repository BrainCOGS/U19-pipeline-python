"""This module defines tables in the schema U19_action"""


import datajoint as dj
from . import lab, reference, subject

schema = dj.schema(dj.config['database.prefix'] + 'action')


@schema
class Weighing(dj.Manual):
    definition = """
    -> subject.Subject
    weighing_time        : datetime
    ---
    -> lab.User.proj(weigh_person="user_id")
    -> lab.Location
    weight               : float                        # in grams
    weight_notice=""     : varchar(255)
    """


@schema
class SubjectStatus(dj.Manual):
    definition = """
    -> subject.Subject
    effective_date       : date
    ---
    subject_status       : enum('InExperiments','WaterRestrictionOnly','AdLibWater','Dead')
    water_per_day=null   : float                        # in mL
    schedule=null        : varchar(255)
    """


@schema
class ActionItem(dj.Manual):
    definition = """
    # action item performed every day on each subject
    -> subject.Subject
    action_date          : date                         # date of action
    action_id            : tinyint                      # action id
    ---
    action               : varchar(255)
    """


@schema
class WaterRestriction(dj.Manual):
    definition = """
    -> subject.Subject
    restriction_start_time : datetime                     # start time
    ---
    restriction_end_time=null : datetime                     # end time
    restriction_narrative="" : varchar(1024)                # comment
    """


@schema
class WaterType(dj.Lookup):
    definition = """
    watertype_name       : varchar(255)
    """
    contents = zip(['Water', 'Water 10% Sucrose', 'Milk', 'Unknown'])


@schema
class WaterAdministration(dj.Manual):
    definition = """
    -> subject.Subject
    administration_date  : date                         # date time
    ---
    earned=null          : float                        # water administered
    supplement=null      : float
    received=null        : float
    -> WaterType
    """


@schema
class Notification(dj.Manual):
    definition = """
    # This table documents whether a notification has been sent to the users.
    -> subject.Subject
    notification_date    : date                         # Date of notification
    ---
    time=null            : datetime                     # Exact time of notification
    cage_notice=""       : varchar(255)                 # Cage-notice. Cage not returned
    health_notice=""     : varchar(255)                 # Health-notice. missed action Items
    weight_notice=""     : varchar(255)                 # Weight-notice. mouse too light
    """


@schema
class SurgeryType(dj.Lookup):
    definition = """
    surgery_type         : varchar(32)
    """
    contents = zip([
        'Craniotomy',
        'Hippocampal window',
        'GRIN lens implant'
    ])

@schema
class Surgery(dj.Manual):
    definition = """
    -> lab.User
    -> subject.Subject
    surgery_start_time   : datetime                     # surgery start time
    ---
    surgery_end_time=null : datetime                     # surgery end time
    -> lab.Location
    surgery_outcome_type : enum('success','death')      # outcome type
    surgery_narrative=null : varchar(1024)                # narrative
    """


@schema
class SurgerySurgeryType(dj.Manual):
    definition = """
    -> Surgery
    -> SurgeryType
    """


@schema
class VirusInjection(dj.Manual):
    definition = """
    -> Surgery
    -> reference.Virus
    ---
    injection_volume     : float                        # injection volume
    rate_of_injection    : float                        # rate of injection
    virus_dilution       : float                        # x dilution of the original virus
    -> reference.BrainArea
    """
