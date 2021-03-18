"""This module defines tables in the schema U19_task"""

import datajoint as dj


schema = dj.schema(dj.config['database.prefix'] + 'task')


@schema
class Task(dj.Lookup):
    definition = """
    task                 : varchar(32)
    ---
    task_description=""  : varchar(512)
    """
    contents = [
        ['AirPuffs', ''],
        ['Towers', ''],
        ['Clicks', ''],
        ['LinearTrack', '']
    ]


@schema
class ParameterCategory(dj.Lookup):
    definition = """
    parameter_category   : varchar(16)
    """
    contents = zip(['maze', 'criterion', 'visible'])


@schema
class Parameter(dj.Lookup):
    definition = """
    parameter            : varchar(32)
    ---
    -> ParameterCategory
    parameter_description="" : varchar(255)                 # info such as the unit
    """


@schema
class TaskLevelParameterSet(dj.Lookup):
    definition = """
    -> Task
    level                : int                          # difficulty level
    set_id=1             : int                          # parameter set id
    """


@schema
class TaskParameter(dj.Lookup):
    definition = """
    -> TaskLevelParameterSet
    -> Parameter
    ---
    parameter_value      : blob
    """
