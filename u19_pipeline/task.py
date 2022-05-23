"""This module defines tables in the schema U19_task"""

import datajoint as dj


schema = dj.schema(dj.config['custom']['database.prefix'] + 'task')


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
class TaskLevelParameterSet(dj.Lookup):
    definition = """
    -> Task
    level                : int                          # difficulty level
    set_id=1             : int                          # parameter set id
    """

