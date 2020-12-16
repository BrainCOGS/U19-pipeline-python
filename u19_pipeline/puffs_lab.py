"""This module defines tables in the schema ahoag_puffs_lab_demo"""

import datajoint as dj
from . import lab

schema = dj.schema(dj.config['database.prefix'] + 'puffs_lab')


@schema
class PuffsCohort(dj.Manual):
    definition = """
    -> lab.User         
    project_name         : varchar(64)                  # Corresponds to the path on bucket /puffs/netid/project_name/cohortX/ ...
    cohort               : varchar(64)                  # Corresponds to the path on bucket /puffs/netid/project_name/cohortX/ ...
    ---
    """
