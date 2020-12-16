"""This module defines tables in the schema ahoag_puffs_acquisition_demo"""

import datajoint as dj
from . import puffs_lab

schema = dj.schema(dj.config['database.prefix'] + 'puffs_acquisition')


@schema
class PuffsFileAcquisition(dj.Manual):
    definition = """
    -> puffs_lab.PuffsCohort
    rig                  : tinyint                      # an integer that describes which rig was used. 0 corresponds to location = "pni-ltl016-05", 1 corresponds to location = "wang-behavior"
    h5_filename          : varchar(256)                 # The full path name, e.g. "data.h5" or "data_compressed_XX.h5" for the h5 behavior data file
    ---
    ingested             : boolean                      # A flag for whether this file has already been ingested.  
    """