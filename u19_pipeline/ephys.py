import datajoint as dj

from u19_pipeline import acquisition


schema = dj.schema(dj.config['database.prefix'] + 'ephys')


@schema
class EphysSession(dj.Manual):
    definition = """
    # General information of an ephys session
    -> acquisition.Session
    ---
    ephys_directory: varchar(255)      # the absolute directory where the ephys data for this session will be stored in bucket
    """


@schema
class EphysSync(dj.Imported):
    definition = """
    -> EphysSession
    ---
    iteration_numbers   : longblob
    ephys_timestamps    : longblob   # time stamps on the ephys clock corresponding to each iteration number on the behavior rig.
    """
