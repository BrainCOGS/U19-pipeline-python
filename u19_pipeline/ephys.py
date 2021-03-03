
import datajoint as dj

from u19_pipeline import acquisition


schema = dj.schema(dj.config['database.prefix'] + 'ephys')


@schema
class EphysSession(dj.Manual):
    definition = """
    # General information of an ephys session
    -> acquisition.Session
    ---
    ephys_filepath              : varchar(255)                  # Path were session file will be stored in bucket
    """
