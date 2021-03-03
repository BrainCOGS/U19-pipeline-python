import datajoint as dj
import pathlib

from u19_pipeline import acquisition, imaging

from elements_ephys import probe as probe_element
from elements_ephys import ephys as ephys_element

"""
------ Gathering requirements to activate the ephys elements ------

To activate the ephys elements, we need to provide:

1. Schema names
    + schema name for the probe module
    + schema name for the ephys module

2. Upstream tables
    + Session table 
    + SkullReference table - Reference table for InsertionLocation, specifying the skull reference
                 used for probe insertion location (e.g. Bregma, Lambda)

3. Utility functions
    + get_ephys_root_data_dir()
    + get_session_directory()

For more detail, check the docstring of the imaging element:

    help(probe_element.activate)
    help(ephys_element.activate)

"""

# 1. Schema names
probe_schema_name = dj.config['database.prefix'] + 'probe_element'
ephys_schema_name = dj.config['database.prefix'] + 'ephys_element'

# 2. Upstream tables
from u19_pipeline.acquisition import Session

schema = dj.schema(dj.config['database.prefix'] + 'reference')


@schema
class SkullReference(dj.Lookup):
    definition = """
    skull_reference   : varchar(60)
    """
    contents = zip(['Bregma', 'Lambda'])


# 3. Utility functions

def get_ephys_root_data_dir():
    data_dir = dj.config.get('custom', {}).get('ephys_root_data_dir', None)
    return pathlib.Path(data_dir) if data_dir else None


def get_session_directory(session_key):
    data_dir = get_ephys_root_data_dir()
    sess_dir = data_dir / (acquisition.DataDirectory & session_key).fetch1('data_dir')

    return sess_dir.as_posix()


# ------------- Activate "ephys" schema -------------
ephys_element.activate(ephys_schema_name, probe_schema_name, linking_module=__name__)
