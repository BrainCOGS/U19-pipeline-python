from importlib.resources import path
import datajoint as dj
import pathlib
import numpy as np
import pandas as pd
import scipy


from element_array_ephys import probe as probe_element
from element_array_ephys import ephys_precluster as ephys_element

from u19_pipeline import recording
from u19_pipeline.utils import path_utils as pu

from element_array_ephys.readers import spikeglx
from element_interface.utils import find_full_path

try:
    from ecephys_spike_sorting.modules.kilosort_helper.__main__ import get_noise_channels
except Exception as e:
    print(f'Error in loading "ecephys_spike_sorting" package - {str(e)}')

schema = dj.schema(dj.config['custom']['database.prefix'] + 'ephys_pipeline')

# Declare upstream table ---------------------------------------------------------------
@schema
class EphysPipelineSession(dj.Computed):
    definition = """
    -> recording.Recording
    """

    @property
    def key_source(self):
        return recording.Recording & {'recording_modality': 'electrophysiology'}

    def make(self, key):
        self.insert1(key)

# Gathering requirements to activate `element-array-ephys` -----------------------------
"""
1. Schema names
    + schema name for the probe module
    + schema name for the ephys module
2. Upstream tables
    + SkullReference table - Reference table for InsertionLocation, specifying the skull reference used for probe insertion location (e.g. Bregma, Lambda)
    + Session table
3. Utility functions
    + get_ephys_root_data_dir()
    + get_session_directory()
For more detail, check the docstring of the ephys element:
    help(probe_element.activate)
    help(ephys_element.activate)
"""

# 1. Schema names
probe_schema_name = dj.config['custom']['database.prefix'] + 'pipeline_probe_element'
ephys_schema_name = dj.config['custom']['database.prefix'] + 'pipeline_ephys_element'

# 2. Upstream tables
reference_schema = dj.schema(dj.config['custom']['database.prefix'] + 'reference')

@reference_schema
class SkullReference(dj.Lookup):
    definition = """
    skull_reference   : varchar(60)
    """
    contents = zip(['Bregma', 'Lambda'])

Session = EphysPipelineSession

# 3. Utility functions
def get_ephys_root_data_dir():
    data_dir = dj.config.get('custom', {}).get('ephys_root_data_dir', None)
    #if isinstance(data_dir, list):
    #    data_dir = data_dir[0]

    return data_dir if data_dir else None

def get_session_directory(session_key):

    root_dir = get_ephys_root_data_dir()

    session_dir = (recording.Recording & session_key).fetch1('recording_directory')
    session_dir = pathlib.Path(root_dir, session_dir).as_posix()

    return session_dir

# Activate `ephys_pipeline` and `probe_pipeline` schemas -------------------------------
ephys_element.activate(ephys_schema_name, probe_schema_name, linking_module=__name__)

# Create Neuropixels probe entries -----------------------------------------------------
for probe_type in ('neuropixels 1.0 - 3A', 'neuropixels 1.0 - 3B',
                   'neuropixels 2.0 - SS', 'neuropixels 2.0 - MS'):
    probe_element.ProbeType.create_neuropixels_probe(probe_type)


def get_spikeglx_meta_filepath(ephys_recording_key):
    # attempt to retrieve from EphysRecording.EphysFile
    spikeglx_meta_filepath = (ephys_element.EphysRecording.EphysFile & ephys_recording_key
                              & 'file_path LIKE "%.ap.meta"').fetch1('file_path')

    print('ephys_recording_key', ephys_recording_key)
    print('spikeglx_meta_filepath', spikeglx_meta_filepath)

    print('get_ephys_root_data_dir', get_ephys_root_data_dir())
    spikeglx_meta_filepath = find_full_path(get_ephys_root_data_dir(),
                                                spikeglx_meta_filepath)
    print('spikeglx_meta_filepath', spikeglx_meta_filepath)

    try:
        spikeglx_meta_filepath = find_full_path(get_ephys_root_data_dir(),
                                                spikeglx_meta_filepath)

        print('spikeglx_meta_filepath', spikeglx_meta_filepath)
    except FileNotFoundError:
        # if not found, search in session_dir again
        if not spikeglx_meta_filepath.exists():
            session_dir = find_full_path(get_ephys_root_data_dir(),
                                      get_session_directory(ephys_recording_key))
            inserted_probe_serial_number = (ephys_element.ProbeInsertion * ephys_element.probe.Probe
                                            & ephys_recording_key).fetch1('probe')

            spikeglx_meta_filepaths = [fp for fp in session_dir.rglob('*.ap.meta')]
            for meta_filepath in spikeglx_meta_filepaths:
                spikeglx_meta = spikeglx.SpikeGLXMeta(meta_filepath)
                if str(spikeglx_meta.probe_SN) == inserted_probe_serial_number:
                    spikeglx_meta_filepath = meta_filepath
                    break
            else:
                raise FileNotFoundError(
                    'No SpikeGLX data found for probe insertion: {}'.format(ephys_recording_key))

    return spikeglx_meta_filepath
