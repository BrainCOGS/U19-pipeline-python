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



def generate_chanmap_file(recording_key, raw_directory,save_path):
    ''' Generate a dictionary with all chanmap data from a recording probe; 
    Input:
    recording_key   =  (dict) recording_id & insertion_number info
    Output:
    mat_file_dict   =  (dict) dictionary with chanmap data
    '''

    print(recording_key)
    chanmap_data = get_recording_channels_details(recording_key)

    raw_directory = find_full_path(get_ephys_root_data_dir(),
                                                raw_directory)
    continuous_files = pu.get_filepattern_paths(raw_directory.as_posix(), '/*.ap.bin')
    continuous_files = continuous_files[0]


    continuous_file = pathlib.Path(raw_directory,'/*.ap.bin')

    _write_channel_map_file(channel_ind=chanmap_data['channel_ind'],
                            x_coords=chanmap_data['x_coords'],
                            y_coords=chanmap_data['y_coords'],
                            shank_ind=chanmap_data['shank_ind'],
                            connected=chanmap_data['connected'],
                            probe_name=chanmap_data['probe_type'],
                            ap_band_file=continuous_files,
                            bit_volts=chanmap_data['uVPerBit'],
                            sample_rate=chanmap_data['sample_rate'],
                            save_path=save_path,
                            is_0_based=True)

    return True


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

def get_recording_channels_details(ephys_recording_key):
    channels_details = {}

    acq_software, sample_rate = (ephys_element.EphysRecording & ephys_recording_key).fetch1('acq_software',
                                                              'sampling_rate')

    probe_type = (ephys_element.ProbeInsertion * ephys_element.probe.Probe & ephys_recording_key).fetch1('probe_type')
    channels_details['probe_type'] = {'neuropixels 1.0 - 3A': '3A',
                                      'neuropixels 1.0 - 3B': 'NP1',
                                      'neuropixels UHD': 'NP1100',
                                      'neuropixels 2.0 - SS': 'NP21',
                                      'neuropixels 2.0 - MS': 'NP24'}[probe_type]

    electrode_config_key = (ephys_element.probe.ElectrodeConfig * ephys_element.EphysRecording & ephys_recording_key).fetch1('KEY')
    channels_details['channel_ind'], channels_details['x_coords'], channels_details[
        'y_coords'], channels_details['shank_ind'] = (
            ephys_element.probe.ElectrodeConfig.Electrode * ephys_element.probe.ProbeType.Electrode
            & electrode_config_key).fetch('electrode', 'x_coord', 'y_coord', 'shank')
    channels_details['sample_rate'] = sample_rate
    channels_details['num_channels'] = len(channels_details['channel_ind'])


    spikeglx_meta_filepath = get_spikeglx_meta_filepath(ephys_recording_key)
    spikeglx_recording = spikeglx.SpikeGLX(spikeglx_meta_filepath.parent)
    channels_details['uVPerBit'] = spikeglx_recording.get_channel_bit_volts('ap')[0]
    channels_details['connected'] = np.array(
        [v for *_, v in spikeglx_recording.apmeta.shankmap['data']])


    return channels_details


def _write_channel_map_file(*, channel_ind, x_coords, y_coords, shank_ind, connected,
                            probe_name, ap_band_file, bit_volts, sample_rate,
                            save_path, is_0_based=True):
    """
    Write channel map into .mat file in 1-based indexing format (MATLAB style)
    """

    assert len(channel_ind) == len(x_coords) == len(y_coords) == len(shank_ind) == len(connected)

    if is_0_based:
        channel_ind += 1
        shank_ind += 1

    channel_count = len(channel_ind)
    chanMap0ind = np.arange(0, channel_count, dtype='float64')
    chanMap0ind = chanMap0ind.reshape((channel_count, 1))
    chanMap = chanMap0ind + 1

    #channels to exclude
    mask = get_noise_channels(ap_band_file,
                              channel_count,
                              sample_rate,
                              bit_volts)
    bad_channel_ind = np.where(mask is False)[0]
    connected[bad_channel_ind] = 0

    mdict = {
        'chanMap': chanMap,
        'chanMap0ind': chanMap0ind,
        'connected': connected,
        'name': probe_name,
        'xcoords': x_coords,
        'ycoords': y_coords,
        'shankInd': shank_ind,
        'kcoords': shank_ind,
        'fs': sample_rate
    }
    
    scipy.io.savemat(save_path, mdict)