
import datajoint as dj
import pathlib
import glob
import re
import subprocess
import json

from element_array_ephys import probe as probe_element
from element_array_ephys import ephys_precluster as ephys_element
from element_array_ephys.readers import spikeglx
from element_interface.utils import find_full_path

from u19_pipeline import recording
import u19_pipeline.utils.ephys_utils as ephys_utils
import u19_pipeline.utils.DemoReadSGLXData.readSGLX as readSGLX
from u19_pipeline.utils.DemoReadSGLXData.readSGLX import readMeta

try:
    from ecephys_spike_sorting.modules.kilosort_helper.__main__ import get_noise_channels
except Exception as e:
    print(f'Error in loading "ecephys_spike_sorting" package - {str(e)}')

schema = dj.schema(dj.config['custom']['database.prefix'] + 'ephys_pipeline')

lfp_filter_params = 'biquad,2,0,500'

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

    #root_dir = get_ephys_root_data_dir()
    
    session_dir = pathlib.Path((recording.Recording & session_key).fetch1('recording_directory')).as_posix()
    #session_dir = pathlib.Path(root_dir, session_dir).as_posix()

    return session_dir

def append_cat_gt_params_from_probedir(probe_dirname):

    extra_cat_gt_params = dict()

    probe_match = re.search("_imec[0-9]$", probe_dirname)
    if probe_match:
        probe_text = probe_match.group()
        extra_cat_gt_params['prb'] = re.search(r'\d+',probe_text).group()
    else:
        raise ValueError(probe_dirname +' is not a valid probe directory')

    session_num_match = re.search("_g[0-9]_", probe_dirname)
    if session_num_match:
        extra_cat_gt_params['run'] = probe_dirname[:session_num_match.start()]
        session_text = session_num_match.group()
        extra_cat_gt_params['g'] = re.search(r'\d+',session_text).group()
    else:
        raise ValueError(probe_dirname +' is not a valid probe directory')

    trigger_num_match = re.search("_t[0-9]_", probe_dirname)
    if trigger_num_match:
        trigger_text = trigger_num_match.group()
        extra_cat_gt_params['t'] = re.search(r'\d+',trigger_text).group()
    else:
        extra_cat_gt_params['t'] = '0'

    return extra_cat_gt_params

def create_lfp_trace(cat_gt_script, recording_directory, probe_directory):

    status = True
    found_lfp_trace = glob.glob(probe_directory + '/*lf.bin')
    if len(found_lfp_trace) > 0:
        return status

    #Create catgt command if no lfp trace was found
    cat_gt_params = append_cat_gt_params_from_probedir(probe_directory)
    cat_gt_command = [cat_gt_script, '-dir='+recording_directory, '-run='+cat_gt_params['run'], '-g='+cat_gt_params['g'],
    '-t='+cat_gt_params['t'], '-prb='+cat_gt_params['prb'], '-prb_fld', '-lf', '-lffilter='+lfp_filter_params]


    print(cat_gt_command)
    p = subprocess.Popen(cat_gt_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    stdout, stderr = p.communicate()

    if stderr:
        error = json.loads(stderr.decode('UTF-8'))
        raise Exception(error)

    return status


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


# downstream tables for ephys element
@schema
class BehaviorSync(dj.Imported):
    definition = """
    -> ephys_pipeline.EphysPipelineSession
    ---
    nidq_sampling_rate    : float        # sampling rate of behavioral iterations niSampRate in nidq meta file
    iteration_index_nidq  : longblob     # Virmen index time series. Length of this longblob should be the number of samples in the nidaq file.
    trial_index_nidq=null : longblob     # Trial index time series. length of this longblob should be the number of samples in the nidaq file.
    """

    class ImecSamplingRate(dj.Part):
        definition = """
        -> master
        -> ephys_element.ProbeInsertion
        ---
        ephys_sampling_rate: float     # sampling rate of the headstage of a probe, imSampRate in imec meta file
        """

    def make(self, key):
        # Pull the Nidaq file/record

        try:
            session_dir = find_full_path(get_ephys_root_data_dir(),
                                        get_session_directory(key))
            print(session_dir)
            nidq_bin_full_path = list(session_dir.glob('*nidq.bin*'))[0]
            # And get the datajoint record
            behavior = dj.create_virtual_module('behavior', 'u19_behavior')

            print(key)

            behavior_key = (recording.Recording.BehaviorSession & key).fetch1()
            behavior_key.pop('recording_id')

            if 'testuser' in behavior_key['subject_fullname']:
                return

            print(behavior_key)

            thissession = behavior.TowersBlock().Trial() & behavior_key
            behavior_time, iterstart = thissession.fetch('trial_time', 'vi_start')

            print('len iterstart', len(iterstart))

            print('after reading behavior data')

            # 1: load meta data, and the content of the NIDAQ file. Its content is digital.
            nidq_meta          = readSGLX.readMeta(nidq_bin_full_path)
            nidq_sampling_rate = readSGLX.SampRate(nidq_meta)
            digital_array      = ephys_utils.spice_glx_utility.load_spice_glx_digital_file(nidq_bin_full_path, nidq_meta)

            print('after reading spikeglx data')

            # Synchronize between pulses and get iteration # vector for each sample
            mode=None
            iteration_dict = ephys_utils.get_iteration_sample_vector_from_digital_lines_pulses(digital_array[1,:], digital_array[2,:], nidq_sampling_rate, behavior_time.shape[0], behavior_time, mode)
            # Check # of trials and iterations match
            status = ephys_utils.assert_iteration_samples_count(iteration_dict['iter_start_idx'], behavior_time)

            if not status:
                return

            #They didn't match, try counter method (if available)
            if (not status) and (digital_array.shape[0] > 3):
                [framenumber_in_trial, trialnumber] = ephys_utils.behavior_sync_frame_counter_method(digital_array, behavior_time, thissession, nidq_sampling_rate, 3, 5)
                iteration_dict['framenumber_vector_samples'] = framenumber_in_trial
                iteration_dict['trialnumber_vector_samples'] = trialnumber


            final_key = dict(key, nidq_sampling_rate = nidq_sampling_rate, 
                iteration_index_nidq = iteration_dict['framenumber_vector_samples'],
                trial_index_nidq = iteration_dict['trialnumber_vector_samples'])

            print(final_key)

            BehaviorSync.insert1(final_key,allow_direct_insert=True)

            self.insert_imec_sampling_rate(key, session_dir)

        except Exception as e:
            print(e)

    def insert_imec_sampling_rate(self, key, session_dir):

        # get the imec sampling rate for a particular probe
        here = ephys_element.ProbeInsertion & key
        for probe_insertion in here.fetch('KEY'):
            #imec_bin_filepath = list(session_dir.glob('*imec{}/*.ap.bin'.format(probe_insertion['insertion_number'])))
            imec_bin_filepath = list(session_dir.glob('*imec{}/*.ap.meta'.format(probe_insertion['insertion_number'])))

            if len(imec_bin_filepath) == 1:    # find the binary file to get meta data
                imec_bin_filepath = imec_bin_filepath[0]
            else:                               # if this fails, get the ap.meta file.
                imec_bin_filepath = list(session_dir.glob('*imec{}/*.ap.meta'.format(probe_insertion['insertion_number'])))
                if len(imec_bin_filepath) == 1:
                    s = str(imec_bin_filepath[0])
                    imec_bin_filepath = pathlib.Path(s.replace(".meta", ".bin"))
                else:   # If this fails too, no imec file exists at the path.
                    raise NameError("No imec meta file found.")

            imec_meta = readMeta(imec_bin_filepath)
            self.ImecSamplingRate.insert1(
                dict(probe_insertion,
                        ephys_sampling_rate=imec_meta['imSampRate']))