import datajoint as dj
import pathlib
import numpy as np

from u19_pipeline import ephys

from element_array_ephys import probe as probe_element
from element_array_ephys import ephys as ephys_element

from u19_pipeline.utils.DemoReadSGLXData.readSGLX import readMeta, SampRate, makeMemMapRaw, ExtractDigital

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
schema_reference = dj.schema(dj.config['database.prefix'] + 'reference')
schema_ephys = dj.schema(dj.config['database.prefix'] + 'ephys')


@schema_reference
class SkullReference(dj.Lookup):
    definition = """
    skull_reference   : varchar(60)
    """
    contents = zip(['Bregma', 'Lambda'])


@schema_ephys
class EphysSession(dj.Manual):
    definition = """
    # General information of an ephys session
    -> acquisition.Session
    ---
    ephys_directory: varchar(255)      # the absolute directory where the ephys data for this session will be stored in bucket
    """


# ephys element requires table with name Session
Session = EphysSession


# 3. Utility functions

def get_ephys_root_data_dir():
    data_dir = dj.config.get('custom', {}).get('ephys_root_data_dir', None)
    return pathlib.Path(data_dir) if data_dir else None


def get_session_directory(session_key):
    sess_dir = pathlib.Path((ephys.EphysSession & session_key).fetch1('ephys_directory'))
    return sess_dir.as_posix()


# ------------- Activate "ephys" schema -------------
ephys_element.activate(ephys_schema_name, probe_schema_name, linking_module=__name__)


# ------------- Create Neuropixels probe entries -------------
for probe_type in ('neuropixels 1.0 - 3A', 'neuropixels 1.0 - 3B',
                   'neuropixels 2.0 - SS', 'neuropixels 2.0 - MS'):
    probe_element.ProbeType.create_neuropixels_probe(probe_type)


# downstream tables for ephys element
@schema_ephys
class BehaviorSync(dj.Imported):
    definition = """
    -> ephys.EphysSession
    ---
    nidq_sampling_rate  : float         # sampling rate of behavioral iterations niSampRate in nidq meta file
    iteration_index_nidq: longblob      # length of this longblob should be the number of iterations in the behavior recording
    trial_index_nidq=null: longblob     # length of this longblob should be the number of iterations in the behavior recording
    """

    class ImecSamplingRate(dj.Part):
        definition = """
        -> master
        -> ephys_element.ProbeInsertion
        ---
        ephys_sampling_rate: float     # sampling rate of the headstage of a probe, imSampRate in imec meta file
        """

    def make(self, key):
        session_dir = pathlib.Path(get_session_directory(key))
        nidq_bin_full_path = list(session_dir.glob('*nidq.bin*'))[0]

        # load meta data
        nidq_meta = readMeta(nidq_bin_full_path)
        t_start = 0
        t_end = np.float(nidq_meta['fileTimeSecs'])
        dw = 0
        d_line_list = [0, 1, 2, 3, 4, 5, 6, 7]

        nidq_sampling_rate = SampRate(nidq_meta)
        first_sample_index = int(nidq_sampling_rate * t_start)
        last_sample_index = int(nidq_sampling_rate * t_end) - 1

        # raw bin data
        nidq_raw_data = makeMemMapRaw(nidq_bin_full_path, nidq_meta)

        # extract interation index
        digital_array = ExtractDigital(
            nidq_raw_data, first_sample_index, last_sample_index,
            dw, d_line_list, nidq_meta)
        iteration_index_nidq = np.where(np.abs(np.diff(np.double(digital_array[1, :]))) > 0.5)[0]

        self.insert1(
            dict(key, nidq_sampling_rate=nidq_sampling_rate,
                 iteration_index_nidq=iteration_index_nidq))

        # get the imec sampling rate for a particular probe
        for probe_insertion in (ephys_element.ProbeInsertion & key).fetch('KEY'):
            imec_bin_filepath = list(
                session_dir.glob('*imec{}/*.ap.bin'.format(probe_insertion["insertion_number"])))[0]
            imec_meta = readMeta(imec_bin_filepath)
            self.ImecSamplingRate.insert1(
                dict(probe_insertion,
                     ephys_sampling_rate=imec_meta['imSampRate']))


@schema_ephys
class CuratedClustersIteration(dj.Computed):
    definition = """
    -> ephys_element.CuratedClustering
    -> BehaviorSync
    """

    class Unit(dj.Part):
        definition = """
        -> master
        -> ephys_element.CuratedClustering.Unit
        ---
        spike_counts_iteration:   longblob   # number of spikes during each iteration. have length as the number of iterations - 1
        firing_rate_before_first_iteration: float
        firing_rate_after_last_iteration: float
        """

    def make(self, key):

        self.insert1(key)

        nidq_sampling_rate, iteration_index_nidq = \
            (BehaviorSync * BehaviorSync.ImecSamplingRate & key).fetch1(
                'nidq_sampling_rate', 'iteration_index_nidq')

        # iteration times on the same clock as ephys data
        iteration_times = iteration_index_nidq/nidq_sampling_rate

        # get end of time from nidq metadata
        session_dir = pathlib.Path(get_session_directory(key))
        nidq_bin_full_path = list(session_dir.glob('*nidq.bin*'))[0]
        nidq_meta = readMeta(nidq_bin_full_path)
        t_end = np.float(nidq_meta['fileTimeSecs'])

        unit_spike_counts = []

        for unit_key in (ephys_element.CuratedClustering.Unit & key).fetch('KEY'):
            spike_times = (ephys_element.CuratedClustering.Unit & unit_key).fetch1('spike_times')
            # vector with length n_iterations + 1
            spike_counts_iteration = np.bincount(np.digitize(spike_times, iteration_times))

            firing_rate_before_first_iteration = spike_counts_iteration[0]/iteration_times[0]
            firing_rate_after_last_iteration = spike_counts_iteration[-1]/(t_end - iteration_times[-1])

            # remove the first and last entries
            spike_counts_iteration = np.delete(spike_counts_iteration, [0, -1])

            unit_spike_counts.append(
                dict(unit_key,
                     spike_counts_iteration=spike_counts_iteration,
                     firing_rate_before_first_iteration=firing_rate_before_first_iteration,
                     firing_rate_after_last_iteration=firing_rate_after_last_iteration)
            )
        self.Unit.insert(unit_spike_counts)


# Find particular position in the maze and look at the firing rates (find place fields)
