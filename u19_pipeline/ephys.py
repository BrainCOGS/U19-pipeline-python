import datajoint as dj
import pathlib
import numpy as np
from bitstring import BitArray

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
probe_schema_name = dj.config['custom']['database.prefix'] + 'probe_element'
ephys_schema_name = dj.config['custom']['database.prefix'] + 'ephys_element'

# 2. Upstream tables
schema_reference = dj.schema(dj.config['custom']['database.prefix'] + 'reference')
schema = dj.schema(dj.config['custom']['database.prefix'] + 'ephys')


@schema_reference
class SkullReference(dj.Lookup):
    definition = """
    skull_reference   : varchar(60)
    """
    contents = zip(['Bregma', 'Lambda'])


@schema
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
@schema
class BehaviorSync(dj.Imported):
    definition = """
    -> ephys.EphysSession
    ---
    nidq_sampling_rate    : float        # sampling rate of behavioral iterations niSampRate in nidq meta file
    iteration_index_nidq  : longblob     # Virmen index time series. Length of this longblob should be the number of iterations in the behavior recording
    trial_index_nidq=null : longblob     # Trial index time series. length of this longblob should be the number of iterations in the behavior recording
    """

    class ImecSamplingRate(dj.Part):
        definition = """
        -> master
        -> ephys_element.ProbeInsertion
        ---
        ephys_sampling_rate: float     # sampling rate of the headstage of a probe, imSampRate in imec meta file
        """

    def make(self, key):
        print(key)
        session_dir = pathlib.Path(get_session_directory(key))
        nidq_bin_full_path = list(session_dir.glob('*nidq.bin*'))[0]

        # Second, get the datajoint record
        behavior = dj.create_virtual_module('behavior', 'u19_behavior')
        thissession = behavior.TowersBlock().Trial() & key
        time, iterstart = thissession.fetch('trial_time', 'vi_start')
        print("Step 0 done")

        # 0: load meta data
        print("Step 1...")
        nidq_meta = readMeta(nidq_bin_full_path)
        t_start = 0
        t_end = np.float(nidq_meta['fileTimeSecs'])
        dw = 0
        d_line_list = [0, 1, 2, 3, 4, 5, 6, 7]
        nidq_sampling_rate = SampRate(nidq_meta)
        first_sample_index = int(nidq_sampling_rate * t_start)
        last_sample_index = int(nidq_sampling_rate * t_end) - 1
        nidq_raw_data = makeMemMapRaw(nidq_bin_full_path, nidq_meta)  # Pull raw bin data
        digital_array = ExtractDigital(                               # extract interation index
            nidq_raw_data, first_sample_index, last_sample_index,
            dw, d_line_list, nidq_meta)
        print("Step 1 done")
        
        # First, transform digital lines into a number, save in an array of integers
        #      ... and also get start and end time
        framenumber = np.zeros(digital_array.shape[1])
        for i in range(digital_array.shape[1]):
            a = BitArray(np.flip(digital_array[1:, i])) # ignore 0-bit, as this is the NPX sync puls, and not virmen.
            framenumber[i] = a.uint
        iterations_raw = np.array(framenumber, dtype=np.int) # Transform frames into integer
        recording_start = np.min(np.where(iterations_raw>0)) #Get start of recording: first change of testlist
        dt = np.int(0.04*nidq_sampling_rate)
        recording_end = np.where(np.abs(np.diff(iterations_raw))>0)[0][-1] + dt  #Get end of recording
        print("Step 2 done")

        # Third, transform `iterations_raw` into `framenumber_in_trial` and `trialnumber`
        framenumber_in_trial = np.zeros(len(iterations_raw))*np.NaN
        trialnumber = np.zeros(len(iterations_raw))*np.NaN
        current_trial = 0
        overflow = 0
        for idx, frame_number in enumerate(iterations_raw):
            if (idx>recording_start) & (idx<recording_end):
                if (frame_number==0) & (iterations_raw[idx-1]==127): # At the reset, add 128
                    overflow = overflow + 1
                if (frame_number==0) & (iterations_raw[idx-1]!=127) & (iterations_raw[idx-1]!=0) &  (iterations_raw[idx-2]==127): # Unlucky reset if happened to be sampled at the wrong time
                    overflow = overflow + 1
                    framenumber_in_trial[idx-1] = frame_number + overflow*128 - 1 # In case this happened, the previous sample has to be corrected
                # Keep track of trial number
                endflag = framenumber_in_trial[idx-1] == (len(time[current_trial])) #Trial end has been reached.
                transitionflag = frame_number < 3 # Next trial should start at zero again
                if endflag & transitionflag:      # Only at the transitions
                    current_trial = current_trial + 1  # Increases trial count
                    overflow = 0                       # Reset the 7 bit counter
                framenumber_in_trial[idx] = frame_number + overflow*128 - 1
                trialnumber[idx] = current_trial
        trial_list = np.array(np.unique(trialnumber[np.isfinite(trialnumber)]), dtype = np.int)
        print("Step 3 done")

        # Fourth, find and remove the nidaq glitches 
        # These are single samples where the iteration number is corrupted
        # ... likely because sampling happeneded faster than output of the behvior PC.
        # This is also where skipped frames are detected.
        # Find the glitches
        din = np.diff(framenumber_in_trial)
        trial_transitions = np.where(np.diff(trialnumber))
        glitches = []
        for candidate in np.where( np.logical_or(din>1, din<0) )[0]: # skipped frames or counting down
            if np.sum(candidate == trial_transitions) == 0:
                glitches = np.append(glitches, candidate)
        glitches = np.array(glitches, dtype = np.int)
        # Attempt to remove them
        skipped_frames = 0
        for g in glitches:
            if framenumber_in_trial[g] < framenumber_in_trial[g+2]:
                if framenumber_in_trial[g+2] -  framenumber_in_trial[g] == 2:  # skipped frame, should be very rare
                    framenumber_in_trial[g+1] = framenumber_in_trial[g]+1
                    skipped_frames = skipped_frames + 1
                else:                          # If random number, nidaq sample in the middle of update.
                    framenumber_in_trial[g+1] = framenumber_in_trial[g]
        print("Step 4 done")

        #iteration_index_nidq = np.where(np.abs(np.diff(np.double(digital_array[1, :]))) > 0.5)[0]

        # A set of final asserts, making sure that the code worked as intended  
        assert len(trial_list) == len(thissession)          # Make sure the trial number is correct.
        assert np.sum(np.diff(framenumber_in_trial)>1) == 0 # No frames should be skipped
        assert np.sum(np.diff(framenumber_in_trial)<0)<len(trial_list) # Negative iterations only at trial transitions
        iterations_test = 0
        for t in trial_list:
            iterations_test = iterations_test + framenumber_in_trial[trialnumber==t][-1]  # Integrate number of iterations
            assert framenumber_in_trial[trialnumber==t][-1] == len(time[t])  # Make sure number of nidaq-frames in each trial is identical to dj record:
            nidaqtime = np.sum(trialnumber == t)/nidq_sampling_rate
            matlabtime = np.max(time[t])
            assert ((nidaqtime - matlabtime) / matlabtime) < 0.1 # # Make sure the nidaq-trial-duration and dj records are consistent; 10% arbitrarily chosen
        nidaq_duration = iterations_test + skipped_frames
        dj_duration = iterstart[-1] + len(time[-1])
        assert np.abs(nidaq_duration - dj_duration) < 3 # at most two frames off - sometimes this happens at the beginning/end of the recording

        # If this is done, and the asserts are passed, insert the data into the database
        self.insert1(
            dict(key, nidq_sampling_rate = nidq_sampling_rate,
                 iteration_index_nidq = framenumber_in_trial,
                 trial_index_nidq = trialnumber))
        print('done')

@schema
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
