import datajoint as dj
import pathlib
import numpy as np

from u19_pipeline import behavior, ephys_pipeline
from u19_pipeline.ephys_pipeline import get_session_directory

import u19_pipeline.utils.DemoReadSGLXData.readSGLX as readSGLX
import u19_pipeline.utils.ephys_utils as ephys_utils
import u19_pipeline.utils.path_utils as pu
import u19_pipeline.automatic_job.params_config as config

# Tables downstream from `ephys_pipeline` module ---------------------------------------
schema = dj.schema(dj.config['custom']['database.prefix'] + 'ephys_sync')

@schema
class BehaviorSync(dj.Imported):
    definition = """
    -> ephys_pipeline.EphysRecording
    ---
    nidq_sampling_rate    : float        # sampling rate of behavioral iterations niSampRate in nidq meta file
    iteration_index_nidq  : longblob     # Virmen index time series. Length of this longblob should be the number of samples in the nidaq file.
    trial_index_nidq=null : longblob     # Trial index time series. length of this longblob should be the number of samples in the nidaq file.
    """

    class ImecSamplingRate(dj.Part):
        definition = """
        -> master
        -> ephys_pipeline.ProbeInsertion
        ---
        ephys_sampling_rate: float     # sampling rate of the headstage of a probe, imSampRate in imec meta file
        """

    def make(self, key):
        # Pull the Nidaq file/record
        session_dir = pathlib.Path(get_session_directory(key))
        nidq_bin_full_path = list(session_dir.glob('*nidq.bin*'))[0]
        # And get the datajoint record
        thissession = behavior.TowersBlock().Trial() & key
        behavior_time, iterstart = thissession.fetch('trial_time', 'vi_start')

        # 1: load meta data, and the content of the NIDAQ file. Its content is digital.
        nidq_meta          = readSGLX.readMeta(nidq_bin_full_path)
        nidq_sampling_rate = readSGLX.SampRate(nidq_meta)
        digital_array      = ephys_utils.spice_glx_utility.load_spice_glx_digital_file(nidq_bin_full_path, nidq_meta)

        # Synchronize between pulses and get iteration # vector for each sample
        mode='counter_bit0'
        iteration_dict = ephys_utils.get_iteration_sample_vector_from_digital_lines_pulses(digital_array[1,:], digital_array[2,:], nidq_sampling_rate, behavior_time.shape[0], mode)
        # Check # of trials and iterations match
        status = ephys_utils.assert_iteration_samples_count(iteration_dict['iter_start_idx'], behavior_time)

        #They didn't match, try counter method (if available)
        if (not status) and (digital_array.shape[0] > 3):
            [framenumber_in_trial, trialnumber] = ephys_utils.behavior_sync_frame_counter_method(digital_array, behavior_time, thissession, nidq_sampling_rate, 3, 5)
            iteration_dict['framenumber_vector_samples'] = framenumber_in_trial
            iteration_dict['trialnumber_vector_samples'] = trialnumber


        final_key = dict(key, nidq_sampling_rate = nidq_sampling_rate, 
               iteration_index_nidq = iteration_dict['framenumber_vector_samples'],
               trial_index_nidq = iteration_dict['trialnumber_vector_samples'])

        print(final_key)

        self.insert1(final_key,allow_direct_insert=True)

        self.insert_imec_sampling_rate(key, session_dir)

        

    def insert_imec_sampling_rate(self, key, session_dir):

        # get the imec sampling rate for a particular probe
        here = ephys_pipeline.ProbeInsertion & key
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

            imec_meta = readSGLX.readMeta(imec_bin_filepath)
            self.ImecSamplingRate.insert1(
                dict(probe_insertion,
                        ephys_sampling_rate=imec_meta['imSampRate']))


@schema
class CuratedClustersIteration(dj.Computed):
    definition = """
    -> ephys_pipeline.CuratedClustering
    -> BehaviorSync
    """

    class Unit(dj.Part):
        definition = """
        -> master
        -> ephys_pipeline.CuratedClustering.Unit
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

        key_session = key.copy()
        del key_session["insertion_number"]

        thissession = behavior.TowersBlock().Trial() & key
        iterstart = thissession.fetch('vi_start')

        first_vr_iteration = iterstart[0]

        # Obtain the precise times when the frames transition.
        # This is obtained from iteration_index_nidq
        ls = np.diff(iteration_index_nidq)
        ls[ls<0] = 1 # These are the trial transitions (see definition above). To get total number of frames, we define this as a transition like all others. 
        ls[np.isnan(ls)] = 0
        iteration_transition_indexes = np.where(ls)[0]

        # First iterations captured not in virmen because vr was not started yet
        #for i in range(first_vr_iteration):

        #    if iteration_index_nidq[iteration_transition_indexes[i]] <= first_vr_iteration:
        #        ls[iteration_transition_indexes[i]] = 0

        print('sum_iterationtrans', np.sum(ls))

        iteration_times = np.where(ls)[0]/nidq_sampling_rate

        # get end of time from nidq metadata
        session_dir = pathlib.Path(get_session_directory(key))
        nidq_bin_full_path = list(session_dir.glob('*nidq.bin*'))[0]
        nidq_meta = readSGLX.readMeta(nidq_bin_full_path)
        t_end = np.float(nidq_meta['fileTimeSecs'])

        unit_spike_counts = []

        for unit_key in (ephys_pipeline.CuratedClustering.Unit & key).fetch('KEY'):
            spike_times = (ephys_pipeline.CuratedClustering.Unit & unit_key).fetch1('spike_times')
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
