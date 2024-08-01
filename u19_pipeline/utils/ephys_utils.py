
import json
import pathlib
import warnings

import datajoint as dj
import numpy as np
from bitstring import BitArray
from scipy import signal as sp
from scipy.io import loadmat

from u19_pipeline.utils.DemoReadSGLXData.readSGLX import (
    ExtractDigital,
    SampRate,
    makeMemMapRaw,
)


class spice_glx_utility:

    @staticmethod    
    def load_spice_glx_digital_file(file_path, nidq_meta, d_line_list=None):
        # Read NIDAQ digital file.
        #Inputs 
        # file_path   = path for the spike glx file
        # nidq_meta   = meta file read from readMeta spike glx utility
        # d_line_list = digital channels to read from the file

        # Get all digitial channels if not provided
        if d_line_list is None:
            list_start_end_chan = nidq_meta['niXDChans1'].split(sep=':')
            if  len(list_start_end_chan) == 2:
                d_line_list = list(range(int(list_start_end_chan[0]), int(list_start_end_chan[1])+1))
            else:
                raise ValueError('Could not infer channel list from nidq_meta["niXDChans1"] ')

        nidq_sampling_rate = SampRate(nidq_meta)

        #Get first and last sample idx from the file
        t_start = 0
        t_end = np.float(nidq_meta['fileTimeSecs'])
        dw = 0
        first_sample_index = int(nidq_sampling_rate * t_start)
        last_sample_index = int(nidq_sampling_rate * t_end) - 1

        #Read binary and digital
        nidq_raw_data = makeMemMapRaw(file_path, nidq_meta)  # Pull raw bin data
        digital_array = ExtractDigital(                               # extract interation index
            nidq_raw_data, first_sample_index, last_sample_index,
            dw, d_line_list, nidq_meta)

        return digital_array


def get_idx_trial_start(trial_pulse_signal):
    #Get index of samples when trial has started based on a pulse signal

    #Get idx samples trial starts
    trial_start_idx = np.where(np.diff(trial_pulse_signal) == 1) 
    trial_start_idx = trial_start_idx[0]

    #Detect fake trial init pulses (a single sample in 1 instead of 5ms signal)
    fake_trial_init = []
    for idx, sample in enumerate(trial_start_idx):
        #Get the mean value of next samples after rising edge detected
        mean_pulse = np.mean(trial_pulse_signal[sample+1:sample+10])
        # Average value should be 1
        if mean_pulse < 0.9:
            fake_trial_init.append(idx)

    trial_start_idx = np.delete(trial_start_idx, fake_trial_init)
    return trial_start_idx


def get_idx_iter_start_pulsesignal(iteration_pulse_signal_trial, trial_start_idx):
    #Get index of iteration starts on a trial based on a pulse start signal

    #Get idx of iteration start during trial
    iter_samples = np.where(np.diff(iteration_pulse_signal_trial) == 1) 
    iter_samples = iter_samples + trial_start_idx
    # First iteration is at trial start, just align first trial start
    iter_samples[0, 0] = trial_start_idx

    iter_samples = np.squeeze(iter_samples)

    return iter_samples


def get_idx_iter_start_counterbit(iteration_pulse_signal_trial, trial_start_idx):
    #Get index of iteration starts on a trial based on a iteration bit0 counter


    #Get idx of odd iteration during trial
    iter_samples = np.where(np.diff(iteration_pulse_signal_trial) == 1) 
    iter_samples = iter_samples + trial_start_idx

    if iteration_pulse_signal_trial[0] == 1:
        #If last iteration was odd, insert a iteration at start
        iter_samples = np.insert(iter_samples, 0, trial_start_idx)
    else:
        # First iteration is at trial start, just align first trial start
        iter_samples[0, 0] = trial_start_idx

    iter_samples = np.squeeze(iter_samples)

    #Get idx of even iteration during trial
    iter_samples2 = np.where(np.diff(iteration_pulse_signal_trial) == 255) 
    iter_samples2 = iter_samples2 + trial_start_idx
    iter_samples2 = np.squeeze(iter_samples2)

    iter_samples = np.concatenate([iter_samples, iter_samples2])
    iter_samples = np.sort(iter_samples) 

    return iter_samples


def get_trial_signal_mode(iteration_pulse_signal_trial, behavior_time_vector_trial):

    print('in get_trial_signal_mode')
    print('iteration_pulse_signal_trial', iteration_pulse_signal_trial.shape)
    print('trial_iterations', behavior_time_vector_trial.shape)
    print('trial_iterations type', type(behavior_time_vector_trial))

    # If iterations in trial are less than the ones in behavior, the mode was the counterbit
    iter_samples = np.where(np.diff(iteration_pulse_signal_trial) == 1)

    print('iter_samples', iter_samples[0].shape[0])
    print('behavior_time_vector_trial', behavior_time_vector_trial.shape[0])

    if iter_samples[0].shape[0] < (behavior_time_vector_trial.shape[0]*3/4):
        mode = 'counter_bit0'
    else:
        mode = 'pulse_signal'

    print('mode deduction: ', mode)

    return mode


def get_iteration_sample_vector_from_digital_lines_pulses(trial_pulse_signal, iteration_pulse_signal, nidq_sampling_rate, num_behavior_trials, behavior_time_vector, mode=None):

    #Output as a dictionary
    iteration_vector_output = dict()

    #Vectors that will contain trial # and iter # for each sample on file
    iteration_vector_output['framenumber_vector_samples'] = np.zeros(trial_pulse_signal.shape[0])*np.NaN
    iteration_vector_output['trialnumber_vector_samples'] = np.zeros(trial_pulse_signal.shape[0])*np.NaN

    #Get idx samples trial starts
    trial_start_idx = get_idx_trial_start(trial_pulse_signal)

    # Just to make sure we get corresponding iter pulse (trial and iter pulse at same time !!)
    ms_before_pulse = 3
    samples_before_pulse = int(nidq_sampling_rate*(ms_before_pulse/1000))

    # num Trials to sync (if behavior stopped before last trial was saved)
    num_trials_sync = min([trial_start_idx.shape[0], num_behavior_trials])

    iter_start_idx = []
    iter_times_idx = []
    for i in range(num_trials_sync):
        #Trial starts and ends idxs ()
        idx_start = trial_start_idx[i] -samples_before_pulse
        if i < trial_start_idx.shape[0]-1:
            idx_end = trial_start_idx[i+1] -samples_before_pulse
        else:
            idx_end = trial_pulse_signal.shape[0] - samples_before_pulse

        if mode is None:
            mode = get_trial_signal_mode(iteration_pulse_signal[idx_start:idx_end], behavior_time_vector[i])

        #Get idx of iteration start of current trial
        if mode == 'counter_bit0':
            iter_samples = get_idx_iter_start_counterbit(iteration_pulse_signal[idx_start:idx_end], trial_start_idx[i])
        else:
            iter_samples = get_idx_iter_start_pulsesignal(iteration_pulse_signal[idx_start:idx_end], trial_start_idx[i])
        
        #Append as an array of arrays (each trial is an array with idx of iterations)
        iter_start_idx.append(iter_samples)
        #Calculate time for each iteration start
        times = iter_samples/nidq_sampling_rate
        times = times - times[0]
        iter_times_idx.append(times)
        
        #Fill vector samples  
        for j in range(iter_samples.shape[0]-1):
            iteration_vector_output['framenumber_vector_samples'][iter_samples[j]:iter_samples[j+1]] = j+1

        #Last iteration # is from start of iteration to end of trial
        if i < trial_start_idx.shape[0]-1:
            iteration_vector_output['trialnumber_vector_samples'][trial_start_idx[i]:trial_start_idx[i+1]] = i+1
            iteration_vector_output['framenumber_vector_samples'][iter_samples[-1]:trial_start_idx[i+1]] = iter_samples.shape[0]
        # For last trial, lets finish it 1s after last iteration detected
        else:
            iteration_vector_output['trialnumber_vector_samples'][trial_start_idx[i]:iter_samples[-1]+int(nidq_sampling_rate)] = i+1
            iteration_vector_output['framenumber_vector_samples'][iter_samples[-1]:iter_samples[-1]+int(nidq_sampling_rate)] = iter_samples.shape[0]

    iteration_vector_output['iter_start_idx'] = np.asarray(iter_start_idx.copy(), dtype=object)
    iteration_vector_output['iter_times_idx'] = np.asarray(iter_times_idx.copy(), dtype=object)

    return iteration_vector_output


def get_iteration_sample_vector_from_digital_lines_word(digital_array, time, iterstart):

    # First, transform digital lines into a number, save in an array of integers
    #      ... and also get start and end time
    framenumber = np.zeros(digital_array.shape[1])
    for i in range(digital_array.shape[1]):
        a = BitArray(np.flip(digital_array[1:, i])) # ignore 0-bit, as this is the NPX sync puls, and not virmen.
        framenumber[i] = a.uint
    iterations_raw = np.array(framenumber, dtype=np.int32) # Transform frames into integer
    recording_start = np.min(np.where(iterations_raw>0)) #first chane of testlist
    recording_end = np.where(np.abs(np.diff(iterations_raw))>0)[0][-1] + 200 # Adding a random 200 measurements, so ~40ms at our usual 5kHz sampling rate.

    # Second, transform `iterations_raw` into `framenumber_in_trial` and `trialnumber`
    framenumber_in_trial = np.zeros(len(iterations_raw))*np.NaN
    trialnumber = np.zeros(len(iterations_raw))*np.NaN
    current_trial = 0
    overflow = 0
    iter_start_idx = []
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
                iter_start_idx.append(idx)         # Make a note when this happened
            framenumber_in_trial[idx] = frame_number + overflow*128 - 1
            trialnumber[idx] = current_trial
    trial_list = np.array(np.unique(trialnumber[np.isfinite(trialnumber)]), dtype = np.int32)

    # Fourth, find and remove the nidaq glitches 
    # These are single samples where the iteration number is corrupted
    # ... likely because sampling happened faster than output of the behavior PC.
    # This is also where skipped frames are detected.

    # Find the glitches
    din = np.diff(framenumber_in_trial)
    trial_transitions = np.where(np.diff(trialnumber))
    glitches = []
    for candidate in np.where( np.logical_or(din>1, din<0) )[0]: # skipped frames or counting down
        if np.sum(candidate == trial_transitions) == 0:
            glitches = np.append(glitches, candidate)
    glitches = np.array(glitches, dtype = np.int32)

    # Attempt to remove them
    skipped_frames = 0
    for g in glitches:
        if framenumber_in_trial[g] < framenumber_in_trial[g+2]:
            if framenumber_in_trial[g+2] -  framenumber_in_trial[g] == 2:  # skipped frame, should be very rare
                framenumber_in_trial[g+1] = framenumber_in_trial[g]+1
                skipped_frames = skipped_frames + 1
            else:                          # If random number, nidaq sample in the middle of update.
                framenumber_in_trial[g+1] = framenumber_in_trial[g]

    # This point we have framenumber_in_trial and trialnumber. Now just some refactoring to fit into the usual data structure
    iteration_vector_output = dict()
    
    iteration_vector_output['trialnumber_vector_samples'] = trialnumber
    iteration_vector_output['framenumber_vector_samples'] = framenumber_in_trial

    iter_start_idx = []
    for t in trial_list:
        iter_start_idx.append( np.arange(0, framenumber_in_trial[trialnumber==t][-1], 1))
    iteration_vector_output['iter_start_idx'] = np.asarray(iter_start_idx.copy(), dtype=object)

    return iteration_vector_output

def assert_iteration_samples_count(iteration_sample_idx_output, behavior_time_vector):
    #Assert that vector sync pulses match behavior time vector

    # Count trial count differences
    trial_count_diff = np.abs(iteration_sample_idx_output.shape[0] - (behavior_time_vector.shape[0]))

    trials_diff_iteration_small = list()
    trials_diff_iteration_big = list()
    for idx_trial, iter_trials in enumerate(iteration_sample_idx_output):
        print(idx_trial)
        print(iter_trials.shape[0])
        print(behavior_time_vector[idx_trial].shape[0])
        #For each trial iteration # should be equal to the behavioral file iterations
        if iter_trials.shape[0] != behavior_time_vector[idx_trial].shape[0]:
            if np.abs(iter_trials.shape[0] - behavior_time_vector[idx_trial].shape[0]) < 3:
                trials_diff_iteration_small.append(idx_trial)
            else:
                trials_diff_iteration_big.append(idx_trial)


    return trial_count_diff, trials_diff_iteration_big, trials_diff_iteration_small


def evaluate_sync_process(trial_count_diff, trials_diff_iteration_big, trials_diff_iteration_small, total_trials):
    #Check if all sync process ran smoothly, we need to redo some trials or it's not worth it
    # Return status
    # = 1, synced perfectly
    # = 0, missed by just a couple pulses, resync
    # = -1, missed by a lot, error

    if len(trials_diff_iteration_big) > 1:
        print('Missed by a lot some trials: ', trials_diff_iteration_big)
        status = -1
        return status

    if len(trials_diff_iteration_big) == 1 and trials_diff_iteration_big[0] == total_trials-1:
        print('Missed by a lot last trial: (Assume recording stopped earlier) ', trials_diff_iteration_big)
        status = 1
        return status

    # All trials synced perfectly
    if trial_count_diff ==0 and len(trials_diff_iteration_small) == 0:
        print('Synced perfectly xxxxxxxxxxxxxx')
        status = 1
        return status

    # We miss last trial (surely recording was stop before behavior)
    if trial_count_diff < 2 and len(trials_diff_iteration_small) == 0:
        print('Only missed last trial (Pass) xxxxxxxxxxxxxx')
        status = 1
        return status

    # Iterations differ in more than two trials
    if len(trials_diff_iteration_small) > 2:
        print('Missed iteration count on many trials: ', len(trials_diff_iteration_small))
        status = -1
        return status

    if trial_count_diff < 2 and len(trials_diff_iteration_small) <= 2:
        print('Missed num trials: ', trial_count_diff)
        print('Missed iteration count in how many trials: ', len(trials_diff_iteration_small))
        print('Trying to fix trials')
        status = 0
        return status

    else:
        status = -1
        print('Missed by a lot of trials, everything different or missing')
        return status



def fix_missing_iteration_trials(trials_diff_iteration_small, iteration_dict, behavior_times, nidq_sampling_rate):
    # Fix and insrtt missing synced iteration vectors

    print('trials_diff_iteration_small', trials_diff_iteration_small)
    print(type(trials_diff_iteration_small))


    # For each bad synced trial (Should be only a few)
    for i in range(len(trials_diff_iteration_small)):

        #Insert missing iterations on synced vector
        idx_trial = trials_diff_iteration_small[i]
        status, new_iter_start = insert_missing_synced_iteration(iteration_dict['iter_start_idx'][idx_trial],\
            iteration_dict['iter_times_idx'][idx_trial], behavior_times[idx_trial].flatten())

        if not status:
            raise ValueError("Coud not find missing iteration in trial")
        
        iteration_dict['iter_start_idx'][idx_trial] = new_iter_start

        # Get new synced time vector for trial
        new_times = new_iter_start/nidq_sampling_rate
        new_times = new_times - new_times[0]
        iteration_dict['iter_times_idx'][idx_trial] = new_times

        # Fix framenumber in the sample vector
        for j in range(new_iter_start.shape[0]-1):
            iteration_dict['framenumber_vector_samples'][new_iter_start[j]:new_iter_start[j+1]] = j+1

        #Last iteration fixed as well
        next_trial = idx_trial+1
        start_iteration_next_trial = iteration_dict['iter_start_idx'][next_trial][0]

        print('next_trial ........', next_trial)
        print('last iteration of this trial so far', new_iter_start[-1])
        print('start next trial iteration', start_iteration_next_trial)

        iteration_dict['framenumber_vector_samples'][new_iter_start[-1]:start_iteration_next_trial] = new_iter_start.shape[0]

        return iteration_dict


def insert_missing_synced_iteration(synced_iteration_vector, synced_time_vector, behavior_time_vector):
    # Check where is more likely we miss an iteration pulse and insert it to iteration_vector

    status = 1
    print('synced_iteration_vector', synced_iteration_vector.shape[0])
    print('synced_time_vector', synced_time_vector.shape[0])
    print('behavior_time_vector', behavior_time_vector.shape[0])

    # Get in which indexes we get a "peak" of non matching times...
    if synced_time_vector.shape[0] >= behavior_time_vector.shape[0]:
        print('More pulses than behavior iterations, check other method')
        status = -1
        return status, np.empty(0)
    else:
        diff_vector = np.diff(synced_time_vector - behavior_time_vector[:synced_time_vector.shape[0]])

    peaks, _ = sp.find_peaks(diff_vector, height=0.05, distance=20)

    print('peaks here .............', peaks)
    print(peaks.shape)


    # Insert extra iterations as a new "iteration" start to match behavior iterations
    new_synced_iteration_vector = synced_iteration_vector.copy()
    for i in range(peaks.shape[0]):
        value_insert = (synced_iteration_vector[peaks[i]] + synced_iteration_vector[peaks[i]+1] ) /2
        new_synced_iteration_vector = np.insert(new_synced_iteration_vector, peaks[i], value_insert)


    if new_synced_iteration_vector.shape[0] != behavior_time_vector.shape[0]:
        print('with peak strategy, could not find correct missing iterations')
        status = -1
        return status, np.empty(0)

    return status, new_synced_iteration_vector



# Deprecated
def behavior_sync_frame_counter_method(digital_array, behavior_time_vector, session_trial_keys, nidq_sampling_rate, bit_start, number_bits):

    max_count = np.power(2, number_bits)-1

  # 2: transform the digital lines into a number, save in an array of integers
    #      ... and also get start and end time
    framenumber = np.zeros(digital_array.shape[1])
    for i in range(digital_array.shape[1]):
        a = BitArray(np.flip(digital_array[bit_start:, i])) # ignore 0-bit, as this is the NPX sync puls, and not virmen.
        framenumber[i] = a.uint
    iterations_raw = np.array(framenumber, dtype=np.int) # Transform frames into integer
    recording_start = np.min(np.where(iterations_raw>0)) #Get start of recording: first change of testlist
    dt = np.int(0.04*nidq_sampling_rate)
    recording_end = np.where(np.abs(np.diff(iterations_raw))>0)[0][-1] + dt  #Get end of recording

    # 3: transform `iterations_raw` into `framenumber_in_trial` and `trialnumber`
    # iterations_raw is just a number between 0 and max_count+1. Some math has to be done to obtain:
    # framenumber_in_trial has the length of the number of samples of the NIDAQ card, and every entry is the currently presented iteration number of the VR in the respective trial.
    # trialnumber has the length of the number of samples of the NIDAQ card, and every entry is the current trial.
    #
    # NOTE: some minor glitches have to be catched, if a NIDAQ sample happenes to be recorded while the VR System updates the iteration number. 
    framenumber_in_trial = np.zeros(len(iterations_raw))*np.NaN
    trialnumber = np.zeros(len(iterations_raw))*np.NaN
    current_trial = 0
    overflow = 0 # This variable keep track whenever the reset from max_count to 0 happens.
    for idx, frame_number in enumerate(iterations_raw):
        if (idx>recording_start) & (idx<recording_end):
            #print(iterations_raw2[idx], frame_number)
            if (frame_number==0) & (iterations_raw[idx-1]==max_count): # At the reset, add max_count+1
                overflow = overflow + 1
            if (frame_number==0) & (iterations_raw[idx-1]!=max_count) & (iterations_raw[idx-1]!=0) &  (iterations_raw[idx-2]==max_count): # Unlucky reset if happened to be sampled at the wrong time
                overflow = overflow + 1
                framenumber_in_trial[idx-1] = frame_number + overflow*(max_count+1) - 1 # In case this happened, the previous sample has to be corrected
            # Keep track of trial number
            endflag = framenumber_in_trial[idx-1] == (len(behavior_time_vector[current_trial])) # Trial end has been reached.
            
            transitionflag = frame_number == 2 # Next trial should start at zero again (it starts with two ??)
            if endflag & transitionflag:      # Only at the transitions
                    
                current_trial = current_trial + 1  # Increases trial count
                overflow = 0                       # Reset the 7 bit counter for the next trial

            if overflow == 0:
                framenumber_in_trial[idx] = frame_number  
            else:  
                framenumber_in_trial[idx] = frame_number + overflow*(max_count+1) -1

            trialnumber[idx] = current_trial
    trial_list = np.array(np.unique(trialnumber[np.isfinite(trialnumber)]), dtype = np.int64)

    # 4: find and remove additional NIDAQ glitches of two types: 
    # a) single samples where the iteration number is corrupted because sampling happened faster than output of the behevior PC.
    # b) Skipped frames are detected and filled in.
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
                pass
                framenumber_in_trial[g+1] = framenumber_in_trial[g]+1
                skipped_frames = skipped_frames + 1
            else:                          # If random number, nidaq sample in the middle of update.
                framenumber_in_trial[g+1] = framenumber_in_trial[g]

    # A set of final asserts, making sure that the code worked as intended  
    assert len(trial_list) == len(session_trial_keys)          # Make sure the trial number is correct.
    assert np.sum(np.diff(framenumber_in_trial)>1) == 0 # No frames should be skipped
    assert np.sum(np.diff(framenumber_in_trial)<0)<len(trial_list) # Negative iterations only at trial transitions
    iterations_test = 0
    for t in trial_list:
        iterations_test = iterations_test + framenumber_in_trial[trialnumber==t][-1]  # Integrate number of iterations
        assert framenumber_in_trial[trialnumber==t][-1] == len(behavior_time_vector[t])  # Make sure number of nidaq-frames in each trial is identical to dj record:
        nidaqtime = np.sum(trialnumber == t)/nidq_sampling_rate
        matlabtime = np.max(behavior_time_vector[t])
        assert ((nidaqtime - matlabtime) / matlabtime) < 0.1 # # Make sure the nidaq-trial-duration and dj records are consistent; 10% arbitrarily chosen
    iterations_test + skipped_frames
    #dj_duration = iterstart[-1] + len(behavior_time_vector[-1])
    #assert np.abs(nidaq_duration - dj_duration) < 3 # at most two frames off - sometimes this happens at the beginning/end of the recording

    # If this is done, and the asserts are passed, insert the data into the database

    return (framenumber_in_trial, trialnumber)



def future_counter_get_signal():
    pass
'''
    # Cleaner way to get iteration number from counter, still need debugging, if necessary

    framenumber = np.zeros(idx_end-idx_start)
    for idx, ii in enumerate(range(idx_start,idx_end)):
        a = BitArray(np.flip(digital_array[start_iteration_counter_bit:, ii])) # ignore 0-bit, as this is the NPX sync puls, and not virmen.
        framenumber[idx] = a.uint
    iterations_raw = np.array(framenumber, dtype=np.int) # Transform frames into integer

    framenumber_in_trial = np.zeros(len(iterations_raw))*np.NaN
    
    current_trial = 0
    almost_overflow = 0
    overflow = 0 # This variable keep track whenever the reset from max_count to 0 happens.
    for idx, frame_number in enumerate(iterations_raw):
        #print(iterations_raw2[idx], frame_number)
        if (frame_number > 15*max_count/16):
            almost_overflow = 1
        if (frame_number < max_count/16) and almost_overflow == 1:
            overflow += 1
            almost_overflow = 0


        framenumber_in_trial[idx] = frame_number + overflow*(max_count+1)

    print(np.max(framenumber_in_trial))
'''

def load_open_ephys_digital_file(file_path):

    pass





class xyz_pick_file_creator:
    '''
    Class that handles probe coordinates locations given initial isertion coordinates & shank coordinates
    '''

    @staticmethod
    def main_xyz_pick_file_function(recording_id, fragment_number, chanmap_file, processed_data_directory):
        """
        Stores xyz_pick_files on ibl_postprocess directory for ibl_atlas_gui
        Input:
        recording_id             (int) = Reference to current directory
        fragment_number          (int) = Reference to probe# of current recording 
        chanmap_file             (str) = Filepath to find current chanmapfile built for the recording 
        processed_data_directory (str) = Filepath of processed job results
        """

        # Check existance of ibl output path
        ibl_output_dir = pathlib.Path(dj.config['custom']['ephys_root_data_dir'][1], processed_data_directory, 'ibl_data')
        if not ibl_output_dir.is_dir():
            pathlib.Path.mkdir(ibl_output_dir)

        # Get recording id
        probe_location = xyz_pick_file_creator.get_probe_insertion_coordinates(recording_id, fragment_number)
        print(probe_location)

        #Load channelmap and check how many probes there are
        chanmap = loadmat(chanmap_file)
        max_shank = int(np.max(chanmap['kcoords']))

        # Calculate probe coordinates and store files
        all_shanks = list()
        for i in range(max_shank):
            probe_track = xyz_pick_file_creator.get_probetrack(chanmap, shank=i+1, **probe_location)
            xyz_pick_file_creator.save_xyz_pick_file(ibl_output_dir, probe_track, shank=i)
            all_shanks.append(probe_track)

        return all_shanks

    @staticmethod
    def get_probe_insertion_coordinates(recording_id, probe_num):
        """
        Get probe insertion coordinates based on a recording id and a probe# (frag)
        Input:
        recording_id             (int) = Reference to current directory
        probe_num                (int) = Reference to probe# of current recording 
        """

        coordinates_columns = ['real_ap_coordinates', 'real_depth_coordinates', 'real_ml_coordinates', 'phi_angle', 'theta_angle', 'rho_angle']
        probes_not_found = False

        # Create virtual modules of needed DBs
        action_db = dj.create_virtual_module('action', 'u19_action')
        recording_db = dj.create_virtual_module('recording', 'u19_recording')

        # Query subject of recording
        query = {'recording_id': recording_id}
        subject_recording = (recording_db.Recording.BehaviorSession & query).fetch('subject_fullname')

        if subject_recording.shape[0] != 0:
            # Query probe insertion table
            query_surgery = {'subject_fullname': subject_recording[0], 'device_idx': probe_num}
            probe_location = (action_db.SurgeryLocation & query_surgery).fetch(*coordinates_columns, as_dict=True)
            if len(probe_location) == 0:
                probes_not_found = True
            else:
                probe_location = probe_location[0]
                # Convert to float
                probe_location = {k:float(v)for (k,v) in probe_location.items()}
        else:
            probes_not_found = True

        # If insertion decive is not found, create a dummy one but raise a warning
        if probes_not_found:
            warnings.warn("Warning probe location was not found on DB for recording_id: " + str(recording_id) + " & probe# " + str(probe_num) )
            probe_location = dict.fromkeys(coordinates_columns, 0)

        return probe_location

    @staticmethod
    def get_probetrack(chanmap, shank=1, real_ml_coordinates=0, real_ap_coordinates=0, real_depth_coordinates=0,  phi_angle=0, theta_angle=0, rho_angle=0):
        """
        Build numpy array with "brain" coordinates from insertion device and probe features
        Input:
        chanmap                (dict) = Mat file created from https://github.com/AllenInstitute/ecephys_spike_sorting.git library from SpikeGLX metadata file
        shank                  (int) =  Shank # (1 index based)  to create file for 
        real_ml_coordinates    (decimal, mm) =  mediolateral coordinates of probe insertion
        real_ap_coordinates    (decimal, mm) =  anteroposterior coordinates of probe insertion
        real_depth_coordinates (decimal, mm) =  depth mm coordinates of probe insertion
        phi_angle              (decimal, deg) =  - azimuth - rotation about the dv-axis [0, 360] - w.r.t the x+ axis   
        theta_angle            (decimal, deg) =  - elevation - rotation about the ml-axis [0, 180] - w.r.t the z+ axis
        rho_angle              (decimal, deg) =  angle rotation on device itself
        """

        # Step 1: Convert degrees to radiants and reformat chanmap
        phi   = phi_angle*np.pi/180
        theta = theta_angle*np.pi/180
        roll  = rho_angle*np.pi/180
        x = np.array([i[0] for i in chanmap['xcoords']])
        y = np.array([i[0] for i in chanmap['ycoords']])
        k = np.array([i[0] for i in chanmap['kcoords']])

        # Step 2: Transform them into 3D, assuming the Probe is perpendicular to x|y plane
        avx           = np.mean(x[k==shank])                                  # Center "X" on middle of probe
        probe_x0      = np.array([np.cos(roll)*avx, -np.sin(roll)*avx])       # coordinates of the shank after "roll" around probe axis
        probe_length  = np.max(y[k==shank]) - np.min(y[k==shank])             # Length off the probe per chanmap
        probe_unitVec = np.array([np.sin(theta)*np.cos(phi), np.sin(theta)*np.sin(phi), np.cos(theta)]) # Unit vector point along insertion direction

        probe_length  = np.arange(-real_depth_coordinates*1000, -real_depth_coordinates*1000 + probe_length, 10)          # Resulution of future xyz_pick.json file

        # Step 3: Produce the 3D coordinates along the probe track
        probe_track = np.zeros((len(probe_length),3))
        for i in range(len(probe_length)):
            probe_track[i,:] = probe_length[i]*probe_unitVec + np.array([probe_x0[0], probe_x0[1], 0])
        
        # Step 4: Shift probe my ML|AP insertion coordinates
        probe_track_shifted = np.zeros(probe_track.shape)
        for i in range(len(probe_track_shifted)):
            probe_track_shifted[i,:] = probe_track[i,:] + np.array([real_ml_coordinates*1000, real_ap_coordinates*1000, 0])

        return probe_track_shifted.tolist()

    @staticmethod
    def save_xyz_pick_file(save_directory, probe_coord_data, shank=0):
        """
        Store xyz_picks file
        save_directory         (str) =  filepath to store xyz_picks file
        probe_coord_data       (np array) = numpy array of electrodes coordinate data
        shank                  (int) =  Shank # (0 index based) (different filename depending on it)
        """

        filenames = ['xyz_picks.json', 'xyz_picks_shank1.json', 'xyz_picks_shank2.json', 'xyz_picks_shank3.json']
        final_filename = pathlib.Path(save_directory, filenames[shank]).as_posix()

        dict_coord = dict()
        dict_coord["xyz_picks"] = probe_coord_data

        with open(final_filename, 'w') as fp:
            json.dump(dict_coord, fp)