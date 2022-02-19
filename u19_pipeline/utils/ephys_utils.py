
from u19_pipeline.utils.DemoReadSGLXData.readSGLX import SampRate, makeMemMapRaw, ExtractDigital
import numpy as np
from bitstring import BitArray
from element_array_ephys import ephys as ephys_element


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


def get_iteration_sample_vector_from_digital_lines_pulses(trial_pulse_signal, iteration_pulse_signal, nidq_sampling_rate, num_behavior_trials, mode='counter_bit0'):

    #Output as a dictionary
    iteration_vector_output = dict()

    #Vectors that will contain trial # and iter # for each sample on file
    iteration_vector_output['framenumber_vector_samples'] = np.zeros(trial_pulse_signal.shape[0])*np.NaN
    iteration_vector_output['trialnumber_vector_samples'] = np.zeros(trial_pulse_signal.shape[0])*np.NaN

    #Get idx samples trial starts
    trial_start_idx = get_idx_trial_start(trial_pulse_signal)

    # Just to make sure we get corresponding iter pulse (trial and iter pulse at same time !!)
    ms_before_pulse = 2
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


def assert_iteration_samples_count(iteration_sample_idx_output, behavior_time_vector):
    #Assert that vector sync pulses match behavior time vector

    status = True

    # Trial start pulses should be +1 of completed trials in behavior file
    if iteration_sample_idx_output.shape[0] != (behavior_time_vector.shape[0]):
        status = False
        return status

    count = 0
    for idx_trial, iter_trials in enumerate(iteration_sample_idx_output):
        count += 1
        print(count)
        print(iter_trials.shape[0])
        print(behavior_time_vector[idx_trial].shape[0])
        #For each trial iteration # should be equal to the behavioral file iterations
        if iter_trials.shape[0] != behavior_time_vector[idx_trial].shape[0]:
            status = False
            return status

    return status


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
    nidaq_duration = iterations_test + skipped_frames
    #dj_duration = iterstart[-1] + len(behavior_time_vector[-1])
    #assert np.abs(nidaq_duration - dj_duration) < 3 # at most two frames off - sometimes this happens at the beginning/end of the recording

    # If this is done, and the asserts are passed, insert the data into the database

    return (framenumber_in_trial, trialnumber)


def future_counter_get_signal():
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


def load_open_ephys_digital_file(file_path):

    pass
