
import datetime
import pathlib
import numpy as np

import u19_pipeline.utils.ephys_utils as ephys_utils


def get_shift_vector(synced_time_vector, behavior_time_vector, base_size=40,initial_sample=0, samples_shift=100):


    diff_size = np.abs(synced_time_vector.shape[0] - behavior_time_vector.shape[0])

    baseline_diff = synced_time_vector[initial_sample:base_size] - behavior_time_vector[initial_sample:base_size] 

    '''
    while(1):

        base_greater = np.where(baseline_diff >0)
        base_greater = base_greater[0]

        if base_greater.shape[0] > 0 and synced_time_vector.shape[0] < behavior_time_vector.shape[0]:
            idx_first = base_greater[0]
            time_bef = behavior_time_vector[idx_first] - behavior_time_vector[idx_first-1]


            synced_time_vector = np.insert(synced_time_vector, idx_first, synced_time_vector[idx_first-1]+time_bef)
            baseline_diff = synced_time_vector[initial_sample:base_size] - behavior_time_vector[initial_sample:base_size]
        else:
            break
    '''


    median_diff = np.median(baseline_diff)
    max_diff = np.median(baseline_diff)+0.007
    min_diff = np.median(baseline_diff)-0.007


    vec_shift = np.zeros((synced_time_vector.shape[0]-base_size), dtype=int)
    for i in range(synced_time_vector.shape[0]-base_size):


        idx_start = initial_sample+i+1
        idx_end = base_size+i+1

        this_bt = behavior_time_vector[idx_start:idx_end]
        this_iv = synced_time_vector[idx_start:idx_end]

        median_ori = np.median(this_iv-this_bt)

        if median_ori >= min_diff and median_ori < max_diff:
            vec_shift[i] = 0
            sign = 0
        elif median_ori < min_diff:
            sign = 1
        else:
            sign = -1
            
        new_sign = sign
        if sign != 0:

            for j in range(1, samples_shift):

                if new_sign == 1:
                    if idx_end+j > synced_time_vector.shape[0]:
                        this_iv = synced_time_vector[idx_start+j:]
                        this_bt = this_bt[:-1]
                    else:
                        this_iv = synced_time_vector[idx_start+j:idx_end+j]
                else:
                    this_iv = synced_time_vector[idx_start-j:idx_end-j]
                
                median_now = np.median(this_iv-this_bt)
                
                if median_now >= min_diff and median_now < max_diff:
                    vec_shift[i] = j*new_sign
                    break
                elif median_now < min_diff:
                    new_sign = 1
                else:
                    new_sign = -1

                if new_sign != sign:
                    if np.abs(median_ori-median_diff) < np.abs(median_now-median_diff):
                        vec_shift[i] = 0
                    else:
                        vec_shift[i] = j*sign                
                    break
                    
                if j == samples_shift-1:
                    #print('Extreme case !!!')
                    vec_shift[i] = j*sign

    new_synced_time_vector = synced_time_vector.copy()
    mid_point = int(initial_sample+base_size/2)
    for i in range(vec_shift.shape[0]):
        new_synced_time_vector[mid_point+i] = synced_time_vector[mid_point+i+vec_shift[i]]
    
    idx_end = mid_point+vec_shift.shape[0]-1
    for i in range(idx_end, new_synced_time_vector.shape[0]):
        if i+vec_shift[-1] < new_synced_time_vector.shape[0]:
            new_synced_time_vector[i] = synced_time_vector[i+vec_shift[-1]]

    max_shift = np.max(np.abs(vec_shift))


    if max_shift > diff_size and diff_size != 1:
        pass
        #print('max_shift', max_shift)
        #print('diff_size', diff_size)
        #print('walaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
        #raise ValueError('more max shift than diff size')

    return new_synced_time_vector, vec_shift, [min_diff, median_diff, max_diff]


def fix_shifted_sync_vector(synced_time_vector, behavior_time_vector, vec_shift, initial_sample=0, base_size=40):

    mid_point = int(initial_sample+base_size/2)
    new_synced_time_vector = synced_time_vector.copy()

    diff_vec_shift = np.diff(vec_shift)
    where_insert_iteration = np.where(diff_vec_shift != 0)
    where_insert_iteration = where_insert_iteration[0]
    #print('where_insert_iteration', where_insert_iteration)

    find = np.full(4, 1)
    correlation_result = np.correlate(diff_vec_shift == 0, find)
    consecutive_zeros = np.flatnonzero(correlation_result == 4)

    index_shift = 0
    index_borrow_virmen = list()
    for i in range(len(where_insert_iteration)):
        stable_parts = np.where(consecutive_zeros > where_insert_iteration[index_shift])
        stable_parts = stable_parts[0]

        if stable_parts.shape[0] > 0:
            index_borrow_virmen.append([where_insert_iteration[index_shift], consecutive_zeros[stable_parts[0]]])
            next_borrow_start = np.where(where_insert_iteration > consecutive_zeros[stable_parts[0]])
            next_borrow_start = next_borrow_start[0]
            if next_borrow_start.shape[0] > 0:
                index_shift = next_borrow_start[0]
            else:
                break
        else:
            print('Extreme case diff vec shift')

            index_borrow_virmen.append([where_insert_iteration[index_shift], diff_vec_shift.shape[0]])
            print('index_borrow_virmen', index_borrow_virmen)
            break



    #print('index_borrow_virmen', index_borrow_virmen)

    #for i in range(where_insert_iteration.shape[0]):
    index_diff_e  = -1
    borrowed_indexes = list()
    for i in range(len(index_borrow_virmen)):

        if index_diff_e > index_borrow_virmen[i][0]+mid_point:
            continue

        for j in range(100):

            index_diff_s = index_borrow_virmen[i][0]+mid_point-1
            index_diff_e = index_borrow_virmen[i][1]+mid_point+1

            if j>=1:
                index_diff_s = index_diff_s-1
            if j >1:
                index_diff_e = index_diff_e+j-1

            #print('index_diff_vec', index_diff_s, index_diff_e)
            #print('diff_vec_shift[index_diff] ', diff_vec_shift[index_diff_vec-1:index_diff_vec+1] )

            #print('behavior_time_vector', behavior_time_vector[index_diff_s-1:index_diff_e+2])
            #print('new_synced_time_vector2', new_synced_time_vector2[index_diff_s-1:index_diff_e+2])

            time_iteration = behavior_time_vector[index_diff_s:index_diff_e+1] - behavior_time_vector[index_diff_s]
            #print('time_iteration', time_iteration)

            new_synced_time_vector[index_diff_s:index_diff_e+1] = np.repeat(new_synced_time_vector[index_diff_s], index_diff_e-index_diff_s+1) +\
                time_iteration
            
            check_diff = np.diff(new_synced_time_vector[index_diff_s:index_diff_e+2])
            idx_back_time = np.where(check_diff <= 0.0005)
            idx_back_time = idx_back_time[0]
            if idx_back_time.shape[0] == 0:
                break
        

        borrowed_indexes.append([index_diff_s,index_diff_e+1])
        #print('new_synced_time_vector2', new_synced_time_vector2[index_diff_s-1:index_diff_e+2])
        #print('diff new_synced_time_vector2', np.diff(new_synced_time_vector2[index_diff_s-1:index_diff_e+2]))
        #print('diff new_synced_time_vector2', np.diff(behavior_time_vector[index_diff_s-1:index_diff_e+2]))



    return new_synced_time_vector, borrowed_indexes


def fix_sync_vector_greater(sync_time_vector, behavior_time_vector):

    new_sync_time_vector = sync_time_vector.copy()

    diff_vecs = (new_sync_time_vector - behavior_time_vector[:new_sync_time_vector.shape[0]])

    idx_plus = np.where(diff_vecs > 0)
    idx_plus = idx_plus[0]

    break_points = np.where(np.diff(idx_plus) != 1)[0] + 1
    grouped_sections = np.split(idx_plus, break_points)

    borrowed_indexes = []
    for i in range(len(grouped_sections)):

        if grouped_sections[i].shape[0] > 0:

            idx_start = grouped_sections[i][0]-1
            idx_end = grouped_sections[i][-1]

            time_vector = behavior_time_vector[idx_start:idx_end+1]-behavior_time_vector[idx_start]
            new_sync_time_vector[idx_start:idx_end+1] =\
                np.repeat(new_sync_time_vector[idx_start], idx_end-idx_start+1) + time_vector
        
            borrowed_indexes.append([idx_start, idx_end])

            
    return new_sync_time_vector, borrowed_indexes


def complete_last_part_sync_vec(sync_time_vector, behavior_time_vector):

    new_sync_time_vector = sync_time_vector.copy()
    
    diff_size = behavior_time_vector.shape[0] - new_sync_time_vector.shape[0]

    borrowed_indexes = []
    if diff_size > 0:
        diff_size = diff_size+1

        last_part_bt = behavior_time_vector[-diff_size:]- behavior_time_vector[-diff_size]
        insert_part = np.repeat(new_sync_time_vector[-1], diff_size) + last_part_bt
        #print('last_part_bt',last_part_bt)

        #print('insert_part',insert_part)

        new_sync_time_vector = np.append(new_sync_time_vector, insert_part[1:])
        
    
    count = -1
    while new_sync_time_vector[count] > (behavior_time_vector[count]+1):
        count -=1
        if count == -10:
            break

    if count < -1:
        #print('count', count)

        time_vector = behavior_time_vector[count:]-behavior_time_vector[count]
            
        new_sync_time_vector[count:] =\
                    np.repeat(new_sync_time_vector[count], -count) + time_vector
                
        borrowed_indexes.append([behavior_time_vector.shape[0]+count, behavior_time_vector.shape[0]-1])
        #print('count', count)
        #print('borrowed_indexes', borrowed_indexes)

    return new_sync_time_vector, borrowed_indexes
    

def fix_iter_vector(synced_iteration_vector, synced_time_vector, original_time_vector, nidq_sampling_rate):

    new_synced_iteration_vector = synced_iteration_vector.copy()

    ori_shape = original_time_vector.shape[0]
    new_shape = synced_time_vector.shape[0]
    diff_iter_times_idx = original_time_vector - synced_time_vector[:ori_shape]

    first_iter = synced_iteration_vector[0]
    for i in range(diff_iter_times_idx.shape[0]):
        if diff_iter_times_idx[i] != 0:
            new_synced_iteration_vector[i] = first_iter+int(synced_time_vector[i]*nidq_sampling_rate)

    if ori_shape != new_shape:
        last_iterations =  (np.round(first_iter+synced_time_vector[ori_shape:]*nidq_sampling_rate))
        new_synced_iteration_vector = np.append(new_synced_iteration_vector, last_iterations.astype(np.int64))

    return new_synced_iteration_vector


def sync_evaluation_process2(synced_time_vector, behavior_time_vector):

    status = 1
    diff_vector = synced_time_vector - behavior_time_vector[:synced_time_vector.shape[0]]
    num_iter = diff_vector.shape[0]

    #max_diff = max(diff_vector)
    median_general = np.median(diff_vector)

    num_div= 10
    median_diff_percent = np.empty([num_div])
    median_diff_abs = np.empty([num_div])
    for j in range(num_div):
        start_iter = int(j*num_iter/num_div)
        end_iter = int((j+1)*num_iter/num_div)
        median_diff_percent[j] = (np.median(diff_vector[start_iter:end_iter])-median_general)*100/median_general
        median_diff_abs[j] = (np.median(diff_vector[start_iter:end_iter])-median_general)


    if np.max(np.abs(median_diff_abs)) < 0.005:
        pass
    else:
        status = -1
        print(median_general)

    return status


def main_ephys_fix_sync_code(iter_start_idx, iter_times_idx, behavior_time, nidq_sampling_rate):

    iteration_dict = dict()
    iteration_dict['iter_start_idx']  = list()
    iteration_dict['iter_times_idx']  = list()

    for i in range(len(iter_start_idx)):

        #print('fixing trial ',i)
        behavior_time_vector = behavior_time[i].flatten()

        synced_time_vector, shift_vec, median_vec = get_shift_vector(iter_times_idx[i],behavior_time_vector)
        

        synced_time_vector,_ =\
            fix_shifted_sync_vector(synced_time_vector, behavior_time_vector, shift_vec)

        #synced_time_vector, trial_stats_dict['borrow_step3'] =\
        #    fix_sync_vector_greater(synced_time_vector, behavior_time_vector)
        synced_time_vector,_ =\
            complete_last_part_sync_vec(synced_time_vector, behavior_time_vector)

        synced_iteration_vector =\
            fix_iter_vector(iter_start_idx[i],synced_time_vector, iter_times_idx[i], nidq_sampling_rate)
        

        iteration_dict['iter_start_idx'].append(synced_iteration_vector.copy())
        iteration_dict['iter_times_idx'].append(synced_time_vector.copy())

    print('end fix sync code 1')

    iteration_dict['iter_start_idx'] = np.asarray(iteration_dict['iter_start_idx'].copy(), dtype=object)
    iteration_dict['iter_times_idx'] = np.asarray(iteration_dict['iter_times_idx'].copy(), dtype=object)

    print('end fix sync code')

    # Check # of trials and iterations match
    trial_count_diff, trials_diff_iteration_big, trials_diff_iteration_small = ephys_utils.assert_iteration_samples_count(iteration_dict['iter_start_idx'], behavior_time)

    print('after assert_iteration_samples_count fix sync code')

    if trial_count_diff != 0:
        print('trial_count_diff', trial_count_diff)
    if len(trials_diff_iteration_big) > 0:
        print('trials_diff_iteration_big', trials_diff_iteration_big)
    if len(trials_diff_iteration_small) > 0:
        print('trials_diff_iteration_small', trials_diff_iteration_small)



    status = ephys_utils.evaluate_sync_process(trial_count_diff, trials_diff_iteration_big, trials_diff_iteration_small,  behavior_time.shape[0])

    print('after evaluate_sync_process fix sync code')

    for i in range(len(iteration_dict['iter_start_idx'])):
        synced_time_vector = iteration_dict['iter_times_idx'][i]
        behavior_time_vector = behavior_time[i].flatten()

        status = sync_evaluation_process2(synced_time_vector, behavior_time_vector)
        if status == -1:
            break

    print('after sync_evaluation_process2', status)
    
    #if status == 1:
    iteration_dict['trial_start_idx'] = ephys_utils.get_index_trial_vector_from_iteration(iteration_dict['iter_start_idx'])

    print('after get_index_trial_vector_from_iteration')

    return status, iteration_dict

