import datajoint as dj

schema = dj.schema('u19_meso_analysis')


@schema
class TrialSelectionParams(dj.Lookup):
    definition = """
    # trial selection for meso_analysis.BinnedTrace
    trial_parameter_set_id : int                          # id of the set of parameters
    ---
    no_excess_travel     : int                          # if == 1, will exclude trials with excess travel
    towers_perf_thresh   : float                        # block performance in towers block must be above this threshold
    towers_bias_thresh   : float                        # block bias in towers block must be below this threshold
    visguide_perf_thresh : float                        # block performance in visually guided block must be above this threshold
    visguide_bias_thresh : float                        # block bias in visually guided block must be above this threshold
    min_trials_per_block : int                          # there must be this many trials in the block (filters out manual changes between multiple mazes)
    """


@schema
class BinningParameters(dj.Lookup):
    definition = """
    # dff binning parameter
    bin_parameter_set_id : int                          # id of the set of parameters
    ---
    epoch_binning        : blob                         # number of bins per epoch 1 x 6 array
    good_morpho_only     : tinyint                      # whether to use just blobs and doughnuts
    """


@schema
class StandardizedTime(dj.Computed):
    definition = """
    # time binned activity by trialStruct
    -> meso.Segmentation
    -> BinningParameters
    ---
    standardized_time    : longblob                     # linearly interpolated behavioral epoch ID per imaging frame
    binned_time          : blob
    """


@schema
class Trialstats(dj.Computed):
    definition = """
    # behavioral info by trial
    -> meso.Scan
    trial_idx            : int                          # virmen trial struct number
    ---
    went_right           : tinyint                      # true when mouse turned right
    went_left            : tinyint                      # true when mouse turned left
    is_right_trial       : tinyint                      # true when trial type is right
    is_left_trial        : tinyint                      # true when trial type is left
    is_correct           : tinyint                      # true when choice is correct
    is_error             : tinyint                      # true when choice is incorrect
    is_towers_task       : tinyint                      # true when trial is towers task
    is_visguided_task    : tinyint                      # true when trial is visually-guided task
    is_hard              : tinyint                      # for towers task, whether trial is on top 50th prctile of trial difficulty -- delta_towers / total_towers
    is_easy              : tinyint                      # for towers task, whether trial is on bottom 50th prctile of trial difficulty -- delta_towers / total_towers
    has_distractor_towers : tinyint                      # true if trial has towers on non-majority side
    has_no_distractor_towers : tinyint                      # true if trial does not have towers on non-majority side
    is_excess_travel     : tinyint                      # true if total travel > 1.1*nominal maze length
    is_not_excess_travel : tinyint                      # true if total travel < 1.1*nominal maze length
    is_first_trial_in_block : tinyint                      # true if first trial in a block
    time                 : blob                         # 1 x virmen iterations, clock time starting from zero on each trial
    position_x           : blob                         # 1 x virmen iterations, x position in the maze
    position_y           : blob                         # 1 x virmen iterations, y position in the maze
    position_theta       : blob                         # 1 x virmen iterations, view angle position in the maze
    dx                   : blob                         # 1 x virmen iterations, x displacement in the maze
    dy                   : blob                         # 1 x virmen iterations, y  displacement in the maze
    dtheta               : blob                         # 1 x virmen iterations, view angle  displacement in the maze
    raw_sensor_data      : blob                         # 5 x virmen iterations, raw readings from the velocity sensors
    run_speed_instant    : blob                         # instantaneous running speed from x-y displacement
    run_speed_avg_stem   : float                        # average running speed at 0 < position_y < 300
    excess_travel        : float                        # distance traveled normalized by nominal maze length
    trial_dur_sec        : float                        # total trial duration in sec
    total_stem_displacement : float                        # total x-y displacement in maze stem
    block_id             : int                          # identity of the trial block
    mean_perf_block      : float                        # average performance during that block
    mean_bias_block      : float                        # average side bias during that block
    stim_train_id        : int                          # identity of the stimulus train in the trial
    left_draw_generative_prob : float                        # underlying generative probability of drawing and left trial
    cue_pos_right=null   : blob                         # array with y positions of right towers
    cue_pos_left=null    : blob                         # array with y positions of left towers
    cue_onset_time_right=null : blob                         # array with onset times of right towers
    cue_onset_time_left=null : blob                         # array with onset times of left towers
    cue_offset_time_right=null : blob                         # array with offset times of right towers
    cue_offset_time_left=null : blob                         # array with offset times of left towers
    ncues_right          : int                          # total number of right towers
    ncues_left           : int                          # total number of left towers
    ncues_right_minus_left : int                          # ncues_right_minus_left / ncues_total
    ncues_total          : int                          # grand total number of towers on both sides
    trial_difficulty     : float                        # average performance during that block
    true_cue_period_length_cm : int                          # effective y length of cue period, ie between first and last tower
    true_mem_period_length_cm : int                          # effective y length of delay period, ie between last tower and end of stem
    true_cue_period_dur_sec : int                          # effective duration of cue period in sec, ie between first and last tower
    true_mem_period_dur_sec : int                          # effective duration of memory period in sec, ie between last tower and end of stem
    meso_frame_per_virmen_iter : blob                         # array with imaging frame ids per virmen iteration
    meso_frame_unique_ids : blob                         # array with unique imaging frame ids per behavioral trial
    behav_time_by_meso_frame : blob                         # average behavior clock time per imaging frame
    position_x_by_meso_frame : blob                         # average x position per imaging frame
    position_y_by_meso_frame : blob                         # average y position per imaging frame
    position_theta_by_meso_frame : blob                         # average theta position per imaging frame
    dx_by_meso_frame     : blob                         # average x position per imaging frame
    dy_by_meso_frame     : blob                         # average y position per imaging frame
    dtheta_by_meso_frame : blob                         # average theta position per imaging frame
    cues_by_meso_frame_right : blob                         # total number of right towers per imaging frame (should typically be ones and zeros)
    cues_by_meso_frame_left : blob                         # total number of right towers per imaging frame (should typically be ones and zeros)
    trial_start_meso_frame=null : int                          # imaging frame id corresponding to trial start
    cue_entry_meso_frame=null : int                          # imaging frame id corresponding to cue period start
    mem_entry_meso_frame=null : int                          # imaging frame id corresponding to delay period start
    arm_entry_meso_frame=null : int                          # imaging frame id corresponding to entry in the side arm
    trial_end_meso_frame=null : int                          # imaging frame id corresponding to trial end (= reward time in correct trials)
    iti_meso_frame=null  : int                          # imaging frame id corresponding to start of ITI
    timeout_meso_frame=null : int                          # imaging frame id corresponding to start of extra ITI (ie timeout) for error trials
    iti_end_meso_frame=null : int                          # imaging frame id corresponding to end of ITI (last frame before next trial)
    """


@schema
class BinnedBehavior(dj.Computed):
    definition = """
    # time binned behavior by trial
    -> Trialstats
    ---
    binned_position_x    : blob                         # 1 row per trial
    binned_position_y    : blob                         # 1 row per trial
    binned_position_theta : blob                         # 1 row per trial
    binned_dx            : blob                         # 1 row per trial
    binned_dy            : blob                         # 1 row per trial
    binned_dtheta        : blob                         # 1 row per trial
    binned_cue_l=null    : blob                         # 1 row per trial
    binned_cue_r=null    : blob                         # 1 row per trials
    """


@schema
class TrialSelectionParameters(dj.Lookup):
    definition = """
    # trial selection for meso_analysis.BinnedTrace
    trial_parameter_set_id : int                          # id of the set of parameters
    ---
    no_excess_travel     : int                          # if == 1, will exclude trials with excess travel
    towers_perf_thresh   : int                          # block performance in towers block must be above this threshold
    towers_bias_thresh   : int                          # block bias in towers block must be below this threshold
    visguide_perf_thresh : int                          # block performance in visually guided block must be above this threshold
    visguide_bias_thresh : int                          # block bias in visually guided block must be above this threshold
    min_trials_per_block : int                          # there must be this many trials in the block (filters out manual changes between multiple mazes)
    """


@schema
class BinnedTrace(dj.Computed):
    definition = """
    # time binned activity by trial
    -> meso.Segmentation
    -> BinningParameters
    -> TrialSelectionParameters
    global_roi_idx       : int                          # roi_idx in SegmentationRoi table
    trial_idx            : int                          # trial number as in meso_analysis.Trialstats
    ---
    binned_dff           : blob                         # binned Dff, 1 row per neuron per trialStruct
    """


@schema
class RoiStatsParamsSet(dj.Lookup):
    definition = """
    # ROI stats parameters
    roi_stats_parameter_set_id : int                          # id of the set of parameters
    ---
    good_morpho_only     : tinyint                      # whether to use just blobs and doughnuts
    min_dff              : float
    min_spike            : float
    min_significance     : int
    min_active_fraction  : float
    min_active_seconds   : float
    """
