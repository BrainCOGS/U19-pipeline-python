import datajoint as dj
import numpy as np

schema = dj.schema('u19_meso')


@schema
class Scan(dj.Imported):
    definition = """
    # existence of an imaging session
    -> acquisition.Session
    ---
    scan_directory       : varchar(255)
    gdd=null             : float
    wavelength=940       : float                        # in nm
    pmt_gain=null        : float
    """


@schema
class ScanInfo(dj.Imported):
    definition = """
    # metainfo about imaging session
    -> Scan
    ---
    file_name_base       : varchar(255)                 # base name of the file
    scan_width           : int                          # width of scanning in pixels
    scan_height          : int                          # height of scanning in pixels
    acq_time             : datetime                     # acquisition time
    n_depths             : tinyint                      # number of depths
    scan_depths          : blob                         # depth values in this scan
    frame_rate           : float                        # imaging frame rate
    inter_fov_lag_sec    : float                        # time lag in secs between fovs
    frame_ts_sec         : longblob                     # frame timestamps in secs 1xnFrames
    power_percent        : float                        # percentage of power used in this scan
    channels             : blob                         # is this the channer number or total number of channels
    cfg_filename         : varchar(255)                 # cfg file path
    usr_filename         : varchar(255)                 # usr file path
    fast_z_lag           : float                        # fast z lag
    fast_z_flyback_time  : float                        # time it takes to fly back to fov
    line_period          : float                        # scan time per line
    scan_frame_period    : float
    scan_volume_rate     : float
    flyback_time_per_frame : float
    flyto_time_per_scan_field : float
    fov_corner_points    : blob                         # coordinates of the corners of the full 5mm FOV, in microns
    nfovs                : int                          # number of field of view
    nframes              : int                          # number of frames in the scan
    nframes_good         : int                          # number of frames in the scan before acceptable sample bleaching threshold is crossed
    last_good_file       : int                          # number of the file containing the last good frame because of bleaching
    """


@schema
class FieldOfView(dj.Imported):
    definition = """
    # meta-info about specific FOV within mesoscope imagining session
    -> Scan
    fov                  : tinyint                      # number of the field of view in this scan
    ---
    fov_directory        : varchar(255)                 # the absolute directory created for this fov
    fov_name=null        : varchar(32)                  # name of the field of view
    fov_depth            : float                        # depth of the field of view  should be a number or a vector?
    fov_center_xy        : blob                         # X-Y coordinate for the center of the FOV in microns. One for each FOV in scan
    fov_size_xy          : blob                         # X-Y size of the FOV in microns. One for each FOV in scan (sizeXY)
    fov_rotation_degrees : float                        # rotation of the FOV with respect to cardinal axes in degrees. One for each FOV in scan
    fov_pixel_resolution_xy : blob                         # number of pixels for rows and columns of the FOV. One for each FOV in scan
    fov_discrete_plane_mode : tinyint                      # true if FOV is only defined (acquired) at a single specifed depth in the volume. One for each FOV in scan should this be boolean?
    """

    class File(dj.Part):
        definition = """
        # list of files per FOV
        -> master
        file_number          : int
        ---
        fov_filename         : varchar(255)                 # file name of the new fov tiff file
        file_frame_range     : blob                         # [first last] frame indices in this file, with respect to the whole imaging session
        """


@schema
class SyncImagingBehavior(dj.Computed):
    definition = """
    # synchronization between imaging and behavior
    -> FieldOfView
    ---
    sync_im_frame        : longblob                     # frame number within tif file
    sync_im_frame_global : longblob                     # global frame number in scan
    sync_behav_block_by_im_frame : longblob                     # array with behavioral block for each imaging frame
    sync_behav_trial_by_im_frame : longblob                     # array with behavioral trial for each imaging frame
    sync_behav_iter_by_im_frame : longblob                     # array with behavioral trial for each imaging frame, some extra zeros in file 1, marking that the behavior recording hasn't started yet.
    sync_im_frame_span_by_behav_block : longblob                     # cell array with first and last imaging frames for for each behavior block
    sync_im_frame_span_by_behav_trial : longblob                     # cell array with first and last imaging frames for for each behavior trial
    sync_im_frame_span_by_behav_iter : longblob                     # cell array with first and last imaging frames for for each behavior iteration within each trial
    """


@schema
class MotionCorrectionMethod(dj.Lookup):
    definition = """
    # available motion correction method
    mcorr_method         : varchar(128)
    ---
    correlation_type="Normalized" : enum('Normalized','NonNormalized')
    tranformation_type="Linear" : enum('Linear','NonLinear')
    """

    contents = [
        ['LinearNormalized', 'Normalized', 'Linear'],
        ['NonLinearNormalized', 'Normalized', 'Nonlinear']
    ]


@schema
class McParameter(dj.Lookup):
    definition = """
    -> MotionCorrectionMethod
    mc_parameter_name : varchar(32)
    ---
    mc_parameter_description: varchar(255)
    """

    contents = [
        ['LinearNormalized', 'mc_max_shift', ''],
        ['LinearNormalized', 'mc_max_iter', ''],
        ['LinearNormalized', 'mc_stop_below_shift', ''],
        ['LinearNormalized', 'mc_black_tolerance', ''],
        ['LinearNormalized', 'mc_median_rebin', '']
    ]


@schema
class McParameterSet(dj.Lookup):
    definition = """
    # pointer for a pre-saved set of parameter values
    -> MotionCorrectionMethod
    mc_parameter_set_id  : int                          # parameter set id
    """

    contents = [['LinearNormalized', 1]]

    class Parameter(dj.Part):
        definition = """
        # pre-saved paraemter values
        -> master
        -> McParameter
        ---
        mc_max_shift         : blob                         # max allowed shift in pixels
        mc_max_iter          : blob                         # max number of iterations
        mc_stop_below_shift  : float                        # tolerance for stopping algorithm
        mc_black_tolerance   : float                        # tolerance for black pixel value
        mc_median_rebin      : float                        # ? (check with Sue Ann)
        """


@schema
class MotionCorrectionWithinFile(dj.Imported):
    definition = """
    # within each tif file, x-y shifts for motion registration
    -> FieldOfView.File
    -> McParameterSet
    ---
    within_file_x_shifts : longblob                     # nFrames x 2, meta file, frameMCorr-xShifts
    within_file_y_shifts : longblob                     # nFrames x 2, meta file, frameMCorr-yShifts
    within_reference_image : longblob                     # 512 x 512, meta file, frameMCorr-reference
    """


@schema
class MotionCorrectionAcrossFiles(dj.Imported):
    definition = """
    # across tif files, x-y shifts for motion registration
    -> FieldOfView
    -> McParameterSet
    ---
    cross_files_x_shifts : blob                         # nFrames x 2, meta file, fileMCorr-xShifts
    cross_files_y_shifts : blob                         # nFrames x 2, meta file, fileMCorr-yShifts
    cross_files_reference_image : blob                         # 512 x 512, meta file, fileMCorr-reference
    """


@schema
class MotionCorrection(dj.Imported):
    definition = """
    # handles motion correction
    -> FieldOfView
    -> McParameterSet
    """


@schema
class SegmentationMethod(dj.Lookup):
    definition = """
    # available segmentation methods
    segmentation_method  : varchar(16)
    """


@schema
class SegParameter(dj.Lookup):
    definition = """
    # segmentation method parameter
    -> SegmentationMethod
    seg_parameter_name   : varchar(64)                  # parameter name of segmentation parameter
    """


@schema
class SegParameterSet(dj.Lookup):
    definition = """
    # parameter set for a segmentation method
    -> SegmentationMethod
    seg_parameter_set_id : int                          # parameter set of a method
    """

    class Parameter(dj.Part):
        definition = """
        # parameter values of a segmentation parameter set
        -> master
        -> SegParameter
        ---
        chunks_auto_select_behav : tinyint                      # select chunks automaticaly based on good behavioral performance
        chunks_auto_select_bleach : tinyint                      # select chunks automaticaly based on bleaching
        chunks_towers_min_n_trials : int                          # min number of towers task trials to include a block
        chunks_towers_perf_thresh : float                        # min performance (fraction correct) of towers task to include a block
        chunks_towers_bias_thresh : float                        # max side bias (fraction trials) of towers task to include a block
        chunks_towers_max_frac_bad : float                        # max fraction of bad motor trials of towers task to include a block
        chunks_visguide_min_n_trials : int                          # min number of towers task trials to include a block
        chunks_visguide_perf_thresh : float                        # min performance (fraction correct) of towers task to include a block
        chunks_visguide_bias_thresh : float                        # max side bias (fraction trials) of towers task to include a block
        chunks_visguide_max_frac_bad : float                        # max fraction of bad motor trials of towers task to include a block
        chunks_min_num_consecutive_blocks : int                          # min good consecuitve blocks to select session
        chunks_break_nonconsecutive_blocks : tinyint                      # set true to break non consecuitve behavior blocks into separate segmentation chunks
        cnmf_num_components  : int                          # number of components to be found, for initialization purposes
        cnmf_tau             : float                        # std of gaussian kernel (size of neuron)
        cnmf_p               : tinyint                      # order of autoregressive system (p = 0 no dynamics, p=1 just decay, p = 2, both rise and decay)
        cnmf_num_iter        : tinyint                      # number of iterations
        cnmf_files_per_chunk : int                          # max allowed files per segmentation chunk
        cnmf_proto_num_chunks : int                          # how many chunks to use when initializing morphological segmentation
        cnmf_zero_is_minimum : tinyint                      # allow min fluorescence to be higher than zero
        cnmf_default_timescale : float                        # ? (ask Sue Ann)
        cnmf_time_resolution : float                        # time resolution in ms, if different than frame rate results in downsampling
        cnmf_dff_rectification : float                        # deemphasize dF/F values below this magnitude when computing component correlations
        cnmf_min_roi_significance : float                        # minimum significance for components to retain; at least some time points must be above this threshold
        cnmf_frame_rate      : float                        # imaging frame rate in fps
        cnmf_min_num_frames  : int                          # min required number of frames for segmentation
        cnmf_max_centroid_dist : float                        # maximum fraction of diameter within which to search for a matching template
        cnmf_min_dist_pixels : int                          # allow searching within this many pixels even if the diameter is very small
        cnmf_min_shape_corr  : float                        # minimum shape correlation for global registration
        cnmf_pixels_surround : blob                         # number of pixels considered to be the roi's surround
        gof_contain_energy   : float                        # goodness of fit, fractional amount of energy used to specify spatial support
        gof_core_energy      : float                        # fractional amount of energy used to specify core of component
        gof_noise_range      : float                        # range in which to search for modal (baseline) activation
        gof_max_baseline     : float                        # number of factors below the data noise to consider as (unambiguously) baseline
        gof_min_activation   : int                          # number of factors above the data noise to consider activity as significant
        gof_high_activation  : int                          # number of factors above the data noise to consider activity as significant with reduced time span
        gof_min_time_span    : int                          # number of timeScale chunks to require activity to be above threshold
        gof_bkg_time_span    : int                          # number of timeScale chunks for smoothing the background activity level in order to determine its baseline
        gof_min_dff          : float                        # minimum dF/F to be considered as a significant transient
        """


@schema
class Segmentation(dj.Imported):
    definition = """
    # ROI segmentation
    -> FieldOfView
    -> SegParameterSet
    ---
    num_chunks           : tinyint                      # number of different segmentation chunks within the session
    cross_chunks_x_shifts : blob                         # nChunks x niter,
    cross_chunks_y_shifts : blob                         # nChunks x niter,
    cross_chunks_reference_image : longblob                     # reference image for cross-chunk registration
    """

    class Roi(dj.Part):
        definition = """
        # metainformation and pixel masks for each ROI
        -> master
        roi_idx              : int                          # index of the roi
        ---
        roi_spatial          : longblob                     # 2d matrix with image for spatial mask for the roi
        roi_global_xy        : blob                         # roi centroid in global image coordinates
        roi_is_in_chunks     : blob                         # array with the chunk ids the roi is present in
        surround_spatial     : longblob                     # same as roi_spatial, for the surrounding neuropil ring
        """

    class RoiMorphologyAuto(dj.Part):
        definition = """
        # automatic morphological classification of the ROIs
        -> master.Roi
        ---
        morphology           : enum('Doughnut','Blob','Puncta','Filament','Other','Noise') # shape classification
        """

    class Chunks(dj.Part):
        definition = """
        # registration between different segmentation chunks within a recording
        -> master
        segmentation_chunk_id : tinyint                      # id for the subsection of the recording this segmentation is for, for cases with multi-chunk segemntation (e.g. because of z drift)
        ---
        tif_file_list        : blob                         # cell array with names of tif files that went into this chunk
        imaging_frame_range  : blob                         # [firstFrame lastFrame] of this chunk with respect to the full session
        region_image_size    : blob                         # x-y size of the cropped image after accounting for motion correction shifts
        region_image_x_range : blob                         # x range of the cropped image after accounting for motion correction shifts
        region_image_y_range : blob                         # x range of the cropped image after accounting for motion correction shifts
        """

    class Background(dj.Part):
        definition = """
        # for each chunck, global background info (from cnmf)
        -> master.Chunks
        ---
        background_spatial   : longblob                     # 2D matrix flagging pixels that belong to global background in cnmf
        background_temporal  : longblob                     # time course of global background in cnmf
        """


@schema
class SegmentationRoiMorphologyManual(dj.Manual):
    definition = """
    # manula curation of morphological classification of the ROIs
    -> Segmentation.Roi
    ---
    morphology           : enum('Doughnut','Blob','Puncta','Filament','Other','Noise')
    """


@schema
class Trace(dj.Imported):
    definition = """
    # activity traces for each ROI
    -> Segmentation.Roi
    ---
    f_roi_raw            : blob@meso                    # raw f for each cell, 1 x nFrames. For all 1 x nFrames attributes, in case of chunks in segmentation, frames with no data are filled with NaN
    f_roi                : blob@meso                    # f for each cell after neuropil correction, 1 x nFrames.
    f0_roi_raw           : float                        # baseline for each cell, calculated on f_roi_raw
    f0_roi               : float                        # baseline for each cell, calculated on f_roi (neurpil corrected)
    f_surround_raw       : blob@meso                    # raw surround f for each cell, 1 x nFrames.
    dff_roi              : blob@meso                    # delta f/f for each cell, 1 x nFrames, after baseline correction and neuropil correction (calculated from f_roi and f0_roi)
    dff_roi_uncorrected  : blob@meso                    # delta f/f, baseline corrected but no neuropil correction, 1 x nFrames (calculated from f_roi_raw and f0_roi_raw)
    spiking              : blob@meso                    # recovered firing rate of the trace using infered f
    time_constants       : blob                         # 2 floats per roi, estimated calcium kernel time constants
    init_concentration   : float                        # estimated initial calcium concentration for estimated kernel
    noise                : float                        # 1 x ROI, noise values for significance estimation
    INDEX (f_roi_raw)
    INDEX (f_roi)
    INDEX (f_surround_raw)
    INDEX (dff_roi)
    INDEX (dff_roi_uncorrected)
    INDEX (spiking)
    """




if __name__ == '__main__':

    key = {'mcorr_method': 'LinearNormalized',
           'mc_parameter_set_id': 1}

    parameters = [
        dict(**key, mc_parameter_name='mc_max_shift',
             mc_parameter_value=np.array([15.])),
        dict(**key, mc_parameter_name='mc_max_iter',
             mc_parameter_value=np.array([5.])),
        dict(**key, mc_parameter_name='mc_stop_below_shift',
             mc_parameter_value=0.3),
        dict(**key, mc_parameter_name='mc_black_tolerance',
             mc_parameter_value=-1.),
        dict(**key, mc_parameter_name='mc_median_rebin',
             mc_parameter_value=10.)]

    McParameterSet.Parameter.insert(parameters)
