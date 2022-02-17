import datajoint as dj
from u19_pipeline import acquisition


schema = dj.schema(dj.config['custom']['database.prefix'] + 'imaging')


@schema
class Scan(dj.Imported):
    definition = """
    -> acquisition.Session
    ---
    scan_directory       : varchar(255)
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
    motion_correction_enabled=0 : tinyint               # 
    motion_correction_mode='N/A': varchar(64)           # 
    stacks_enabled=0            : tinyint               # 
    stack_actuator='N/A'        : varchar(64)           # 
    stack_definition='N/A'      : varchar(64)           # 
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
    fov_pixel_resolution_xy : blob                      # number of pixels for rows and columns of the FOV. One for each FOV in scan
    fov_discrete_plane_mode : tinyint                   # true if FOV is only defined (acquired) at a single specifed depth in the volume. One for each FOV in scan should this be boolean?
    power_percent           :  float                    # percentage of power used for this field of view
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
class McMethod(dj.Lookup):
    definition = """
    # available motion correction method
    mc_method            : varchar(128)
    ---
    correlation_type="Normalized" : enum('Normalized','NonNormalized')
    tranformation_type="Linear" : enum('Linear','NonLinear')
    """


@schema
class McParameter(dj.Lookup):
    definition = """
    # parameter definition for a motion correction method
    -> McMethod
    mc_parameter_name    : varchar(64)
    ---
    mc_parameter_description : varchar(255)                 # description of this parameter
    """


@schema
class McParameterSet(dj.Manual):
    definition = """
    # pointer for a pre-saved set of parameter values
    -> McMethod
    mc_parameter_set_id  : int                          # parameter set id
    """

    class Parameter(dj.Part):
        definition = """
        # pre-saved parameter values
        -> master
        -> McParameter
        ---
        mc_parameter_value   : blob                         # value of parameter
        """


@schema
class MotionCorrection(dj.Imported):
    definition = """
    -> FieldOfView
    -> McParameterSet
    ---
    mc_results_directory=null : varchar(255)
    """

    class AcrossFiles(dj.Part):
        definition = """
        # across tif files, x-y shifts for motion registration
        -> master
        ---
        cross_files_x_shifts : blob                         # nFrames x 2, meta file, fileMCorr-xShifts
        cross_files_y_shifts : blob                         # nFrames x 2, meta file, fileMCorr-yShifts
        cross_files_reference_image : longblob                     # 512 x 512, meta file, fileMCorr-reference
        """

    class WithinFile(dj.Part):
        definition = """
        # within each tif file, x-y shifts for motion registration
        -> master
        -> FieldOfView.File
        ---
        within_file_x_shifts : longblob                     # nFrames x 2, meta file, frameMCorr-xShifts
        within_file_y_shifts : longblob                     # nFrames x 2, meta file, frameMCorr-yShifts
        within_reference_image : longblob                     # 512 x 512, meta file, frameMCorr-reference
        """


@schema
class SegmentationMethod(dj.Lookup):
    definition = """
    # available segmentation methods
    seg_method           : varchar(16)
    """


@schema
class SegParameter(dj.Lookup):
    definition = """
    # segmentation method parameter
    -> SegmentationMethod
    seg_parameter_name   : varchar(64)                  # parameter name of segmentation parameter
    ---
    seg_parameter_description : varchar(255)                 # description of this parameter
    """


@schema
class SegParameterSet(dj.Manual):
    definition = """
    # pointer for a pre-saved set of parameter values
    -> SegmentationMethod
    seg_parameter_set_id : int                          # parameter set id
    """

    class Parameter(dj.Part):
        definition = """
        # pre-saved parameter values
        -> master
        -> SegParameter
        ---
        seg_parameter_value  : blob                         # value of parameter
        """


@schema
class Segmentation(dj.Imported):
    definition = """
    # ROI segmentation
    -> MotionCorrection
    -> SegParameterSet
    ---
    num_chunks           : tinyint                      # number of different segmentation chunks within the session
    cross_chunks_x_shifts : blob                         # nChunks x niter,
    cross_chunks_y_shifts : blob                         # nChunks x niter,
    cross_chunks_reference_image : longblob                     # reference image for cross-chunk registration
    seg_results_directory : varchar(255)                 # directory where segmentation results are stored
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
class Trace(dj.Imported):
    definition = """
    # activity traces for each ROI
    -> Segmentation.Roi
    ---
    dff_roi              : longblob                     # delta f/f for each cell, 1 x nFrames. In case of chunks in segmentation, frames with no data are filled with NaN
    dff_roi_is_significant : longblob                     # same size as dff_roi, true where transitents are significant
    dff_roi_is_baseline  : longblob                     # same size as dff_roi, true where values correspond to baseline
    dff_surround         : longblob                     # delta f/f for the surrounding neuropil ring
    spiking              : longblob                     # recovered firing rate of the trace
    time_constants       : blob                         # 2 floats per roi, estimated calcium kernel time constants
    init_concentration   : float                        # estimated initial calcium concentration for estimated kernel
    """
