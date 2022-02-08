import datajoint as dj

from u19_pipeline import acquisition, subject, recording

schema = dj.schema(dj.config['custom']['database.prefix'] + 'imaging_rec')


@schema
class Scan(dj.Computed):
    definition = """
    # General information of an imaging session
    -> recording.Recording
    ---
    """
    @property
    def key_source(self):
        return recording.Recording & {'recording_modality': 'imaging'}

    def make(self, key):
        self.insert1(key)


@schema
class ImagingProcessing(dj.Computed):
    definition = """
    -> Scan
    -> recording.RecordingProcess
    -----
    """  

    def make(self, key):
        self.insert1(key)


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
