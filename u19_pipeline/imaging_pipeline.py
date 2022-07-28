
import datajoint as dj
import pathlib
import subprocess

from u19_pipeline import lab, acquisition, subject, recording
import u19_pipeline.automatic_job.params_config as config
import u19_pipeline.utils.dj_shortcuts as dj_short

from element_calcium_imaging import scan as scan_element
from element_calcium_imaging import imaging_preprocess as imaging_element
from element_interface.utils import find_full_path


schema = dj.schema(dj.config['custom']['database.prefix'] + 'imaging_pipeline')

# Declare upstream imaging tables ------------------------------------------------------
@schema
class ImagingPipelineSession(dj.Computed):
    definition = """
    # General information of an imaging session
    -> recording.Recording
    """
    @property
    def key_source(self):
        return recording.Recording & {'recording_modality': 'imaging'}

    def make(self, key):
        self.insert1(key)


@schema
class AcquiredTiff(dj.Imported):
    definition = """
    # metainfo about imaging session
    # `make` function is declared in the `U19-pipeline-matlab`
    -> ImagingPipelineSession
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

    def make(self, key):

        str_key = dj_short.get_string_key(key)
        command = [config.ingest_scaninfo_script, config.startup_pipeline_matlab_dir, str_key]
        print(command)
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.wait()
        print('aftercommand before comm')
        stdout, stderr = p.communicate()
        print('aftercommand after comm')
        print(stdout.decode('UTF-8'))
        print(stderr.decode('UTF-8'))


@schema
class TiffSplit(dj.Imported):
    definition = """
    # meta-info about specific FOV within mesoscope imaging session
    # `make` function is declared in the `U19-pipeline-matlab` repository
    -> AcquiredTiff
    tiff_split           : tinyint                      # number of the tiff split in this scan
    ---
    tiff_split_directory : varchar(255)                 # the absolute directory created for this tiff_split
    tiff_split_name=null : varchar(32)                  # name of the tiff_split
    fov_depth            : float                        # depth of the field of view  should be a number or a vector?
    fov_center_xy        : blob                         # X-Y coordinate for the center of the FOV in microns. One for each FOV in scan
    fov_size_xy          : blob                         # X-Y size of the FOV in microns. One for each FOV in scan (sizeXY)
    fov_rotation_degrees : float                        # rotation of the FOV with respect to cardinal axes in degrees. One for each FOV in scan
    fov_pixel_resolution_xy : blob                      # number of pixels for rows and columns of the FOV. One for each FOV in scan
    fov_discrete_plane_mode : tinyint                   # true if FOV is only defined (acquired) at a single specifed depth in the volume. One for each FOV in scan should this be boolean?
    power_percent           :  float                    # percentage of power used for this field of view
    """

    #def populate(self, key):

    #    str_key = dj_short.get_string_key(key)
    #    command = [config.ingest_scaninfo_script, config.startup_pipeline_matlab_dir, str_key]
    #    print(command)
    #    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #    p.wait()
    #    print('aftercommand before comm')
    #    stdout, stderr = p.communicate()
    #    print('aftercommand after comm')
    #    print(stdout.decode('UTF-8'))
    #    print(stderr.decode('UTF-8'))

    class File(dj.Part):
        definition = """
        # list of files per tiff split
        -> master
        file_number          : int
        ---
        tiff_split_filename         : varchar(255)                 # file name of the new tiff file
        file_frame_range     : blob                         # [first last] frame indices in this file, with respect to the whole imaging session
        """


# Gathering requirements to activate the imaging element -------------------------------
"""
Requirements to activate the imaging element:

1. Schema names
    + schema name for the scan module
    + schema name for the imaging module

2. Upstream tables
    + Session table
    + Location table (location of the scan - e.g. brain region)
    + Equipment table (scanner information)

3. Utility functions
    + get_imaging_root_data_dir()
    + get_scan_image_files()
    + get_processed_dir()

For more detail, check the docstring of the element:
    help(scan_element.activate)
    help(imaging_element.activate)
"""

# 1. Schema names ----------------------------------------------------------------------
scan_schema_name = dj.config['custom']['database.prefix'] + 'pipeline_scan_element'
imaging_schema_name = dj.config['custom']['database.prefix'] + 'pipeline_imaging_element'

# 2. Upstream tables -------------------------------------------------------------------
from u19_pipeline.reference import BrainArea as Location

Session = TiffSplit

@lab.schema
class Equipment(dj.Manual):
    definition = """
    equipment             : varchar(32)
    ---
    modality              : varchar(64)
    description=null      : varchar(256)
    """

# 3. Utility functions -----------------------------------------------------------------

def get_imaging_root_data_dir():
    data_dir = dj.config.get('custom', {}).get('imaging_root_data_dir', None)
    return data_dir if data_dir else None

def get_scan_image_files(scan_key):

    # Folder structure: root / subject / session / .tif (raw)
    sess_dir = (ImagingPipelineSession & scan_key).fetch1('session_dir')

    if not find_full_path(get_imaging_root_data_dir(), sess_dir).exists():
        raise FileNotFoundError(f'Session directory ({sess_dir}) not found.')

    scan_filepaths_ori = (TiffSplit.File * TiffSplit & scan_key).fetch('tiff_split_directory', 'tiff_split_filename', as_dict=True)

    scan_filepaths_conc = list()
    for i in range(len(scan_filepaths_ori)):
        scan_filepaths_conc.append((pathlib.Path(scan_filepaths_ori[i]['tiff_split_directory']) / scan_filepaths_ori[i]['tiff_split_filename']).as_posix())

    tiff_filepaths = [find_full_path(get_imaging_root_data_dir(),
                                     pathlib.Path(scan_filepaths['fov_directory']) / 
                                     scan_filepaths['fov_filename']).as_posix() 
                      for fp in scan_filepaths]

    if tiff_filepaths:
        return tiff_filepaths
    else:
        raise FileNotFoundError(f'No tiff file found in {sess_dir}')


def get_processed_dir(processing_task_key, process_method):
    sess_key = (ImagingPipelineSession & processing_task_key).fetch1('KEY')
    bucket_scan_dir = (TiffSplit & sess_key &
                             {'tiff_split': processing_task_key['scan_id']}).fetch1('tiff_split_directory')
    user_id = (subject.Subject & processing_task_key).fetch1('user_id')

    sess_dir = find_full_path(get_imaging_root_data_dir(), bucket_scan_dir)
    relative_suite2p_dir = (pathlib.Path(bucket_scan_dir)  / process_method).as_posix()

    if not sess_dir.exists():
        raise FileNotFoundError(f'Session directory not found ({sess_dir})')

    if process_method == 'suite2p':
        # Check if ops.npy is inside suite2p_dir
        suite2p_dirs = set([fp.parent.parent for fp in sess_dir.rglob('*ops.npy')])
        if len(suite2p_dirs) != 1:
            raise FileNotFoundError(f'Error searching for Suite2p output directory in {bucket_scan_dir} - Found {suite2p_dirs}')
    elif process_method == 'caiman':
        raise NotImplementedError('CaImAn is not currented implemented.')

    return sess_dir

# 4. Activate imaging schema -----------------------------------------------------------
imaging_element.activate(imaging_schema_name, scan_schema_name, linking_module=__name__)