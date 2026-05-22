
import datajoint as dj
import pathlib
import subprocess
from pathlib import Path

from u19_pipeline import lab, subject, recording
import u19_pipeline.automatic_job.params_config as config
import u19_pipeline.utils.dj_shortcuts as dj_short
import u19_pipeline.utils.tiff_utils as tu


import datajoint as dj

#from element_calcium_imaging import scan as scan_element
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
    -> imaging_pipeline.ImagingPipelineSession
    ---
    file_name_base              : varchar(255)
    scan_width                  : int
    scan_height                 : int
    acq_time                    : datetime
    n_depths                    : tinyint
    scan_depths                 : blob
    frame_rate                  : float
    inter_fov_lag_sec           : float
    frame_ts_sec                : longblob
    power_percent               : float
    channels                    : blob
    cfg_filename                : varchar(255)
    usr_filename                : varchar(255)
    fast_z_lag                  : float
    fast_z_flyback_time         : float
    line_period                 : float
    scan_frame_period           : float
    scan_volume_rate            : float
    flyback_time_per_frame      : float
    flyto_time_per_scan_field   : float
    fov_corner_points           : blob
    nfovs                       : int
    nframes                     : int
    nframes_good                : int
    last_good_file              : int
    motion_correction_enabled=0 : tinyint
    motion_correction_mode="N/A": varchar(64)
    stacks_enabled=0            : tinyint
    stack_actuator="N/A"        : varchar(64)
    stack_definition="N/A"      : varchar(64)
    """

    photon_micro_acq = ['2photon', '3photon']
    mesoscope_acq = ['mesoscope']

    def make(self, key, test_mode=False):

        scan_info = (
            ImagingPipelineSession
            * recording.Recording
            * lab.Location
            & key
        ).fetch1()

        imaging_root = dj.config['custom']['imaging_root_data_dir'][0]
        scan_directory = Path(imaging_root) / scan_info['recording_directory']
        acq_type = scan_info['acquisition_type']

        is_mesoscope = acq_type in self.mesoscope_acq
        is_2photon = acq_type in self.photon_micro_acq

        print(f'Preparing {scan_directory}')

        if is_mesoscope:
            original_stacks_dir = scan_directory / 'originalStacks'

            tif_files = list(scan_directory.glob('*tif*'))

            if not tif_files and original_stacks_dir.exists():
                tif_dir = original_stacks_dir
                skip_parsing = True
            else:
                tif_dir = scan_directory
                original_stacks_dir.mkdir(exist_ok=True)
                skip_parsing = False
        else:
            tif_dir = scan_directory

        fl, basename, is_compressed = tu.check_tif_files(tif_dir)

        fl = [Path(tif_dir,x).as_posix() for x in fl]

        if is_mesoscope:
            imheader, parsed_info = tu.get_parsed_info_mesoscope(fl)
        else:
            imheader, parsed_info = tu.get_parsed_info_2photon(fl)

        rec_info, frames_per_file = tu.get_recording_info(
            fl,
            imheader,
            parsed_info
        )

        rec_info['nfovs'] = tu.get_nfovs(rec_info, is_mesoscope)

        last_good_file, cumulative_frames = tu.get_last_good_frame(
            frames_per_file,
            tif_dir
        )

        rec_info['nframes_good'] = cumulative_frames[last_good_file]
        rec_info['last_good_file'] = last_good_file+1

        rec_info['AcqTime'] = tu.check_acqtime(
            rec_info['AcqTime'],
            scan_directory
        )

        if is_compressed:
            tu.remove_compressed_videos(fl, scan_directory)

        scan_info_key = tu.create_scan_info_key(
            key,
            rec_info,
            scan_info['recording_directory']
        )
        if not test_mode:
            self.insert1(scan_info_key)

        if is_mesoscope:
            tiffsplit_mesoscope_keys,tiff_splitfiles_mesoscope_keys = tu.get_fov_mesoscope(
                fl,
                key,
                skip_parsing,
                imheader,
                rec_info,
                basename,
                cumulative_frames,
                scan_info,
                imaging_root
            )
            for i in range(len(tiffsplit_mesoscope_keys)):
                if not test_mode:
                    TiffSplit.insert(tiffsplit_mesoscope_keys[i])
                    TiffSplit.File.insert(tiff_splitfiles_mesoscope_keys[i])

            if test_mode:
                return scan_info_key, tiffsplit_mesoscope_keys, tiff_splitfiles_mesoscope_keys

        elif is_2photon:
            tiffsplit_2photon_key = tu.get_fov_photonmicro(key, rec_info, scan_info)
            tiffsplitfile_2photon_key = tu.get_fovfile_photonmicro(key, fl, imheader)
            if not test_mode:
                TiffSplit.insert(tiffsplit_2photon_key)
                TiffSplit.File.insert(tiffsplitfile_2photon_key)

            if test_mode:
                return scan_info_key, tiffsplit_2photon_key, tiffsplitfile_2photon_key

        else:
            raise ValueError("Invalid acquisition type")
        
    def old_make(self, key):

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
from u19_pipeline import recording_process

def get_imaging_root_data_dir():
    return dj.config.get('custom', {}).get('imaging_root_data_dir', None)

def get_scan_image_files(job_id):
    #scan_key = (TiffSplit * recording_process.Processing.proj('recording_id', tiff_split='fragment_number') & job_id).fetch1('KEY')

    scan_key = (TiffSplit & job_id).fetch1('KEY')

    filepaths = (TiffSplit.File * TiffSplit & scan_key).fetch('tiff_split_directory', 'tiff_split_filename', as_dict=True)

    tiff_filepaths = [find_full_path(get_imaging_root_data_dir(), 
                      pathlib.Path(file['tiff_split_directory']) / 
                                   file['tiff_split_filename']).as_posix()
                      for file in filepaths]

    return tiff_filepaths

def get_calcium_imaging_files(scan_key, acq_software):

    filepaths = (TiffSplit.File * TiffSplit & scan_key).fetch('tiff_split_directory', 'tiff_split_filename', as_dict=True)

    tiff_filepaths = [find_full_path(get_imaging_root_data_dir(), 
                      pathlib.Path(file['tiff_split_directory']) / 
                                   file['tiff_split_filename']).as_posix()
                      for file in filepaths]

    return tiff_filepaths
    
def get_calcium_imaging_files(scan_key, acq_software):

    filepaths = (TiffSplit.File * TiffSplit & scan_key).fetch('tiff_split_directory', 'tiff_split_filename', as_dict=True)

    tiff_filepaths = [find_full_path(get_imaging_root_data_dir(), 
                      pathlib.Path(file['tiff_split_directory']) / 
                                   file['tiff_split_filename']).as_posix()
                      for file in filepaths]

    return tiff_filepaths

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