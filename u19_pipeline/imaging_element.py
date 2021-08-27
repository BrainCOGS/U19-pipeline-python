import datajoint as dj
import pathlib

from u19_pipeline import acquisition, imaging

from element_calcium_imaging import scan as scan_element
from element_calcium_imaging import imaging as imaging_element

"""
------ Gathering requirements to activate the imaging elements ------

To activate the imaging elements, we need to provide:

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
    + get_suite2p_dir()

For more detail, check the docstring of the imaging element:

    help(scan_element.activate)
    help(imaging_element.activate)

"""

# 1. Schema names
imaging_schema_name = dj.config['custom']['database.prefix'] + 'imaging_element'
scan_schema_name = dj.config['custom']['database.prefix'] + 'scan_element'

# 2. Upstream tables
from u19_pipeline.acquisition import Session
from u19_pipeline.reference import BrainArea as Location


schema = dj.schema(dj.config['custom']['database.prefix'] + 'lab')


@schema
class Equipment(dj.Manual):
    definition = """
    scanner: varchar(32)
    """


# 3. Utility functions

def get_imaging_root_data_dir():
    data_dir = dj.config.get('custom', {}).get('imaging_root_data_dir', None)
    return pathlib.Path(data_dir) if data_dir else None


def get_scan_image_files(scan_key):
    fov_key = scan_key.copy()
    #Replace scan_id with fov, we are going to search files by fov
    if 'scan_id' in fov_key:
        fov_key['fov'] = fov_key.pop('scan_id')
    scan_filepaths_ori = list((imaging.FieldOfView.File * imaging.FieldOfView & fov_key).proj(
    full_path='concat(relative_fov_directory, fov_filename)').fetch('full_path'))

    # if rel paths start with / remove it for Pathlib library
    scan_filepaths_ori = [x[1:] if x[0] == '/' else x for x in scan_filepaths_ori]
    
    data_dir = get_imaging_root_data_dir()
    tiff_filepaths = [(data_dir / x).as_posix() for x in scan_filepaths_ori]
 
    if tiff_filepaths:
        return tiff_filepaths
    else:
        raise FileNotFoundError(f'No tiff file found in {data_dir}')


def get_suite2p_dir(processing_task_key):
    sess_key = (acquisition.Session & processing_task_key).fetch1('KEY')
    bucket_scan_dir = (imaging.FieldOfView & sess_key &
                             {'fov': processing_task_key['scan_id']}).fetch1('relative_fov_directory')

    if bucket_scan_dir[0] == '/':
        bucket_scan_dir = bucket_scan_dir[1:]
        
    data_dir = get_imaging_root_data_dir()
    sess_dir = data_dir / bucket_scan_dir  / 'suite2p'
    relative_suite2p_dir = (pathlib.Path(bucket_scan_dir)  / 'suite2p').as_posix()

    # Check if suite2p dir exists
    if not sess_dir.exists():
        raise FileNotFoundError(f'Session directory not found ({scan_dir})')

    # Check if ops.npy is inside suite2pdir
    suite2p_dirs = set([fp.parent.parent for fp in sess_dir.rglob('*ops.npy')])
    if len(suite2p_dirs) != 1:
        raise FileNotFoundError(f'Error searching for Suite2p output directory in {scan_dir} - Found {suite2p_dirs}')
    
    return relative_suite2p_dir


# ------------- Activate "imaging" schema -------------
imaging_element.activate(imaging_schema_name,  scan_schema_name,  linking_module=__name__)
