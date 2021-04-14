import datajoint as dj
import pathlib

from u19_pipeline import acquisition, imaging

from elements_imaging import scan as scan_element
from elements_imaging import imaging as imaging_element
from u19_pipeline.lab import Path

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
imaging_schema_name = dj.config['database.prefix'] + 'imaging_element'
scan_schema_name = dj.config['database.prefix'] + 'scan_element'

# 2. Upstream tables
from u19_pipeline.acquisition import Session
from u19_pipeline.reference import BrainArea as Location


schema = dj.schema(dj.config['database.prefix'] + 'lab')


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
    sess_key = (acquisition.Session & scan_key).fetch1('KEY')
    bucket_scan_dir = (imaging.FieldOfView & sess_key &
                             {'fov': scan_key['scan_id']}).fetch1('fov_directory')

    scan_dir = Path().get_local_path2(bucket_scan_dir)
    print(scan_dir)

    if not scan_dir.exists():
        raise FileNotFoundError(f'Session directory not found ({scan_dir})')

    tiff_filepaths = [fp.as_posix() for fp in scan_dir.glob('*.tif')]
    if tiff_filepaths:
        return tiff_filepaths
    else:
        raise FileNotFoundError(f'No tiff file found in {scan_dir}')


def get_suite2p_dir(processing_task_key):
    sess_key = (acquisition.Session & processing_task_key).fetch1('KEY')
    bucket_scan_dir = (imaging.FieldOfView & sess_key
                            & {'fov': processing_task_key['scan_id']}).fetch1(
                                'fov_directory')

    scan_dir = Path().get_local_path2(bucket_scan_dir)

    if not scan_dir.exists():
        raise FileNotFoundError(f'Session directory not found ({scan_dir})')

    suite2p_dirs = set([fp.parent.parent for fp in scan_dir.rglob('*ops.npy')])
    if len(suite2p_dirs) != 1:
        raise FileNotFoundError(f'Error searching for Suite2p output directory in {scan_dir} - Found {suite2p_dirs}')

    return suite2p_dirs.pop()


# ------------- Activate "imaging" schema -------------
imaging_element.activate(imaging_schema_name,  scan_schema_name,  linking_module=__name__)
