"""
Requirements to activate the imaging element

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

For more detail, check the docstring of the element:
    help(scan_element.activate)
    help(imaging_element.activate)

"""

# 1. Schema names --------------------------------------------------------------
import datajoint as dj
import pathlib

from u19_pipeline import acquisition, imaging

from element_calcium_imaging import scan as scan_element
from element_calcium_imaging import imaging as imaging_element

imaging_schema_name = dj.config['custom']['database.prefix'] + 'imaging_element'
scan_schema_name = dj.config['custom']['database.prefix'] + 'scan_element'


# 2. Upstream tables -----------------------------------------------------------
from u19_pipeline.acquisition import Session
from u19_pipeline.reference import BrainArea as Location

schema = dj.schema(dj.config['custom']['database.prefix'] + 'lab')


@schema
class Equipment(dj.Manual):
    definition = """
    scanner: varchar(32)
    """


# 3. Utility functions ---------------------------------------------------------

def get_imaging_root_data_dir():
    data_dir = dj.config.get('custom', {}).get('imaging_root_data_dir', None)
    return pathlib.Path(data_dir) if data_dir else None


def get_scan_image_files(scan_key):
    fov_key = scan_key.copy()
    #Replace scan_id with fov, we are going to search files by fov
    if 'scan_id' in fov_key:
        fov_key['fov'] = fov_key.pop('scan_id')
    relative_fov_directory, fov_filename = (imaging.FieldOfView.File * imaging.FieldOfView & fov_key).fetch('relative_fov_directory', 'fov_filename')
    # relative_fov_directory = [re.findall("braininit/RigData/mesoscope/imaging", x) for x in relative_fov_directory]
    relative_fov_directory = [x[37:] for x in relative_fov_directory]
    subject_name = (subject.Subject & fov_key).fetch1('user_id')
    relative_fov_directory = [subject_name+ '_K' + str(x) for x in relative_fov_directory]
    data_dir = get_imaging_root_data_dir().as_posix()
    scan_filepaths_ori = [data_dir + '/' + subject_name +'/'+ relative_fov_directory[i] + '/' + fov_filename[i] for i in range(0,len(relative_fov_directory))]
    if scan_filepaths_ori:
        return scan_filepaths_ori
    else:
        raise FileNotFoundError(f'No tiff file found in {data_dir}')#TODO search for TIFF files in directory


def get_suite2p_dir(processing_task_key):
    sess_key = (acquisition.Session & processing_task_key).fetch1('KEY')
    bucket_scan_dir = (imaging.FieldOfView & sess_key &
                             {'fov': processing_task_key['scan_id']}).fetch1('relative_fov_directory')
    # if bucket_scan_dir[0] == '/':
    #     bucket_scan_dir = bucket_scan_dir[1:]
    bucket_scan_dir = bucket_scan_dir[37:]
    bucket_scan_dir = pathlib.Path('koay_'+str(bucket_scan_dir))
    #TODO: The imaging root data dir can be a list, modify the code to support list
    data_dir = get_imaging_root_data_dir()
    sess_dir = data_dir / 'koay_' / bucket_scan_dir / 'suite2p'
    relative_suite2p_dir = bucket_scan_dir  / 'suite2p'
    # Check if suite2p dir exists
    if not sess_dir.exists():
        raise FileNotFoundError(f'Session directory not found ({bucket_scan_dir})')

    # Check if ops.npy is inside suite2pdir
    suite2p_dirs = set([fp.parent.parent for fp in sess_dir.rglob('*ops.npy')])
    if len(suite2p_dirs) != 1:
        raise FileNotFoundError(f'Error searching for Suite2p output directory in {bucket_scan_dir} - Found {suite2p_dirs}')
    return sess_dir


# 4. Activate imaging schema ---------------------------------------------------
imaging_element.activate(imaging_schema_name, 
                         scan_schema_name, 
                         linking_module=__name__)
