from os.path import join, dirname
import os
from dotenv import load_dotenv
load_dotenv(join(dirname(__file__), '..', '.env'))

import datajoint as dj

dj.config['database.prefix'] = 'u19_'

dj.config['database.host'] = os.getenv('DJ_HOST')
dj.config['database.user'] = os.getenv('DJ_USER')
dj.config['database.password'] = os.getenv('DJ_PASS')

dj.config['custom'] = dict(
    imaging_root_data_dir='/mnt/bucket/PNI-centers/Bezos/RigData/scope/bay3/testuser/imaging/')

dj.config['stores'] = {
    'meso':
    {
        'location': '/Volumes/u19_dj/external_dj_blobs/meso',
        'protocol': 'file',
        'subfolding': [2, 2]
    }
}
