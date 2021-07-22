from os.path import join, dirname
import os
import datajoint as dj

dj.config['enable_python_native_blobs'] = True

dj.config['stores'] = {
    'meso':
    {
        'location': '/Volumes/u19_dj/external_dj_blobs/meso',
        'protocol': 'file',
        'subfolding': [2, 2]
    }
}
