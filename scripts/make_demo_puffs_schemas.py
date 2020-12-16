import os
import datajoint as dj

dj.config['database.host'] = 'datajoint00.pni.princeton.edu'
dj.config['database.user'] = os.environ.get('DJ_DB_USER')
dj.config['database.password'] = os.environ.get('DJ_DB_PASS')
dj.config['database.prefix'] = 'u19_'
from u19_pipeline import puffs_lab, puffs_acquisition, puffs_behavior
