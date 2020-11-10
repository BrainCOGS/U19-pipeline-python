import datajoint as dj
dj.config['database.prefix'] = 'u19_'

dj.config['stores']['meso'] = {
    'location': '/Volumes/u19_dj/external_dj_blobs/meso',
    'protocol': 'file',
    'subfolding': [2, 2]
}
