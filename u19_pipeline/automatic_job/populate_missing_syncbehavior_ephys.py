
import time
from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()

time.sleep(1)

import numpy as np
import u19_pipeline.ephys_pipeline as ep
import u19_pipeline.recording as recording
import datajoint as dj
import pandas as pd
import datetime


def get_rec_key_dict(recording_id):

    return {'recording_id': recording_id}


past_date = datetime.date.today() - datetime.timedelta(days=30)
query_session_date =  'session_date >="' + past_date.strftime('%Y-%m-%d') +'"'

all_recs = ((recording.Recording & "recording_modality='electrophysiology'") * (recording.Recording.BehaviorSession & query_session_date)).join(ep.BehaviorSync, left=True)
all_recs = pd.DataFrame(all_recs.fetch('recording_id','subject_fullname','session_date','session_number', as_dict=True))

not_sync_recs = pd.DataFrame((recording.Recording * ep.BehaviorSync).fetch('KEY', as_dict=True))

not_sync_recs2 = pd.merge(all_recs,not_sync_recs, how='left', indicator=True)
not_sync_recs2 = not_sync_recs2.loc[not_sync_recs2['_merge']=='left_only',:]
not_sync_recs2 = not_sync_recs2.sort_values(by='session_date', ascending=False)
not_sync_recs2['rec_key'] = not_sync_recs2['recording_id'].apply(get_rec_key_dict)
not_sync_recs2 = not_sync_recs2.reset_index(drop=True)

for i in range(not_sync_recs2.shape[0]):
    try:
        ep.BehaviorSync.populate(not_sync_recs2.loc[i,'rec_key'])
    except Exception as e:
        print(e)