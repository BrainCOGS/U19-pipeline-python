from scripts.conf_file_finding import try_find_conf_file
try_find_conf_file()
import time

time.sleep(2)


import numpy as np
import pandas as pd
import u19_pipeline.ephys_pipeline as ep

recordings_missing_sync = pd.DataFrame(ep.EphysPipelineSession.join(ep.BehaviorSync, left=True).fetch('recording_id','nidq_sampling_rate', as_dict=True))
recordings_missing_sync = recordings_missing_sync.loc[recordings_missing_sync['nidq_sampling_rate'].isnull(), :]
recordings_missing_sync = recordings_missing_sync.loc[recordings_missing_sync['recording_id'] > 400, :]
recordings_missing_sync = recordings_missing_sync.sort_values(by='recording_id', ascending=False)
dict_recs = pd.DataFrame(recordings_missing_sync['recording_id']).to_dict('records')
ep.BehaviorSync.populate(dict_recs)