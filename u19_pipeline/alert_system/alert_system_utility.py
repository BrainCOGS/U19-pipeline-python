

import pandas as pd
import datajoint as dj
import datetime

import u19_pipeline.utils.dj_shortcuts as djs

def get_acquisition_data_alert_system(type='subject_fullname', data_days=60, min_sessions=20):
    '''
    Get and filter data for alert system
    Inputs:
        type         = subject_fullname/location  => Data grouping by subject or by rig 
        data_days    = Amount of days to look back for sessions
        min_sessions = Min amount of sessions for a subject/rig to draw conclusions about it
    Outputs:
        session_df   = filtered DataFrame from acquisition.Session table
        key_list     = list of dictionaries with primary key fields for each session (for querying behavior) 
    '''

    acquisition = dj.create_virtual_module('acquisition', 'u19_acquisition')

    # Filter sessiond for # data days only
    today = datetime.date.today()
    past_date = today - datetime.timedelta(days=data_days)
    query_sessions =  'session_date >="' + past_date.strftime('%Y-%m-%d') +'"'
    query_sessions +=  'and session_date < "' + today.strftime('%Y-%m-%d') +'"'
    query_sessions += ' and subject_fullname not like "testuser%"'

    session_df = pd.DataFrame((acquisition.Session & query_sessions).fetch(as_dict=True))

    #Filter subjects/rigs with >= min_sessions
    num_sessions_df = session_df.groupby(type).agg({'session_date': [('total_sessions', 'count')]})
    num_sessions_df.columns = num_sessions_df.columns.droplevel()
    num_sessions_df = num_sessions_df.reset_index()
    num_sessions_df = num_sessions_df.loc[num_sessions_df['total_sessions'] >= min_sessions, :]
    session_df = session_df.merge(num_sessions_df, on=type)

    # Get key list of sessions for further (behavior) querying
    key_fields = djs.get_primary_key_fields(acquisition.Session)
    key_list = session_df.loc[:, key_fields]
    key_list = key_list.to_dict('records')


    return session_df, key_list