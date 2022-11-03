
import os
import pathlib
import numpy as np
import datajoint as dj


def add_researcher_user_table(user_id, full_name, email, phone):

    # Values sent by function caller
    new_key = dict()
    new_key['user_id'] = user_id
    new_key['user_nickname'] = user_id
    new_key['full_name'] = full_name
    new_key['email'] = email
    new_key['phone'] = phone

    # Write default values for User
    new_key['mobile_carrier'] = 'none'
    new_key['slack'] = full_name
    new_key['contact_via'] = 'Slack'
    new_key['primary_tech'] = 'N/A'
    new_key['tech_responsibility']='yes'
    new_key['day_cutoff_time']= np.array([18, 0])

    # Find conf file (to load it even if we are in different path)
    repository_dir = os.path.abspath(os.path.realpath(__file__)+ "/../../..")
    conf_file = str(pathlib.Path(repository_dir, 'dj_local_conf.json'))
    config = dj.settings.Config()
    config.load(conf_file)

    # Connect and insert record
    dj.conn()
    import u19_pipeline.lab as lab
    lab.User.insert1(new_key)
