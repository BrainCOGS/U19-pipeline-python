
import argparse
from getpass import getpass

import pandas as pd


def initial_conf(save_user=True, replace_user=False, global_config_flag=True):
    '''
    Inital configuration for datajoint DB (stores local conf file with dj custom config and dj stores)
    '''

    from scripts.conf_file_finding import try_find_conf_file
    try:
        try_find_conf_file()
    except FileNotFoundError:
        pass
        print("DataJoint configuration file not found. Running configuration script...")
        if global_config_flag:
            print("Global configuration flag is set to True. The configuration will be saved in the global configuration file.")
        else:
            print("Global configuration flag is set to False. The configuration will be saved in the local configuration file.")


    import datajoint as dj

    host='datajoint00.pni.princeton.edu'

    user_already = False
    if 'database.user' in dj.config and dj.config.instance._conf['database.user'] is not None:
        user_already = True

    if replace_user or not user_already:
        print('Enter your username (Princeton NETID):')
        user = input()
        password = getpass()
        dj.conn(host=host, user=user, password=password)
    else:
        dj.conn(host=host)

    if (save_user and not user_already) or replace_user:
        dj.config['database.user'] = user
        dj.config['database.password'] = password


    dj.config['database.host'] = host

    if 'custom' not in dj.config:
        dj.config['custom'] = dict()
        dj.config['custom']['database.prefix'] = 'u19_'

    import u19_pipeline.lab as lab

    # Get all DjCustomVariables variables
    custom_vars = pd.DataFrame(lab.DjCustomVariables.fetch(as_dict=True))
    custom_vars_names = custom_vars['custom_variable'].unique()

    # Transform variables to list and path if applicable
    for custom_var in custom_vars_names:
        this_var = custom_vars.loc[custom_vars['custom_variable'] == custom_var, 'value'].tolist()

        # If custom variables are directories, get local path for this system
        if 'dir' in custom_var:
            this_var = [lab.Path().get_local_path2(x).as_posix() for x in this_var]

        # If only one instance of this variable it must be string not list
        if len(this_var) == 1:
            this_var = this_var[0]

        dj.config['custom'][custom_var] = this_var

    # Get store info
    if 'stores' not in dj.config:
        dj.config['stores'] = dict()

    dj_stores = lab.DjStores.fetch(as_dict=True)

    dj_stores_dict = dict()
    for i in dj_stores:
        store_name = i.pop('store_name')
        dj_stores_dict[store_name] = i
        dj_stores_dict[store_name]['location'] = lab.Path().get_local_path2(i['location']).as_posix()

    dj.config['stores'] = dj_stores_dict

    if global_config_flag:
        print("Global configuration flag is set to True. The configuration will be saved in the global configuration file.")
        dj.config.save_global()
    else:
        dj.config.save_local()


if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument('--not_save_user', '-ns', help="prevent to save user into conf file", action="store_true")
    parser.add_argument('--replace_user', '-r', help="replace user in conf file", action="store_true")
    parser.add_argument('--not_global_config', '-ng', help="prevent to store global configuration file", action="store_true")

    default_args = dict()
    default_args['save_user'] = True
    default_args['replace_user'] = False
    default_args['global_config'] = True


    #print(parser.format_help())
    # usage: initial_conf.py [-h] [--save_user SAVE_USER] [--replace_user REPLACE_USER]
    #
    # optional arguments:
    #   -h, --help         show this help message and exit
    #   --save_user SAVE_USER,  (True/False) save user into conf file (default=True)
    #   --replace_user REPLACE_USER,  (True/False) replace user in conf file (default=False)

    args = parser.parse_args()


    if args.not_save_user:
        default_args['save_user'] = False
    if args.replace_user:
        default_args['replace_user'] = True
    if args.not_global_config:
        default_args['global_config'] = False
    
    initial_conf(save_user=default_args['save_user'],
                 replace_user=default_args['replace_user'],
                 global_config_flag=default_args['global_config'])
