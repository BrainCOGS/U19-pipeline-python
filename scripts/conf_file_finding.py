
import os
import pathlib

def chdir_to_root():

    root_dir_found = 0
    conf_file_found = 0
    while 1:
        
        current_dir = os.getcwd()
        u19_dir = pathlib.Path(current_dir,'u19_pipeline')
        if os.path.isdir(u19_dir):
            root_dir_found = 1
            if os.path.isfile(pathlib.Path(current_dir,'dj_local_conf.json')):
                conf_file_found = 1
        if root_dir_found:
            break
        os.chdir('..')
        new_current_dir = os.getcwd()
        if str(current_dir) == str(new_current_dir):
            break

    return root_dir_found, conf_file_found

def get_root_directory():

    root_dir_found = 0
    current_dir = pathlib.Path(os.getcwd())
    while 1:
        
        u19_dir = pathlib.Path(current_dir,'u19-pipeline_python')
        if os.path.isdir(u19_dir):
            root_dir_found = 1
            break
        new_current_dir = current_dir.parent
        if new_current_dir == current_dir:
            break
        current_dir = new_current_dir

    return root_dir_found, u19_dir


def try_find_conf_file():

    root_dir_found, conf_file_found = chdir_to_root()
    if root_dir_found and conf_file_found:
        print('Local configuration file found !!, no need to run the configuration (unless configuration has changed)')
    elif root_dir_found:
        print('Local configuration file not found. Ignore this if you have a global config. Run configuration notebook otherwise')
    else:
        print('Root dir not found, change this notebook to the project folder')