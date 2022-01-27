"""Authors: Ben Dichter, Cody Baker."""
import os
import sys
from pathlib import Path
from shutil import which

import numpy as np
from datetime import datetime
from scipy.io import loadmat, matlab
from collections import Iterable


try:
    from typing import ArrayLike
except ImportError:
    from numpy import ndarray
    from typing import Union, Sequence

    # adapted from numpy typing
    ArrayLike = Union[bool, int, float, complex, list, ndarray, Sequence]


def check_module(nwbfile, name, description=None):
    """
    Check if processing module exists. If not, create it. Then return module.
    Parameters
    ----------
    nwbfile: pynwb.NWBFile
    name: str
    description: str | None (optional)
    Returns
    -------
    pynwb.module
    """
    if name in nwbfile.modules:
        return nwbfile.modules[name]
    else:
        if description is None:
            description = name
        return nwbfile.create_processing_module(name, description)


def find_discontinuities(tt, factor=10000):
    """Find discontinuities in a timeseries. Returns the indices before each discontinuity."""
    dt = np.diff(tt)
    before_jumps = np.where(dt > np.median(dt) * factor)[0]

    if len(before_jumps):
        out = np.array([tt[0], tt[before_jumps[0]]])
        for i, j in zip(before_jumps, before_jumps[1:]):
            out = np.vstack((out, [tt[i + 1], tt[j]]))
        out = np.vstack((out, [tt[before_jumps[-1] + 1], tt[-1]]))
        return out
    else:
        return np.array([[tt[0], tt[-1]]])


def mat_obj_to_dict(mat_struct):
    """Recursive function to convert nested matlab struct objects to dictionaries."""
    dict_from_struct = {}
    for field_name in mat_struct.__dict__['_fieldnames']:
        dict_from_struct[field_name] = mat_struct.__dict__[field_name]
        if isinstance(dict_from_struct[field_name], matlab.mio5_params.mat_struct):
            dict_from_struct[field_name] = mat_obj_to_dict(dict_from_struct[field_name])
        elif isinstance(dict_from_struct[field_name], np.ndarray):
            try:
                dict_from_struct[field_name] = mat_obj_to_array(dict_from_struct[field_name])
            except TypeError:
                continue
    return dict_from_struct


def mat_obj_to_array(mat_struct_array):
    """Construct array from matlab cell arrays.
    Recursively converts array elements if they contain mat objects."""
    if has_struct(mat_struct_array):
        array_from_cell = [mat_obj_to_dict(mat_struct) for mat_struct in mat_struct_array]
        array_from_cell = np.array(array_from_cell)
    else:
        array_from_cell = mat_struct_array

    return array_from_cell


def has_struct(mat_struct_array):
    """Determines if a matlab cell array contains any mat objects."""
    return any(
        isinstance(mat_struct, matlab.mio5_params.mat_struct) for mat_struct in mat_struct_array)


def convert_mat_file_to_dict(mat_file_name):
    """
    Convert mat-file to dictionary object.
    It calls a recursive function to convert all entries
    that are still matlab objects to dictionaries.
    """
    data = loadmat(mat_file_name, struct_as_record=False, squeeze_me=True)
    for key in data:
        if isinstance(data[key], matlab.mio5_params.mat_struct):
            data[key] = mat_obj_to_dict(data[key])
    return data


def array_to_dt(array):
    """Convert array of floats to datetime object."""
    dt_input = [int(x) for x in array]
    dt_input.append(round(np.mod(array[-1], 1) * 10**6))
    return datetime(*dt_input)


def create_indexed_array(ndarray):
    """Creates an indexed array from an irregular array of arrays.
    Returns the flat array and its indices."""
    flat_array = []
    array_indices = []
    for array in ndarray:
        if isinstance(array, Iterable):
            flat_array.extend(array)
            array_indices.append(len(array))
        else:
            flat_array.append(array)
            array_indices.append(1)
    array_indices = np.cumsum(array_indices, dtype=np.uint64)

    return flat_array, array_indices


def flatten_nested_dict(nested_dict):
    """Recursively flattens a nested dictionary."""
    flatten_dict = {}
    for k, v in nested_dict.items():
        if isinstance(v, dict):
            if v:
                flatten_sub_dict = flatten_nested_dict(v).items()
                flatten_dict.update({k2: v2 for k2, v2 in flatten_sub_dict})
            else:
                flatten_dict[k] = np.array([])
        else:
            flatten_dict[k] = v

    return flatten_dict


def convert_function_handle_to_str(mat_file_path):
    """Executes a matlab script which converts function handle values to str
     if matlab is installed on the system."""
    matlab_class = '''
    classdef Choice < uint32
  
        enumeration
            L(1)
            R(2)
            nil(inf)
        end
  
        methods (Static)
            function choices = all()
                choices = enumeration('Choice')';
                choices = choices(1:end-1);
            end
    
            function num = count()
                num = numel(enumeration('Choice'));
            end
        end
  
        methods
            function opp = opposite(obj)
                numValues   = numel(Choice.all());
                assert(numValues == 2);     % the concept of "opposite" only works for sets of 2
      
                flipped     = double(obj);
                flipped     = numValues+1 - flipped;
                opp         = obj;
                sel         = opp >= 1 & opp <= numValues;
                opp(sel)    = flipped(sel);
            end
        end
  
    end
    '''
    matlab_code = r'''

    %Support when behavioral files has 2 "versions"
    if length(log.version) > 1
        log.version = log.version(1);
    end

    str_func = char(log.version.code);
    code_version = 'code_version.txt';
    fid = fopen(code_version, 'wt');
    fprintf(fid, str_func);
    fclose(fid);
    str_func = char(log.animal.protocol);
    protocol = 'protocol.txt';
    fid = fopen(protocol, 'wt');
    fprintf(fid, str_func);
    fclose(fid);
    
    choice_data = [];
    shapingProtocol = [];
    for i = 1 : size(log.block, 2)
        for j = 1 : size(log.block(i).trial, 2)
            choice_data = [choice_data; string(log.block(i).trial(j).choice)];
        end
        shapingProtocol = [shapingProtocol string(char(log.block(i).shapingProtocol))] 
    end

    shaping_protocol_file = 'shaping_protocol.txt';
    fid = fopen(shaping_protocol_file, 'wt');
    fprintf(fid,'%s\n', shapingProtocol);
    fclose(fid);
    
    choice = 'trial_choice.txt';
    fid = fopen(choice, 'wt');
    fprintf(fid,'%s\n', choice_data);
    fclose(fid);
    
    trial_type_data = [];
    for i = 1 : size(log.block, 2)
        for j = 1 : size(log.block(i).trial, 2)
            trial_type_data = [trial_type_data; string(log.block(i).trial(j).trialType)];
        end
    end
    
    trial_type = 'trial_type.txt';
    fid = fopen(trial_type, 'wt');
    fprintf(fid,'%s\n', trial_type_data);
    fclose(fid);
    
    quit;
    '''

    with Path('Choice.m').open('w') as f:
        f.write(matlab_class)

    metadata = {}
    convert_script_code = f"filePath = '{mat_file_path}';\nload(filePath);{matlab_code}"
    convert_script_path = Path("convert_function_to_txt.m")

    with convert_script_path.open('w') as f:
        f.write(convert_script_code)

    if 'win' in sys.platform and sys.platform != 'darwin':
        matlab_cmd = '''
                     #!/bin/bash
                     matlab -nosplash -wait -log -r convert_function_to_txt
                     '''
    else:
        matlab_cmd = '''
                     #!/bin/bash
                     matlab -nosplash -nodisplay -log -r convert_function_to_txt
                     '''

    if which('matlab') is not None:
        try:
            os.system(matlab_cmd)
        
            with open("trial_choice.txt", "r") as f:
                trial_choice = f.read().splitlines()
            with open("trial_type.txt", "r") as f:
                trial_type = f.read().splitlines()
            with open("shaping_protocol.txt", "r") as f:
                shaping_protocol = f.read().splitlines()
            with open("code_version.txt", "r") as f:
                version = f.readline()
            with open("protocol.txt", "r") as f:
                protocol = f.readline()

            metadata['experiment_name'] = version
            metadata['protocol_name'] = protocol
            metadata['trial_choice'] = trial_choice
            metadata['trial_type'] = trial_type
            metadata['shaping_protocol'] = shaping_protocol

            os.remove("code_version.txt")
            os.remove("protocol.txt")
            os.remove("trial_choice.txt")
            os.remove("trial_type.txt")
            os.remove("shaping_protocol.txt")

        except Exception as e:
            print(f"There was an error while trying to execute {convert_script_path}:\n{e}")
    else:
        print("A working matlab version was not found. "
              "Code version, animal protocol, type of trial, and choice could not be saved to NWB.")

    os.remove("Choice.m")
    os.remove("convert_function_to_txt.m")

    return metadata

    def convert_towers_block_trial_2_df(current_block_trial, block_num):
        """
        Convert block trial data from matlab file to a Pandas DataFrame
        Parameters
        ----------
        current_block_trial: dict | numpy.ndarray
        block_num: int
        Returns
        -------
        pandas.DataFrame
        """
        
        valid_block = 0
        # "Normal" blocks are stored as numpy arrays and its length is greater than 0
        if isinstance(current_block_trial, np.ndarray) and current_block_trial.shape[0] > 0:
            current_block_trial = current_block_trial.tolist()
            valid_block = 1 
        # One trial blocks are stored as dictionaries
        if isinstance(current_block_trial, dict):
            current_block_trial = [current_block_trial]
            valid_block = 1 

        if valid_block:
            block_trial_df = pd.DataFrame(current_block_trial)
            block_trial_df.insert(loc=0, column='trial_idx', value=np.arange(len(block_trial_df))+1)
            block_trial_df.insert(loc=0, column='block', value=block_num)
        else:
            block_trial_df = pd.DataFrame()

        return valid_block, block_trial_df

    def convert_towers_block_2_df(current_block, num_block):
        """
        Convert block data from matlab file to a Pandas DataFrame
        Parameters
        ----------
        current_block_trial: dict | numpy.ndarray
        block_num: int
        Returns
        -------
        pandas.DataFrame
        """
        valid_block = 0

        # "Normal" blocks are stored as numpy arrays and its length is greater than 0
        if isinstance(current_block, np.ndarray) and current_block_trial.shape[0] > 0:
            current_block = current_block.tolist()
            valid_block = 1 
        # One trial blocks are stored as dictionaries
        if isinstance(current_block, dict):
            current_block = [current_block]
            valid_block = 1 

        if valid_block:
            block_df = pd.DataFrame(current_block)
            block_df.insert(loc=0, column='block', value=num_block)
            block_df = block_df.drop(['trial'], axis=1)
        else:
            block_df = pd.DataFrame()

        return valid_block, block_df


    def convert_behavior_file(mat_file):
        """
        Convert a matlab behavior file to a session dictionary object and a block/trial pandas DataFrame
        ----------
        mat_file: str
        Returns
        -------
        dict
        pandas.DataFrame
        """

        matin = convert_mat_file_to_dict(mat_file)
        converted_metadata = convert_function_handle_to_str(mat_file_path=mat_file)

        session_block_trial_df = pd.DataFrame()

        #Convert all blocks trials to dataframe and append them

        #For a single block sessions
        if isinstance(matin['log']['block'], dict):
            length_blocks = 1
            dict_block = 1
        #For multiple block sessions
        else:
            length_blocks = matin['log']['block'].shape[0]
            dict_block = 0

        #Convert all blocks of the sesison
        for i in range(matin['log']['block'].shape[0]):

            if dict_block:
                block = matin['log']['block']
            else:
                block = matin['log']['block'][i]

            #Convert trial df and block df
            valid_block, block_trial_df = convert_towers_block_trial_2_df(block['trial'],i+1)
            valid_blocks, block_df = convert_towers_block_2_df(block, i+1)
            #Write string of block level Protocol  (from matlab obscured data)
            block_df['shapingProtocol'] = converted_metadata['shaping_protocol'][i]

            if valid_block and valid_blocks:
                session_current_block_trial_df = block_trial_df.merge(block_df, on='block', suffixes=['_block', '_trial'])
                if num_blocks_conv == 0:
                    session_block_trial_df = session_current_block_trial_df.copy()
                else:
                    session_block_trial_df = session_block_trial_df.append(session_current_block_trial_df)
                num_blocks_conv +=1


        #Write choice and trial type of each trial (from matlab obscured data)
        session_block_trial_df['choice'] = converted_metadata['trial_choice']
        session_block_trial_df['trialType'] = converted_metadata['trial_type']
        session_block_trial_df = session_block_trial_df.reset_index(drop=True)
        return session_block_trial_df
