
import os
from scipy.io import savemat

this_dir = os.path.dirname(__file__)
os.chdir(this_dir)

import datajoint as dj
dj.conn()

def replace_none_inplace(data, replacement=[]):
    """Recursively replaces None values in a dictionary in-place."""
    if isinstance(data, dict):
        for key, value in data.items():
            if value is None:
                data[key] = replacement
            elif isinstance(data, dict):
                replace_none_inplace(value, replacement)
    elif isinstance(data, list):
        for index, item in enumerate(data):
            if item is None:
                data[index] = replacement
            elif isinstance(item, (dict, list)):
                replace_none_inplace(item, replacement)


ephys_element =dj.create_virtual_module('u19_pipeline_ephys_element','u19_pipeline_ephys_element')
imaging_element =dj.create_virtual_module('u19_pipeline_imaging_element','u19_pipeline_imaging_element')

modalities             = ['electrophysiology', 'imaging']

params_tables          = [ephys_element.ClusteringParamSet, imaging_element.ProcessingParamSet]
preparams_steps_tables = [(ephys_element.PreClusterParamSteps * ephys_element.PreClusterParamSteps.Step * ephys_element.PreClusterParamSet)]
preparams_tables       = [ephys_element.PreClusterParamSet]

#method_tables          = [ephys_element.ClusteringMethod, imaging_element.ProcessingMethod]
#pre_method_tables      = [ephys_element.PreClusterMethod]

###################################Fetch all params from all modalities
params_dict_list = []
for table in params_tables:
    params_dict_list.append(table.fetch(as_dict=True))


#Append all params in the same dictionary
params_dict_dict = {}
num_params = 0
for idx, param_modality_list in enumerate(params_dict_list):
    for dicto in param_modality_list:
        dicto['recording_modality'] = modalities[idx]
        dicto['param_set_hash'] = str(dicto['param_set_hash'])
        if 'clustering_method' in dicto:
            dicto['processing_method'] = dicto.pop('clustering_method')
    
        params_dict_dict['param_'+str(num_params)] = dicto
        num_params +=1

#################################################Fetch all preparamsStepList from all modalities
preparams_steps = []
for table in preparams_steps_tables:
    preparams_steps.append(table.fetch(as_dict=True))

#Append all preparams in the same dictionary
preparams_steps_dict_dict = {}
num_preparams_steps = 0
for idx, preparam_modality_list in enumerate(preparams_steps):
    for dicto in preparam_modality_list:
        dicto['param_set_hash'] = str(dicto['param_set_hash'])
        dicto['recording_modality'] = modalities[idx]
        if 'precluster_param_steps_id' in dicto:
            dicto['preprocess_param_steps_id'] = dicto.pop('precluster_param_steps_id')
        if 'precluster_method' in dicto:
            dicto['preprocess_method'] = dicto.pop('precluster_method')
        if 'precluster_param_steps_name' in dicto:
            dicto['preprocess_param_steps_name'] = dicto.pop('precluster_param_steps_name')
        if 'precluster_param_steps_desc' in dicto:
            dicto['preprocess_param_steps_desc'] = dicto.pop('precluster_param_steps_desc')

        preparams_steps_dict_dict['param_'+str(num_preparams_steps)] = dicto
        num_preparams_steps +=1

#################################################Fetch all preparams from all modalities
preparams_dict_list = []
for table in preparams_tables:
    preparams_dict_list.append(table.fetch(as_dict=True))


#Append all preparams in the same dictionary
preparams_dict_dict = {}
num_preparams = 0
for idx, preparam_modality_list in enumerate(preparams_dict_list):
    for dicto in preparam_modality_list:
        dicto['recording_modality'] = modalities[idx]
        dicto['param_set_hash'] = str(dicto['param_set_hash'])
        if 'precluster_method' in dicto:
            dicto['preprocess_method'] = dicto.pop('precluster_method')
    
        preparams_dict_dict['param_'+str(num_preparams)] = dicto
        num_preparams +=1

'''
#################################################Fetch all methods from all modalities
all_methods_data = []
for table in method_tables:
    all_methods_data.append(table.fetch("KEY",as_dict=True))

#Append all preparams in the same dictionary
methods_dict = {}
num_methods = 0
for idx, method_list in enumerate(all_methods_data):
    for dict in method_list:
        if 'clustering_method' in dict:
            dict['processing_method'] = dict.pop('clustering_method')

        methods_dict['method_'+str(num_preparams_steps)] = dict
        num_methods +=1 

#################################################Fetch all premethods from all modalities
all_premethods_data = []
for table in pre_method_tables:
    all_premethods_data.append(table.fetch("KEY",as_dict=True))

#Append all preparams in the same dictionary
premethods_dict = {}
num_premethods = 0
for idx, premethod_list in enumerate(all_methods_data):
    for dict in premethod_list:
        if 'precluster_method' in dict:
            dict['preprocessing_method'] = dict.pop('precluster_method')

        premethods_dict['premethod_'+str(num_preparams_steps)] = dict
        num_premethods +=1        
'''

dj.conn().close()

replace_none_inplace(params_dict_dict)

savemat('params.mat', params_dict_dict)
savemat('preparams.mat', preparams_dict_dict)
savemat('preparams_list.mat', preparams_steps_dict_dict)

#savemat('methods.mat', methods_dict)
#savemat('premethods.mat', premethods_dict)
