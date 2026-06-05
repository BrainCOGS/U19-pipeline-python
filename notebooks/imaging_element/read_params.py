
import os
from scipy.io import savemat

this_dir = os.path.dirname(__file__)
os.chdir(this_dir)

import datajoint as dj
dj.conn()

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
    for dict in param_modality_list:
        dict['recording_modality'] = modalities[idx]
        dict['param_set_hash'] = str(dict['param_set_hash'])
        if 'clustering_method' in dict:
            dict['processing_method'] = dict.pop('clustering_method')
    
        params_dict_dict['param_'+str(num_params)] = dict
        num_params +=1

#################################################Fetch all preparamsStepList from all modalities
preparams_steps = []
for table in preparams_steps_tables:
    preparams_steps.append(table.fetch(as_dict=True))

#Append all preparams in the same dictionary
preparams_steps_dict_dict = {}
num_preparams_steps = 0
for idx, preparam_modality_list in enumerate(preparams_steps):
    for dict in preparam_modality_list:
        dict['param_set_hash'] = str(dict['param_set_hash'])
        dict['recording_modality'] = modalities[idx]
        if 'precluster_param_steps_id' in dict:
            dict['preprocess_param_steps_id'] = dict.pop('precluster_param_steps_id')
        if 'precluster_method' in dict:
            dict['preprocess_method'] = dict.pop('precluster_method')
        if 'precluster_param_steps_name' in dict:
            dict['preprocess_param_steps_name'] = dict.pop('precluster_param_steps_name')
        if 'precluster_param_steps_desc' in dict:
            dict['preprocess_param_steps_desc'] = dict.pop('precluster_param_steps_desc')

        preparams_steps_dict_dict['param_'+str(num_preparams_steps)] = dict
        num_preparams_steps +=1

#################################################Fetch all preparams from all modalities
preparams_dict_list = []
for table in preparams_tables:
    preparams_dict_list.append(table.fetch(as_dict=True))


#Append all preparams in the same dictionary
preparams_dict_dict = {}
num_preparams = 0
for idx, preparam_modality_list in enumerate(preparams_dict_list):
    for dict in preparam_modality_list:
        dict['recording_modality'] = modalities[idx]
        dict['param_set_hash'] = str(dict['param_set_hash'])
        if 'precluster_method' in dict:
            dict['preprocess_method'] = dict.pop('precluster_method')
    
        preparams_dict_dict['param_'+str(num_preparams)] = dict
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


savemat('params.mat', params_dict_dict)
savemat('preparams.mat', preparams_dict_dict)
savemat('preparams_list.mat', preparams_steps_dict_dict)

#savemat('methods.mat', methods_dict)
#savemat('premethods.mat', premethods_dict)
