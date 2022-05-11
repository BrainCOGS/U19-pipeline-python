import os
import pathlib
import subprocess
import json
import time
import re
import traceback
import pandas as pd
import datajoint as dj
from u19_pipeline import recording, recording_process
from u19_pipeline.imaging_pipeline import imaging_element
from u19_pipeline.ephys_pipeline import ephys_element
import u19_pipeline.utils.dj_shortcuts as dj_short
import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
import u19_pipeline.automatic_job.slurm_creator as slurmlib
import u19_pipeline.automatic_job.parameter_file_creator as paramfilelib
import u19_pipeline.automatic_job.params_config as config
from u19_pipeline.utility import create_str_from_dict, is_this_spock

recording_process_status_df = pd.DataFrame(config.recording_process_status_dict)

class RecProcessHandler():

    default_update_value_dict ={
        'value_update': None,
        'error_info': None
    }

    @staticmethod
    def pipeline_handler_main():
        '''
        Call all processing functions according to the current status of each process job
        Update status of each process job accordingly
        '''

        #Get info from all the possible status for a processjob 
        df_all_process_job = RecProcessHandler.get_active_process_jobs()

        #For all active process jobs
        for i in range(df_all_process_job.shape[0]):

            #Filter current process job
            rec_process_series = df_all_process_job.loc[i, :].copy()

            if rec_process_series['recording_modality'] == 'ephys':
                
                precluster_param_list_id, paramset_idx = (recording_process.Processing.EphysParams & rec_process_series).fetch1('precluster_param_list_id', 'paramset_idx')
                
                precluster_param_list = (ephys_element.PreClusterParamList.ParamOrder & f'precluster_param_list_id={precluster_param_list_id}').fetch()

                rec_process_series['preprocess_paramset'] = []
                for precluster_param in precluster_param_list:
                    params = (ephys_element.PreClusterParamSet & f'paramset_idx={precluster_param["paramset_idx"]}').fetch1('params')
                    
                    rec_process_series['preprocess_paramset'].append(dict(
                        order_id=precluster_param['order_id'],
                        params=params))

                rec_process_series['process_paramset'] = (ephys_element.ClusteringParamSet & f'paramset_idx={paramset_idx}').fetch1('params')
            
            elif rec_process_series['recording_modality'] == 'imaging':
                
                paramset_idx = (recording_process.Processing.ImagingParams & rec_process_series).fetch1('paramset_idx')

                rec_process_series['process_paramset'] = (imaging_element.ProcessingParamSet & f'paramset_idx={paramset_idx}').fetch1('params')

            #ALS, correct preprocess params if OLD or outdated

            #Filter current status info
            current_status = rec_process_series['status_pipeline_idx']
            current_status_series = config.recording_process_status_df.loc[config.recording_process_status_df['Value'] == current_status, :].squeeze()
            next_status_series    = config.recording_process_status_df.loc[config.recording_process_status_df['Value'] == current_status+1, :].squeeze()

            # Get processing function    
            function_status_process = getattr(RecProcessHandler, next_status_series['ProcessFunction'])

            #Trigger process, if success update recording process record
            try:
                success_process, update_dict = function_status_process(rec_process_series, next_status_series) 

                print('update_dict', update_dict)

                if success_process:
                    #Get dictionary of record process
                    key = rec_process_series['query_key']
                    #Get values to update
                    next_status = next_status_series['Value']
                    value_update = update_dict['value_update']
                    field_update = next_status_series['UpdateField']

                    print('key to update', key)
                    print('old status', current_status, 'new status', next_status)
                    print('value_update', value_update, 'field_update', field_update)
                    print('function executed:', next_status_series['ProcessFunction'])

                    RecProcessHandler.update_status_pipeline(key, next_status, field_update, value_update)
            except Exception as err:
                raise(err)
                print(traceback.format_exc())
                ## Send notification error, update recording to error

            time.sleep(2)


    @staticmethod
    def transfer_request(rec_series, status_series):
        """
        Request a transfer from PNI to Tiger Cluster
        Input:
        rec_series     (pd.Series)  = Series with information about the recording process
        status_series  (pd.Series)  = Series with information about the next status of the process (if neeeded)
        Returns:
        status_update       (int)     = 1  if recording process status has to be updated to next step in recording.RecordingProcess
                                      = 0  if recording process status not to be changed 
                                      = -1 if recording process status has to be updated to ERROR in recording.RecordingProcess
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """

        status_update = False
        update_value_dict = RecProcessHandler.default_update_value_dict.copy()
        directory_path = status_series['FunctionField']

        print('process_cluter:', rec_series['preprocess_paramset']['process_cluster'])

        # If tiger, we trigger globus transfer 
        if rec_series['preprocess_paramset']['process_cluster'] == "tiger":
            print('si fue tiger')
            if status_series['Key'] is 'RAW_FILE_TRANSFER_REQUEST':
                transfer_request = ft.globus_transfer_to_tiger(directory_path)
            elif status_series['Key'] is 'PROC_FILE_TRANSFER_REQUEST':
                #ALS, which recording directory for processed file
                transfer_request = ft.globus_transfer_to_pni(directory_path)

            if transfer_request['code'] == 'Accepted':
                status_update = True
                update_value_dict['value_update'] = transfer_request['task_id']
        # If not tiger let's go to next status
        else:
            print('si fue spock')
            status_update = True

        return (status_update, update_value_dict)

    @staticmethod
    def transfer_check(rec_series, status_series):
        """
        Check status of globus transfer from local to PNI
        Input:
        rec_series     (pd.Series) = Series with information about the recording process
        status_series  (pd.Series) = Series with information about the next status of the process (if neeeded)
        Returns:
        status_update       (int)     = 1  if recording process status has to be updated to next step in recording.RecordingProcess
                                      = 0  if recording process status not to be changed 
                                      = -1 if recording process status has to be updated to ERROR in recording.RecordingProcess
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """

        status_update = False
        update_value_dict = RecProcessHandler.default_update_value_dict.copy()
        id_task = status_series['FunctionField']

        transfer_request = ft.request_globus_transfer_status(str(id_task))
        if transfer_request['status'] == 'SUCCEEDED':
            status_update = True

        return (status_update, update_value_dict)
    

    @staticmethod
    def slurm_job_queue(rec_series, status_series):
        """
        Request a transfer from local machine to PNI drive
        Input:
        rec_series     (pd.Series) = Series with information about the recording process
        status_series  (pd.Series) = Series with information about the next status of the process (if neeeded)
        Returns:
        status_update       (int)     = 1  if recording process status has to be updated to next step in recording.RecordingProcess
                                      = 0  if recording process status not to be changed 
                                      = -1 if recording process status has to be updated to ERROR in recording.RecordingProcess
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """
        
        status_update = False
        update_value_dict = RecProcessHandler.default_update_value_dict.copy()

        #Create and transfer parameter files
        status = paramfilelib.generate_parameter_file(rec_series)

        #Create and transfer slurm file
        if status == config.system_process['SUCCESS']:
            status, slurm_filepath = slurmlib.generate_slurm_file(rec_series)
        
        #Queue slurm file
        if status == config.system_process['SUCCESS']:
            slurm_queue_status, slurm_jobid = slurmlib.queue_slurm_file(rec_series, slurm_filepath)
            
        if status == config.system_process['SUCCESS']:
            status_update = True
            update_value_dict['value_update'] = slurm_jobid
            #update_status_pipeline(key, status_dict['JOB_QUEUE']['Task_Field'], slurm_jobid, status_dict['JOB_QUEUE']['Value'])

        return (status_update, update_value_dict)

    @staticmethod
    def slurm_job_check(rec_series, status_series):
        """
        Check slurm job in cluster machine
        Input:
        rec_series     (pd.Series) = Series with information about the recording process
        status_series  (pd.Series) = Series with information about the next status of the process (if neeeded)
        Returns:
        status_update       (int)     = 1  if recording process status has to be updated to next step in recording.RecordingProcess
                                        = 0  if recording process status not to be changed 
                                        = -1 if recording process status has to be updated to ERROR in recording.RecordingProcess
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """

        status_update = False
        update_value_dict = RecProcessHandler.default_update_value_dict.copy()
        
        local_user = False
        preprocess_params = rec_series['preprocess_paramset']
        if preprocess_params['process_cluster'] == 'spock' and is_this_spock():
            local_user = True

        ssh_user = ft.cluster_vars[preprocess_params['process_cluster']]['user']

        slurm_jobid_field = status_series['FunctionField']
        slurm_jobid = rec_series[slurm_jobid_field]

        job_status = slurmlib.check_slurm_job(ssh_user, slurm_jobid, local_user=local_user)

        print('job status', job_status)
        print(slurmlib.slurm_states['SUCCESS'])

        print('job status encode uft8 ', job_status.encode('UTF-8'))
        print(slurmlib.slurm_states['SUCCESS'].encode('UTF-8'))

        if job_status == slurmlib.slurm_states['SUCCESS']:
            status_update = True
            print('si fue successss')


        return (status_update, update_value_dict)

    @staticmethod
    def get_active_process_jobs():
        '''
        get all process jobs that have to go through some action in the pipeline
        Return:
            df_process_jobs (pd.DataFrame): all jobs that are going to be processed in the pipeline
        '''

        status_query = 'status_pipeline_idx > ' + str(recording_process_status_df['Value'].min())
        status_query += ' and status_pipeline_idx < ' + str(recording_process_status_df['Value'].max())

        
        jobs_active = (recording.Recording.proj('recording_modality') * recording_process.Processing & status_query)
        df_process_jobs = pd.DataFrame(jobs_active.fetch(as_dict=True))

        if df_process_jobs.shape[0] > 0:
            key_list = dj_short.get_primary_key_fields(jobs_active)
            df_process_jobs['query_key'] = df_process_jobs.loc[:, key_list].to_dict(orient='records')

        return df_process_jobs

    @staticmethod
    def update_status_pipeline(recording_process_key_dict, status, update_field=None, update_value=None):
        """
        Update recording.RecordingProcess table status and optional task field
        Args:
            recording_process_key_dict (dict): key to find recording_process record
            status                     (int):  value of the status to be updated
            update_field               (str):  name of the field to be updated as extra (only applicable to some status)
            update_value             (str|int):  field value to be inserted on in task_field 
        """

        if update_field is not None:
            update_task_id_dict = recording_process_key_dict.copy()
            update_task_id_dict[update_field] = update_value
            recording_process.Status.update1(update_task_id_dict)
        
        update_status_dict = recording_process_key_dict.copy()
        update_status_dict['status_pipeline_idx'] = status
        recording_process.Status.update1(update_status_dict)


    '''
    @staticmethod
    def filter_session_status(df_rec_process, status):
        """
        Filter dataframe with rec_process with a given status
        Args:
            df_rec_process         (pd.DataFrame): recording process dataframe 
            status                 (int):  value of the status to be filtered with
        Returns:
            df_rec_process_status  (pd.DataFrame): recording process dataframe filtered with given status
        """
        
        df_rec_process_status = df_rec_process.loc[df_sessions['status_pipeline_idx'] == status, :]
        df_rec_process_status = df_rec_process_status.reset_index(drop=True)
    '''
