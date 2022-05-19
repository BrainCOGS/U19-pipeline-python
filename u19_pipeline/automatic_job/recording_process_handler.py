
import time
import traceback
import pandas as pd
import datajoint as dj
import copy

from u19_pipeline.automatic_job import recording_handler

import u19_pipeline.utils.dj_shortcuts as dj_short
import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
import u19_pipeline.automatic_job.slurm_creator as slurmlib
import u19_pipeline.automatic_job.parameter_file_creator as paramfilelib
import u19_pipeline.automatic_job.params_config as config

from datetime import datetime
from u19_pipeline import recording, recording_process, ephys_pipeline, imaging_pipeline, utility
from u19_pipeline.utility import create_str_from_dict, is_this_spock

class RecProcessHandler():

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

            #Filter current status info
            current_status = rec_process_series['status_processing_id']
            current_status_series = config.recording_process_status_df.loc[config.recording_process_status_df['Value'] == current_status, :].squeeze()
            next_status_series    = config.recording_process_status_df.loc[config.recording_process_status_df['Value'] == current_status+1, :].squeeze()

            # Get processing function    
            function_status_process = getattr(RecProcessHandler, next_status_series['ProcessFunction'])

            #Trigger process, if success update recording process record
            try:
                status, update_dict = function_status_process(rec_process_series, next_status_series) 

                print('update_dict', update_dict)
                #Get dictionary of record process
                key = rec_process_series['query_key']

                if status == config.status_update_idx['NEXT_STATUS']:
                    
                    
                    #Get values to update
                    next_status = next_status_series['Value']
                    value_update = update_dict['value_update']
                    field_update = next_status_series['UpdateField']

                    print('key to update', key)
                    print('old status', current_status, 'new status', next_status)
                    print('value_update', value_update, 'field_update', field_update)
                    print('function executed:', next_status_series['ProcessFunction'])

                    RecProcessHandler.update_status_pipeline(key, next_status, field_update, value_update)

                
                #An error occurred in process
                if status == config.status_update_idx['ERROR_STATUS']:
                    next_status = config.RECORDING_STATUS_ERROR_ID
                    RecProcessHandler.update_status_pipeline(key,next_status, None, None)

                #if success or error update status timestamps table
                if status != config.status_update_idx['NO_CHANGE']:
                    RecProcessHandler.update_job_id_log(rec_process_series['job_id'], current_status, next_status, update_dict['error_info'])


            except Exception as err:
                raise(err)
                print(traceback.format_exc())
                ## Send notification error, update recording to error

            time.sleep(2)


    @staticmethod
    @recording_handler.exception_handler
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

        status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        directory_path = status_series['FunctionField']
        job_id = rec_series['job_id']
        raw_rel_path = rec_series['recording_process_pre_path']
        proc_rel_path = rec_series['recording_process_post_path']
        modality = rec_series['recording_modality']

        print('directory_path', directory_path)

        print('process_cluter:', rec_series['program_selection_params']['process_cluster'])

        # If tiger, we trigger globus transfer 
        if rec_series['program_selection_params']['process_cluster'] == "tiger":
            print('si fue tiger')
            if status_series['Key'] is 'RAW_FILE_TRANSFER_REQUEST':
                transfer_request = ft.globus_transfer_to_tiger(job_id, raw_rel_path, modality)
            elif status_series['Key'] is 'PROC_FILE_TRANSFER_REQUEST':
                #ALS, which recording directory for processed file
                transfer_request = ft.globus_transfer_to_pni(job_id, proc_rel_path, modality)

            if transfer_request['code'] == config.system_process['SUCCESS']:
                status_update = True
                update_value_dict['value_update'] = transfer_request['task_id']
        # If not tiger let's go to next status
        else:
            print('si fue spock')
            status_update = True

        return (status_update, update_value_dict)

    @staticmethod
    @recording_handler.exception_handler
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

        status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)
        id_task = status_series['FunctionField']

        transfer_request = ft.request_globus_transfer_status(str(id_task))
        if transfer_request['status'] == 'SUCCEEDED':
            status_update = True

        return (status_update, update_value_dict)
    

    @staticmethod
    @recording_handler.exception_handler
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
        
        status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        #Create and transfer parameter file
        status = paramfilelib.generate_parameter_file(rec_series['job_id'], rec_series['params'], 'params', rec_series['program_selection_params'])

        #Create and transfer preparameter file
        if status == config.system_process['SUCCESS']:
            status = paramfilelib.generate_parameter_file(rec_series['job_id'], rec_series['preparams'], 'preparams', rec_series['program_selection_params'])

        #Create and transfer slurm file
        if status == config.system_process['SUCCESS']:
            status, slurm_filepath = slurmlib.generate_slurm_file(rec_series)
        
        #Queue slurm file
        if status == config.system_process['SUCCESS']:

            status, slurm_jobid = slurmlib.queue_slurm_file(rec_series['job_id'], rec_series['program_selection_params'],
            rec_series['recording_process_pre_path'], rec_series['recording_process_post_path'],
            rec_series['recording_modality'], slurm_filepath
            )
            
        if status == config.system_process['SUCCESS']:
            status_update = True
            update_value_dict['value_update'] = slurm_jobid
            #update_status_pipeline(key, status_dict['JOB_QUEUE']['Task_Field'], slurm_jobid, status_dict['JOB_QUEUE']['Value'])

        return (status_update, update_value_dict)

    @staticmethod
    @recording_handler.exception_handler
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

        status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)
        
        local_user = False
        program_selection_params = rec_series['program_selection_params']
        if program_selection_params['process_cluster'] == 'spock' and is_this_spock():
            local_user = True

        ssh_user = ft.cluster_vars[program_selection_params['process_cluster']]['user']

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

        status_query = 'status_processing_id > ' + str(config.recording_process_status_df['Value'].min())
        status_query += ' and status_processing_id < ' + str(config.recording_process_status_df['Value'].max())

        jobs_active = (recording.Recording.proj('recording_modality') * \
            recording_process.Processing & status_query)
        df_process_jobs = pd.DataFrame(jobs_active.fetch(as_dict=True))

        if df_process_jobs.shape[0] > 0:
            key_list = dj_short.get_primary_key_fields(recording_process.Processing)
            df_process_jobs['query_key'] = df_process_jobs.loc[:, key_list].to_dict(orient='records')

        # Get parameters for all modalities
        all_modalities = df_process_jobs['recording_modality'].unique()
        for this_modality in all_modalities:

            this_mod_df = df_process_jobs.loc[df_process_jobs['recording_modality'] == this_modality,:]
            these_process_keys = this_mod_df['query_key'].to_list()

            if this_modality == 'electrophysiology':
                params_df = RecProcessHandler.get_ephys_params_jobs(these_process_keys)
                df_process_jobs = df_process_jobs.merge(params_df, how='left')

        df_process_jobs['program_selection_params'] = [config.program_selection_params for _ in range(df_process_jobs.shape[0])]

        return df_process_jobs

    @staticmethod
    def get_ephys_params_jobs(rec_process_keys):
        '''
        get all parameters (precluster & cluster) for each of the recording process
        Join precluster param list into a list
        Args:
            rec_process_keys (dict): key to find recording_process records
        Return:
            params_df (pd.DataFrame): recording_process & params df
        '''

        # Get cluster param sets
        params_df = pd.DataFrame((ephys_pipeline.ephys_element.ClusteringParamSet.proj('params') * \
        recording_process.Processing.EphysParams.proj('paramset_idx') \
        & rec_process_keys).fetch(as_dict=True))
        params_df = params_df.drop('paramset_idx', axis=1)

        # Get precluster param sets
        preparams_df = pd.DataFrame((ephys_pipeline.ephys_element.PreClusterParamList * \
        utility.smart_dj_join(ephys_pipeline.ephys_element.PreClusterParamList.ParamOrder, ephys_pipeline.ephys_element.PreClusterParamSet.proj('precluster_method', 'params')) *
        recording_process.Processing.EphysParams.proj('precluster_param_list_id') \
        & rec_process_keys).fetch(as_dict=True))
        
        # Join precluster params for the same recording_process
        preparams_df['preparams'] = preparams_df.apply(lambda x : {x['precluster_method']: x['params']}, axis=1)
        preparams_df = preparams_df.sort_values(by=['job_id', 'order_id'])
        preparams_df = preparams_df[['job_id', 'preparams']].groupby("job_id").agg(lambda x: list(x))
        preparams_df = preparams_df.reset_index()

        params_df = params_df.merge(preparams_df)

        return params_df

    @staticmethod
    def get_imaging_params_jobs(rec_process_keys):
        '''
        get all parameters (precluster & cluster) for each of the recording process
        Join precluster param list into a list
        Args:
            rec_process_keys (dict): key to find recording_process records
        Return:
            params_df (pd.DataFrame): recording_process & params df
        '''

        # Get process param sets
        params_df = pd.DataFrame((imaging_pipeline.imaging_element.ProcessingParamSet.proj('params') * \
        recording_process.Processing.ImagingParams.proj('paramset_idx') \
        & rec_process_keys).fetch(as_dict=True))
        params_df = params_df.drop('paramset_idx', axis=1)

        # Get preprocess param sets
        preparams_df = pd.DataFrame((imaging_pipeline.imaging_element.PreClusterParamList * \
        utility.smart_dj_join(imaging_pipeline.imaging_element.PreProcessParamList.ParamOrder, imaging_pipeline.imaging_element.PreClusterParamSet.proj('preprocess_method', 'params')) *
        recording_process.Processing.ImagingParams.proj('precluster_param_list_id') \
        & rec_process_keys).fetch(as_dict=True))
        
        # Join precluster params for the same recording_process
        preparams_df['preparams'] = preparams_df.apply(lambda x : {x['precluster_method']: x['params']}, axis=1)
        preparams_df = preparams_df.sort_values(by=['job_id', 'order_id'])
        preparams_df = preparams_df[['job_id', 'preparams']].groupby("job_id").agg(lambda x: list(x))
        preparams_df = preparams_df.reset_index()

        params_df = params_df.merge(preparams_df)

        return params_df


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
            recording_process.Processing.update1(update_task_id_dict)
        
        update_status_dict = recording_process_key_dict.copy()
        update_status_dict['status_processing_id'] = status
        recording_process.Processing.update1(update_status_dict)

    
    @staticmethod
    def update_job_id_log(job_id, current_status, next_status, error_info_dict):
        """
        Update recording.RecordingLog table status and optional task field
        Args:

        """

        now = datetime.now()
        date_time = now.strftime("%Y-%m-%d %H:%M:%S")

        print('error_info_dict', error_info_dict)

        key = dict()
        key['job_id'] = job_id
        key['status_processing_id_old'] = current_status
        key['status_processing_id_new'] = next_status
        key['status_timestamp'] = date_time
        key['error_message'] = error_info_dict['error_message']
        key['error_exception'] = error_info_dict['error_exception']

        recording_process.Log.insert1(key)


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
        
        df_rec_process_status = df_rec_process.loc[df_sessions['status_processing_id'] == status, :]
        df_rec_process_status = df_rec_process_status.reset_index(drop=True)
    '''

