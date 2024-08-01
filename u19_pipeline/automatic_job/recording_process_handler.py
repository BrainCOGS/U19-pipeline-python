
import copy
import pathlib
import time
import traceback
from datetime import datetime

import pandas as pd
from ecephys_spike_sorting.common.SGLXMetaToCoords import MetaToCoords

import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
import u19_pipeline.automatic_job.ephys_element_populate as ep
import u19_pipeline.automatic_job.imaging_element_populate as ip
import u19_pipeline.automatic_job.parameter_file_creator as paramfilelib
import u19_pipeline.automatic_job.params_config as config
import u19_pipeline.automatic_job.slurm_creator as slurmlib
import u19_pipeline.utils.dj_shortcuts as dj_short
import u19_pipeline.utils.slack_utils as slack_utils
from u19_pipeline import (
    ephys_pipeline,
    imaging_pipeline,
    recording,
    recording_process,
    utility,
)
from u19_pipeline.automatic_job import recording_handler
from u19_pipeline.utility import is_this_spock


class RecProcessHandler:

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
            config.recording_process_status_df.loc[config.recording_process_status_df['Value'] == current_status, :].squeeze()
            next_status_series    = config.recording_process_status_df.loc[config.recording_process_status_df['Value'] == current_status+1, :].squeeze()

            # Get processing function    
            function_status_process = getattr(RecProcessHandler, next_status_series['ProcessFunction'])

            #Trigger process, if success update recording process record
            try:
                status, update_dict = function_status_process(rec_process_series, next_status_series) 

                print('update_dict', update_dict)
                #Get dictionary of record process
                key = rec_process_series['query_key']

                print('rec_process_series')
                print(rec_process_series)

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

                    if next_status_series['SlackMessage']:
                        slack_utils.send_slack_update_notification(config.slack_webhooks_dict['automation_pipeline_update_notification'],\
                             next_status_series['SlackMessage'], rec_process_series)

                #if success or error update status timestamps table
                if status != config.status_update_idx['NO_CHANGE']:
                    if status == config.status_update_idx['ERROR_STATUS']:
                        next_status = config.RECORDING_STATUS_ERROR_ID

                        #Crop error messages to fit in DB
                        if len(update_dict['error_info']['error_message']) > 255:
                            print('Cropping error message')
                            update_dict['error_info']['error_message'] = update_dict['error_info']['error_message'][-255:]

                        if isinstance(update_dict['error_info']['error_exception'], str) and len(update_dict['error_info']['error_exception']) > 4095:
                            print('Cropping error error_exception')
                            update_dict['error_info']['error_exception'] = update_dict['error_info']['error_exception'][-4095:]

                    RecProcessHandler.update_job_id_log(rec_process_series['job_id'], current_status, next_status, update_dict['error_info'])

                #An error occurred in process
                if status == config.status_update_idx['ERROR_STATUS']:

                    next_status = config.RECORDING_STATUS_ERROR_ID
                    RecProcessHandler.update_status_pipeline(key,next_status, None, None)

                    #Crop even more error message to fit in slack notification
                    if isinstance(update_dict['error_info']['error_exception'], str) and len(update_dict['error_info']['error_exception']) > 1024:
                        print('Cropping error error_exception for slack')
                        update_dict['error_info']['error_exception'] = update_dict['error_info']['error_exception'][-1023:]

                    # Send slack notification with original error exception, retry if exception has invalid format
                    try:
                        slack_utils.send_slack_error_notification(config.slack_webhooks_dict['automation_pipeline_error_notification'],\
                            update_dict['error_info'] ,rec_process_series)
                    except:
                        update_dict['error_info']['error_exception'] = 'Error exception not accepted in slack, check log file for error'
                        slack_utils.send_slack_error_notification(config.slack_webhooks_dict['automation_pipeline_error_notification'],\
                            update_dict['error_info'] ,rec_process_series)


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

        job_id = rec_series['job_id']
        raw_rel_path = rec_series['recording_process_pre_path']
        proc_rel_path = rec_series['recording_process_post_path']
        modality = rec_series['recording_modality']

        # If tiger, we trigger globus transfer 
        if rec_series['program_selection_params']['process_cluster'] == "tiger":

            if status_series['Key'] == 'RAW_FILE_TRANSFER_REQUEST':

                #status, task_id = scp_tr.call_scp_background(ip_address=rec_series['ip_address'], system_user=rec_series['system_user'],
                #recording_system_directory=rec_series['local_directory'], data_directory=data_directory.as_posix())

                transfer_request = ft.globus_transfer_to_tiger(job_id, raw_rel_path, modality)
            elif status_series['Key'] == 'PROC_FILE_TRANSFER_REQUEST':
                #ALS, which recording directory for processed file
                transfer_request = ft.globus_transfer_to_pni(job_id, proc_rel_path, modality)

            if transfer_request['status'] == config.system_process['SUCCESS']:
                status_update = config.status_update_idx['NEXT_STATUS']
                update_value_dict['value_update'] = transfer_request['task_id']
            
            else:
                status_update = config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = transfer_request['error_info']
                
        # If not tiger let's go to next status
        else:
            status_update = config.status_update_idx['NEXT_STATUS']

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
        id_task = rec_series[status_series['FunctionField']]

        # If tiger, we trigger globus transfer 
        if rec_series['program_selection_params']['process_cluster'] == "tiger":

            transfer_request = ft.request_globus_transfer_status(str(id_task))

            if transfer_request['status'] == config.system_process['COMPLETED']:
                status_update = config.status_update_idx['NEXT_STATUS']
            elif transfer_request['status'] == config.system_process['ERROR']:
                status_update = config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = 'An error occured during globus transfer'

        else:
            status_update = config.status_update_idx['NEXT_STATUS']

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

        print(rec_series['program_selection_params'])


        #Create and transfer parameter file
        status = paramfilelib.generate_parameter_file(rec_series['job_id'], rec_series['params'], 'params', rec_series['program_selection_params'])

        print(status)

        #Create and transfer preparameter file
        if status == config.system_process['SUCCESS']:
            status = paramfilelib.generate_parameter_file(rec_series['job_id'], rec_series['preparams'], 'preparams', rec_series['program_selection_params'])
        else:
            status_update = config.status_update_idx['ERROR_STATUS']
            update_value_dict['error_info']['error_message'] = 'Error while generating/transfering parameter file'
            return (status_update, update_value_dict)

        #If electrophysiology, transfer chanmap as well
        if status == config.system_process['SUCCESS'] and rec_series['recording_modality'] == 'electrophysiology':
            recording_key = (recording_process.Processing.proj('recording_id', insertion_number='fragment_number') & rec_series['query_key']).fetch1()
            del recording_key["job_id"]
            

            chanmap_filename = config.default_chanmap_filename % (rec_series['job_id'])
            chanmap_file_local_path =  pathlib.Path(config.chanmap_files_filepath,chanmap_filename).as_posix()
            rec_series['recording_process_pre_path']

            spikeglx_meta_filepath = ephys_pipeline.get_spikeglx_meta_filepath(recording_key)
            # Chanmap mat file generation
            MetaToCoords(spikeglx_meta_filepath, 1, destFullPath =chanmap_file_local_path)
            status = paramfilelib.generate_chanmap_file(rec_series['job_id'], rec_series['program_selection_params'])
            if status != config.system_process['SUCCESS']:
                status_update = config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = 'Error while generating/transfering channelmap file'
                return (status_update, update_value_dict)

        # Only queue if processing in tiger
        if rec_series['program_selection_params']['local_or_cluster'] == "cluster":

            print('lets transfer slum file ..............xxxxxxxx............')

            #Create and transfer slurm file
            if status == config.system_process['SUCCESS']:
                status, slurm_filepath = slurmlib.generate_slurm_file(rec_series['job_id'], rec_series['program_selection_params'])
            else:
                status_update = config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = 'Error while generating/transfering slurm file'
                return (status_update, update_value_dict)
            
            #Queue slurm file
            if status == config.system_process['SUCCESS']:

                status, slurm_jobid, error_message = slurmlib.queue_slurm_file(rec_series['job_id'], rec_series['program_selection_params'],
                rec_series['recording_process_pre_path'], rec_series['recording_process_post_path'],
                rec_series['recording_modality'], slurm_filepath
                )
            else:
                status_update = config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = 'Error while generating/transfering slurm file'
                return (status_update, update_value_dict)
                        
            if status == config.system_process['SUCCESS']:
                status_update = config.status_update_idx['NEXT_STATUS']
                update_value_dict['value_update'] = slurm_jobid
            else:
                status_update = config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = 'Error while queuing job in SLURM'
                return (status_update, update_value_dict)

        else:
            #Just check we succeded with parameter files
            if status == config.system_process['SUCCESS']:
                status_update = config.status_update_idx['NEXT_STATUS']

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
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        # Only queue if processing in tiger
        if rec_series['program_selection_params']['local_or_cluster'] == "cluster":
            status_update = config.status_update_idx['NO_CHANGE']
            
            local_user = False
            program_selection_params = rec_series['program_selection_params']
            if program_selection_params['process_cluster'] == 'spock' and is_this_spock():
                local_user = True

            ssh_user = ft.cluster_vars[program_selection_params['process_cluster']]['user']
            ssh_host = ft.cluster_vars[program_selection_params['process_cluster']]['hostname']
            slurm_jobid = str(rec_series['slurm_id'])

            status_update, message = slurmlib.check_slurm_job(ssh_user, ssh_host, slurm_jobid, local_user=local_user)

            # Get message from slurm status check
            update_value_dict['error_info']['error_message'] = message

            #If job finished copy over output and/or error log
            if status_update == config.status_update_idx['NEXT_STATUS'] or status_update == config.status_update_idx['ERROR_STATUS']:

                ft.transfer_log_file(rec_series['job_id'], program_selection_params, ssh_host, log_type='ERROR')
                ft.transfer_log_file(rec_series['job_id'], program_selection_params, ssh_host, log_type='OUTPUT')
                error_log = ft.get_error_log_str(rec_series['job_id'])

                # If error log is not empty, get info about it
                if error_log:
                    status_update = config.status_update_idx['ERROR_STATUS']
                    update_value_dict['error_info']['error_message'] = 'An error occured in processing (check LOG)'
                    update_value_dict['error_info']['error_exception'] = error_log
        else:
            status_update = config.status_update_idx['NEXT_STATUS']
                
        return (status_update, update_value_dict)

    @staticmethod
    @recording_handler.exception_handler
    def populate_element(rec_series, status_series):
        """
        Populate corresponding element Database
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

        update_value_dict = copy.deepcopy(config.default_update_value_dict)
        if rec_series['recording_modality'] == 'electrophysiology':
            # Add extra function to create xyz picks file 
            #chanmap_filename = config.default_chanmap_filename % (rec_series['job_id'])
            #chanmap_file_local_path =  pathlib.Path(config.chanmap_files_filepath,chanmap_filename).as_posix()
            #ephys_utils.xyz_pick_file_creator.main_xyz_pick_file_function(rec_series['recording_id'], rec_series['fragment_number'], chanmap_file_local_path, rec_series['recording_process_post_path'])
                        
            status_update = ep.populate_element_data(rec_series['job_id'])

        elif rec_series['recording_modality'] == 'imaging':
            status_update = ip.populate_element_data(rec_series['job_id'])

        return (status_update, update_value_dict)

    @staticmethod
    def get_program_selection_params(modality):
        '''
        get default processing variables (cluster, repository to process, etc) for a given modality
        Return:
            program_selection_params (pd.DataFrame): processing variables default dictionary
        '''
        # Get df from config file
        this_modality_program_selection_params =\
                    config.recording_modality_df.loc[config.recording_modality_df['recording_modality'] == modality, :]

        # Pack all features in a dictionary
        this_modality_program_selection_params_dict = this_modality_program_selection_params.to_dict('records')
       

        # Get two columns, (recording_modality & "packed" program_selection_params)
        this_modality_program_selection_params = this_modality_program_selection_params.loc[:, 'recording_modality'].to_frame().copy()
        this_modality_program_selection_params['program_selection_params'] = this_modality_program_selection_params_dict

        return this_modality_program_selection_params

    @staticmethod
    def get_active_process_jobs(active=True):
        '''
        get all process jobs that have to go through some action in the pipeline
        Return:
            df_process_jobs (pd.DataFrame): all jobs that are going to be processed in the pipeline
        '''

        if active:
            status_query = 'status_processing_id > ' + str(config.JOB_STATUS_ERROR_ID)
            status_query += ' and status_processing_id < ' + str(config.JOB_STATUS_PROCESSED)
        else:
            status_query = 'status_processing_id = ' + str(config.JOB_STATUS_ERROR_ID)
            status_query += ' or status_processing_id = ' + str(config.JOB_STATUS_PROCESSED)

        jobs_active = (recording.Recording.proj('recording_modality', 'location', 'recording_directory') * \
            recording_process.Processing & status_query)
        df_process_jobs = pd.DataFrame(jobs_active.fetch(as_dict=True))

        if df_process_jobs.shape[0] > 0:
            key_list = dj_short.get_primary_key_fields(recording_process.Processing)
            df_process_jobs['query_key'] = df_process_jobs.loc[:, key_list].to_dict(orient='records')

            # Get parameters for all modalities
            all_modalities = df_process_jobs['recording_modality'].unique()

            all_mods_df = list()
            # Get process df for each modality
            for this_modality in all_modalities:

                this_mod_df = df_process_jobs.loc[df_process_jobs['recording_modality'] == this_modality,:].copy()
                these_process_keys = this_mod_df['query_key'].to_list()

                this_mod_program_selection_params = RecProcessHandler.get_program_selection_params(this_modality)

                if this_modality == 'electrophysiology':
                    params_df = RecProcessHandler.get_ephys_params_jobs(these_process_keys)

                if this_modality == 'imaging':
                    params_df = RecProcessHandler.get_imaging_params_jobs(these_process_keys)
                
                this_mod_df = this_mod_df.merge(params_df, on='job_id', how='left')
                this_mod_df = this_mod_df.merge(this_mod_program_selection_params, on='recording_modality', how='left')

                all_mods_df.append(this_mod_df)

            #Concatenate all process_jobs for each modality
            df_process_jobs = all_mods_df[0].copy()

            for this_mod_df in all_mods_df[1:]:
                df_process_jobs = pd.concat([df_process_jobs, this_mod_df], ignore_index=True)

            df_process_jobs = df_process_jobs.reset_index(drop=True)
        
        print(df_process_jobs)

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
        params_df = pd.DataFrame((ephys_pipeline.ephys_element.ClusteringParamSet.proj('params', 'clustering_method') * \
        recording_process.Processing.EphysParams.proj('paramset_idx') & rec_process_keys).fetch(as_dict=True))
        params_df = params_df.drop('paramset_idx', axis=1)

        #Insert clustering method in params itself (for BrainCogsEphysSorters)
        params_df['params'] = params_df.apply(lambda x: {**x['params'], **{'clustering_method':x['clustering_method']}},axis=1)

        # Get precluster param sets
        preparams_df = pd.DataFrame((ephys_pipeline.ephys_element.PreClusterParamSteps * \
        utility.smart_dj_join(ephys_pipeline.ephys_element.PreClusterParamSteps.Step, ephys_pipeline.ephys_element.PreClusterParamSet.proj('precluster_method', 'params')) *
        recording_process.Processing.EphysParams.proj('precluster_param_steps_id') & rec_process_keys).fetch(as_dict=True))
        
        # Join precluster params for the same recording_process
        preparams_df['preparams'] = preparams_df.apply(lambda x : {x['precluster_method']: x['params']}, axis=1)
        preparams_df = preparams_df.sort_values(by=['job_id', 'step_number'])
        preparams_df = preparams_df[['job_id', 'preparams']].groupby("job_id").agg(lambda x: list(x))
        preparams_df = preparams_df.reset_index()

        params_df = params_df.merge(preparams_df)

        return params_df

    @staticmethod
    def get_imaging_params_jobs(rec_process_keys):
        '''
        get all parameters (precluster & cluster) for each of the recording process
        Join precluster param steps into a list
        Args:
            rec_process_keys (dict): key to find recording_process records
        Return:
            params_df (pd.DataFrame): recording_process & params df
        '''

        params_df = pd.DataFrame((imaging_pipeline.imaging_element.ProcessingParamSet.proj('params', 'processing_method') * \
        recording_process.Processing.ImagingParams.proj('paramset_idx') & rec_process_keys).fetch(as_dict=True))
        params_df = params_df.drop('paramset_idx', axis=1)

        #Insert processing_method in params itself (for BrainCogsImagingSorters)
        params_df['params'] = params_df.apply(lambda x: {**x['params'], **{'processing_method':x['processing_method']}},axis=1)

        # Get preprocess param sets
        preparams_df = pd.DataFrame((imaging_pipeline.imaging_element.PreprocessParamSteps * \
        utility.smart_dj_join(imaging_pipeline.imaging_element.PreprocessParamSteps.Step, imaging_pipeline.imaging_element.PreprocessParamSet.proj('preprocess_method', 'params')) *
        recording_process.Processing.ImagingParams.proj('preprocess_param_steps_id') & rec_process_keys).fetch(as_dict=True))

        #If there is no preprocess steps for this jobs fill with empty values
        if preparams_df.shape[0] > 0:

            # Join precluster params for the same recording_process
            preparams_df['preparams'] = preparams_df.apply(lambda x : {x['preprocess_method']: x['params']}, axis=1)
            preparams_df = preparams_df.sort_values(by=['job_id', 'step_number'])
            preparams_df = preparams_df[['job_id', 'preparams']].groupby("job_id").agg(lambda x: list(x))
            preparams_df = preparams_df.reset_index()

        else:
            preparams_df = pd.DataFrame((imaging_pipeline.imaging_element.PreprocessParamSteps * \
                    recording_process.Processing.ImagingParams.proj('preprocess_param_steps_id') & rec_process_keys).fetch(as_dict=True))
            preparams_df['preparams'] = None

        preparams_df = preparams_df[['job_id', 'preparams']]
        params_df = params_df.merge(preparams_df)

        return params_df


    @staticmethod
    def update_status_pipeline(recording_process_key_dict, status, update_field=None, update_value=None):
        """
        Update recording_process.Processing table status and optional task field
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
        Update recording_process.LogStatus table status and optional task field
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
        
        if error_info_dict['error_message'] is not None and len(error_info_dict['error_message']) >= 256:
            error_info_dict['error_message'] =error_info_dict['error_message'][:255]

        if error_info_dict['error_exception'] is not None and len(error_info_dict['error_exception']) >= 4096:
            error_info_dict['error_exception'] =error_info_dict['error_exception'][:4095]

        key['error_exception'] = error_info_dict['error_exception']
        key['error_message'] = error_info_dict['error_message']

        recording_process.LogStatus.insert1(key)


    @staticmethod
    @recording_handler.exception_handler
    def check_job_process_deletion():
        """
        Delete files in cluster and update status correspondingly for each processed or error+7days jobs
        """
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        # Get jobs with error status or finished
        df_inactive_jobs = RecProcessHandler.get_active_process_jobs(active=False)

        #If no inactive job, the end
        if df_inactive_jobs.shape[0] == 0:
            return

        jobs_ids = df_inactive_jobs['job_id'].to_frame().to_dict('records')

        #Get latest time they changed status
        now = datetime.now()
        timestamp_jobs = pd.DataFrame((recording_process.Processing & jobs_ids).aggr(recording_process.LogStatus, max_date="max(status_timestamp)").fetch(as_dict=True))
        df_inactive_jobs = df_inactive_jobs.merge(timestamp_jobs, on='job_id')

        # Get jobs that were processed in cluster
        df_inactive_jobs['local_or_cluster'] = df_inactive_jobs['program_selection_params'].map(lambda x: x['local_or_cluster'])
        df_inactive_jobs['process_cluster'] = df_inactive_jobs['program_selection_params'].map(lambda x: x['process_cluster'])
        
        #Update all local jobs
        df_not_cluster = df_inactive_jobs.loc[df_inactive_jobs['local_or_cluster'] == 'local'].reset_index(drop=True)
        if df_not_cluster.shape[0] > 0:
            # Update local jobs post-processed jobs
            df_not_cluster_post_processed = df_not_cluster.loc[df_not_cluster['status_processing_id'] == config.JOB_STATUS_PROCESSED, :]
            df_not_cluster_post_processed = df_not_cluster_post_processed.reset_index(drop=True)
            for i in range(df_not_cluster_post_processed.shape[0]):
                RecProcessHandler.update_status_pipeline(df_not_cluster_post_processed.loc[i, 'query_key'], config.JOB_STATUS_POST_PROCESSED)
                RecProcessHandler.update_job_id_log(df_not_cluster_post_processed.loc[i, 'job_id'], config.JOB_STATUS_PROCESSED, config.JOB_STATUS_POST_PROCESSED, update_value_dict['error_info'])

            # Update also local jobs post-error jobs
            df_not_cluster_post_error = df_not_cluster.loc[df_not_cluster['status_processing_id'] == config.JOB_STATUS_ERROR_ID, :]
            df_not_cluster_post_error = df_not_cluster_post_error.reset_index(drop=True)
            for i in range(df_not_cluster_post_error.shape[0]):
                RecProcessHandler.update_status_pipeline(df_not_cluster_post_error.loc[i, 'query_key'], config.JOB_STATUS_ERROR_DELETED)
                RecProcessHandler.update_job_id_log(df_not_cluster_post_error.loc[i, 'job_id'], config.JOB_STATUS_ERROR_ID, config.JOB_STATUS_ERROR_DELETED, update_value_dict['error_info'])
        
        #If no inactive job, after locals the end
        df_inactive_jobs = df_inactive_jobs.loc[df_inactive_jobs['local_or_cluster'] == 'cluster'].reset_index(drop=True)
        if df_inactive_jobs.shape[0] == 0:
            return

        # Get jobs to delete (finished or with error >= 7 days)
        df_inactive_jobs['days_from_last_update'] = (now - df_inactive_jobs['max_date']).dt.days
        df_inactive_jobs['ready_delete'] = 1
        df_inactive_jobs.loc[(df_inactive_jobs['status_processing_id'] == config.JOB_STATUS_ERROR_ID) & (df_inactive_jobs['days_from_last_update'] < 7), 'ready_delete'] = 0
        df_inactive_jobs = df_inactive_jobs.loc[df_inactive_jobs['ready_delete'] == 1,:]

        #If no inactive job, after ready delete data return now
        if df_inactive_jobs.shape[0] == 0:
            return

        # Check if raw directories are present in cluster
        df_inactive_jobs['raw_dir']  = df_inactive_jobs.apply(lambda x: ft.check_directory_exists_cluster(x['recording_process_pre_path'],\
            x['process_cluster'], x['recording_modality'], type_dir='raw'), axis=1)
        df_inactive_jobs['raw_dir'] = df_inactive_jobs['raw_dir'].astype(str)
        df_inactive_jobs['raw_dir_deleted'] = 0
        df_inactive_jobs.loc[df_inactive_jobs['raw_dir'] == "0", 'raw_dir_deleted'] = 1

        # Check if processed directories are present in cluster
        df_inactive_jobs['processed_dir'] = df_inactive_jobs.apply(lambda x: ft.check_directory_exists_cluster(x['recording_process_post_path'],\
            x['process_cluster'], x['recording_modality'], type_dir='processed'), axis=1)
        df_inactive_jobs['processed_dir'] = df_inactive_jobs['processed_dir'].astype(str)
        df_inactive_jobs['processed_dir_deleted'] = 0
        df_inactive_jobs.loc[df_inactive_jobs['processed_dir'] == "0", 'processed_dir_deleted'] = 1
        df_inactive_jobs = df_inactive_jobs.reset_index(drop=True)

        #Delete raw directories tiger
        for i in range(df_inactive_jobs.shape[0]):
            modality = df_inactive_jobs.loc[i, 'recording_modality']
            dir_delete = df_inactive_jobs.loc[i, 'recording_process_pre_path']
            to_delete = df_inactive_jobs.loc[i, 'raw_dir']
            if str(to_delete) != "0":
                status = ft.delete_directory_tiger_globus(modality, dir_delete)
                if status == config.system_process['SUCCESS']:
                    df_inactive_jobs.loc[i, 'raw_dir_deleted'] = 1

        #Delete processed directories tiger
        for i in range(df_inactive_jobs.shape[0]):
            cluster = df_inactive_jobs.loc[i, 'process_cluster']
            dir_delete = df_inactive_jobs.loc[i, 'processed_dir']
            if str(dir_delete) != "0":
                status = ft.delete_directory_cluster(dir_delete, cluster)
                if status == config.system_process['SUCCESS']:
                    df_inactive_jobs.loc[i, 'processed_dir_deleted'] = 1

        #Delete empty directories in clusters
        clusters = df_inactive_jobs['process_cluster'].unique().tolist()
        for this_cluster in clusters:
            ft.delete_empty_data_directory_cluster(this_cluster, type="raw")
            ft.delete_empty_data_directory_cluster(this_cluster, type="processed")
        

        # Update status for post-processed jobs
        df_post_processed = df_inactive_jobs.loc[(df_inactive_jobs['raw_dir_deleted'] == 1) & (df_inactive_jobs['processed_dir_deleted'] == 1) & (df_inactive_jobs['status_processing_id'] == config.JOB_STATUS_PROCESSED)]
        df_post_processed = df_post_processed.reset_index(drop=True)
        for i in range(df_post_processed.shape[0]):
            RecProcessHandler.update_status_pipeline(df_post_processed.loc[i, 'query_key'], config.JOB_STATUS_POST_PROCESSED)
            RecProcessHandler.update_job_id_log(df_post_processed.loc[i, 'job_id'], config.JOB_STATUS_PROCESSED, config.JOB_STATUS_POST_PROCESSED, update_value_dict['error_info'])

        # Update status for post-error jobs
        df_post_error = df_inactive_jobs.loc[(df_inactive_jobs['raw_dir_deleted'] == 1) & (df_inactive_jobs['processed_dir_deleted'] == 1) & (df_inactive_jobs['status_processing_id'] == config.JOB_STATUS_ERROR_ID)]
        df_post_error = df_post_error.reset_index(drop=True)
        for i in range(df_post_error.shape[0]):
            RecProcessHandler.update_status_pipeline(df_post_error.loc[i, 'query_key'], config.JOB_STATUS_ERROR_DELETED)
            RecProcessHandler.update_job_id_log(df_post_error.loc[i, 'job_id'], config.JOB_STATUS_ERROR_ID, config.JOB_STATUS_ERROR_DELETED, update_value_dict['error_info'])
        
        return

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

