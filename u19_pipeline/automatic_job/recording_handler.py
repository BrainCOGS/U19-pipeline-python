
import pathlib
import time
import traceback
import pandas as pd
import datajoint as dj
import copy

from datetime import datetime
from u19_pipeline import recording, ephys_pipeline, imaging_pipeline, recording, recording_process, lab

import u19_pipeline.utils.dj_shortcuts as dj_short
import u19_pipeline.utils.slack_utils as slack_utils
import u19_pipeline.utils.scp_transfers as scp_tr

from u19_pipeline.automatic_job import ephys_element_ingest
import u19_pipeline.automatic_job.params_config as config



def exception_handler(func):
    '''
    Decorator function to get error message when a workflow manager function fails
    '''
    def inner_function(*args, **kwargs):
        try:
             argout = func(*args, **kwargs)
             return argout
        except Exception as e:
            print('Exception HERE ................')
            update_value_dict = copy.deepcopy(config.default_update_value_dict)
            update_value_dict['error_info']['error_message'] = str(e)
            update_value_dict['error_info']['error_exception'] = (''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)))
            return (config.RECORDING_STATUS_ERROR_ID, update_value_dict)
    return inner_function

class RecordingHandler():

    @staticmethod
    def pipeline_handler_main():
        '''
        Call all processing functions according to the current status of each recording
        Update status of each process job accordingly
        '''

        #Get info from all the possible status for a processjob 
        df_all_recordings = RecordingHandler.get_active_recordings()

        #For all active process jobs
        for i in range(df_all_recordings.shape[0]):

            #Filter current process job
            recording_series = df_all_recordings.loc[i, :]

            #Filter current status info
            current_status = recording_series['status_recording_id']
            next_status_series    = config.recording_status_df.loc[config.recording_status_df['Value'] == current_status+1, :].squeeze()

            print('function to apply:', next_status_series['ProcessFunction'])

            # Get processing function    
            function_status_process = getattr(RecordingHandler, next_status_series['ProcessFunction'])

            #Trigger process, if success update recording process record
            try:
                status, update_dict = function_status_process(recording_series) 

                #Get dictionary of record process
                key = recording_series['query_key']

                if status == config.status_update_idx['NEXT_STATUS']:
                    #Get values to update
                    next_status = next_status_series['Value']
                    value_update = update_dict['value_update']
                    field_update = next_status_series['UpdateField']

                    # Update status in u19_recording.recording table (possibly other field as well)
                    RecordingHandler.update_status_pipeline(key, next_status, field_update, value_update)

                    # Send slack Message to webhook if slack message activated for status
                    if next_status_series['SlackMessage']:
                        slack_utils.send_slack_update_notification(config.slack_webhooks_dict['automation_pipeline_update_notification'],\
                             next_status_series['SlackMessage'], recording_series)
                
                #An error occurred in process
                if status == config.status_update_idx['ERROR_STATUS']:
                    next_status = config.RECORDING_STATUS_ERROR_ID
                    RecordingHandler.update_status_pipeline(key,next_status, None, None)
                    slack_utils.send_slack_error_notification(config.slack_webhooks_dict['automation_pipeline_error_notification'],\
                        update_dict['error_info'] ,recording_series)

                #if success or error update status timestamps table
                if status != config.status_update_idx['NO_CHANGE']:
                    RecordingHandler.update_recording_log(recording_series['recording_id'], current_status, next_status, update_dict['error_info'])


            except Exception as err:
                raise(err)
                ## Send notification error, update recording to error

            time.sleep(2)

    
    @staticmethod
    @exception_handler
    def local_transfer_request(rec_series):
        """
        Request a transfer from PNI to Tiger Cluster
        Input:
        rec_series     (pd.Series)  = Series with information about the recording
        status_series  (pd.Series)  = Series with information about the next status of the recording (if neeeded)
        Returns:
        status_update       (int)     = 1  if recording status has to be updated to next step in recording.Recording
                                      = 0  if recording status not to be changed 
                                      = -1 if recording status has to be updated to ERROR in recording.Recording
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """

        status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        data_directory = pathlib.Path(dj.config['custom']['root_data_dir'], rec_series['recording_modality'], rec_series['recording_directory']).as_posix()
        data_directory = pathlib.Path(data_directory).parent
        
        pathlib.Path(data_directory).mkdir(parents=True, exist_ok=True)

        

        #Encode Windows like directory for scp to work

        status, task_id = scp_tr.call_scp_background(ip_address=rec_series['ip_address'], system_user=rec_series['system_user'],
        recording_system_directory=rec_series['local_directory'], data_directory=data_directory.as_posix())

        if status:
            status_update = config.status_update_idx['NEXT_STATUS']
            update_value_dict['value_update'] = task_id

        return (status_update, update_value_dict)

    @staticmethod
    @exception_handler
    def local_transfer_check(rec_series):
        """
        Check status of transfer from local to PNI
        Input:
        rec_series     (pd.Series)  = Series with information about the recording
        status_series  (pd.Series)  = Series with information about the next status of the recording (if neeeded)
        Returns:
        status_update       (int)     = 1  if recording status has to be updated to next step in recording.Recording
                                      = 0  if recording status not to be changed 
                                      = -1 if recording status has to be updated to ERROR in recording.Recording
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """

        status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        id_task = rec_series['task_copy_id_pni']

        is_finished, exit_code = scp_tr.check_scp_transfer(id_task)

        if is_finished:
            if exit_code == 0:
                status_update = config.status_update_idx['NEXT_STATUS']
            else:
                status_update = config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = 'Return code scp not = 0'

        print('is_finished', is_finished, 'status_update', status_update)

        return (status_update, update_value_dict)

    @staticmethod
    @exception_handler
    def modality_preingestion(in_rec_series):
        """
        Ingest "first" tables of modality specific recordings
        Input:
        rec_series     (pd.Series)  = Series with information about the recording
        Returns:
        status_update       (int)     = 1  if recording status has to be updated to next step in recording.Recording
                                      = 0  if recording status not to be changed 
                                      = -1 if recording status has to be updated to ERROR in recording.Recording
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """
        
        rec_series = in_rec_series.copy()

        if rec_series['recording_modality'] == 'electrophysiology':
            status_update, update_value_dict = RecordingHandler.electrophysiology_preingestion(rec_series)

        if rec_series['recording_modality'] == 'imaging':
            status_update, update_value_dict = RecordingHandler.imaging_preingestion(rec_series)
            
              
        return (status_update, update_value_dict)


    @staticmethod
    def get_active_recordings():
        '''
        get all recordings that have to go through some action in the pipeline
        Return:
            df_recordings (pd.DataFrame): all recordings that are going to be processed in the pipeline
        '''

        status_query = 'status_recording_id > ' + str(config.recording_status_df['Value'].min())
        status_query += ' and status_recording_id < ' + str(config.recording_status_df['Value'].max())

        print(status_query)

        recordings_active = recording.Recording * lab.Location.proj('ip_address', 'system_user', 'acquisition_type') & status_query

        print(recordings_active)

        df_recordings = pd.DataFrame(recordings_active.fetch(as_dict=True))

        if df_recordings.shape[0] > 0:
            key_list = dj_short.get_primary_key_fields(recording.Recording)
            df_recordings['query_key'] = df_recordings.loc[:, key_list].to_dict(orient='records')

        return df_recordings

    
    @staticmethod
    def update_status_pipeline(recording_key_dict, status, update_field=None, update_value=None):
        """
        Update recording.Recording table status and optional task field
        Args:
            recording_key_dict        (dict):    key to find recording record
            status                     (int):    value of the status to be updated
            update_field               (str):    name of the field to be updated as extra (only applicable to some status)
            update_value             (str|int):  field value to be inserted on in task_field 
        """

        print('recording_key_dict', recording_key_dict)
        print('status', status)
        print('update_field', update_field)
        print('update_value', update_value)

        if update_field is not None:
            update_task_id_dict = recording_key_dict.copy()
            update_task_id_dict[update_field] = update_value
            print('update_task_id_dict', update_task_id_dict)
            recording.Recording.update1(update_task_id_dict)
        
        update_status_dict = recording_key_dict.copy()
        update_status_dict['status_recording_id'] = status
        print('update_status_dict', update_status_dict)
        recording.Recording.update1(update_status_dict)

    @staticmethod
    def update_recording_log(recording_id, current_status, next_status, error_info_dict):
        """
        Update recording.RecordingLog table status and optional task field
        Args:

        """

        now = datetime.now()
        date_time = now.strftime("%Y-%m-%d %H:%M:%S")

        print('error_info_dict', error_info_dict)

        key = dict()
        key['recording_id'] = recording_id
        key['status_recording_id_old'] = current_status
        key['status_recording_id_new'] = next_status
        key['recording_status_timestamp'] = date_time

        if error_info_dict['error_message'] is not None and len(error_info_dict['error_message']) >= 256:
            error_info_dict['error_message'] =error_info_dict['error_message'][:255]

        if error_info_dict['error_exception'] is not None and len(error_info_dict['error_exception']) >= 4096:
            error_info_dict['error_exception'] =error_info_dict['error_exception'][:4095]

        key['recording_error_message'] = error_info_dict['error_message']
        key['recording_error_exception'] = error_info_dict['error_exception']

        recording.LogStatus.insert1(key)


    @staticmethod
    def electrophysiology_preingestion(rec_series):
        """
        Ingest "first" tables of ephys_pipelne, ephys_ement tables and recording_process.Processing
        Input:
        rec_series     (pd.Series)  = Series with information about the recording
        Returns:
        status_update       (int)     = 1  if recording status has to be updated to next step in recording.Recording
                                      = 0  if recording status not to be changed 
                                      = -1 if recording status has to be updated to ERROR in recording.Recording
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """

        status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        #Insert first ephysSession and firs tables of ephys elements
        ephys_pipeline.EphysPipelineSession.populate(rec_series['query_key'])
        ephys_element_ingest.process_session(rec_series['query_key'])
        ephys_pipeline.ephys_element.EphysRecording.populate(rec_series['query_key'])

        ingested_recording = (ephys_pipeline.ephys_element.EphysRecording & rec_series['query_key']).fetch("KEY", as_dict=True)

        print('before BehaviorSync')
        print("rec_series['query_key']", rec_series['query_key'])

        ephys_pipeline.BehaviorSync.populate(rec_series['query_key'])

        if len(ingested_recording) == 0:
            status_update = config.status_update_idx['ERROR_STATUS']
            update_value_dict['error_info']['error_message'] = 'Ephys Recording was not ingested (probably recording not in location)'
            return (status_update, update_value_dict)

        #Insert recording processes records
        old_recording_process = (recording_process.Processing() & rec_series['query_key']).fetch("KEY", as_dict=True)
        if len(old_recording_process) == 0:

            connection = recording.Recording.connection 
            with connection.transaction:
                        
                probe_files = (ephys_pipeline.ephys_element.EphysRecording.EphysFile & rec_series['query_key']).fetch(as_dict=True)
                probe_files = [dict(item, recording_process_pre_path=pathlib.Path(item['file_path']).parents[0].as_posix()) for item in probe_files]

                recording_process.Processing().insert_recording_process(probe_files, 'insertion_number')

                #Get parameters for recording processes
                recording_processes = (recording_process.Processing() & rec_series['query_key']).fetch('job_id', 'recording_id', 'fragment_number', 'recording_process_pre_path', as_dict=True)
                default_params_record_df = pd.DataFrame((recording.DefaultParams & rec_series['query_key']).fetch(as_dict=True))
                params_rec_process = recording.DefaultParams.get_default_params_rec_process(recording_processes, default_params_record_df)

                # Rename preprocess_param_steps_id with the electrophysiology one
                for i in params_rec_process:
                    i['precluster_param_steps_id'] = i.pop('preprocess_param_steps_id')

                recording_process.Processing.EphysParams.insert(params_rec_process)

                #Update recording_process_post_path
                recording_process.Processing().set_recording_process_post_path(recording_processes)

                #Create lfp trace if needed (neuropixel 2.0 probes)
                recording_directory = (recording.Recording & rec_series['query_key']).fetch1('recording_directory')
                recording_directory = pathlib.Path(dj.config['custom']['ephys_root_data_dir'][0], recording_directory).parent.as_posix()
                for i in recording_processes:
                    probe_dir = pathlib.Path(dj.config['custom']['ephys_root_data_dir'][0], i['recording_process_pre_path']).as_posix()
                    ephys_pipeline.create_lfp_trace(config.catgt_script, recording_directory, probe_dir)
        
        
        status_update = config.status_update_idx['NEXT_STATUS']

        return (status_update, update_value_dict)


    @staticmethod
    def imaging_preingestion(rec_series):
        """
        Ingest "first" tables of imaging_pipeline, imaging_ement tables and recording_process.Processing
        Input:
        rec_series     (pd.Series)  = Series with information about the recording
        Returns:
        status_update       (int)     = 1  if recording status has to be updated to next step in recording.Recording
                                      = 0  if recording status not to be changed 
                                      = -1 if recording status has to be updated to ERROR in recording.Recording
        update_value_dict   (dict)    = Dictionary with next keys:
                                        {'value_update': value to be updated in this stage (if applicable)
                                        'error_info':    error info to be inserted if error occured }
        """

        status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        print("rec_series['query_key']")
        print(rec_series['query_key'])

        #Populate ImagingPipelineSession and call matlab script that handles TiffSplits
        imaging_pipeline.ImagingPipelineSession.populate(rec_series['query_key'])
        imaging_pipeline.AcquiredTiff.populate(rec_series['query_key'])

        #Retrieve all fovs records ingested in matlab Script
        fovs_ingested = (imaging_pipeline.TiffSplit & rec_series['query_key']).fetch("KEY", as_dict=True)

        if len(fovs_ingested) == 0:
            status_update = config.status_update_idx['ERROR_STATUS']
            update_value_dict['error_info']['error_message'] = 'Imaging TiffSplit process failed'
            return (status_update, update_value_dict)

        #Ingest Scan for each fov from the TiffSplit process
        for this_fov in fovs_ingested:

            # Scan_id always zero because TIFF splitted (FOVs) already on imaging_pipeline schema
            scan_id = 0
            # Acquisition type will have Mesoscope or 2Photon
            scanner = rec_series['acquisition_type']
            # Hardcoded acquisition software
            acq_software = 'ScanImage'

             #Insert Scan and ScanInfo 
            imaging_pipeline.scan_element.Scan.insert1(
            {**this_fov, 'scan_id': 0, 'scanner': scanner, 'acq_software': acq_software}, skip_duplicates=True)
            
        #Populate ScanInfo for all fovs
        imaging_pipeline.scan_element.ScanInfo.populate(rec_series['query_key'], display_progress=True)

        #ingested_recording = (imaging_pipeline.scan_element.Scan & rec_series['query_key']).fetch("KEY", as_dict=True)

        # Get fov directories for each recording process:
        fov_files_df = pd.DataFrame((imaging_pipeline.scan_element.ScanInfo.ScanFile & rec_series['query_key']).fetch(as_dict=True))

        fov_files = fov_files_df.groupby('tiff_split').first().reset_index()


        #Insert recording processes records
        old_recording_process = (recording_process.Processing() & rec_series['query_key']).fetch("KEY", as_dict=True)
        if len(old_recording_process) == 0:

            connection = recording.Recording.connection 
            with connection.transaction:
                        
                # Get fov directories for each recording process:
                fov_files_df = pd.DataFrame((imaging_pipeline.scan_element.ScanInfo.ScanFile & rec_series['query_key']).fetch(as_dict=True))
                fov_files = fov_files_df.groupby('tiff_split').first().reset_index().to_dict('records')

                fov_files = [dict(item, recording_process_pre_path=pathlib.Path(item['file_path']).parent.as_posix()) for item in fov_files]

                recording_process.Processing().insert_recording_process(fov_files, 'tiff_split')

                #Get parameters for recording processes
                recording_processes = (recording_process.Processing() & rec_series['query_key']).fetch('job_id', 'recording_id', 'fragment_number', 'recording_process_pre_path', as_dict=True)
                default_params_record_df = pd.DataFrame((recording.DefaultParams & rec_series['query_key']).fetch(as_dict=True))
                params_rec_process = recording.DefaultParams.get_default_params_rec_process(recording_processes, default_params_record_df)

                recording_process.Processing.ImagingParams.insert(params_rec_process, skip_duplicates=True)

                #Update recording_process_post_path
                recording_process.Processing().set_recording_process_post_path(recording_processes)

        status_update = config.status_update_idx['NEXT_STATUS']

        return (status_update, update_value_dict)

