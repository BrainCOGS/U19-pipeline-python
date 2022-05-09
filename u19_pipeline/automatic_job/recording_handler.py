
import os
import pathlib
import subprocess
import json
import time
import traceback
import re
import pandas as pd
import datajoint as dj
import copy

from datetime import datetime
from u19_pipeline import recording, ephys_pipeline, imaging_pipeline, recording, recording_process, lab
import u19_pipeline

import u19_pipeline.utils.dj_shortcuts as dj_short
import u19_pipeline.utils.scp_transfers as scp_tr
import u19_pipeline.utils.path_utils as pu

import u19_pipeline.ingest as element_ingest

from u19_pipeline.ingest import ephys_element_ingest



import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
import u19_pipeline.automatic_job.slurm_creator as slurmlib
import u19_pipeline.automatic_job.params_config as config


def exception_handler(func):
    def inner_function(*args, **kwargs):
        try:
             argout = func(*args, **kwargs)
             return argout
        except Exception as e:
            print('Soy exception ................')
            update_value_dict = copy.deepcopy(config.default_update_value_dict)
            update_value_dict['error_info']['recording_error_message'] = str(e)
            update_value_dict['error_info']['recording_error_exception'] = (''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)))
            print(config.RECORDING_STATUS_ERROR_ID)
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
                success_process, update_dict = function_status_process(recording_series, next_status_series) 

                #Get dictionary of record process
                key = recording_series['query_key']

                if success_process == 1:
                    #Get values to update
                    next_status = next_status_series['Value']
                    value_update = update_dict['value_update']
                    field_update = next_status_series['UpdateField']

                    RecordingHandler.update_status_pipeline(key, next_status, field_update, value_update)
                
                #An error occurred in process
                if success_process == -1:
                    next_status = config.RECORDING_STATUS_ERROR_ID
                    RecordingHandler.update_status_pipeline(key,next_status, None, None)

                #if success or error update status timestamps table
                if success_process != 0:
                    RecordingHandler.update_recording_log(recording_series['recording_id'], current_status, next_status, update_dict['error_info'])


            except Exception as err:
                raise(err)
                ## Send notification error, update recording to error

            time.sleep(2)

    
    @staticmethod
    @exception_handler
    def local_transfer_request(rec_series, status_series):
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

        full_remote_path = pathlib.Path(dj.config['custom']['root_data_dir'], rec_series['recording_modality'], rec_series['recording_directory']).as_posix()
        pathlib.Path(full_remote_path).mkdir(parents=True, exist_ok=True)

        status, task_id = scp_tr.call_scp_background(rec_series, full_remote_path)

        if status:
            status_update = config.status_update_idx['NEXT_STATUS']
            update_value_dict['value_update'] = task_id


        return (status_update, update_value_dict)

    @staticmethod
    @exception_handler
    def local_transfer_check(rec_series, status_series):
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
                update_value_dict['error_info']['recording_error_message'] = 'Return code scp not = 0'

        print('is_finished', is_finished, 'status_update', status_update)

        return (status_update, update_value_dict)

    @staticmethod
    @exception_handler
    def modality_preingestion(in_rec_series, status_series):
        """
        Ingest "first" tables of modality specific recordings
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

        rec_series = in_rec_series.copy()

        # Get fieldname directory for processing unit for current recording modality
        process_unit_fieldnames = \
        config.recording_modality_df.loc[config.recording_modality_df['RecordingModality'] == rec_series['recording_modality'], 
        ['ProcessUnitDirectoryField', 'ProcessUnitField']].squeeze()
        process_unit_dir_fieldname = process_unit_fieldnames['ProcessUnitDirectoryField']
        process_unit_fieldname     = process_unit_fieldnames['ProcessUnitField']



        if rec_series['recording_modality'] == 'electrophysiology':

            ephys_pipeline.EphysPipelineSession.populate(rec_series['query_key'])
            ephys_element_ingest.process_session(rec_series['query_key'])
            ephys_pipeline.ephys_element.EphysRecording.populate(rec_series['query_key'])

            probe_files = (ephys_pipeline.ephys_element.EphysRecording.EphysFile & rec_series['query_key']).fetch("KEY")
            recording_process.Processing().insert_recording_process(probe_files, 'insertion_number')

            #recording_process.Processing.insert
            
        '''
        elif rec_series['recording_modality'] == 'imaging':
            
            this_modality_recording_table = imaging_pipeline.ImagingPipelineSession
            element_recording_table       = imaging_pipeline.scan_element.Scan

            #print('imaging scan insert', rec_series)
            #this_modality_recording_table.populate(rec_series['query_key'])

            # Insert this modality recording and recording "unit"
            #imaging_rec.ScanInfo.populate(rec_series['query_key'])
            
        
        # Insert this modality recording and recording "unit"
        print('............... before ')
        print(rec_series['query_key'])
        this_modality_recording_table.populate(rec_series['query_key'])
       
        # Get all recording probes ("units") from this recording 
        recording_units = (this_modality_recording_unit_table  & rec_series['query_key']).fetch(as_dict=True)

        print('recording_units', recording_units)

        if len(recording_units) > 0:
            # Select only primary keys for recording unit (EphysRecordingProbes, FieldOfView, etc)
            rec_units_primary_key_fields = dj_short.get_primary_key_fields(this_modality_recording_unit_table)
            
            rec_process_table = recording.RecordingProcess()

            # rename default process params (from recording) to process params
            rec_series['preprocess_paramset_idx'] = rec_series.pop('def_preprocess_paramset_idx')
            rec_series['process_paramset_idx'] = rec_series.pop('def_process_paramset_idx')

            #Insert recording Process for all ("units") (one by one to get matching recording process id)
            connection = recording_process.Processing.connection 
            with connection.transaction:
                for rec_unit in recording_units:

                    #Insert recording process and associated ephysProcessing records for this probe                    
                    rec_process_table.insert_recording_process(rec_series, rec_unit, process_unit_dir_fieldname, process_unit_fieldname)
                    recording_process = recording_process.Processing.fetch('recording_process_id', order_by='recording_process_id DESC', limit=1)

                    print('recording_process', recording_process)

                    # Get recording unit key fields
                    recording_unit_key = {k: v for k, v in rec_unit.items() if k in rec_units_primary_key_fields}
                   
                    #Insert modality processing unit
                    this_mod_processing = recording_unit_key.copy()
                    this_mod_processing['recording_process_id'] = recording_process[0] 

                    print('this_mod_processing', this_mod_processing)

                    this_modality_processing_unit_table.insert1(this_mod_processing)
            status_update = config.status_update_idx['NEXT_STATUS']

        '''
        
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

        recordings_active = recording.Recording * lab.Location.proj('ip_address', 'system_user') & status_query
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
        key['recording_error_message'] = error_info_dict['recording_error_message']
        key['recording_error_exception'] = error_info_dict['recording_error_exception']

        recording.Log.insert1(key)

