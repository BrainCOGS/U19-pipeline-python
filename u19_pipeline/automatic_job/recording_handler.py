
import os
import pathlib
import subprocess
import json
import time
import re
import pandas as pd
import datajoint as dj
from u19_pipeline import recording, ephys_rec, imaging_rec
import u19_pipeline.utils.dj_shortcuts as dj_short
import u19_pipeline.automatic_job.clusters_paths_and_transfers as ft
import u19_pipeline.automatic_job.slurm_creator as slurmlib
import u19_pipeline.automatic_job.params_config as config

class RecordingHandler():

    default_update_value_dict ={
        'value_update': None,
        'error_info': None
    }

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
            current_status = recording_series['status_recording_idx']
            current_status_series = config.recording_status_df.loc[config.recording_status_df['Value'] == current_status, :].squeeze()
            next_status_series    = config.recording_status_df.loc[config.recording_status_df['Value'] == current_status+1, :].squeeze()

            print('function to apply:', next_status_series['ProcessFunction'])

            # Get processing function    
            function_status_process = getattr(RecordingHandler, next_status_series['ProcessFunction'])

            #Trigger process, if success update recording process record
            try:
                success_process, update_dict = function_status_process(recording_series, next_status_series) 

                if success_process:
                    #Get dictionary of record process
                    key = recording_series['query_key']
                    #Get values to update
                    next_status = next_status_series['Value']
                    value_update = update_dict['value_update']
                    field_update = next_status_series['UpdateField']

                    print('key to update', key)

                    RecordingHandler.update_status_pipeline(key, next_status, field_update, value_update)
            except Exception as err:
                raise(err)
                ## Send notification error, update recording to error

            time.sleep(2)

    
    @staticmethod
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

        status_update = False
        update_value_dict = RecordingHandler.default_update_value_dict.copy()
        directory_path = status_series['FunctionField']

        status_update = True
        return (status_update, update_value_dict)

        #smbclient
        '''
        if transfer_request['code'] == 'Accepted':
            status_update = True
            update_value_dict['value_update'] = transfer_request['task_id']

        return (status_update, update_value_dict)
        '''

    @staticmethod
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

        status_update = False
        update_value_dict = RecordingHandler.default_update_value_dict.copy()
        id_task = status_series['FunctionField']

        status_update = True
        return (status_update, update_value_dict)
        #smbclient copy check ???

    @staticmethod
    def modality_preingestion(rec_series, status_series):
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
        
        status_update = False
        update_value_dict = RecordingHandler.default_update_value_dict.copy()

        # Get fieldname directory for processing unit for current recording modality
        process_unit_fieldnames = \
        config.recording_modality_df.loc[config.recording_modality_df['RecordingModality'] == rec_series['recording_modality'], 
        ['ProcessUnitDirectoryField', 'ProcessUnitField']].squeeze()
        process_unit_dir_fieldname = process_unit_fieldnames['ProcessUnitDirectoryField']
        process_unit_fieldname     = process_unit_fieldnames['ProcessUnitField']


        if rec_series['recording_modality'] == 'electrophysiology':

            this_modality_recording_table = ephys_rec.EphysRecording
            this_modality_recording_unit_table = ephys_rec.EphysRecordingProbes
            this_modality_processing_unit_table = ephys_rec.EphysProcessing

            print('this_modality_recording_table', this_modality_recording_table)

        elif rec_series['recording_modality'] == 'imaging':
            
            this_modality_recording_table = imaging_rec.Scan
            this_modality_recording_unit_table = imaging_rec.FieldOfView
            this_modality_processing_unit_table = imaging_rec.ImagingProcessing

            #print('imaging scan insert', rec_series)
            #this_modality_recording_table.populate(rec_series['query_key'])

            # Insert this modality recording and recording "unit"
            #imaging_rec.ScanInfo.populate(rec_series['query_key'])
            
        
        # Insert this modality recording and recording "unit"
        this_modality_recording_table.populate(rec_series['query_key'])
        this_modality_recording_unit_table.populate(rec_series['query_key']) # In imaging this is to call imaging.ScanInfo populate Script

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
            connection = recording.RecordingProcess.connection 
            with connection.transaction:
                for rec_unit in recording_units:

                    #Insert recording process and associated ephysProcessing records for this probe                    
                    rec_process_table.insert_recording_process(rec_series, rec_unit, process_unit_dir_fieldname, process_unit_fieldname)
                    recording_process = recording.RecordingProcess.fetch('recording_process_id', order_by='recording_process_id DESC', limit=1)

                    print('recording_process', recording_process)

                    # Get recording unit key fields
                    recording_unit_key = {k: v for k, v in rec_unit.items() if k in rec_units_primary_key_fields}
                   
                    #Insert modality processing unit
                    this_mod_processing = recording_unit_key.copy()
                    this_mod_processing['recording_process_id'] = recording_process[0] 

                    print('this_mod_processing', this_mod_processing)

                    this_modality_processing_unit_table.insert1(this_mod_processing)
            status_update = True

        return (status_update, update_value_dict)


    @staticmethod
    def get_active_recordings():
        '''
        get all recordings that have to go through some action in the pipeline
        Return:
            df_recordings (pd.DataFrame): all recordings that are going to be processed in the pipeline
        '''

        status_query = 'status_recording_idx > ' + str(config.recording_status_df['Value'].min())
        status_query += ' and status_recording_idx < ' + str(config.recording_status_df['Value'].max())

        recordings_active = recording.Recording & status_query
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
        update_status_dict['status_recording_idx'] = status
        print('update_status_dict', update_status_dict)
        recording.Recording.update1(update_status_dict)
