# ingest_hdf5_to_db.py
# Python >= 3.7
# A proposed script to ingest Air Puffs hdf5 files on bucket into datajoint databases
# Folder structure for hdf5 files on bucket will be:
# /jukebox/braininit/puffs/{netid}/{project_name}/{cohort}/{rig}/{hdf5_filename}

import datajoint as dj
import os, sys, glob, json
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

dj.config['database.host'] = 'datajoint00.pni.princeton.edu'
dj.config['database.user'] = os.environ.get('DJ_DB_USER')
dj.config['database.password'] = os.environ.get('DJ_DB_PASS')

# Link to existing databases
u19_lab = dj.create_virtual_module('u19_lab','u19_lab')
u19_subject = dj.create_virtual_module('u19_subject','u19_subject')
u19_acq = dj.create_virtual_module('u19_acquisition','u19_acquisition')

# Link to new Puffs dbs
u19_puffs = dj.create_virtual_module('u19_puffs','u19_puffs')
# puffs_lab = dj.create_virtual_module('u19_puffs_lab','u19_puffs_lab')
# puffs_behavior = dj.create_virtual_module('u19_puffs_behavior','u19_puffs_behavior')

# Currently a hardcoded data folder to one of Marlies' cohorts for this example,
# but in the future will loop over folders starting from 
# The root folder: /jukebox/braininit/puffs/
data_folder = '/jukebox/braininit/puffs/oostland/Tsc1_evidence_accumulation/cohort_10/rig0/'
h5_files = glob.glob(data_folder + '/data*.h5')
mouse_info_file = os.path.join(data_folder,'mouse_info_0821_datajoint_format.csv')

# first read in the mouse_info file into a pandas dataframe
df_info = pd.read_csv(mouse_info_file,keep_default_na=False)

# # Use mouse_info file and do the u19_subject and u19_lab inserts
# Tables we need to insert into (**In this order**) are:
# - u19_lab.User # in case user is not already in the database
# - u19_lab.Protocol # in case protocol is not already in the database
# - u19_lab.Location # in case the location of each subject is not already in the database
# 
# - u19_subject.Species # in case they are NOT training on mice
# - u19_subject.Strain # in case they are NOT training on a strain of mice already in the db
# - u19_subject.Line # in case they are using a new genetic line
# - u19_subject.Subject # in case they are using new subjects


# u19_lab.User() inserts
unique_netids = df_info['netid'].unique()
full_names = df_info['experimenter_fullname']
user_insert_list = []
for netid in unique_netids:
    netid_mask = df_info['netid'] == netid
    full_name = full_names[netid_mask].iloc[0]
    user_insert_dict = {
      'user_id': netid,
      'user_nickname': netid,
      'full_name': full_name,
      'email': f'{netid}@princeton.edu',
      'contact_via': 'Email', 
      'presence': 'Available',
      'tech_responsibility': 'no',
      'day_cutoff_time': np.array([[18.,  0.]])
       }
    user_insert_list.append(user_insert_dict)
u19_lab.User.insert(user_insert_list,skip_duplicates=True)

# u19_lab.Protocol() inserts
unique_protocols = df_info['protocol'].unique()
protocol_insert_list = []
for protocol in unique_protocols:
    protocol_insert_dict = {
      'protocol': protocol,
      'protocol_description': '',
       }
    protocol_insert_list.append(protocol_insert_dict)
u19_lab.Protocol.insert(protocol_insert_list,skip_duplicates=True)


# u19_lab.Location() inserts
unique_locations = df_info['location'].unique()
location_descriptions = df_info['location_description']
location_insert_list = []
for location in unique_locations:
    location_mask = df_info['location'] == location
    location_description = location_descriptions[netid_mask].iloc[0]
    location_insert_dict = {
      'location': location,
      'location_description': location_description,
       }
    location_insert_list.append(location_insert_dict)
u19_lab.Location.insert(location_insert_list,skip_duplicates=True)

# u19_subject.Species() inserts
unique_species = df_info['binomial'].unique()
species_insert_list = []
for species in unique_species:
    species_insert_dict = {
      'binomial': species,
      'species_nickname': '',
       }
    species_insert_list.append(species_insert_dict)
u19_subject.Species.insert(species_insert_list,skip_duplicates=True)

# u19_subject.Strain() inserts
unique_strains = df_info['strain'].unique()
strain_insert_list = []
for strain in unique_strains:
    strain_insert_dict = {
      'strain_name': strain,
      'strain_description': '',
       }
    strain_insert_list.append(strain_insert_dict)
u19_subject.Strain.insert(strain_insert_list,skip_duplicates=True)

# u19_subject.Line() inserts
unique_lines = df_info['genetic_line'].unique()
binomials = df_info['binomial']
strains = df_info['strain']
line_insert_list = []
for line in unique_lines:
    line_mask = df_info['genetic_line'] == line
    binomial = binomials[line_mask].iloc[0]
    strain = strains[line_mask].iloc[0]
    line_insert_dict = {
      'line': line,
      'binomial': binomial,
      'strain_name':strain,
      'line_description':'',
      'target_phenotype':'',
      'is_active':1,
       }
    line_insert_list.append(line_insert_dict)
u19_subject.Line.insert(line_insert_list,skip_duplicates=True)


# u19_subject.Subject() inserts
unique_subjects = df_info['subj'].unique()
project_names = df_info['project_name']
subject_descriptions = df_info['subj_description']
netids = df_info['netid']
sexes = df_info['sex']
DOBs = df_info['DOB']
locations = df_info['location']
lines = df_info['genetic_line']
protocols = df_info['protocol']
subject_insert_list = []
for subject in unique_subjects:
    subject_mask = df_info['subj'] == subject
    netid = netids[subject_mask].iloc[0]
    project_name = project_names[subject_mask].iloc[0]
    subject_fullname = '_'.join([netid,project_name,str(subject)])
    sex = sexes[subject_mask].iloc[0]
    DOB = DOBs[subject_mask].iloc[0]
    location = locations[subject_mask].iloc[0]
    line = lines[subject_mask].iloc[0]
    protocol = protocols[subject_mask].iloc[0]
    subject_description = subject_descriptions[subject_mask].iloc[0]
    subject_insert_dict = {
      'subject_fullname': subject_fullname,
      'subject_nickname': str(subject),
      'subject_description': subject_description,
      'user_id':netid,
      'sex': 'Male' if sex == 'M' else 'Female',
      'dob': DOB,
      'location':location,
      'line':line,
      'protocol':protocol 
       }
    subject_insert_list.append(subject_insert_dict)
u19_subject.Subject.insert(subject_insert_list,skip_duplicates=True)


# ## Now use h5 files to make inserts into u19_acquisition tables and puffs specific tables
# 
# Tables we need to insert into (**In this order**) are:
# - u19_puffs.PuffsCohort # in case cohort is not already in the database
# - u19_puffs.PuffsFileAcquisition # as we process the various files
# 
# - u19_acquisition.SessionStarted # as we process the session data in each h5 file
# - u19_acquisition.Session # as we process the session data in each h5 file
# 
# - u19_puffs.PuffsSession 
# - u19_puffs.PuffsSession.Trial 
# - u19_puffs.PuffsSession.TrialPhase
# - u19_puffs.PuffsSession.Puff
# 

# first find list of h5 files that are already processed so we do not
# repeat the ingestion on those
already_processed_filenames = u19_puffs.PuffsFileAcquisition().fetch('h5_filename')

# Loop over h5 files in this cohort/rig folder
for h5_file in h5_files:
    if h5_file in already_processed_filenames:
        print(f"file: {h5_file} already processed!")
        continue
    print(h5_file)
    
    """ Grab the relevant dataframes from the hdf5 file """ 
    with pd.HDFStore(h5_file) as data:
        df_trials = data.trials
        df_puffs = data.trials_timing
        df_phases = data.phases
        
        """ Sometimes there are extra session metadata stored in other dataframes
        with the key names: 
            'sessions/{session_number}/notes'
            'sessions/{session_number}/params'
            'sessions/{session_number}/sync'
        If they exist we want to ingest their metadata, but they do not always exist 
        """
        stored_session_keys = [x for x in data if 'sessions' in x]
        
        """ Figure out the information from the h5 filepath """
        rig = h5_file.split('/')[-2][-1] # just want the 0 or 1 (not "rig0" or "rig1")
        cohort = h5_file.split('/')[-3].split('_')[-1] # 'want C10' or just '10' 
        project_name = h5_file.split('/')[-4].strip() 
        username = h5_file.split('/')[-5].strip()
        puffs_cohort_insert_dict = {
            'user_id':username,
            'project_name':project_name,
            'cohort':cohort
        }


        df_trials['rig'] = rig
        df_trials['cohort'] = cohort
        df_trials['project_name'] = project_name

        """ Figure out session performance for each trial """
        df_trials['answered_correct'] = (df_trials['side'] == df_trials['outcome'].astype(int))
        """ Make a new dataframe for unique sessions, i.e. where subj and session are unique
        in trials """ 
        df_sessions = df_trials.groupby(['subj','session']).max().reset_index().sort_values(
            ['subj','session'])
        sessions = df_sessions['session']
        last_levels = df_sessions['level']
        session_ends_rel = df_sessions['end']
        """ make a session_dates column """
        session_dates = sessions.apply(lambda x: str(x)[:10])
        df_sessions['session_date'] = session_dates
        subjs = df_sessions['subj']
        """ look up where these subjects are """
        rigs = df_sessions['rig'].astype('int')
        """ make a session number column - i.e.
        the 0-indexed counter for the session on a day for a single subject """
        session_numbers = df_sessions.groupby(['session_date','subj']).cumcount() 
        df_sessions['session_number'] = session_numbers

        """ Figure out fraction correct for each session """
        fraction_trials_correct_df = df_trials.groupby(['session','subj']).agg({'answered_correct': 'sum','side':'count'})
        fraction_trials_correct_df['fraction_correct'] = fraction_trials_correct_df['answered_correct']/fraction_trials_correct_df['side']
        fraction_trials_correct_df = fraction_trials_correct_df.sort_values(['subj','session']).reset_index()
        df_sessions['fraction_correct'] = fraction_trials_correct_df['fraction_correct']
        fractions_correct = df_sessions['fraction_correct']
        
        """ Initialize lists to fill and then insert into db """
        session_started_insert_list = []
        session_insert_list = []
        puffs_session_insert_list = []

        """ Loop over sessions and assemble the inserts """
        n_sessions = len(df_sessions)
        for ii in range(len(df_sessions)):
            print(f"session {ii}/{len(df_sessions)}")
            subj = str(int(subjs.iloc[ii]))
            subject_fullname = '_'.join([username,project_name,subj])
            last_level = last_levels.iloc[ii]
            date = session_dates.iloc[ii]
            session_datetime = sessions.iloc[ii]
            session_end_rel = session_ends_rel[ii]
            session_end_datetime = session_datetime + timedelta(seconds=session_end_rel)
            session_number = session_numbers.iloc[ii]
            fraction_correct = fractions_correct.iloc[ii]
            rig = rigs.iloc[ii]
            
            if rig == 0:
                location = "pni-ltl016-05"
            elif rig == 1:
                location = "wang-behavior"
            else:
                sys.exit(f'Rig: {rig} is not one of the rigs. Check data type (needs to be integer)')
            session_compressed_str = session_datetime.strftime('%Y%m%d%H%M%S')
            
            """ Check to see if the session metadata dataframes exist """
            this_session_params_key = f'sessions/{session_compressed_str}/params'
            if this_session_params_key in stored_session_keys:
                session_param_dict = json.loads(data[this_session_params_key].iloc[0])
            else:
                session_param_dict = None
            this_session_notes_key = f'sessions/{session_compressed_str}/notes'
            if this_session_notes_key in stored_session_keys:
                session_notes_dict = json.loads(data[this_session_notes_key].iloc[0])
                if 'notes' in session_notes_dict:
                    session_notes = session_notes_dict['notes']
                else:
                    session_notes = None
                if 'stdout' in session_notes_dict:
                    session_stdout = session_notes_dict['stdout']
                else:
                    session_stdout = None
                if 'stderr' in session_notes_dict:
                    session_stderr = session_notes_dict['stderr']
                else:
                    session_stderr = None
            else:
                session_notes = None
                session_stdout = None
                session_stderr = None
            this_session_sync_key = f'sessions/{session_compressed_str}/sync'
            if this_session_sync_key in stored_session_keys:
                session_sync_dict = data[this_session_notes_key].todict()
            else:
                session_sync_dict = None
            
            """ Assemble the u19_acquistion inserts for this session """ 
            session_started_insert_dict = {
                'subject_fullname':subject_fullname,
                'session_date':date,
                'session_number':session_number,
                'session_start_time':session_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'session_location':location,
                'task':'AirPuffs',
                'local_path_behavior_file':'',
                'remote_path_behavior_file':h5_file,
                'is_finished':1
            }
            session_insert_dict = {
                'subject_fullname':subject_fullname,
                'session_date':date,
                'session_number':session_number,
                'session_start_time':session_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'session_end_time':session_end_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'session_location':location,
                'task':'AirPuffs',
                'level':last_level,
                'set_id':1,
                'stimulus_bank':"",
                'stimulus_commit':"",
                'session_performance':fraction_correct,
                'session_narrative': '',

            }
            """ Assemble the puffs inserts for this session """ 
            # PuffsSession() table
            puffs_session_insert_dict = {
                'subject_fullname':subject_fullname,
                'session_date':date,
                'session_number':session_number,
                'session_params':session_param_dict,
                'rig':rig,
                'notes':session_notes,
                'stdout':session_stdout,
                'stderr':session_stderr,
                'sync':session_sync_dict,
                
            }
            # Trial() table
            df_trials_this_session = df_trials[df_trials['session'] == session_datetime][[
                'idx','level','side','draw_p','start','end','dur','nL_intended',
                'nL','nR_intended','nR','reward','reward_scale','rule']]
            df_trials_this_session['subject_fullname'] = subject_fullname
            df_trials_this_session['session_date'] = date
            df_trials_this_session['session_number'] = session_number
            df_trials_this_session['task'] = 'AirPuffs'
            df_trials_this_session['set_id'] = 1
            df_trials_this_session['trial_duration'] = df_trials_this_session['end'] - df_trials_this_session['start']
            df_trials_this_session.rename(columns={
                'idx': 'trial_idx',
                'side': 'choice',
                'draw_p': 'trial_prior_p_left',
                'start':'trial_rel_start',
                'end':'trial_rel_finish',
                'dur':'cue_period',
                'nL':'num_puffs_received_l',
                'nL_intended':'num_puffs_intended_l',
                'nR':'num_puffs_received_r',
                'nR_intended':'num_puffs_intended_r',
                'reward':'reward_rel_start'
            },inplace=True)
            df_trials_this_session['choice'] = df_trials_this_session['choice'].apply(lambda x: 'L' if x==0 else 'R')
            trials_insert_list = df_trials_this_session.to_dict('records')
            
            # Puff() table
            df_puffs_this_session = df_puffs[df_puffs['session'] == session_datetime]
            df_puffs_this_session['subject_fullname'] = subject_fullname
            df_puffs_this_session['session_date'] = date
            df_puffs_this_session['session_number'] = session_number
            df_puffs_this_session['puff_idx'] = df_puffs_this_session.groupby('trial').cumcount()
            df_puffs_this_session.rename(columns={
                'time':'puff_rel_time',
                'trial':'trial_idx',
            },inplace=True)
            df_puffs_this_session['session_date'] = date
            df_puffs_this_session['session_number'] = session_number
            df_puffs_this_session = df_puffs_this_session[[
                'subject_fullname',
                'session_date',
                'session_number',
                'trial_idx',
                'puff_idx',
                'side',
                'puff_rel_time'
            ]]
            puffs_insert_list = df_puffs_this_session.to_dict('records')
            
            # TrialPhase() table
            df_phases_this_session = df_phases[df_phases['session'] == session_datetime]
            df_phases_this_session['subject_fullname'] = subject_fullname
            df_phases_this_session['session_date'] = date
            df_phases_this_session['session_number'] = session_number
            df_phases_this_session.rename(columns={
                'trial':'trial_idx',
                'start_time':'phase_rel_start',
                'end_time':'phase_rel_finish',
            },inplace=True)
            df_phases_this_session = df_phases_this_session[[
                'subject_fullname',
                'session_date',
                'session_number',
                'trial_idx',
                'phase',
                'phase_rel_start',
                'phase_rel_finish'
            ]] 
            """ Some phases seem to reference trials that do not exist
            restrict phase rows to trials that exist """
            trial_idxs = df_trials_this_session['trial_idx']
            phase_trial_mask = df_phases_this_session['trial_idx'].isin(trial_idxs.values)
            df_phases_this_session_goodtrials = df_phases_this_session[phase_trial_mask]
            phases_insert_list = df_phases_this_session_goodtrials.to_dict('records')
            """ Start a transaction and do the inserts for this session """
            connection = u19_acq.SessionStarted.connection 
            with connection.transaction:
                u19_acq.SessionStarted().insert1(session_started_insert_dict,skip_duplicates=True)
                u19_acq.Session().insert1(session_insert_dict,skip_duplicates=True)
                u19_puffs.PuffsSession().insert1(puffs_session_insert_dict,skip_duplicates=True)
                u19_puffs.PuffsSession.Trial().insert(trials_insert_list,skip_duplicates=True)
                u19_puffs.PuffsSession.Puff().insert(puffs_insert_list,skip_duplicates=True)
                u19_puffs.PuffsSession.TrialPhase().insert(phases_insert_list,skip_duplicates=True)
    """ If the inserts for all sessions in this hdf5 file were successful,
    then we need to mark this hdf5 file as processed by inserting it 
    into the PuffsFileAcquisition() table in u19_puffs_acquisition table """
    file_acq_insert_dict = {
        'user_id':username,
        'project_name':project_name,
        'cohort':cohort,
        'rig':rig,
        'h5_filename':h5_file,
        'ingested':1
    }
    u19_puffs.PuffsFileAcquisition.insert1(file_acq_insert_dict,replace=True) # want to overwrite with ingested=1 if ingested=0 for some reason