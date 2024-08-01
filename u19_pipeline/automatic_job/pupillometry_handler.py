import copy
import glob
import os
import pathlib
import re
import subprocess
import sys
import traceback

import datajoint as dj
import deeplabcut
import numpy as np
import pandas as pd
from scipy import stats
from skimage.measure import EllipseModel

import u19_pipeline.acquisition as acquisition
import u19_pipeline.automatic_job.params_config as config
import u19_pipeline.automatic_job.slurm_creator as slurmlib
import u19_pipeline.pupillometry as pupillometry
import u19_pipeline.utils.slack_utils as slack_utils
from u19_pipeline.utils.file_utils import write_file


def pupillometry_exception_handler(func):
    def inner_function(*args, **kwargs):
        try:
             argout = func(*args, **kwargs)
             return argout
        except Exception as e:
            print('Exception HERE ................')

            update_value_dict = copy.deepcopy(config.default_update_value_dict)
            update_value_dict['error_info']['error_message'] = str(e)
            update_value_dict['error_info']['error_exception'] = (''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)))

            print(update_value_dict['error_info']['error_message'])
            print(update_value_dict['error_info']['error_exception'])


            return (config.RECORDING_STATUS_ERROR_ID, update_value_dict)
    return inner_function

class PupillometryProcessingHandler:

    spock_home_dir = '/mnt/cup/braininit/Shared/repos/U19-pipeline_python/'
    spock_log_dir = spock_home_dir + "u19_pipeline/automatic_job/OutputLog/"
    process_script_path = spock_home_dir + "u19_pipeline/automatic_job/pupillometry_handler.py"
    spock_error_dir = spock_home_dir + "u19_pipeline/automatic_job/ErrorLog/" 
    spock_slurm_filepath = spock_home_dir + "u19_pipeline/"
    spock_system_name = 'spockmk2-loginvm.pni.princeton.edu'
    
    pupillometry_slurm_filepath = os.path.abspath(os.path.realpath(__file__)+ "/../")

    #pupillometry_slurm_filepath = 'u19_pipeline/'
    pupillometry_slurm_filename = 'slurm_pupillometry.slurm'

    slurm_dict_pupillometry_spock = {
        'job-name': 'dj_ingestion',
        'nodes': 4,
        'cpus-per-task': 4,
        'time': '20:00:00',
        'mem': '50G',
        'mail-type': ['END', 'FAIL'],
    }

    @staticmethod
    def generate_slurm_pupillometry(slurm_dict):

        slurm_text = '#!/bin/bash\n'
        slurm_text += PupillometryProcessingHandler.create_pupillometry_slurm_params_file(slurm_dict)
        slurm_text += '''
        echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
        echo "SLURM_SUBMIT_DIR: ${SLURM_SUBMIT_DIR}"
        echo "VIDEO_DIR: ${video_dir}"
        echo "MODEL_DIR: ${model_dir}"
        echo "REPOSITORY_DIR: ${repository_dir}"
        echo "OUTPUT_DIR: ${output_dir}"
        echo "PROCESS_SCRIPT_PATH:  ${process_script_path}"

        module load anacondapy/2023.07

        conda activate u19_pipeline_python_env2

        cd ${repository_dir}
        python ${process_script_path} ${video_dir} ${model_dir} ${output_dir}
        #python ${process_script_path} ${recording_process_id}
        '''
        
        return slurm_text   

    @staticmethod
    def create_pupillometry_slurm_params_file(slurm_dict):

        text_dict = ''
        for slurm_param in slurm_dict:

            if isinstance(slurm_dict[slurm_param], list):
                for list_param in slurm_dict[slurm_param]:
                    text_dict += '#SBATCH --' + str(slurm_param) + '=' + str(list_param) + '\n'
            else:
                text_dict += '#SBATCH --' + str(slurm_param) + '=' + str(slurm_dict[slurm_param]) + '\n'

        return text_dict
    
    @staticmethod
    def generate_slurm_file(video_dir):
        '''
        Generate and send slurm file to be queued in processing cluster
        '''

        #Get all associated directories given the selected processing cluster

        # Start with default values
        slurm_dict = copy.deepcopy(PupillometryProcessingHandler.slurm_dict_pupillometry_spock)
        label_rec_process = 'job_id_'+str(video_dir.stem)
        slurm_dict['job-name'] = label_rec_process
        print('PupillometryProcessingHandler.spock_log_dir')
        print(PupillometryProcessingHandler.spock_log_dir)
        print('label_rec_process', label_rec_process)
        print('video_dir', video_dir)
        slurm_dict['output'] = PupillometryProcessingHandler.spock_log_dir+label_rec_process+'.log'
        slurm_dict['error'] = PupillometryProcessingHandler.spock_error_dir+label_rec_process+'.log'

        print('slurm_dict', slurm_dict)

        slurm_text = PupillometryProcessingHandler.generate_slurm_pupillometry(slurm_dict)
        
        slurm_file_local_path = str(pathlib.Path(PupillometryProcessingHandler.pupillometry_slurm_filepath,
                                                 PupillometryProcessingHandler.pupillometry_slurm_filename))

        print(slurm_file_local_path)

        print('slurm text', slurm_text)

        write_file(slurm_file_local_path, slurm_text)

        slurm_destination = pathlib.Path(PupillometryProcessingHandler.spock_slurm_filepath,
                                         PupillometryProcessingHandler.pupillometry_slurm_filename).as_posix()

        status = PupillometryProcessingHandler.transfer_pupillometry_slurm_file(slurm_file_local_path, slurm_destination)

        print(status)
        print(slurm_destination)
        
        return status, slurm_destination

    @staticmethod
    def queue_pupillometry_slurm_file(video_dir, model_dir, repository_dir, output_dir, process_script_path, slurm_location):

        id_slurm_job = -1

        #Get all associated variables given the selected processing cluster
        command = ['ssh', 'u19prod@'+PupillometryProcessingHandler.spock_system_name, 'sbatch', 
        "--export=video_dir='"+str(video_dir)+
        "',model_dir='"+str(model_dir)+
        "',repository_dir='"+str(repository_dir)+
        "',process_script_path='"+str(process_script_path)+
        "',output_dir='"+str(output_dir)+"'",
        slurm_location
        ]

        print(command)
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #p = os.popen(command_new).read()
        p.wait()
        stdout, stderr = p.communicate()

        print(p.returncode)
        print(stderr)
        print(stdout)

        if p.returncode == 0:
            error_message = ''
            batch_job_sentence = stdout.decode('UTF-8')
            print('batch_job_sentence', batch_job_sentence)
            id_slurm_job   = batch_job_sentence.replace("Submitted batch job ","")
            id_slurm_job   = re.sub(r"[\n\t\s]*", "", id_slurm_job)
        else:
            error_message = stderr.decode('UTF-8')

        return p.returncode, id_slurm_job, error_message


    @staticmethod
    def transfer_pupillometry_slurm_file(slurm_file_local_path, slurm_destination):
        '''
        Create scp command from cluster directories and local slurm file
        '''

        print("cp", slurm_file_local_path, slurm_destination)

        print(["cp", slurm_file_local_path, slurm_destination])

        p = subprocess.Popen(["cp", slurm_file_local_path, slurm_destination])
        transfer_status = p.wait()
        return transfer_status

        return status

    @staticmethod
    def getPupilDiameter(analyzedVideoDataPath):
        """
        Returns a pupil diameter numpy array from an analized video data stored in analyzedVideoDataPath
        Arguments:
            analyzedVideoDataPath: path of the video to analyze
        Returns:
            An array that contains the pupil diameter (index is the video frame) [numpy Array]
        """
        # Read the analyzed video data h5 file
        labels = pd.read_hdf(analyzedVideoDataPath)

        # Create a data frame of the same size ad the analyzed video data filled with zeros
        df = pd.DataFrame(np.zeros(1), columns=['PupilDiameter'])
        # For each frame, get the x and y coordinates of the points around the pupil, fit an ellipse and calculate the diameter of a circle with the same area as the ellipse
        for i in range(labels.index.size):
            subset = labels.loc[i]
            x = subset.xs('x', level='coords').to_numpy()[0:8]
            y = subset.xs('y', level='coords').to_numpy()[0:8]
            xy = np.column_stack((x,y))
            # Fit the points to an ellipse and get the parameters (estimate X center coordinate, estimate Y center coordinate, a, b, theta)
            ellipse = EllipseModel()
            ellipse.estimate(xy)
            # Calculate the area of the ellipse from the parameters a and b
            ellipseArea = np.pi * ellipse.params[2] * ellipse.params[3]
            # Get the diameter of a circle from the area of the ellipse
            pupilDiameter = 2 * np.sqrt(ellipseArea/np.pi)
            df.loc[i] = pupilDiameter

        # Get outliers (frames where either the mice have the eyes closed (blink or groom) or deeplabcut fails to track the pupil correctly)

        # Calculate the zscore of the data frame
        zscore = np.abs(stats.zscore(df))
        # Set a treshold for a valid zscore value (determined empirically)
        outlierFlags = np.abs(zscore) > 2
        # Get a boolean array where true correspond to the frame with an outlier diameter
        outlierFlags = outlierFlags.rename(columns={outlierFlags.columns[0]: "OutlierFlag"})
        # Concatenate outlier flags array to remove outliers from pupil diameter array
        temp = pd.concat([df, outlierFlags], axis=1)
        temp.loc[temp['OutlierFlag'] is True, 'PupilDiameter'] = None
        pupilDiameter = temp['PupilDiameter']
        
        return pupilDiameter.to_numpy()

    @staticmethod
    def analyze_videos_pupillometry(configPath, videoPath, output_dir):

        # Analyze video and get pupil diameter data
        deeplabcut.analyze_videos(configPath, videoPath, destfolder=output_dir)


    @staticmethod
    @pupillometry_exception_handler
    def check_pupillometry_sessions_queue():

        config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        sessions_missing_process = (acquisition.SessionVideo * 
            pupillometry.PupillometrySessionModelData & 'pupillometry_job_id is NULL').fetch(as_dict=True)
        
        print(sessions_missing_process)

        for pupillometry_2_process in sessions_missing_process:
            
            # If error, job id = -1
            key_insert = dict((k, pupillometry_2_process[k]) for k in ('subject_fullname', 'session_date', 'session_number', 'model_id'))
            key_insert['pupillometry_job_id'] = -1

            # Get model location
            model_key = (pupillometry.PupillometryModels & "model_id = "+str(pupillometry_2_process['model_id'])).fetch(as_dict=True)[0]
            models_dir = dj.config.get('custom', {}).get('root_data_dir',None)
            if models_dir is None:
                raise Exception('root_data_dir in not found in config, , run initial_conf.py again')
            models_dir = models_dir[0]
            model_path = pathlib.Path(models_dir, model_key['model_path'])

            print('model path', model_path)

            # Get video location
            pupillometry_dir = dj.config.get('custom', {}).get('pupillometry_root_data_dir',None)
            if pupillometry_dir is None:
                raise Exception('pupillometry_root_data_dir not found in config, run initial_conf.py again')
            pupillometry_raw_dir = pupillometry_dir[0]
            videoPath = pathlib.Path(pupillometry_raw_dir, pupillometry_2_process['remote_path_video_file'])

            print('videoPath', videoPath)

            # Create output location
            pupillometry_processed_dir = pupillometry_dir[1]
            output_dir = pathlib.Path(pupillometry_processed_dir,pathlib.Path(pupillometry_2_process['remote_path_video_file']).parent)
            if not output_dir.exists():
                output_dir.mkdir(parents=True, exist_ok=True)

            # Generate slurm file and transfer it to spock
            status, slurm_filepath = PupillometryProcessingHandler.generate_slurm_file(videoPath)
            print('slurm_filepath', slurm_filepath)

            # Error handling (generating slurm file)
            if status != config.system_process['SUCCESS']:
                config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = 'Error while generating/transfering pupillometry slurm file'
                pupillometry.PupillometrySessionModelData.update1(key_insert)

                slack_utils.send_slack_error_pupillometry_notification(config.slack_webhooks_dict['automation_pipeline_error_notification'],\
                        update_value_dict['error_info'] ,pupillometry_2_process)

                #return (status_update, update_value_dict)
                continue
            
            # Queue slurm file in spock
            status, slurm_jobid, error_message = PupillometryProcessingHandler.queue_pupillometry_slurm_file(
                videoPath, model_path, PupillometryProcessingHandler.spock_home_dir, output_dir, 
                PupillometryProcessingHandler.process_script_path, slurm_filepath)
            
            # Error handling (queuing slurm file)
            if status != config.system_process['SUCCESS']:
                config.status_update_idx['ERROR_STATUS']
                update_value_dict['error_info']['error_message'] = 'Error to queue pupillometry slurm file'
                pupillometry.PupillometrySessionModelData.update1(key_insert)

                slack_utils.send_slack_error_pupillometry_notification(config.slack_webhooks_dict['automation_pipeline_error_notification'],\
                        update_value_dict['error_info'] ,pupillometry_2_process)

                #return (status_update, update_value_dict)
                continue
            
            # If success, store job_id
            key_insert['pupillometry_job_id'] = slurm_jobid
            pupillometry.PupillometrySessionModelData.update1(key_insert)
            slack_utils.send_slack_pupillometry_update_notification(config.slack_webhooks_dict['automation_pipeline_update_notification'],\
                             'Pupillometry job submitted', pupillometry_2_process)

    @staticmethod
    @pupillometry_exception_handler
    def check_processed_pupillometry_sessions():

        #status_update = config.status_update_idx['NO_CHANGE']
        update_value_dict = copy.deepcopy(config.default_update_value_dict)

        sessions_to_check = (acquisition.SessionVideo * pupillometry.PupillometrySessionModelData & 'pupillometry_job_id > 0 and pupil_diameter is NULL').fetch(as_dict=True)

        for session_check in sessions_to_check:

            key_update = dict((k, session_check[k]) for k in ('subject_fullname', 'session_date', 'session_number', 'model_id'))
            print('key_update1', key_update)

            status_update, message = slurmlib.check_slurm_job('u19prod', PupillometryProcessingHandler.spock_system_name, str(session_check['pupillometry_job_id']), local_user=False)
            
            #If job finished copy over output and/or error log
            if status_update == config.status_update_idx['ERROR_STATUS']:

                update_value_dict['error_info']['error_message'] = 'An error occured in processing (check LOG)'
                update_value_dict['error_info']['error_exception'] = message

                key_update['pupillometry_job_id'] = -1
                pupillometry.PupillometrySessionModelData.update1(key_update)
                slack_utils.send_slack_error_pupillometry_notification(config.slack_webhooks_dict['automation_pipeline_error_notification'],\
                        update_value_dict['error_info'] ,session_check)
                continue
                

            # Get video location
            if status_update == config.status_update_idx['NEXT_STATUS']:
                pupillometry_dir = dj.config.get('custom', {}).get('pupillometry_root_data_dir',None)
                if pupillometry_dir is None:
                    raise Exception('pupillometry_root_data_dir not found in config, run initial_conf.py again')

                # Create output location
                pupillometry_processed_dir = pupillometry_dir[1]
                output_dir = pathlib.Path(pupillometry_processed_dir,pathlib.Path(session_check['remote_path_video_file']).parent)

                print(output_dir)

                #Find h5 files
                h5_files = glob.glob(str(output_dir) + '/*.h5')
                if len(h5_files) != 1:
                    update_value_dict['error_info']['error_message'] = 'Didn''t find any h5 files after deeplabcut analyze_video'
                    slack_utils.send_slack_error_pupillometry_notification(config.slack_webhooks_dict['automation_pipeline_error_notification'],\
                        update_value_dict['error_info'] ,session_check)
                    key_update['pupillometry_job_id'] = -1
                    pupillometry.PupillometrySessionModelData.update1(key_update)
                    continue
                else:
                    h5_files = h5_files[0]

                try:
                    pupil_data = PupillometryProcessingHandler.getPupilDiameter(h5_files)
                except Exception:
                    update_value_dict['error_info']['error_message'] = 'Could not get pupil diameter (check h5 or video file)'
                    slack_utils.send_slack_error_pupillometry_notification(config.slack_webhooks_dict['automation_pipeline_error_notification'],\
                        update_value_dict['error_info'] ,session_check)
                    key_update['pupillometry_job_id'] = -1
                    pupillometry.PupillometrySessionModelData.update1(key_update)
                    continue

                key_update['pupil_diameter'] = pupil_data
                print('key_update', key_update)
                pupillometry.PupillometrySessionModelData.update1(key_update)
                slack_utils.send_slack_pupillometry_update_notification(config.slack_webhooks_dict['automation_pipeline_update_notification'],\
                             'Pupillometry job finished', session_check)

            

if __name__ == '__main__':
    
    import time

    from scripts.conf_file_finding import try_find_conf_file
    try_find_conf_file()
    time.sleep(1)

    args = sys.argv[1:]
    args[1] = args[1] + '/config.yaml'
    print(args)
    #
    


    PupillometryProcessingHandler.analyze_videos_pupillometry(args[1], args[0], args[2])

