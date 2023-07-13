import datajoint as dj
import pathlib
import deeplabcut

from u19_pipeline import acquisition, subject, recording
import u19_pipeline.automatic_job.params_config as config
import u19_pipeline.utils.dj_shortcuts as dj_short
import subprocess

schema = dj.schema(dj.config['custom']['database.prefix'] + 'pupillometry')


@schema
class PupillometrySession(dj.Imported):
    definition = """
    # Information of a pupillometry session
    -> acquisition.Session
    ---
    """
    
    @property
    def key_source(self):
        return acquisition.SessionVideo & {'video_type': 'pupillometry'}

    def make(self, key):
        pass


@schema
class PupillometrySyncBehavior(dj.Imported):
    definition = """
    # Matrix to sync behavior and pupillometry videos   
    -> pupillometry.PupillometrySession
    ---
    sync_video_frame_matrix:     longblob               # matrix with corresponding iteration for each video frame
    sync_behavior_matrix:        longblob               # matrix with corresponding video frame for each iteration
    """

    def make(self, key):
        pass 


@schema
class PupillometryModels(dj.Imported):
    definition = """
    # Table to store reference for each model
    model_id:          INT(11) AUTO_INCREMENT
    ---
    model_description: varchar(255)                 # description of the model
    model_path:        varchar(255)                 # description of this parameter
    """

    def make(self, key):
        pass

@schema
class PupillometryData(dj.Imported):
    definition = """
    # Table for pupillometry data (pupil diameter)
    ->pupillometry.PupillometrySession
    ---
    pupil_diameter:      longblob                       # array with pupil diameter for each video frame
    """

    def make(self, key):

        # Get video data information
        key_pupil = (acquisition.SessionVideo * schema.PupillometrySession & key).fetch(as_dict=True)[0]
        
        # Get model location
        model_key = (schema.PupillometryModels & "model_id = "+str(key_pupil['model_id'])).fetch(as_dict=True)[0]
        models_dir = dj.config.get('custom', {}).get('root_data_dir',None)
        if models_dir is None:
            raise('root_data_dir in not found in config, , run initial_conf.py again')
        models_dir = models_dir[0]
        configPath = pathlib.Path(models_dir, model_key['model_path'],'config.yaml')

        # Get video location
        pupillometry_dir = dj.config.get('custom', {}).get('pupillometry_root_data_dir',None)
        if pupillometry_dir is None:
            raise('pupillometry_root_data_dir not found in config, run initial_conf.py again')
        pupillometry_raw_dir = pupillometry_dir[0]
        videoPath = pathlib.Path(pupillometry_raw_dir, key_pupil['remote_path_video_file'])

        # Create output location
        pupillometry_processed_dir = pupillometry_dir[1]
        output_dir = pathlib.Path(pupillometry_processed_dir,pathlib.Path(key_pupil['remote_path_video_file']).parent)
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        deeplabcut.analyze_videos(configPath, videoPath, destfolder=output_dir)
        


        pass
