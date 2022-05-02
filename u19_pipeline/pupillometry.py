import datajoint as dj

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
    # Table for pupillometry data (pupil diameter)
    ->pupillometry.PupillometrySession
    ---
    pupil_diameter:      longblob                       # array with pupil diameter for each video frame
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
        pass
