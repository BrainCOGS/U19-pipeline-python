import datajoint as dj

from u19_pipeline import acquisition

schema = dj.schema(dj.config['custom']['database.prefix'] + 'pupillometry')


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
class PupillometrySessionModel(dj.Manual):
    definition = """
    # Table for pupillometry sessions reference with model
    ->pupillometry.PupillometrySession
    model_id: int
    ---
    """


@schema
class PupillometrySessionModelData(dj.Imported):
    definition = """
    # Table for pupillometry data (pupil diameter)
    ->pupillometry.PupillometrySessionModel
    ---
    pupil_diameter:      longblob                       # array with pupil diameter for each video frame
    """

    def make(self, key):
        pass