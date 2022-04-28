import datajoint as dj
from u19_pipeline import recording

schema = dj.schema(dj.config['custom']['database.prefix'] + 'ephys_recording')

# Declare upstream ephys table ---------------------------------------------------------
@schema
class EphysRecordingSession(dj.Computed):
    definition = """
    -> recording.Recording
    """

    @property
    def key_source(self):
        return recording.Recording & {'recording_modality': 'electrophysiology'}

    def make(self, key):
        self.insert1(key)