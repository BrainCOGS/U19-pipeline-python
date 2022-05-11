import os
if os.path.basename(os.getcwd())=='imaging_element': os.chdir('../..')

from u19_pipeline import lab, subject, acquisition, behavior, task, reference, recording, recording_process, ephys_pipeline, imaging_pipeline, ephys_sync
from u19_pipeline.imaging_pipeline import scan_element, imaging_element
from u19_pipeline.ephys_pipeline import probe_element, ephys_element

behavior.schema.drop()
recording_process.schema.drop()
imaging_element.schema.drop()
scan_element.schema.drop()
ephys_sync.schema.drop()
ephys_element.schema.drop()
probe_element.schema.drop()
imaging_pipeline.schema.drop()
ephys_pipeline.schema.drop()
recording.schema.drop()
acquisition.schema.drop()
subject.schema.drop()
lab.schema.drop()
reference.schema.drop()
task.schema.drop()