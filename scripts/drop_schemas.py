from u19_pipeline import (
    acquisition,
    behavior,
    ephys_pipeline,
    ephys_sync,
    imaging_pipeline,
    lab,
    recording,
    recording_process,
    reference,
    subject,
    task,
)
from u19_pipeline.ephys_pipeline import ephys_element, probe_element
from u19_pipeline.imaging_pipeline import imaging_element, scan_element

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