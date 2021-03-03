from u19_pipeline.ephys_element import ephys_element


def populate(display_progress=True):

    populate_settings = {'display_progress': display_progress, 'reserve_jobs': False, 'suppress_errors': False}

    print('\n---- Populate ephys.EphysRecording ----')
    ephys_element.EphysRecording.populate(**populate_settings)

    print('\n---- Populate ephys.LFP ----')
    ephys_element.LFP.populate(**populate_settings)

    print('\n---- Populate ephys.Clustering ----')
    ephys_element.Clustering.populate(**populate_settings)

    print('\n---- Populate ephys.Waveform ----')
    ephys_element.Waveform.populate(**populate_settings)


if __name__ == '__main__':
    populate()
