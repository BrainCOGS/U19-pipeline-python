from u19_pipeline.imaging_pipeline import scan_element, imaging_element


def run(display_progress=True):

    populate_settings = {'display_progress': display_progress, 'reserve_jobs': False, 'suppress_errors': False}

    print('\n---- Populate imported and computed tables ----')
    scan_element.ScanInfo.populate(**populate_settings)

    imaging_element.Processing.populate(**populate_settings)

    imaging_element.MotionCorrection.populate(**populate_settings)

    imaging_element.Segmentation.populate(**populate_settings)

    imaging_element.Fluorescence.populate(**populate_settings)

    imaging_element.Activity.populate(**populate_settings)

    print('\n---- Successfully completed workflow_imaging/process.py ----')


if __name__ == '__main__':
    run()