from u19_pipeline.imaging_element import scan_element, imaging_element


def populate(display_progress=True):

    populate_settings = {'display_progress': display_progress, 'reserve_jobs': False, 'suppress_errors': False}

    print('\n---- Populate scan.ScanInfo ----')
    scan_element.ScanInfo.populate(**populate_settings)

    print('\n---- Populate imaging.Processing ----')
    imaging_element.Processing.populate(**populate_settings)

    print('\n---- Populate imaging.MotionCorrection ----')
    imaging_element.MotionCorrection.populate(**populate_settings)

    print('\n---- Populate imaging.Segmentation ----')
    imaging_element.Segmentation.populate(**populate_settings)

    print('\n---- Populate imaging.MaskClassification ----')
    imaging_element.MaskClassification.populate(**populate_settings)

    print('\n---- Populate imaging.Fluorescence ----')
    imaging_element.Fluorescence.populate(**populate_settings)

    print('\n---- Populate imaging.Activity ----')
    imaging_element.Activity.populate(**populate_settings)

    print('\n---- Successfully completed workflow_imaging/populate.py ----')


if __name__ == '__main__':
    populate()