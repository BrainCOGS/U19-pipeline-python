import datajoint as dj

dj.config['database.host'] = 'datajoint00.pni.princeton.edu'
dj.config['database.user'] = 'shans'
dj.config['database.password'] = 'Linhuiyin8*0823'


import traceback
from u19_pipeline.temp import acquisition, behavior, imaging, meso, meso_analysis
from u19_pipeline import subject
from tqdm import tqdm


behavior_original = dj.create_virtual_module(
    'behavior_original', 'u19_behavior')

meso_original = dj.create_virtual_module(
    'meso_original', 'u19_meso'
)

meso_analysis_original = dj.create_virtual_module(
    'meso_analysis_original', 'u19_meso_analysis'
)

imaging_original = dj.create_virtual_module(
    'imaging_original', 'u19_imaging'
)


def copy_table(target_schema, src_schema, table_name, **kwargs):

    if '.' in table_name:
        attrs = table_name.split('.')

        target_table = target_schema
        src_table = src_schema
        for a in attrs:
            target_table = getattr(target_table, a)
            src_table = getattr(src_table, a)
    else:
        target_table = getattr(target_schema, table_name)
        src_table = getattr(src_schema, table_name)

    pk = src_table.heading.primary_key
    if 'session_number' in pk:
        q_insert = (src_table & acquisition.SessionStarted.proj()) - \
            target_table.proj()
    else:
        q_insert = src_table - target_table.proj()

    try:
        target_table.insert(q_insert, skip_duplicates=True, **kwargs)

    except Exception:
        for t in (q_insert).fetch(as_dict=True):
            try:
                target_table.insert1(t, skip_duplicates=True, **kwargs)
            except Exception:
                print("Error when inserting {}".format(t))
                traceback.print_exc()


def copy_behavior_tables():

    BEHAVIOR_TABLES = [
        'DataDirectory',
        'TowersSession',
        'TowersBlock',
        'TowersBlock.Trial',
        'TowersBlockTrialVideo',
        'TowersSubjectCumulativePsych',
        'TowersSessionPsych',
    ]

    for table in BEHAVIOR_TABLES:

        print(f'Copying table {table}')
        if '.' in table:
            if table == 'TowersBlock.Trial':
                for subj in tqdm((subject.Subject & behavior.TowersBlock).fetch('KEY')):
                    behavior.TowersBlock.Trial.insert(
                        behavior_original.TowersBlock.Trial & subj,
                        skip_duplicates=True)
            else:
                if table == 'TowersBlockVideo':
                    for subj in tqdm((subject.Subject & behavior.TowersBlock).fetch('KEY')):
                        behavior.TowersBlockTrialVideo.insert(
                            behavior_original.TowersBlockTrialVideo & subj & behavior.TowersBlock,
                            skip_duplicates=True)
                else:
                    copy_table(behavior, behavior_original, table)
        else:
            copy_table(behavior, behavior_original, table,
                       allow_direct_insert=True)


def copy_imaging_tables():

    IMAGING_TABLES = [
        'Scan',
        'ScanInfo',
        'FieldOfView',
        'FieldOfView.File',
        'McMethod',
        'McParameter',
        'McParameterSet',
        'McParameterSet.Parameter',
        'MotionCorrection',
        'SegmentationMethod',
        'SegParameter',
        'SegParameterSet',
        'SegParameterSet.Parameter',
        'Segmentation',
        'Segmentation.Roi',
        'Segmentation.RoiMorphologyAuto',
        'Segmentation.Chunks',
        'Segmentation.Background',
        'Trace'
    ]

    for table in IMAGING_TABLES:

        print(f'Copying table {table}...')

        if '.' in table:
            copy_table(imaging, imaging_original, table)

        else:
            temp_table = getattr(imaging, table)
            if isinstance(temp_table, dj.Lookup) or \
                    isinstance(temp_table, dj.Manual):
                copy_table(imaging, imaging_original, table)
            else:
                copy_table(imaging, imaging_original, table,
                           allow_direct_insert=True)


def copy_meso_tables():

    MESO_TABLES = [
        'Scan',
        'ScanInfo',
        'FieldOfView',
        'FieldOfView.File',
        'SyncImagingBehavior',
        'MotionCorrectionMethod',
        'McParameter',
        'McParameterSet',
        'McParameterSet.Parameter',
        'MotionCorrectionWithinFile',
        'MotionCorrectionAcrossFiles',
        'MotionCorrection',
        'SegmentationMethod',
        'SegParameter',
        'SegParameterSet',
        'SegParameterSet.Parameter',
        'Segmentation',
        'Segmentation.Roi',
        'Segmentation.RoiMorphologyAuto',
        'Segmentation.Chunks',
        'Segmentation.Background',
        'SegmentationRoiMorphologyManual'
    ]

    for table in MESO_TABLES:

        print(f'Copying table {table}...')

        if '.' in table:
            copy_table(meso, meso_original, table)

        else:
            temp_table = getattr(meso, table)
            if isinstance(temp_table, dj.Lookup) or \
                    isinstance(temp_table, dj.Manual):
                copy_table(meso, meso_original, table)
            else:
                copy_table(meso, meso_original, table,
                           allow_direct_insert=True)


def copy_meso_analysis_tables():

    MESO_ANALYSIS_TABLES = [
        'TrialSelectionParams',
        'BinningParameters',
        'StandardizedTime',
        'Trialstats',
        'BinnedBehavior',
        'TrialSelectionParameters',
        'BinnedTrace'
    ]

    for table in MESO_ANALYSIS_TABLES:

        print(f'Copying table {table}...')

        if '.' in table:
            copy_table(meso_analysis, meso_analysis_original, table)

        else:
            temp_table = getattr(meso_analysis, table)
            if isinstance(temp_table, dj.Lookup) or \
                    isinstance(temp_table, dj.Manual):
                copy_table(meso_analysis, meso_analysis_original, table)
            else:
                copy_table(meso_analysis, meso_analysis_original, table,
                           allow_direct_insert=True)


def main():

    copy_behavior_tables()
    copy_imaging_tables()
    copy_meso_tables()
    copy_meso_analysis_tables()


if __name__ == '__main__':
    main()
