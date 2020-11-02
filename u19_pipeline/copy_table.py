import datajoint as dj

import traceback
from u19_pipeline.temp import acquisition, behavior
from u19_pipeline import behavior as behavior_original
from u19_pipeline import subject
from tqdm import tqdm


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


def main():

    BEHAVIOR_TABLES = [
        # 'DataDirectory',
        # 'TowersSession',
        # 'TowersBlock',
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


if __name__ == '__main__':
    main()
