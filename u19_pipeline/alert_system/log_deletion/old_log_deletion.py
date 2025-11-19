
import pandas as pd
import pathlib
import re
import datetime


def extract_filename(x):
    return x.name


def main_old_log_deletion():

    DIRECTORY_PATH = pathlib.Path('/home/u19prod@pu.win.princeton.edu/log')
    date_pattern = r"_\d{8}_"
    reference_date = datetime.date.today() - datetime.timedelta(days=10)

    files = [p for p in DIRECTORY_PATH.rglob("*") if p.is_file() and re.search(date_pattern, p.name)]


    files_df = pd.DataFrame(files, columns=['filepaths'])
    files_df['filename'] = files_df['filepaths'].apply(extract_filename)
    files_df['exctract_date'] = files_df['filename'].str.extract(r'(\d{8,' + str(8) + r'})')
    files_df['exctract_date'] = pd.to_datetime(files_df['exctract_date'], format='%Y%m%d')
    files_df['before_ref_date'] = files_df['exctract_date'] <= pd.Timestamp(reference_date)

    files_for_deletion = files_df.loc[files_df['before_ref_date']==True, 'filepaths'].to_list()

    for file_path in files_for_deletion:
        if file_path.is_file():  # Check if it's actually a file before attempting to delete
            file_path.unlink()