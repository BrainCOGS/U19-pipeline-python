
import datajoint as dj

def main_live_session_stats_deletion():

    acquisition = dj.create_virtual_module('acquisition', 'u19_acquisition')

    old_session_stats_query = "current_datetime < NOW() - INTERVAL 15 DAY"

    order_cols = "'subject_fullname', 'session_date', 'session_number', 'trial_idx'"
    connection = acquisition.LiveSessionStats.connection
    with connection.transaction:
        old_session_stats = (acquisition.LiveSessionStats & old_session_stats_query).fetch(as_dict=True, order_by=[order_cols])
        (acquisition.HistoricSessionStats).insert(old_session_stats, skip_duplicates=True)
        (acquisition.LiveSessionStats & old_session_stats_query).delete(safemode=False)

