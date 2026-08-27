import datetime
import time

import datajoint as dj
import pandas as pd

import u19_pipeline.utils.slack_utils as su
import u19_pipeline.lab as lab

# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['dev_notifications']
}


def main_locked_tables_alert():

    # Source of the "Locked Tables Alert" Slack message: this raw MySQL
    # `SHOW OPEN TABLES` query (not a DataJoint table query) is the only
    # place `in_use` is read. A table is reported here whenever any open
    # connection is holding a lock on it (e.g. an in-flight transaction
    # from a `.populate()`/`.update1()` call), not necessarily a stuck one.
    locked_tables_query = 'show open tables where in_use > 0'
    conn = dj.conn()
    locked_tables_df = pd.DataFrame(conn.query(locked_tables_query, as_dict=True).fetchall())

    if locked_tables_df.shape[0] == 0:
        return
    else:
        locked_tables_df = locked_tables_df.head()
        locked_tables_df = locked_tables_df.drop('Name_locked',axis=1)
        locked_tables_df = su.format_df_for_slack_message(locked_tables_df)

        active_processes_df = get_active_processes(conn)
        active_processes_str = format_active_processes(active_processes_df)

        slack_json_message = slack_alert_message_format_locked_tables(locked_tables_df, active_processes_str)

        webhooks_list = su.get_webhook_list(slack_configuration_dictionary, lab)
        # Send alert
        for this_webhook in webhooks_list:
            su.send_slack_notification(this_webhook, slack_json_message)
            time.sleep(1)


def get_active_processes(conn):
    """Return all non-idle connections from `SHOW FULL PROCESSLIST`.

    `SHOW OPEN TABLES` reports which table is locked but has no
    connection/process id. `SHOW PROCESSLIST` has no field that reliably
    maps back to the schema/table a connection has locked either: its `db`
    column is only the connection's *current default* database (set by the
    last `USE`), which is frequently different from the database of a table
    the connection has open-cache-locked (e.g. via an explicit
    `LOCK TABLES other_db.table` or a query that just qualifies the table
    name). So instead of guessing a match and risking a wrong answer, list
    every active connection here so a human can cross-reference it against
    the locked tables above.

    Reliably attributing a specific locked table to a specific connection
    would require `performance_schema.metadata_locks` (joined to
    `processlist` on thread id), which is not enabled by default in
    MariaDB/MySQL and requires a server config change - out of scope here.
    """

    processlist_df = pd.DataFrame(conn.query('show full processlist', as_dict=True).fetchall())
    if processlist_df.shape[0] == 0:
        return processlist_df

    return processlist_df[processlist_df['Command'] != 'Sleep']


def format_active_processes(active_processes_df):
    if active_processes_df.shape[0] == 0:
        return 'No active (non-idle) connections found.'

    process_descriptions = []
    for _, this_process in active_processes_df.iterrows():
        query_info = (this_process['Info'] or '').strip().replace('\n', ' ')
        query_info = (query_info[:60] + '...') if len(query_info) > 60 else query_info
        db_name = this_process['db'] if pd.notna(this_process['db']) else '<none>'
        process_descriptions.append(
            'Id={} User={} Host={} DB={} Command={} Time={}s Query={}'.format(
                this_process['Id'], this_process['User'], this_process['Host'],
                db_name, this_process['Command'], this_process['Time'], query_info or '<none>'
            )
        )
    return '\n'.join(process_descriptions)


def slack_alert_message_format_locked_tables(locked_tables_df, active_processes_str):
    now = datetime.datetime.now()
    datestr = now.strftime("%d-%b-%Y %H:%M:%S")

    msep = dict()
    msep["type"] = "divider"

    # Title#
    m1 = dict()
    m1["type"] = "section"
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ":rotating_light: *Locked Tables Alert *"
    m1["text"] = m1_1

    # Locked tables
    m2 = dict()
    m2["type"] = "section"
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    m2_1["text"] = "*Locked tables:*" + "\n"
    m2_1["text"] += locked_tables_df
    m2["text"] = m2_1

    # Active connections, for cross-referencing against the locked tables above.
    # We cannot reliably tell which connection holds which table's lock (see
    # get_active_processes docstring), so all active connections are listed.
    m3 = dict()
    m3["type"] = "section"
    m3_1 = dict()
    m3_1["type"] = "mrkdwn"

    m3_1["text"] = "*Active connections (cross-reference to find the lock holder):*" + "\n"
    m3_1["text"] += "```" + active_processes_str + "```"
    m3["text"] = m3_1

    message = dict()
    message["blocks"] = [m1, msep, m2, msep, m3, msep]
    message["text"] = "Locked Tables Alert"

    return message
