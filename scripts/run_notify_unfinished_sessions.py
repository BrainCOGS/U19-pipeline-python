
from notify_unfinished_sessions import send_unfinished_sessions
import datajoint as dj
import os


if __name__ == '__main__':

    dj.config['database.host'] = 'datajoint00.pni.princeton.edu'
    conn = dj.conn(host=dj.config['database.host'])
    send_unfinished_sessions()