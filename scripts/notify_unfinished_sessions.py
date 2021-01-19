
import datajoint as dj
import pandas as pd
import numpy as np
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from cryptography.fernet import Fernet
import os


def send_unfinished_sessions():
    """
    Send an email with information about unfinished sessions during the day
    """

    vrrig_mail = 'vrrigs.bi.pni@gmail.com'

    missing_sessions_df = get_unfinished_sessions()
    status, server = login_into_smtp(vrrig_mail)

    if status:
        send_email_users(server, missing_sessions_df, vrrig_mail)
        server.close()


def get_unfinished_sessions():
    """
    Get a dataframe with all information about yesterday's unfinished sessions
    """
    acquisition = dj.create_virtual_module('acquisition', 'u19_acquisition')
    subject = dj.create_virtual_module('subject', 'u19_subject')
    lab = dj.create_virtual_module('lab', 'u19_lab')

    is_finished = "is_finished = 1"
    session_date = 'session_date = "' + (datetime.today() - timedelta(1)).strftime('%Y-%m-%d') + '"'

    # Merge Subject, user & sessions information
    min_subject = subject.Subject.proj('subject_fullname', 'user_id')
    min_user = lab.User.proj('user_id', 'email', 'full_name')
    missing_sessions = acquisition.SessionStarted & is_finished & session_date

    missing_sessions = missing_sessions * min_subject * min_user

    # Fetch it to Dataframe
    missing_sessions_df = pd.DataFrame(missing_sessions.fetch())
    missing_sessions_df = missing_sessions_df.drop(['remote_path_behavior_file', 'is_finished'], axis=1)

    return missing_sessions_df


def send_email_users(server, missing_sessions_df, vrrig_mail):
    """
    Send emails to users with unfinished sessions

    :param server: smtp server to send emails
    :param missing_sessions_df: dataframe with unfinished sessions info
    :param vrrig_mail: vrrig "official" email 
    """

    to_mail = 'alvalunasan@gmail.com'

    html_body_skel = """<p style="color:black;">{0}<br><br>
                        The following sessions were not finished properly:
                        <br><br> </p>"""
    html_df_skel = """<html><head></head><body>{0}</body></html>"""

    mail_users = set(missing_sessions_df['email'].to_list())

    for i in mail_users:

        # Filter sessions for current user
        user_sessions = missing_sessions_df.loc[missing_sessions_df['email'] == i, :]
        user_name = user_sessions['full_name'].values[0]
        user_id = user_sessions['user_id'].values[0]
        user_sessions = user_sessions.drop(['email', 'full_name', 'user_id'], axis=1)
        user_sessions = user_sessions.sort_values(by=['session_start_time'])

        # Create mail subject
        date = user_sessions['session_date'].values[0].strftime('%Y-%m-%d')
        mail_subject = 'VRRigs sessions not finished properly for ' + user_id + ' on ' + date

        msg = MIMEMultipart()
        msg['Subject'] = mail_subject
        msg['From'] = vrrig_mail
        msg['To'] = to_mail

        body = html_body_skel.format(user_name)
        body = MIMEText(body, 'html')  # convert the body to a MIME compatible string
        msg.attach(body)  # attach it to your main message

        # Format dataframe as html
        styled_df = html_style_basic(user_sessions, index=False)
        html = html_df_skel.format(styled_df)
        part1 = MIMEText(html, 'html')
        msg.attach(part1)

        try:
            server.sendmail(msg['From'], msg['To'], msg.as_string())

        except:
            print('Could not send email to user' + user_id)


def login_into_smtp(email):
    """
    Returns smtp server to send mails with given email

    :param email: email to loginto
    """

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    with open(os.path.join(__location__, 'aux_files', 'fernet.bin'), 'rb') as file_object:
        for line in file_object:
            fernet = line

    with open(os.path.join(__location__, 'aux_files', 'vrrigs.bin'), 'rb') as file_object:
        for line in file_object:
            crypt = line

    cipher_suite = Fernet(fernet)

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(email, bytes(cipher_suite.decrypt(crypt)).decode("utf-8"))
        return True, server
    except:
        print('Could not log in smtplib')
        return False, []


def html_style_basic(df, index=True):
    """
    Apply simple style to a dataframe html table
    """

    x = df.to_html(index=index)
    x = x.replace('<table border="1" class="dataframe">',
                  '<table style="border-collapse: collapse; border-spacing: 0; width: 98%;">')
    x = x.replace('<th>',
                  '<th style="text-align: left; padding: 3px; border-left: 1px solid #777777; color:black" '
                  'align="left">')
    x = x.replace('<td>',
                  '<td style="text-align: left; padding: 3px; border-left: 1px solid #777777; '
                  'border-right: 1px solid #777777; color:black" align="left">')
    x = x.replace('<tr style="text-align: right;">', '<tr>')

    x = x.split()
    count = 2
    index = 0
    for i in x:
        if '<tr>' in i:
            count += 1
            if count % 2 == 0:
                x[index] = x[index].replace('<tr>', '<tr style="background-color: #f2f2f2;" bgcolor="#f2f2f2">')
        index += 1
    return ' '.join(x)
