

import json
import sys
from datetime import datetime

import requests


def send_slack_notification(webhook_url, slack_json_message):

    byte_length = str(sys.getsizeof(slack_json_message))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    print(headers)
    print(slack_json_message)
    response = requests.post(webhook_url, data=json.dumps(slack_json_message), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

def send_slack_update_notification(webhook_url, base_message, session_info):

    now = datetime.now() 
    datestr = now.strftime('%d-%b-%Y %H:%M:%S')

    msep = dict()
    msep['type'] = "divider"

    #Title#
    m1 = dict()
    m1['type'] = 'section'
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ':white_check_mark: *Automation pipeline update* on ' + datestr + '\n\n'
    m1['text'] = m1_1

    #Info#
    m2 = dict()
    m2['type'] = 'section'
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"
    m2_1["text"] = '*' + base_message +'* \n' +\
        ' *recording_id* : ' + str(session_info['recording_id']) +'\n'+\
        ' *job_id* : ' + str(session_info['job_id']) +'\n'+\
        ' *data_path* : ' + session_info['recording_directory'] +'\n'+\
        ' *session_location* : ' + session_info['location'] + '\n'+\
        ' *modality* : ' + session_info['recording_modality']
    m2['text'] = m2_1

    message = dict()
    message['blocks'] = [m1,msep,m2]
    message['text'] = 'Automation pipeline update recording:' + str(session_info['recording_id'])

    print(message)

    send_slack_notification(webhook_url, message)


def send_slack_error_notification(webhook_url, error_info, session_info):

    now = datetime.now() 
    datestr = now.strftime('%d-%b-%Y %H:%M:%S')

    if 'job_id' not in session_info:
        session_info['job_id'] = 'Not a job'

    msep = dict()
    msep['type'] = "divider"

    #Title#
    m1 = dict()
    m1['type'] = 'section'
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ':rotating_light: *Automation pipeline error* on ' + datestr + '\n\n'
    m1['text'] = m1_1

    #Info#
    m2 = dict()
    m2['type'] = 'section'
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"
    m2_1["text"] = '*Automation pipeline failed for:* \n' +\
        ' *recording_id* : ' + str(session_info['recording_id']) +'\n'+\
        ' *job_id* : ' + str(session_info['job_id']) +'\n'+\
        ' *data_path* : ' + session_info['recording_directory'] +'\n'+\
        ' *session_location* : ' + session_info['location'] + '\n'+\
        ' *modality* : ' + session_info['recording_modality']
    m2['text'] = m2_1

    #Error#
    m3 = dict()
    m3['type'] = 'section'
    m3_1 = dict()
    m3_1["type"] = "mrkdwn"
    m3_1["text"] = '*Error info* \n' +\
        ' *error message* : ' + str(error_info['error_message']) +'\n'+\
        ' *error_stack* : ' + str(error_info['error_exception'])
    m3['text'] = m3_1

    message = dict()
    message['blocks'] = [m1,msep,m2,msep,m3]
    message['text'] = 'Automation pipeline error in recording:' + str(session_info['recording_id'])

    print(message)

    send_slack_notification(webhook_url, message)


def send_slack_error_pupillometry_notification(webhook_url, error_info, session_info):

    now = datetime.now() 
    datestr = now.strftime('%d-%b-%Y %H:%M:%S')

    if 'job_id' not in session_info:
        session_info['job_id'] = 'Not a job'

    msep = dict()
    msep['type'] = "divider"

    #Title#
    m1 = dict()
    m1['type'] = 'section'
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ':rotating_light: *Automation pipeline error* on ' + datestr + '\n\n'
    m1['text'] = m1_1

    #Info#
    m2 = dict()
    m2['type'] = 'section'
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"
    m2_1["text"] = '*Pupillometry pipeline failed for:* \n' +\
        ' *subject_fullname* : ' + str(session_info['subject_fullname']) +'\n'+\
        ' *session_date* : ' + str(session_info['session_date']) +'\n'+\
        ' *session_number* : ' + str(session_info['session_number']) +'\n'+\
        ' *path_video_file* : ' + session_info['remote_path_video_file']
    m2['text'] = m2_1

    #Error#
    m3 = dict()
    m3['type'] = 'section'
    m3_1 = dict()
    m3_1["type"] = "mrkdwn"
    m3_1["text"] = '*Error info* \n' +\
        ' *error message* : ' + str(error_info['error_message']) +'\n'+\
        ' *error_stack* : ' + str(error_info['error_exception'])
    m3['text'] = m3_1

    message = dict()
    message['blocks'] = [m1,msep,m2,msep,m3]
    message['text'] = 'Pupillometry pipeline error in session'

    print(message)

    send_slack_notification(webhook_url, message)

def send_slack_pupillometry_update_notification(webhook_url, base_message, session_info):

    now = datetime.now() 
    datestr = now.strftime('%d-%b-%Y %H:%M:%S')

    msep = dict()
    msep['type'] = "divider"

    #Title#
    m1 = dict()
    m1['type'] = 'section'
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ':white_check_mark: *Pupillometry pipeline update* on ' + datestr + '\n\n'
    m1['text'] = m1_1

    #Info#
    m2 = dict()
    m2['type'] = 'section'
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"
    m2_1["text"] = '*' + base_message +'* \n' +\
        ' *subject_fullname* : ' + str(session_info['subject_fullname']) +'\n'+\
        ' *session_date* : ' + str(session_info['session_date']) +'\n'+\
        ' *session_number* : ' + str(session_info['session_number']) +'\n'+\
        ' *path_video_file* : ' + session_info['remote_path_video_file']
    m2['text'] = m2_1

    message = dict()
    message['blocks'] = [m1,msep,m2]
    message['text'] = 'Pupillometry pipeline update'

    print(message)

    send_slack_notification(webhook_url, message)

'''
def format_error_slack_message()
        message = ("A Sample Message")
        title = (f"New Incoming Message :zap:")
        slack_data = {
            "username": "NotificationBot",
            "icon_emoji": ":satellite:",
            #"channel" : "#somerandomcahnnel",
            "attachments": [
                {
                    "color": "#9733EE",
                    "fields": [
                        {
                            "title": title,
                            "value": message,
                            "short": "false",
                        }
                    ]
                }
            ]
        }
'''