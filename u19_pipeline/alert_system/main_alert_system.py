
import datetime
import importlib
import pkgutil
import time
import traceback

import u19_pipeline.alert_system.custom_alerts as ca
import u19_pipeline.lab as lab
import u19_pipeline.utils.slack_utils as su

# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['custom_alerts']
}

def main_alert_system():
    'Call main function of all alerts defined in "custom_alerts'

    all_alert_submodules = pkgutil.iter_modules(ca.__path__)

    for this_alert_submodule in all_alert_submodules:

        print('executing '+ this_alert_submodule.name+ " alert code")

        my_alert_module = importlib.import_module('u19_pipeline.alert_system.custom_alerts.'+this_alert_submodule.name)
        try:
            alert_df = my_alert_module.main()
            if hasattr(my_alert_module, 'slack_configuration_dictionary'):
                slack_dict = my_alert_module.slack_configuration_dictionary
            else:
                slack_dict = slack_configuration_dictionary

            if alert_df.shape[0] > 0:

                alert_dict = alert_df.to_dict('records')
                webhooks_list = []
                
                if 'slack_notification_channel' in slack_dict:
                    query_slack_webhooks = [{'webhook_name' : x} for x in slack_dict['slack_notification_channel']]
                    webhooks_list += (lab.SlackWebhooks & query_slack_webhooks).fetch('webhook_url').tolist()

                if 'slack_users_channel' in slack_dict:
                    query_slack_user_channels = [{'user_id' : x} for x in slack_dict['slack_users_channel']]
                    webhooks_list += (lab.User & query_slack_user_channels).fetch('slack_webhook').tolist()

                for this_alert_record in alert_dict:
                    slack_json_message = slack_alert_message_format(this_alert_record, this_alert_submodule.name)

                    for this_webhook in webhooks_list:
                        su.send_slack_notification(this_webhook, slack_json_message)
                        time.sleep(1)

        except Exception as e:
            dict_error = dict()
            dict_error['message'] = 'error while executing ' + this_alert_submodule.name + " alert code"
            dict_error['error_exception'] = (''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)))
            slack_json_message = slack_alert_message_format(dict_error, this_alert_submodule.name)
            webhook_custom = (lab.SlackWebhooks & "webhook_name='custom_alerts'").fetch('webhook_url').tolist()
            if webhook_custom:
                su.send_slack_notification(webhook_custom[0], slack_json_message)
            
        del my_alert_module

def slack_alert_message_format(alert_dictionaty, alert_module_name):

    now = datetime.datetime.now() 
    datestr = now.strftime('%d-%b-%Y %H:%M:%S')

    msep = dict()
    msep['type'] = "divider"

    #Title#
    m1 = dict()
    m1['type'] = 'section'
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ':rotating_light: *' +alert_module_name + '* on ' + datestr + '\n\n'
    m1['text'] = m1_1

    #Info#
    m2 = dict()
    m2['type'] = 'section'
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"
    
    m2_1["text"] = ''
    for key in alert_dictionaty:
        m2_1["text"] += '*' + key + '* : ' + str(alert_dictionaty[key]) + '\n'
    m2['text'] = m2_1

    message = dict()
    message['blocks'] = [m1,msep,m2,msep,msep]
    message['text'] = alert_module_name+ ' alert'

    return message
