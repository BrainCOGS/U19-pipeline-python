import datetime
import os
import time

import u19_pipeline.lab as lab
import u19_pipeline.utils.slack_utils as su

# Slack Configuration dictionary
slack_configuration_dictionary = {
    'slack_notification_channel': ['dev_notifications']
}

# Paths monitored for low disk space
MONITORED_PATHS = [
    '/',
    '/mnt/cup/braininit',
    '/mnt/cup/u19_dj',
]

# A path is flagged once free space drops below whichever is smaller:
# 1% of the filesystem's total size, or this many bytes.
MAX_FREE_SPACE_THRESHOLD_TB = 5
BYTES_PER_TB = 1024 ** 4
PERCENT_FREE_SPACE_THRESHOLD = 0.01


def get_free_space_threshold_bytes(total_bytes):
    """
    Alert threshold for a filesystem of size `total_bytes`: the smaller of
    1% of total capacity or MAX_FREE_SPACE_THRESHOLD_TB.
    """
    return min(total_bytes * PERCENT_FREE_SPACE_THRESHOLD, MAX_FREE_SPACE_THRESHOLD_TB * BYTES_PER_TB)


def get_disk_usage(path):
    """
    Space available to unprivileged users on the filesystem containing
    `path`, matching what `df` reports (i.e. `f_bavail`, not the raw
    `f_bfree`, which can include blocks reserved for root).

    Returns (total_bytes, available_bytes), where total_bytes is likewise
    the denominator `df` uses: blocks in use plus blocks available to
    users (excludes blocks reserved for root, which `os.statvfs().f_blocks`
    would otherwise include).
    """

    vfs = os.statvfs(path)
    available_bytes = vfs.f_bavail * vfs.f_frsize
    used_bytes = (vfs.f_blocks - vfs.f_bfree) * vfs.f_frsize
    total_bytes = used_bytes + available_bytes

    return total_bytes, available_bytes


def get_low_disk_space_alerts(monitored_paths=MONITORED_PATHS):
    """
    Check each path in `monitored_paths` and return a list of alert dicts
    for any path that is unreachable or below its free-space threshold.
    """

    alert_rows = []
    for this_path in monitored_paths:
        try:
            total_bytes, available_bytes = get_disk_usage(this_path)
        except OSError:
            alert_rows.append({
                'alert_message': 'Could not check disk space (path not found or not mounted)',
                'path': this_path,
            })
            continue

        threshold_bytes = get_free_space_threshold_bytes(total_bytes)
        if available_bytes < threshold_bytes:
            alert_rows.append({
                'alert_message': 'Low disk space',
                'path': this_path,
                'available_space(tb)': round(available_bytes / BYTES_PER_TB, 2),
                'available_space(%)': round(100 * available_bytes / total_bytes, 2),
            })

    return alert_rows


def main_disk_space_alert():

    alert_rows = get_low_disk_space_alerts()

    if not alert_rows:
        return

    slack_json_message = slack_alert_message_format_disk_space(alert_rows)

    webhooks_list = su.get_webhook_list(slack_configuration_dictionary, lab)
    for this_webhook in webhooks_list:
        su.send_slack_notification(this_webhook, slack_json_message)
        time.sleep(1)


def slack_alert_message_format_disk_space(alert_rows):
    now = datetime.datetime.now()
    datestr = now.strftime("%d-%b-%Y %H:%M:%S")

    msep = dict()
    msep["type"] = "divider"

    # Title #
    m1 = dict()
    m1["type"] = "section"
    m1_1 = dict()
    m1_1["type"] = "mrkdwn"
    m1_1["text"] = ":rotating_light: *Low Disk Space Alert* on " + datestr + "\n\n"
    m1["text"] = m1_1

    # Info #
    m2 = dict()
    m2["type"] = "section"
    m2_1 = dict()
    m2_1["type"] = "mrkdwn"

    m2_1["text"] = ""
    for this_row in alert_rows:
        for key, value in this_row.items():
            m2_1["text"] += "*" + key + "* : " + str(value) + "\n"
        m2_1["text"] += "\n"
    m2["text"] = m2_1

    message = dict()
    message["blocks"] = [m1, msep, m2, msep]
    message["text"] = "Low Disk Space Alert"

    return message
