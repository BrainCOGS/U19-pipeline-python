from unittest.mock import patch

import pytest

from u19_pipeline.alert_system.custom_alerts import disk_space_alert as dsa

TB = dsa.BYTES_PER_TB


class TestGetFreeSpaceThresholdBytes:

    def test_percent_threshold_wins_on_large_filesystem(self):
        # 1% of 1000 TB = 10 TB, which is larger than the 5 TB cap, so the
        # cap (the smaller of the two) should be returned.
        total_bytes = 1000 * TB
        assert dsa.get_free_space_threshold_bytes(total_bytes) == 5 * TB

    def test_absolute_cap_wins_on_small_filesystem(self):
        # 1% of 100 TB = 1 TB, smaller than the 5 TB cap.
        total_bytes = 100 * TB
        assert dsa.get_free_space_threshold_bytes(total_bytes) == pytest.approx(1 * TB)

    def test_zero_total_bytes(self):
        assert dsa.get_free_space_threshold_bytes(0) == 0


class TestGetLowDiskSpaceAlerts:

    def test_no_alert_when_space_is_plentiful(self):
        with patch.object(dsa.shutil, 'disk_usage', return_value=(1000 * TB, 0, 900 * TB)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert alerts == []

    def test_alert_when_free_space_below_percent_threshold(self):
        # Small filesystem: 1% threshold (smaller than 5TB cap) is 1 TB free required.
        total = 100 * TB
        free = 0.5 * TB
        with patch.object(dsa.shutil, 'disk_usage', return_value=(total, 0, free)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert len(alerts) == 1
        assert alerts[0]['path'] == '/some/path'
        assert alerts[0]['free_space(tb)'] == pytest.approx(0.5)

    def test_alert_when_free_space_below_absolute_cap(self):
        # Huge filesystem: 5TB cap applies (smaller than 1% of total).
        total = 10000 * TB
        free = 4 * TB
        with patch.object(dsa.shutil, 'disk_usage', return_value=(total, 0, free)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert len(alerts) == 1

    def test_no_alert_exactly_at_threshold_boundary(self):
        # free_bytes == threshold_bytes should NOT trigger (strict less-than).
        total = 100 * TB
        threshold = dsa.get_free_space_threshold_bytes(total)
        with patch.object(dsa.shutil, 'disk_usage', return_value=(total, 0, threshold)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert alerts == []

    def test_alert_just_below_threshold_boundary(self):
        total = 100 * TB
        threshold = dsa.get_free_space_threshold_bytes(total)
        with patch.object(dsa.shutil, 'disk_usage', return_value=(total, 0, threshold - 1)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert len(alerts) == 1

    def test_zero_free_space(self):
        total = 100 * TB
        with patch.object(dsa.shutil, 'disk_usage', return_value=(total, 0, 0)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert len(alerts) == 1
        assert alerts[0]['free_space(tb)'] == 0
        assert alerts[0]['free_space(%)'] == 0

    def test_missing_or_unmounted_path_is_flagged(self):
        with patch.object(dsa.shutil, 'disk_usage', side_effect=OSError('No such file or directory')):
            alerts = dsa.get_low_disk_space_alerts(['/not/mounted'])
        assert len(alerts) == 1
        assert alerts[0]['path'] == '/not/mounted'
        assert 'not found' in alerts[0]['alert_message'] or 'not mounted' in alerts[0]['alert_message']

    def test_checks_all_configured_paths_independently(self):
        def fake_disk_usage(path):
            if path == '/low':
                return (100 * TB, 0, 0)
            if path == '/missing':
                raise OSError('No such path')
            return (100 * TB, 0, 99 * TB)

        with patch.object(dsa.shutil, 'disk_usage', side_effect=fake_disk_usage):
            alerts = dsa.get_low_disk_space_alerts(['/low', '/missing', '/plenty'])

        flagged_paths = {row['path'] for row in alerts}
        assert flagged_paths == {'/low', '/missing'}

    def test_default_monitored_paths_include_required_mounts(self):
        assert dsa.MONITORED_PATHS == ['/', '/mnt/cup/braininit', '/mnt/cup/u19_dj']


class TestMainDiskSpaceAlert:

    def test_no_slack_call_when_no_alerts(self):
        with patch.object(dsa, 'get_low_disk_space_alerts', return_value=[]), \
                patch.object(dsa.su, 'get_webhook_list') as mock_get_webhooks, \
                patch.object(dsa.su, 'send_slack_notification') as mock_send:
            dsa.main_disk_space_alert()

        mock_get_webhooks.assert_not_called()
        mock_send.assert_not_called()

    def test_sends_to_each_webhook_when_alerts_present(self):
        fake_alerts = [{'alert_message': 'Low disk space', 'path': '/'}]
        with patch.object(dsa, 'get_low_disk_space_alerts', return_value=fake_alerts), \
                patch.object(dsa.su, 'get_webhook_list', return_value=['hook1', 'hook2']) as mock_get_webhooks, \
                patch.object(dsa.su, 'send_slack_notification') as mock_send, \
                patch.object(dsa.time, 'sleep'):
            dsa.main_disk_space_alert()

        mock_get_webhooks.assert_called_once_with(dsa.slack_configuration_dictionary, dsa.lab)
        assert mock_send.call_count == 2
        mock_send.assert_any_call('hook1', mock_send.call_args_list[0][0][1])
        mock_send.assert_any_call('hook2', mock_send.call_args_list[1][0][1])
