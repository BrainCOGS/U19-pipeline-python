from unittest.mock import MagicMock, patch

import pytest

from u19_pipeline.alert_system.custom_alerts import disk_space_alert as dsa

TB = dsa.BYTES_PER_TB


def make_statvfs(f_frsize=1, f_blocks=0, f_bfree=0, f_bavail=0):
    return MagicMock(f_frsize=f_frsize, f_blocks=f_blocks, f_bfree=f_bfree, f_bavail=f_bavail)


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


class TestGetDiskUsage:

    def test_uses_bavail_not_bfree(self):
        # f_bfree (raw free) includes blocks reserved for root; f_bavail
        # (available to unprivileged users) excludes them. get_disk_usage
        # must report the user-available figure, i.e. use f_bavail.
        statvfs = make_statvfs(f_frsize=1024, f_blocks=1000, f_bfree=100, f_bavail=50)
        with patch.object(dsa.os, 'statvfs', return_value=statvfs):
            total_bytes, available_bytes = dsa.get_disk_usage('/some/path')

        assert available_bytes == 50 * 1024
        # total = used + available = (blocks - bfree) + bavail, in bytes,
        # NOT f_blocks * f_frsize (which would include the root reserve).
        assert total_bytes == ((1000 - 100) + 50) * 1024

    def test_propagates_oserror_for_missing_path(self):
        with patch.object(dsa.os, 'statvfs', side_effect=OSError('No such file or directory')), \
                pytest.raises(OSError):
            dsa.get_disk_usage('/not/mounted')


class TestGetLowDiskSpaceAlerts:

    def test_no_alert_when_space_is_plentiful(self):
        with patch.object(dsa, 'get_disk_usage', return_value=(1000 * TB, 900 * TB)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert alerts == []

    def test_alert_when_available_space_below_percent_threshold(self):
        # Small filesystem: 1% threshold (smaller than 5TB cap) is 1 TB available required.
        total = 100 * TB
        available = 0.5 * TB
        with patch.object(dsa, 'get_disk_usage', return_value=(total, available)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert len(alerts) == 1
        assert alerts[0]['path'] == '/some/path'
        assert alerts[0]['available_space(tb)'] == pytest.approx(0.5)

    def test_alert_when_available_space_below_absolute_cap(self):
        # Huge filesystem: 5TB cap applies (smaller than 1% of total).
        total = 10000 * TB
        available = 4 * TB
        with patch.object(dsa, 'get_disk_usage', return_value=(total, available)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert len(alerts) == 1

    def test_no_alert_exactly_at_threshold_boundary(self):
        # available_bytes == threshold_bytes should NOT trigger (strict less-than).
        total = 100 * TB
        threshold = dsa.get_free_space_threshold_bytes(total)
        with patch.object(dsa, 'get_disk_usage', return_value=(total, threshold)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert alerts == []

    def test_alert_just_below_threshold_boundary(self):
        total = 100 * TB
        threshold = dsa.get_free_space_threshold_bytes(total)
        with patch.object(dsa, 'get_disk_usage', return_value=(total, threshold - 1)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert len(alerts) == 1

    def test_zero_available_space(self):
        total = 100 * TB
        with patch.object(dsa, 'get_disk_usage', return_value=(total, 0)):
            alerts = dsa.get_low_disk_space_alerts(['/some/path'])
        assert len(alerts) == 1
        assert alerts[0]['available_space(tb)'] == 0
        assert alerts[0]['available_space(%)'] == 0

    def test_missing_or_unmounted_path_is_flagged(self):
        with patch.object(dsa, 'get_disk_usage', side_effect=OSError('No such file or directory')):
            alerts = dsa.get_low_disk_space_alerts(['/not/mounted'])
        assert len(alerts) == 1
        assert alerts[0]['path'] == '/not/mounted'
        assert 'not found' in alerts[0]['alert_message'] or 'not mounted' in alerts[0]['alert_message']

    def test_checks_all_configured_paths_independently(self):
        def fake_get_disk_usage(path):
            if path == '/low':
                return (100 * TB, 0)
            if path == '/missing':
                raise OSError('No such path')
            return (100 * TB, 99 * TB)

        with patch.object(dsa, 'get_disk_usage', side_effect=fake_get_disk_usage):
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
