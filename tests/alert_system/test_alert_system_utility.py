from unittest.mock import MagicMock

from u19_pipeline.alert_system import alert_system_utility as asu


class TestRunCronjobAlertJob:

    def test_successful_job_does_not_notify_slack(self):
        job_function = MagicMock()
        su = MagicMock()

        asu.run_cronjob_alert_job('some_job', job_function, lab=MagicMock(), su=su, slack_configuration_dictionary={})

        job_function.assert_called_once()
        su.get_webhook_list.assert_not_called()
        su.send_slack_notification.assert_not_called()

    def test_failing_job_notifies_slack_and_does_not_raise(self):
        def job_function():
            raise ValueError('boom')

        su = MagicMock()
        su.get_webhook_list.return_value = ['hook1', 'hook2']

        # Should not raise, even though job_function raises.
        asu.run_cronjob_alert_job('some_job', job_function, lab=MagicMock(), su=su, slack_configuration_dictionary={})

        assert su.send_slack_notification.call_count == 2

    def test_failing_job_does_not_prevent_next_job_from_running(self):
        def failing_job():
            raise RuntimeError('boom')

        next_job = MagicMock()
        su = MagicMock()
        su.get_webhook_list.return_value = []

        asu.run_cronjob_alert_job('failing_job', failing_job, lab=MagicMock(), su=su, slack_configuration_dictionary={})
        asu.run_cronjob_alert_job('next_job', next_job, lab=MagicMock(), su=su, slack_configuration_dictionary={})

        next_job.assert_called_once()

    def test_slack_notification_failure_does_not_raise(self):
        def failing_job():
            raise RuntimeError('boom')

        su = MagicMock()
        su.get_webhook_list.side_effect = Exception('slack lookup failed')

        # Should not raise even though the Slack notification path itself fails.
        asu.run_cronjob_alert_job('failing_job', failing_job, lab=MagicMock(), su=su, slack_configuration_dictionary={})

    def test_error_message_included_in_slack_payload(self):
        def failing_job():
            raise ValueError('specific error text')

        su = MagicMock()
        su.get_webhook_list.return_value = ['hook1']

        asu.run_cronjob_alert_job('some_job', failing_job, lab=MagicMock(), su=su, slack_configuration_dictionary={})

        sent_message = su.send_slack_notification.call_args[0][1]
        block_text = sent_message['blocks'][0]['text']['text']
        assert 'specific error text' in block_text
        assert 'some_job' in block_text
