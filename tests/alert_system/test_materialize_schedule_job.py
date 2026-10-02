import datetime
import pathlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from u19_pipeline.alert_system.schedule_rules import materialize_schedule_job as msj
from u19_pipeline.utils import schedule_rules as sr

D = datetime.date(2024, 1, 2)


def conflict(**overrides):
    fields = dict(
        date=D,
        location="rig1",
        timeslot=2,
        rule_id=7,
        existing_subject="manual_mouse",
        existing_rule_id=None,
    )
    fields.update(overrides)
    return sr.Conflict(**fields)


class TestFormatConflictMessage:
    def test_no_conflicts_no_message(self):
        assert msj.format_conflict_message([]) is None

    def test_manual_booking_named(self):
        text = msj.format_conflict_message([conflict()])["blocks"][0]["text"]["text"]
        assert "2024-01-02 rig1 slot 2: rule 7 blocked by manual_mouse" in text

    def test_other_rule_named(self):
        text = msj.format_conflict_message(
            [conflict(existing_subject=None, existing_rule_id=3)]
        )["blocks"][0]["text"]["text"]
        assert "blocked by another rule (rule 3)" in text

    def test_long_lists_are_truncated(self):
        many = [conflict(timeslot=i) for i in range(msj.MAX_CONFLICTS_LISTED + 4)]
        text = msj.format_conflict_message(many)["blocks"][0]["text"]["text"]
        assert text.count("•") == msj.MAX_CONFLICTS_LISTED
        assert "and 4 more" in text

    def test_exactly_at_limit_has_no_overflow_line(self):
        text = msj.format_conflict_message(
            [conflict(timeslot=i) for i in range(msj.MAX_CONFLICTS_LISTED)]
        )["blocks"][0]["text"]["text"]
        assert "more" not in text


class TestMainMaterializeSchedule:
    def test_noop_before_migration(self):
        scheduler = SimpleNamespace(Schedule=MagicMock())  # no ScheduleRule attribute
        assert msj.main_materialize_schedule(scheduler=scheduler) is None
        scheduler.Schedule.insert.assert_not_called()

    def test_conflicts_are_sent_to_every_webhook(self):
        su = MagicMock()
        su.get_webhook_list.return_value = ["a", "b"]
        plan = sr.MaterializationPlan(conflicts=[conflict()])
        with patch.object(sr, "materialize_schedule", return_value=plan):
            assert (
                msj.main_materialize_schedule(
                    scheduler=MagicMock(), lab=MagicMock(), su=su
                )
                is plan
            )
        assert su.send_slack_notification.call_count == 2

    def test_clean_run_sends_nothing(self):
        su = MagicMock()
        with patch.object(
            sr, "materialize_schedule", return_value=sr.MaterializationPlan()
        ):
            msj.main_materialize_schedule(scheduler=MagicMock(), lab=MagicMock(), su=su)
        su.get_webhook_list.assert_not_called()

    def test_horizon_is_passed_through(self):
        with patch.object(
            sr, "materialize_schedule", return_value=sr.MaterializationPlan()
        ) as mat:
            msj.main_materialize_schedule(
                scheduler=MagicMock(), lab=MagicMock(), su=MagicMock(), horizon_days=3
            )
        assert mat.call_args.kwargs["horizon_days"] == 3


def test_schedule_check_alert_uses_configured_prefix():
    """Regression: the alert hardcoded u19_scheduler, so it always read production."""
    source = (
        pathlib.Path(__file__).resolve().parents[2]
        / "u19_pipeline/alert_system/schedule_check_alert/schedule_check_alert.py"
    ).read_text()
    assert '"u19_scheduler"' not in source
    assert "database.prefix" in source
