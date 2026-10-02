"""Nightly job: keep scheduler.Schedule's rolling window in line with ScheduleRule.

Run it after U19-pipeline-matlab's ``populate_schedule_for_tomorrow`` (which copies
only legacy, rule_id IS NULL, rows), so rule-generated rows and copied rows never
race for the same slot. Rows the generator could not place (a manual booking or a
second rule already holds the slot) are reported to Slack.
"""

import datajoint as dj

import u19_pipeline.utils.schedule_rules as sr

slack_configuration_dictionary = {
    'slack_notification_channel': ['rig_scheduling'],
}

MAX_CONFLICTS_LISTED = 15


def get_scheduler():
    return dj.create_virtual_module('scheduler', dj.config['custom']['database.prefix'] + 'scheduler')


def format_conflict_message(conflicts):
    """Slack payload listing slots a rule could not claim, or None when there are none."""
    if not conflicts:
        return None
    lines = []
    for c in sorted(conflicts, key=lambda c: (c.date, c.location, c.timeslot))[:MAX_CONFLICTS_LISTED]:
        holder = c.existing_subject or 'another rule'
        if c.existing_rule_id is not None:
            holder += f' (rule {c.existing_rule_id})'
        lines.append(f'• {c.date.isoformat()} {c.location} slot {c.timeslot}: rule {c.rule_id} blocked by {holder}')
    hidden = len(conflicts) - MAX_CONFLICTS_LISTED
    if hidden > 0:
        lines.append(f'_… and {hidden} more_')
    text = ':warning: *Recurring schedule conflicts*\n' + '\n'.join(lines)
    return {'text': 'Recurring schedule conflicts', 'blocks': [{'type': 'section', 'text': {'type': 'mrkdwn', 'text': text}}]}


def main_materialize_schedule(scheduler=None, lab=None, su=None, horizon_days=sr.DEFAULT_HORIZON_DAYS):
    scheduler = scheduler or get_scheduler()
    if not hasattr(scheduler, 'ScheduleRule'):
        print('scheduler.ScheduleRule does not exist yet (migration not run); nothing to do.')
        return None

    plan = sr.materialize_schedule(scheduler, horizon_days=horizon_days)
    print(f'inserted {len(plan.inserts)}, updated {len(plan.updates)}, deleted {len(plan.deletes)}, '
          f'conflicts {len(plan.conflicts)}')

    message = format_conflict_message(plan.conflicts)
    if message is not None:
        if lab is None or su is None:
            import u19_pipeline.lab as lab
            import u19_pipeline.utils.slack_utils as su
        for webhook in su.get_webhook_list(slack_configuration_dictionary, lab):
            su.send_slack_notification(webhook, message)
    return plan
