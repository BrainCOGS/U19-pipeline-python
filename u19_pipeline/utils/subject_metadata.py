from typing import Iterable

from u19_pipeline import lab, subject


def fetch_slack_handles_for_lab_managers_by_subject(subjects: list[str] | str) -> list[str]:
    """Returns the Slack handles of lab managers for the labs associated with the given subjects."""
    if isinstance(subjects, str):
        subjects = [subjects]
    sql_formatted_names = ", ".join([f"'{name}'" for name in subjects]) if subjects else "''"
    expanded_animal = (subject.Subject & f"subject_fullname in ({sql_formatted_names})") * lab.User() * lab.UserLab()
    labs = set(expanded_animal.fetch("lab"))

    sql_formatted_labs = ", ".join([f"'{lab}'" for lab in labs]) if labs else "''"
    lab_managers = (lab.LabManager() & f"lab in ({sql_formatted_labs})").proj(user_id="lab_manager") * lab.User()

    slack_tags = lab_managers.fetch("slack")
    return slack_tags


def fetch_slack_handles_for_lab_managers_by_user(user_ids: Iterable[str] | str) -> list[str]:
    """Returns the Slack handles of lab managers for the given user IDs."""

    if isinstance(user_ids, str):
        user_ids = [user_ids]

    sql_formatted_user_ids = ", ".join([f"'{user_id}'" for user_id in user_ids])
    lab_managers = (lab.LabManager() & f"user_id in ({sql_formatted_user_ids})").proj(
        user_id="lab_manager"
    ) * lab.User()

    slack_tags = lab_managers.fetch("slack")
    return slack_tags
