"""Add recurring schedule rules to the scheduler schema.

1. Importing ``u19_pipeline.scheduler`` declares the new tables that do not exist
   yet: ``ScheduleRule`` (+ ``ScheduleRule.Day``), ``ScheduleRuleException`` and
   ``LabClosure``.
2. DataJoint never alters an existing table, so ``Schedule.rule_id`` (a nullable
   foreign key to ``ScheduleRule``) is added with plain SQL.

Run it as a user with CREATE/ALTER rights on the scheduler schema, against the test
prefix first::

    uv run python -m scripts.migrations.add_schedule_rules --dry-run
    uv run python -m scripts.migrations.add_schedule_rules

The script is idempotent: it skips the ALTER when ``rule_id`` already exists.
"""

from __future__ import annotations

import argparse

SCHEDULE_TABLE = "schedule"
RULE_TABLE = "#schedule_rule"


def build_alter_statements(database: str) -> list[str]:
    """SQL that adds ``rule_id`` to ``{database}.schedule``, matching DataJoint's FK style."""
    return [
        f"ALTER TABLE `{database}`.`{SCHEDULE_TABLE}` "
        "ADD COLUMN `rule_id` int DEFAULT NULL "
        "COMMENT 'rule that generated this row; NULL for manual or copy-forward rows'",
        f"ALTER TABLE `{database}`.`{SCHEDULE_TABLE}` "
        f"ADD FOREIGN KEY (`rule_id`) REFERENCES `{database}`.`{RULE_TABLE}` (`rule_id`) "
        "ON UPDATE CASCADE ON DELETE RESTRICT",
    ]


def column_exists_query(database: str) -> tuple[str, tuple[str, str, str]]:
    return (
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",
        (database, SCHEDULE_TABLE, "rule_id"),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="print the SQL instead of running it"
    )
    args = parser.parse_args(argv)

    from scripts.conf_file_finding import try_find_conf_file

    try_find_conf_file()
    import datajoint as dj

    database = dj.config["custom"]["database.prefix"] + "scheduler"
    statements = build_alter_statements(database)
    if args.dry_run:
        print(
            f"-- would declare ScheduleRule, ScheduleRule.Day, ScheduleRuleException, LabClosure in {database}"
        )
        print(";\n".join(statements) + ";")
        return

    import u19_pipeline.scheduler  # noqa: F401  (declares the new tables)

    connection = dj.conn()
    query, params = column_exists_query(database)
    if connection.query(query, args=params).fetchone()[0]:
        print(f"{database}.{SCHEDULE_TABLE}.rule_id already exists; nothing to alter.")
        return
    for statement in statements:
        print(statement)
        connection.query(statement)
    print("Done.")


if __name__ == "__main__":
    main()
