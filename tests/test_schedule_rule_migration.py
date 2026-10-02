"""Static checks on the ScheduleRule schema and its migration. No database needed."""

import ast
import pathlib
import re

from scripts.migrations import add_schedule_rules as migration

SCHEDULER_PY = (
    pathlib.Path(__file__).resolve().parents[1] / "u19_pipeline" / "scheduler.py"
)


def table_definitions() -> dict[str, str]:
    """Map 'Class' and 'Class.Part' to their DataJoint definition strings without importing DataJoint."""
    found = {}

    def visit(node, prefix=""):
        for child in node.body:
            if isinstance(child, ast.ClassDef):
                name = prefix + child.name
                for stmt in child.body:
                    if isinstance(stmt, ast.Assign) and any(
                        isinstance(t, ast.Name) and t.id == "definition"
                        for t in stmt.targets
                    ):
                        found[name] = ast.literal_eval(stmt.value)
                visit(child, name + ".")

    visit(ast.parse(SCHEDULER_PY.read_text()))
    return found


def attribute_lines(definition: str) -> list[str]:
    return [
        line.split("#")[0].strip()
        for line in definition.splitlines()
        if line.split("#")[0].strip()
    ]


class TestDefinitions:
    def test_new_tables_declared(self):
        defs = table_definitions()
        for name in (
            "ScheduleRule",
            "ScheduleRule.Day",
            "ScheduleRuleException",
            "LabClosure",
        ):
            assert name in defs

    def test_rule_declared_before_schedule(self):
        # DataJoint resolves "-> ScheduleRule" from module context at declaration time.
        names = list(table_definitions())
        assert names.index("ScheduleRule") < names.index("Schedule")

    def test_schedule_rule_id_is_nullable_secondary(self):
        lines = attribute_lines(table_definitions()["Schedule"])
        divider = lines.index("---")
        assert "-> [nullable] ScheduleRule" in lines[divider:]

    def test_schedule_primary_key_unchanged(self):
        lines = attribute_lines(table_definitions()["Schedule"])
        assert lines[: lines.index("---")] == [
            "date                         : date",
            "-> lab.Location",
            "timeslot                     : int",
        ]

    def test_weekday_enum_matches_python_constants(self):
        from u19_pipeline.utils.schedule_rules import WEEKDAY_NAMES

        enum = re.search(
            r"enum\(([^)]*)\)", table_definitions()["ScheduleRule.Day"]
        ).group(1)
        assert tuple(v.strip(" '") for v in enum.split(",")) == WEEKDAY_NAMES

    def test_exception_actions_match_python_constants(self):
        from u19_pipeline.utils.schedule_rules import EXCEPTION_ACTIONS

        enum = re.search(
            r"enum\(([^)]*)\)", table_definitions()["ScheduleRuleException"]
        ).group(1)
        assert tuple(v.strip(" '") for v in enum.split(",")) == EXCEPTION_ACTIONS

    def test_rule_statuses_match_python_constants(self):
        from u19_pipeline.utils.schedule_rules import RULE_STATUSES

        enum = re.search(
            r"status.*enum\(([^)]*)\)", table_definitions()["ScheduleRule"]
        ).group(1)
        assert tuple(v.strip(" '") for v in enum.split(",")) == RULE_STATUSES

    def test_rule_payload_covers_schedule_secondaries(self):
        """Every Schedule secondary attribute must come from the rule, or generated rows would be incomplete."""
        from u19_pipeline.utils.schedule_rules import PAYLOAD_KEYS

        lines = attribute_lines(table_definitions()["Schedule"])
        secondary = lines[lines.index("---") + 1 :]
        names = {
            line.split(":")[0].strip()
            for line in secondary
            if not line.startswith("->")
        }
        assert names <= set(PAYLOAD_KEYS)

    def test_instructions_width_matches_validation(self):
        from u19_pipeline.utils.schedule_rules import MAX_INSTRUCTIONS_LENGTH

        assert (
            f"varchar({MAX_INSTRUCTIONS_LENGTH})" in table_definitions()["ScheduleRule"]
        )

    def test_new_attributes_compile_with_datajoint(self):
        """Run every non-foreign-key attribute through DataJoint's own compiler (no connection needed)."""
        from datajoint.declare import compile_attribute

        for name in (
            "ScheduleRule",
            "ScheduleRule.Day",
            "ScheduleRuleException",
            "LabClosure",
        ):
            lines = [
                line for line in table_definitions()[name].splitlines() if line.strip()
            ]
            attributes = [
                line.strip()
                for line in lines
                if not line.strip().startswith(("#", "->", "---"))
            ]
            for line in attributes:
                attr_name, sql, _ = compile_attribute(line, False, [], {})
                assert sql.startswith(f"`{attr_name}`"), (name, line)


class TestMigrationSql:
    def test_alter_targets_given_database(self):
        statements = migration.build_alter_statements("u19_test_scheduler")
        assert all("`u19_test_scheduler`.`schedule`" in s for s in statements)

    def test_rule_id_column_is_nullable(self):
        add_column = migration.build_alter_statements("db")[0]
        assert "`rule_id` int DEFAULT NULL" in add_column
        assert "NOT NULL" not in add_column

    def test_foreign_key_references_hidden_rule_table(self):
        fk = migration.build_alter_statements("db")[1]
        assert "REFERENCES `db`.`#schedule_rule` (`rule_id`)" in fk
        assert "ON DELETE RESTRICT" in fk

    def test_existence_check_is_parameterised(self):
        query, params = migration.column_exists_query("u19_scheduler")
        assert "%s" in query and "u19_scheduler" not in query
        assert params == ("u19_scheduler", "schedule", "rule_id")
