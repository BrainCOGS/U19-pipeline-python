"""Upgrade the existing NWB queue in place. Run with the NWB worker stopped."""

import argparse
import re
from contextlib import closing

import datajoint as dj


def upgrade(conn, schema_name: str, *, apply: bool) -> None:
    if re.fullmatch(r"[a-zA-Z0-9_]+", schema_name) is None:
        raise ValueError("Provide an existing NWB schema name.")
    table = dj.FreeTable(conn, f"`{schema_name}`.`nwb_export_job`")
    if not table.is_declared:
        raise ValueError("The existing NWB job table could not be found.")
    fields = ("subject_fullname", "session_date", "session_number")
    with closing(
        conn.query(
            """
        SELECT constraint_name, column_name, referenced_table_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s AND table_name = %s AND referenced_table_name IS NOT NULL
        """,
            args=(table.database, table.table_name),
        )
    ) as cursor:
        foreign_keys = cursor.fetchall()
    session_keys = {key for key, column, _ in foreign_keys if column in fields}
    for key in session_keys:
        columns = {
            column
            for name, column, target in foreign_keys
            if name == key and target == "session"
        }
        if columns != set(fields):
            raise RuntimeError(
                f"Unexpected session foreign key {key!r}. Check the existing schema."
            )

    with closing(
        conn.query(f"SHOW COLUMNS FROM {table.full_table_name}", as_dict=True)
    ) as cursor:
        columns = {row["Field"]: row for row in cursor.fetchall()}
    expected = (*fields, "source_request")
    if not set(fields) <= columns.keys():
        raise RuntimeError(
            "The existing job table is missing expected session columns."
        )
    clauses = [
        f"DROP FOREIGN KEY `{key.replace('`', '``')}`" for key in sorted(session_keys)
    ]
    for field, sql_type in zip(fields, ("VARCHAR(64)", "DATE", "INT"), strict=True):
        if columns[field]["Null"] == "NO":
            clauses.append(f"MODIFY COLUMN `{field}` {sql_type} NULL DEFAULT NULL")
    if "request_id" in columns:
        clauses.append("DROP COLUMN request_id")
    if "source_request" not in columns:
        clauses.append("ADD COLUMN source_request LONGBLOB NULL")
    if not clauses:
        print(f"{table.full_table_name} already has {', '.join(expected)}.")
        return

    sql = f"ALTER TABLE {table.full_table_name} " + ", ".join(clauses)
    print(sql)
    if apply:
        conn.query(sql, reconnect=False).close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Apply the displayed migration."
    )
    parser.add_argument(
        "--schema", required=True, help="Existing NWB production schema name."
    )
    args = parser.parse_args()
    upgrade(dj.conn(), args.schema, apply=args.apply)
