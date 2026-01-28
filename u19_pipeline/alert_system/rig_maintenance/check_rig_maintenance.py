#!/usr/bin/env python3
"""
Script to check if rig maintenance is overdue for each rig and maintenance type.

This script queries the rig_maintenance schema to find the most recent maintenance
record for each rig and maintenance type combination, then compares it against
the required maintenance interval to identify overdue maintenance.

Features:
- Logs all output to file with rich formatting
- Sends Slack notifications for overdue maintenance summary
- Uses the rigs_issues_and_troubleshooting webhook for notifications
"""

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    # When installed/used as a package, import directly
    from u19_pipeline import lab, rig_maintenance, scheduler
    from u19_pipeline.utils import slack_utils as su
except Exception as e:  # pragma: no cover - only happens when package not available
    print(f"Error importing modules: {e}")
    print("Make sure u19_pipeline is properly installed and configured.")
    sys.exit(1)

logger = logging.getLogger("rig_maintenance")


def setup_logging():
    """
    Set up logging with rich formatting to both file and console.

    Returns:
        tuple: (logger, file_console, console, log_file_path, log_file_handle)
    """
    # Create logs directory in the user's home directory (allow override via env var)
    # Default: ~/.u19_pipeline/logs
    env_log_dir = os.getenv("U19_PIPELINE_LOG_DIR")
    if env_log_dir:
        log_dir = Path(env_log_dir)
    else:
        log_dir = Path.home() / "u19_pipeline_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"rig_maintenance_check_{timestamp}.log"

    # Open log file handle
    log_file_handle = open(log_file, "a", encoding="utf-8")

    # Configure standard logging: StreamHandler (console) + FileHandler (file)
    logger = logging.getLogger("rig_maintenance")
    logger.setLevel(logging.INFO)

    # Avoid adding handlers multiple times if setup_logging is called more than once
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        # File handler
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="[%X]")
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    # Prevent messages from also being handled by the root logger
    logger.propagate = False

    return logger, log_file, log_file_handle


def get_slack_webhook():
    """
    Get the Slack webhook URL for rig issues and troubleshooting.

    Returns:
        str or None: Webhook URL if found, None otherwise
    """
    try:
        slack_configuration_dictionary = {"webhook_name": "rigs_issues_and_troubleshooting"}
        webhook_url = (lab.SlackWebhooks & slack_configuration_dictionary).fetch1("webhook_url")
        return webhook_url
    except Exception as e:
        logger.warning(f"Failed to get Slack webhook: {e}")
        return None


def create_slack_message(overdue_items, current_date):
    """
    Create a Slack message for overdue maintenance summary.

    Args:
        overdue_items (list): List of overdue maintenance items
        current_date (date): Current date

    Returns:
        dict: Slack message payload
    """
    if not overdue_items:
        # No overdue items - success message
        message = {
            "text": "🎉 All rig maintenance is up to date!",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🎉 *All rig maintenance is up to date!*\n\nChecked on {current_date}",
                    },
                }
            ],
        }
        return message

    # Split into overdue vs upcoming items based on status
    overdue_list = [item for item in overdue_items if item.get("status") == "OVERDUE"]
    upcoming_list = [item for item in overdue_items if item.get("status") == "UPCOMING"]

    rigs_overdue = {item["location"] for item in overdue_list}
    rigs_upcoming = {item["location"] for item in upcoming_list}
    rigs_with_issues = sorted(rigs_overdue | rigs_upcoming)

    overdue_lines = "\n".join(f"• {loc}" for loc in sorted(rigs_overdue)) or "• None"
    upcoming_lines = "\n".join(f"• {loc}" for loc in sorted(rigs_upcoming)) or "• None"

    text = (
        f"⚠️ *Rig Maintenance Alert* - {current_date}\n\n"
        f"*Overdue maintenance* rigs ({len(rigs_overdue)}):\n{overdue_lines}\n\n"
        f"*Upcoming maintenance within warning window* rigs ({len(rigs_upcoming)}):\n{upcoming_lines}\n\n"
        "For full details of overdue and upcoming items per rig, please visit "
        "<https://braincogs-webgui.pni.princeton.edu/rig_maintenance|the rig maintenance web page>."
    )

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text,
            },
        }
    ]

    message = {
        "text": f"Rig Maintenance Alert - {len(rigs_with_issues)} rigs have overdue or upcoming maintenance",
        "blocks": blocks,
    }

    return message


def send_slack_notification(overdue_items, current_date):
    """
    Send Slack notification with maintenance summary.

    Args:
        overdue_items (list): List of overdue maintenance items
        current_date (date): Current date
    """
    webhook_url = get_slack_webhook()
    if not webhook_url:
        logger.warning("No Slack webhook found - skipping notification")
        return

    try:
        message = create_slack_message(overdue_items, current_date)
        # Log the message payload for debugging
        logger.debug("Slack payload: %s", json.dumps(message, default=str))
        su.send_slack_notification(webhook_url, message)
    except Exception as e:
        logger.exception(f"❌ Failed to prepare/send Slack notification: {e}")


def check_overdue_maintenance():
    """
    Check for overdue maintenance across all rigs and maintenance types.

    Returns:
        list: List of dictionaries containing overdue maintenance information
    """
    overdue_items = []
    upcoming_items = []
    current_date = datetime.now().date()

    # Determine which locations are active based on recent schedules (last 30 days)

    five_weeks_ago = date.today() - timedelta(weeks=5)
    query = scheduler.Schedule & f'date >= "{five_weeks_ago}"' & 'subject_fullname not like "test%"'
    locations = query.fetch("location")
    unique_locations = sorted(set(locations))

    # Get all maintenance types and their intervals
    maintenance_fetch = getattr(rig_maintenance.MaintenanceType, "fetch")
    maintenance_types = maintenance_fetch(as_dict=True)

    # Get all locations (rigs)
    queries = [
        'system_type = "rig"',
        'acquisition_type in ("behavior", "electrophysiology", "2photon")',
        "(location_description is not NULL and length(trim(location_description)) > 0)",
        f"location in ({', '.join([f'"{loc}"' for loc in unique_locations])})",
    ]

    merged_queries = " and ".join(queries)

    loc_query = lab.Location & merged_queries

    loc_fetch = getattr(loc_query, "fetch")
    locations = loc_fetch(as_dict=True)

    logger.info(f"🔍 Checking maintenance status as of {current_date}")
    logger.debug("=" * 60)

    for location in locations:
        location_name = location["location"]
        rig_number_of_lines = location["number_of_lines"]
        logger.info(f"🔧 Checking rig: {location_name}")
        logger.debug("-" * 61)

        for mtype in maintenance_types:
            maintenance_type = mtype["maintenance_type"]
            main_num_of_lines = mtype.get("number_of_lines", 0)
            if not (main_num_of_lines == 0 or main_num_of_lines == rig_number_of_lines):
                # logger.info(
                #     f"  {maintenance_type:.<30} SKIPPED (rig has {rig_number_of_lines} lines, "
                #     f"maintenance type for {main_num_of_lines} lines)"
                # )
                continue
            interval_days = mtype["interval_days"]
            notification_window = mtype.get("notification_window", 0)

            # Find the most recent maintenance record for this rig and type
            recent_q = rig_maintenance.RigMaintenance & {
                "location": location_name,
                "maintenance_type": maintenance_type,
            }
            recent_fetch = getattr(recent_q, "fetch")
            recent_maintenance = recent_fetch("maintenance_date", order_by="maintenance_date DESC", limit=1)

            if len(recent_maintenance) == 0:
                # No maintenance record exists
                overdue_items.append(
                    {
                        "location": location_name,
                        "maintenance_type": maintenance_type,
                        "last_maintenance": None,
                        "days_since_last": None,
                        "interval_days": interval_days,
                        "status": "NO_RECORD",
                        "message": f"No maintenance record found for {maintenance_type}",
                    }
                )
                logger.info(f"  {maintenance_type:.<40} NO RECORD FOUND ❌")
            else:
                last_maintenance_date = recent_maintenance[0]
                days_since_last = (current_date - last_maintenance_date).days

                if days_since_last > interval_days:
                    # Maintenance is overdue
                    days_overdue = days_since_last - interval_days
                    overdue_items.append(
                        {
                            "location": location_name,
                            "maintenance_type": maintenance_type,
                            "last_maintenance": last_maintenance_date,
                            "days_since_last": days_since_last,
                            "interval_days": interval_days,
                            "days_overdue": days_overdue,
                            "status": "OVERDUE",
                            "message": f"{maintenance_type} is {days_overdue} days overdue",
                        }
                    )
                    logger.info(f"  {maintenance_type:.<30} OVERDUE by {days_overdue} days ❌")
                else:
                    # Not yet overdue: check if within notification window
                    days_until_due = interval_days - days_since_last
                    if 0 < days_until_due <= notification_window:
                        upcoming_items.append(
                            {
                                "location": location_name,
                                "maintenance_type": maintenance_type,
                                "last_maintenance": last_maintenance_date,
                                "days_since_last": days_since_last,
                                "interval_days": interval_days,
                                "days_until_due": days_until_due,
                                "notification_window": notification_window,
                                "status": "UPCOMING",
                                "message": f"{maintenance_type} due in {days_until_due} days",
                            }
                        )
                        logger.info(
                            f"  {maintenance_type:.<30} DUE SOON in {days_until_due} days (window {notification_window}) ⚠️"
                        )
                    else:
                        logger.info(f"  {maintenance_type:.<30} OK ({days_until_due} days until due) ✅")

    # Return combined list; status distinguishes OVERDUE vs UPCOMING
    return overdue_items + upcoming_items


def log_summary(overdue_items):
    """
    Log a summary of overdue maintenance items using plain-text formatting.

    Args:
        overdue_items (list): List of overdue maintenance items
    """
    if not overdue_items:
        logger.info("\n" + "=" * 60)
        logger.info("🎉 All maintenance is up to date!")
        logger.debug("=" * 60)
        return

    logger.debug("=" * 60)
    logger.info("⚠️  OVERDUE MAINTENANCE SUMMARY")
    logger.debug("=" * 60)

    # Group by status
    no_record_items = [item for item in overdue_items if item["status"] == "NO_RECORD"]
    overdue_items_list = [item for item in overdue_items if item["status"] == "OVERDUE"]

    if no_record_items:
        # Print a plain-text table for no-record items
        logger.warning(f"📋 RIGS WITH NO MAINTENANCE RECORDS ({len(no_record_items)} items)")
        loc_w = 30
        type_w = 40
        logger.debug(f"{'.' * (loc_w + type_w + 5)}")
        logger.debug(f"{'Location':<{loc_w}} | {'Maintenance Type':<{type_w}}")
        logger.debug(f"{'.' * (loc_w + type_w + 5)}")
        for item in no_record_items:
            logger.debug(f"{item['location']:<{loc_w}} | {item['maintenance_type']:<{type_w}}")
        logger.debug(f"{'.' * (loc_w + type_w + 5)}")

    if overdue_items_list:
        # Sort by days overdue (most overdue first)
        overdue_items_list.sort(key=lambda x: x["days_overdue"], reverse=True)

        # Print a plain-text table for overdue items
        logger.info(f"🚨 OVERDUE MAINTENANCE ({len(overdue_items_list)} items)")
        loc_w = 30
        type_w = 30
        days_w = 12
        last_w = 20
        total_w = loc_w + type_w + days_w + last_w + 9
        logger.debug(f"{'.' * total_w}")
        header = (
            f"{'Location':<{loc_w}} | {'Maintenance Type':<{type_w}} | "
            f"{'Days Overdue':>{days_w}} | {'Last Done':<{last_w}}"
        )
        logger.debug(header)
        logger.debug(f"{'.' * total_w}")
        for item in overdue_items_list:
            row = (
                f"{item['location']:<{loc_w}} | {item['maintenance_type']:<{type_w}} | "
                f"{str(item.get('days_overdue', '')):>{days_w}} | {str(item.get('last_maintenance', '')):<{last_w}}"
            )
            logger.debug(row)
        logger.debug(f"{'.' * total_w}")

    logger.warning(f"Total items requiring attention: {len(overdue_items)}")
    logger.debug("=" * 60)


def dataframe_to_slack_table_block(df: pd.DataFrame, truncate_cell: int = 200, use_markdown: bool = False) -> dict:
    """
    Convert a DataFrame into one Slack table block.
    Docs: https://docs.slack.dev/reference/block-kit/blocks/table-block/

    Args:
        df: DataFrame to render.
        truncate_cell: Max chars per cell (Slack text field limit safety).
        use_markdown: Use mrkdwn cells instead of plain_text.

    Returns:
        dict: Slack table block.
    """
    cell_type = "mrkdwn" if use_markdown else "raw_text"

    def mk_cell(val):
        text = "" if pd.isna(val) else str(val)
        if len(text) > truncate_cell:
            text = text[: truncate_cell - 3] + "..."
        return {"type": cell_type, "text": text}

    headers = [[mk_cell(c) for c in df.columns]]
    rows = [[mk_cell(v) for v in row] for row in df.itertuples(index=False, name=None)]

    block = {
        "type": "table",
        "column_settings": [{"is_wrapped": True}],
        # Using your approach: make the header the first row in the rows array
        "rows": headers + rows,
    }
    return block


def dataframe_to_slack_table_blocks(df: pd.DataFrame, chunk_size: int = 15, **kwargs) -> list[dict]:
    """
    Chunk a DataFrame into multiple Slack table blocks by rows.

    Args:
        df: Source DataFrame.
        chunk_size: Rows per table block.
        **kwargs: Passed to dataframe_to_slack_table_block.

    Returns:
        list[dict]: List of table blocks.
    """
    blocks = []
    for start in range(0, len(df), chunk_size):
        sub = df.iloc[start : start + chunk_size]
        blocks.append(dataframe_to_slack_table_block(sub, **kwargs))
    return blocks


def main():
    """Main function to run the maintenance check."""
    log_file_handle = None
    try:
        # Set up logging
        _, log_file, log_file_handle = setup_logging()

        logger.info("🔧 Rig Maintenance Status Checker")
        logger.info("=" * 60)
        logger.info(f"📝 Log file: {log_file}")

        # Check for overdue maintenance
        overdue_items = check_overdue_maintenance()

        # Log summary with rich formatting
        log_summary(overdue_items)

        records = pd.DataFrame(overdue_items)
        records.columns = [" ".join(w.capitalize() for w in col.split("_")) for col in records.columns]
        print(records)

        records = records[["Location", "Maintenance Type", "Status"]]

        # for loc in records["Location"].unique():
        #     table_blocks = dataframe_to_slack_table_blocks(records[records["Location"] == loc], chunk_size=100)
        #     payload = {"blocks": table_blocks}
        #     value = requests.post(webhook, json=payload, headers={"Content-Type": "application/json"})
        #     repr(value)

        # table_blocks = dataframe_to_slack_table_blocks(records, chunk_size=10)
        # # payload = {"blocks": table_blocks[-1]}
        # payload = {"blocks": table_blocks[1:3]}
        # pprint(payload)
        # value = requests.post(webhook, json=payload, headers={"Content-Type": "application/json"})
        # repr(value)
        # pprint(value)

        # for loc in records["Location"].unique():
        #     loc_df = records[records["Location"] == loc]
        #     payload = {"text": f"```{loc_df.to_markdown(index=False)}```"}
        #     requests.post(
        #         webhook,
        #         json=payload,
        #         headers={"Content-Type": "application/json"},
        #     )

        # chunk_md = [records.iloc[i : i + 10].to_markdown(index=False) for i in range(0, len(records), 10)]

        # merged_string = "```" + "```\n ```".join(chunk_md) + "```"

        # # for idx, markdown in enumerate(chunk_md):
        # #     payload = {"text": f"""\n```{markdown}```\n"""}
        # #     requests.post(webhook, json=payload, headers={"Content-Type": "application/json"})

        # payload = {
        #     "text":
        #     merged_string
        #     # f"""\n```{records.to_string(index=False)}```\n
        # }
        # requests.post(
        #     webhook,
        #     json=payload,
        #     headers={"Content-Type": "application/json"},
        # )

        # Send Slack notification with summary only if there are issues
        if overdue_items:
            current_date = datetime.now().date()
            send_slack_notification(overdue_items, current_date)

        # Exit with error code if there are overdue items
        if overdue_items:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        logger.exception(f"❌ Error during maintenance check: {e}")
        sys.exit(1)
    finally:
        # Close the log file handle
        if log_file_handle:
            try:
                log_file_handle.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
