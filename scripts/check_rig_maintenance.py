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
from datetime import datetime
from pathlib import Path

# Add the u19_pipeline to the path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

try:
    from u19_pipeline import lab, rig_maintenance
    from u19_pipeline.utils import slack_utils as su
except ImportError as e:
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

    # Group items by status
    no_record_items = [item for item in overdue_items if item["status"] == "NO_RECORD"]
    overdue_items_list = [item for item in overdue_items if item["status"] == "OVERDUE"]

    # Build message blocks. Slack limits: max 50 blocks and ~3000 chars per text field.
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"⚠️ *Rig Maintenance Alert* - {current_date}\n\n{len(overdue_items)} items require attention",
            },
        },
        {"type": "divider"},
    ]

    if no_record_items:
        no_record_text = "\n".join([f"• {item['location']} - {item['maintenance_type']}" for item in no_record_items])
        # Truncate if too long for Slack
        if len(no_record_text) > 2500:
            truncated = no_record_text[:2400] + "\n• ... (truncated)"
            no_record_text = truncated
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"📋 *No Maintenance Records* ({len(no_record_items)} items):\n{no_record_text}",
                },
            }
        )

    if overdue_items_list:
        if no_record_items:
            blocks.append({"type": "divider"})

        # Sort by most overdue first
        overdue_items_list.sort(key=lambda x: x["days_overdue"], reverse=True)
        overdue_text = "\n".join(
            [
                f"• {item['location']} - {item['maintenance_type']}: "
                f"{item['days_overdue']} days overdue (last: {item['last_maintenance']})"
                for item in overdue_items_list
            ]
        )

        # If the overdue_text is too large or blocks would exceed limits, fall back to a short list
        if len(overdue_text) > 2500 or len(blocks) + 1 > 45:
            # Provide a short top-N list and include full details in the log only
            top_n = overdue_items_list[:25]
            short_text = "\n".join(
                [f"• {it['location']} - {it['maintenance_type']} ({it['days_overdue']} days)" for it in top_n]
            )
            if len(overdue_items_list) > len(top_n):
                short_text += f"\n• ... and {len(overdue_items_list) - len(top_n)} more items (see logs)"

            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (f"🚨 *Overdue Maintenance* ({len(overdue_items_list)} items):\n{short_text}"),
                    },
                }
            )
        else:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🚨 *Overdue Maintenance* ({len(overdue_items_list)} items):\n{overdue_text}",
                    },
                }
            )

    message = {"text": f"Rig Maintenance Alert - {len(overdue_items)} items require attention", "blocks": blocks}

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
    current_date = datetime.now().date()

    # Get all maintenance types and their intervals
    maintenance_fetch = getattr(rig_maintenance.MaintenanceType, "fetch")
    maintenance_types = maintenance_fetch(as_dict=True)

    # Get all locations (rigs)
    queries = [
        'system_type = "rig"',
        'acquisition_type in ("behavior", "electrophysiology", "2photon")',
        "(location_description is not NULL and length(trim(location_description)) > 0)",
    ]

    merged_queries = " and ".join(queries)

    loc_query = lab.Location & merged_queries
    loc_fetch = getattr(loc_query, "fetch")
    locations = loc_fetch(as_dict=True)

    logger.info(f"🔍 Checking maintenance status as of {current_date}")
    logger.debug("=" * 60)

    for location in locations:
        location_name = location["location"]
        logger.info(f"🔧 Checking rig: {location_name}")
        logger.debug("-" * 61)

        for mtype in maintenance_types:
            maintenance_type = mtype["maintenance_type"]
            interval_days = mtype["interval_days"]

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
                    # Maintenance is up to date
                    days_until_due = interval_days - days_since_last
                    logger.info(f"  {maintenance_type:.<30} OK ({days_until_due} days until due) ✅")

    return overdue_items


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

        # Send Slack notification with summary only
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
