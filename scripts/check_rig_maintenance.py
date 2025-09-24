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

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Rich imports for enhanced logging
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

# Add the u19_pipeline to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from u19_pipeline import lab, rig_maintenance
    from u19_pipeline.utils import slack_utils as su
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure u19_pipeline is properly installed and configured.")
    sys.exit(1)


def setup_logging():
    """
    Set up logging with rich formatting to both file and console.
    
    Returns:
        tuple: (logger, console, log_file_path)
    """
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Create log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"rig_maintenance_check_{timestamp}.log"
    
    # Create console for rich output using context manager
    log_file_handle = open(log_file, "w")
    console = Console(file=log_file_handle, width=120)
    
    # Set up logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(console=console, rich_tracebacks=True),
            logging.FileHandler(log_file, mode='a')
        ]
    )
    
    logger = logging.getLogger("rig_maintenance")
    return logger, console, log_file, log_file_handle


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
        logging.error(f"Failed to get Slack webhook: {e}")
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
                        "text": f"🎉 *All rig maintenance is up to date!*\n\nChecked on {current_date}"
                    }
                }
            ]
        }
        return message
    
    # Group items by status
    no_record_items = [item for item in overdue_items if item['status'] == 'NO_RECORD']
    overdue_items_list = [item for item in overdue_items if item['status'] == 'OVERDUE']
    
    # Build message blocks
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"⚠️ *Rig Maintenance Alert* - {current_date}\n\n{len(overdue_items)} items require attention"
            }
        },
        {"type": "divider"}
    ]
    
    if no_record_items:
        no_record_text = "\n".join([f"• {item['location']} - {item['maintenance_type']}" 
                                   for item in no_record_items])
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn", 
                "text": f"📋 *No Maintenance Records* ({len(no_record_items)} items):\n{no_record_text}"
            }
        })
    
    if overdue_items_list:
        if no_record_items:
            blocks.append({"type": "divider"})
            
        # Sort by most overdue first
        overdue_items_list.sort(key=lambda x: x['days_overdue'], reverse=True)
        overdue_text = "\n".join([f"• {item['location']} - {item['maintenance_type']}: "
                                 f"{item['days_overdue']} days overdue (last: {item['last_maintenance']})"
                                 for item in overdue_items_list])
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"🚨 *Overdue Maintenance* ({len(overdue_items_list)} items):\n{overdue_text}"
            }
        })
    
    message = {
        "text": f"Rig Maintenance Alert - {len(overdue_items)} items require attention",
        "blocks": blocks
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
        logging.warning("No Slack webhook found - skipping notification")
        return
    
    try:
        message = create_slack_message(overdue_items, current_date)
        su.send_slack_notification(webhook_url, message)
        logging.info("✅ Slack notification sent successfully")
    except Exception as e:
        logging.error(f"❌ Failed to send Slack notification: {e}")


def check_overdue_maintenance():
    """
    Check for overdue maintenance across all rigs and maintenance types.

    Returns:
        list: List of dictionaries containing overdue maintenance information
    """
    overdue_items = []
    current_date = datetime.now().date()

    # Get all maintenance types and their intervals
    maintenance_types = rig_maintenance.MaintenanceType.fetch(as_dict=True)

    # Get all locations (rigs)
    queries = [
        'system_type = "rig"',
        'acquisition_type in ("behavior", "electrophysiology", "2photon")',
        "(location_description is not NULL and length(trim(location_description)) > 0)",
    ]

    merged_queries = " and ".join(queries)

    locations = (lab.Location & merged_queries).fetch(as_dict=True)

    logging.info(f"🔍 Checking maintenance status as of [bold blue]{current_date}[/bold blue]")
    logging.info("=" * 60)

    for location in locations:
        location_name = location['location']
        logging.info(f"\n🔧 Checking rig: [bold yellow]{location_name}[/bold yellow]")
        logging.info("-" * 61)

        for mtype in maintenance_types:
            maintenance_type = mtype['maintenance_type']
            interval_days = mtype['interval_days']

            # Find the most recent maintenance record for this rig and type
            recent_maintenance = (rig_maintenance.RigMaintenance &
                                {'location': location_name,
                                 'maintenance_type': maintenance_type}).fetch(
                                'maintenance_date', order_by='maintenance_date DESC', limit=1)

            if len(recent_maintenance) == 0:
                # No maintenance record exists
                overdue_items.append({
                    'location': location_name,
                    'maintenance_type': maintenance_type,
                    'last_maintenance': None,
                    'days_since_last': None,
                    'interval_days': interval_days,
                    'status': 'NO_RECORD',
                    'message': f'No maintenance record found for {maintenance_type}'
                })
                logging.info(f"  {maintenance_type:.<40} [red]NO RECORD FOUND ❌[/red]")
            else:
                last_maintenance_date = recent_maintenance[0]
                days_since_last = (current_date - last_maintenance_date).days

                if days_since_last > interval_days:
                    # Maintenance is overdue
                    days_overdue = days_since_last - interval_days
                    overdue_items.append({
                        'location': location_name,
                        'maintenance_type': maintenance_type,
                        'last_maintenance': last_maintenance_date,
                        'days_since_last': days_since_last,
                        'interval_days': interval_days,
                        'days_overdue': days_overdue,
                        'status': 'OVERDUE',
                        'message': f'{maintenance_type} is {days_overdue} days overdue'
                    })
                    logging.info(f"  {maintenance_type:.<30} [red]OVERDUE by {days_overdue} days ❌[/red]")
                else:
                    # Maintenance is up to date
                    days_until_due = interval_days - days_since_last
                    logging.info(f"  {maintenance_type:.<30} [green]OK ({days_until_due} days until due) ✅[/green]")

    return overdue_items


def log_summary(overdue_items):
    """
    Log a summary of overdue maintenance items using rich formatting.
    
    Args:
        overdue_items (list): List of overdue maintenance items
    """
    if not overdue_items:
        logging.info("\n" + "=" * 60)
        logging.info("🎉 [bold green]All maintenance is up to date![/bold green]")
        logging.info("=" * 60)
        return
    
    logging.info("\n" + "=" * 60)
    logging.info("⚠️  [bold red]OVERDUE MAINTENANCE SUMMARY[/bold red]")
    logging.info("=" * 60)
    
    # Group by status
    no_record_items = [item for item in overdue_items if item['status'] == 'NO_RECORD']
    overdue_items_list = [item for item in overdue_items if item['status'] == 'OVERDUE']
    
    if no_record_items:
        # Create a table for no record items
        table = Table(title=f"📋 RIGS WITH NO MAINTENANCE RECORDS ({len(no_record_items)} items)")
        table.add_column("Location", style="cyan")
        table.add_column("Maintenance Type", style="magenta")
        
        for item in no_record_items:
            table.add_row(item['location'], item['maintenance_type'])
        
        logging.info(table)
    
    if overdue_items_list:
        # Sort by days overdue (most overdue first)
        overdue_items_list.sort(key=lambda x: x['days_overdue'], reverse=True)
        
        # Create a table for overdue items
        table = Table(title=f"🚨 OVERDUE MAINTENANCE ({len(overdue_items_list)} items)")
        table.add_column("Location", style="cyan")
        table.add_column("Maintenance Type", style="magenta")
        table.add_column("Days Overdue", justify="right", style="red")
        table.add_column("Last Done", style="yellow")
        
        for item in overdue_items_list:
            table.add_row(
                item['location'], 
                item['maintenance_type'],
                str(item['days_overdue']),
                str(item['last_maintenance'])
            )
        
        logging.info(table)
    
    logging.info(f"\n[bold]Total items requiring attention: {len(overdue_items)}[/bold]")
    logging.info("=" * 60)


def main():
    """Main function to run the maintenance check."""
    log_file_handle = None
    try:
        # Set up logging
        logger, console, log_file, log_file_handle = setup_logging()
        
        logging.info("🔧 [bold blue]Rig Maintenance Status Checker[/bold blue]")
        logging.info("=" * 60)
        logging.info(f"📝 Log file: {log_file}")
        
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
        logging.error(f"❌ Error during maintenance check: {e}")
        sys.exit(1)
    finally:
        # Close the log file handle
        if log_file_handle:
            log_file_handle.close()


if __name__ == "__main__":
    main()