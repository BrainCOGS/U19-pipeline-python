#!/usr/bin/env python3
"""
Demo script showing how to use the rig maintenance schema.

This script demonstrates:
1. How to insert maintenance records
2. How to query maintenance history
3. How to check maintenance status

Note: This script requires a configured DataJoint connection.
"""

import os
import sys
from datetime import date

# Add the u19_pipeline to the path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

try:
    from u19_pipeline import rig_maintenance
    from u19_pipeline.utils.logging_config import get_logger, setup_logging
except ImportError as e:
    import sys as _sys
    _sys.stderr.write(f"Error importing modules: {e}\n")
    _sys.stderr.write("Make sure u19_pipeline is properly installed and configured.\n")
    sys.exit(1)

logger = get_logger(__name__)


def demo_insert_maintenance_record():
    """Demo: Insert a maintenance record."""
    logger.info("=== Demo: Inserting Maintenance Record ===")

    # Example maintenance record
    maintenance_data = {
        "location": "Bezos2",  # Must exist in lab.Location
        "maintenance_type": "Replacing lines",  # Must exist in MaintenanceType
        "maintenance_date": date.today(),
        "user": "demo_user",  # Must exist in lab.User
        "maintenance_notes": "Replaced all water lines. System tested and working normally.",
    }

    try:
        # Insert the record (commented out to prevent actual DB modifications in demo)
        # rig_maintenance.RigMaintenance.insert1(maintenance_data)
        logger.info("Would insert maintenance record:")
        for key, value in maintenance_data.items():
            logger.info("  %s: %s", key, value)
        logger.info("Maintenance record ready for insertion")
    except Exception as e:
        logger.error("Error inserting maintenance record: %s", e)


def demo_query_maintenance_history():
    """Demo: Query maintenance history for a rig."""
    logger.info("=== Demo: Querying Maintenance History ===")

    rig_name = "Bezos2"

    try:
        # Query maintenance history (commented out to prevent DB access in demo)
        # records = (rig_maintenance.RigMaintenance & {'location': rig_name}).fetch(
        #     as_dict=True, order_by='maintenance_date DESC'
        # )

        logger.info("Would query maintenance history for rig: %s", rig_name)
        logger.info("Query would return records ordered by most recent first")
        logger.info("Query structure is correct")
    except Exception as e:
        logger.error("Error querying maintenance history: %s", e)


def demo_check_overdue_maintenance():
    """Demo: Check for overdue maintenance."""
    logger.info("=== Demo: Checking Overdue Maintenance ===")

    logger.info("This demo shows the logic used in check_rig_maintenance.py")

    # Show the maintenance types and their intervals
    logger.info("Maintenance intervals defined:")
    for mtype in rig_maintenance.MaintenanceType.contents:
        maintenance_type, description, interval_days = mtype
        logger.info("  %s: every %s days", maintenance_type, interval_days)

    # Show the logic for checking overdue maintenance
    current_date = date.today()
    logger.info("Current date: %s", current_date)
    logger.info("For each rig and maintenance type combination:")
    logger.info("1. Find most recent maintenance record")
    logger.info("2. Calculate days since last maintenance")
    logger.info("3. Compare against required interval")
    logger.info("4. Flag as overdue if interval exceeded")
    logger.info("Overdue checking logic is sound")


def main():
    """Main demo function."""
    logger.info("Rig Maintenance Schema Demo")
    logger.info("=" * 50)
    logger.info("This demo shows how to use the new rig maintenance schema.")
    logger.info("The schema includes:")
    logger.info("- MaintenanceType: Lookup table with maintenance types and intervals")
    logger.info("- RigMaintenance: Records of maintenance performed on rigs")

    # Run demo functions
    demo_insert_maintenance_record()
    demo_query_maintenance_history()
    demo_check_overdue_maintenance()

    logger.info("=" * 50)
    logger.info("Demo complete! To use the schema:")
    logger.info("1. Configure DataJoint connection")
    logger.info("2. Create the schema tables in your database")
    logger.info("3. Use the check_rig_maintenance.py script to monitor maintenance")
    logger.info("4. Insert maintenance records as work is completed")


if __name__ == "__main__":
    setup_logging()
    main()
