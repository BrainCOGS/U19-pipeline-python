#!/usr/bin/env python3
"""
Script to check if rig maintenance is overdue for each rig and maintenance type.

This script queries the rig_maintenance schema to find the most recent maintenance
record for each rig and maintenance type combination, then compares it against
the required maintenance interval to identify overdue maintenance.
"""

import os
import sys
from datetime import datetime

# Add the u19_pipeline to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from u19_pipeline import lab, rig_maintenance
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure u19_pipeline is properly installed and configured.")
    sys.exit(1)


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

    print(f"Checking maintenance status as of {current_date}")
    print("=" * 60)

    for location in locations:
        location_name = location['location']
        print(f"\nChecking rig: {location_name}")
        print("-" * 61)

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
                print(f"  {maintenance_type:.<40} NO RECORD FOUND ❌")
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
                    print(f"  {maintenance_type:.<30} OVERDUE by {days_overdue} days ❌")
                else:
                    # Maintenance is up to date
                    days_until_due = interval_days - days_since_last
                    print(f"  {maintenance_type:.<30} OK ({days_until_due} days until due) ✅")

    return overdue_items


def print_summary(overdue_items):
    """
    Print a summary of overdue maintenance items.

    Args:
        overdue_items (list): List of overdue maintenance items
    """
    if not overdue_items:
        print("\n" + "=" * 60)
        print("🎉 All maintenance is up to date!")
        print("=" * 60)
        return

    print("\n" + "=" * 60)
    print("⚠️  OVERDUE MAINTENANCE SUMMARY")
    print("=" * 60)

    # Group by status
    no_record_items = [item for item in overdue_items if item['status'] == 'NO_RECORD']
    overdue_items_list = [item for item in overdue_items if item['status'] == 'OVERDUE']

    if no_record_items:
        print(f"\n📋 RIGS WITH NO MAINTENANCE RECORDS ({len(no_record_items)} items):")
        for item in no_record_items:
            print(f"  • {item['location']} - {item['maintenance_type']}")

    if overdue_items_list:
        print(f"\n🚨 OVERDUE MAINTENANCE ({len(overdue_items_list)} items):")
        # Sort by days overdue (most overdue first)
        overdue_items_list.sort(key=lambda x: x['days_overdue'], reverse=True)
        for item in overdue_items_list:
            print(f"  • {item['location']} - {item['maintenance_type']}: "
                  f"{item['days_overdue']} days overdue "
                  f"(last done: {item['last_maintenance']})")

    print(f"\nTotal items requiring attention: {len(overdue_items)}")
    print("=" * 60)


def main():
    """Main function to run the maintenance check."""
    try:
        print("Rig Maintenance Status Checker")
        print("=" * 60)

        # Check for overdue maintenance
        overdue_items = check_overdue_maintenance()

        # Print summary
        print_summary(overdue_items)

        # Exit with error code if there are overdue items
        if overdue_items:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        print(f"Error during maintenance check: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()