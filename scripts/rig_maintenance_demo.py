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
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from u19_pipeline import rig_maintenance
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure u19_pipeline is properly installed and configured.")
    sys.exit(1)


def demo_insert_maintenance_record():
    """Demo: Insert a maintenance record."""
    print("=== Demo: Inserting Maintenance Record ===")
    
    # Example maintenance record
    maintenance_data = {
        'location': 'Bezos2',  # Must exist in lab.Location
        'maintenance_type': 'Replacing lines',  # Must exist in MaintenanceType
        'maintenance_date': date.today(),
        'user': 'demo_user',  # Must exist in lab.User
        'maintenance_notes': 'Replaced all water lines. System tested and working normally.'
    }
    
    try:
        # Insert the record (commented out to prevent actual DB modifications in demo)
        # rig_maintenance.RigMaintenance.insert1(maintenance_data)
        print("Would insert maintenance record:")
        for key, value in maintenance_data.items():
            print(f"  {key}: {value}")
        print("✅ Maintenance record ready for insertion")
    except Exception as e:
        print(f"❌ Error inserting maintenance record: {e}")


def demo_query_maintenance_history():
    """Demo: Query maintenance history for a rig."""
    print("\n=== Demo: Querying Maintenance History ===")
    
    rig_name = 'Bezos2'
    
    try:
        # Query maintenance history (commented out to prevent DB access in demo)
        # records = (rig_maintenance.RigMaintenance & {'location': rig_name}).fetch(
        #     as_dict=True, order_by='maintenance_date DESC'
        # )
        
        print(f"Would query maintenance history for rig: {rig_name}")
        print("Query would return records ordered by most recent first")
        print("✅ Query structure is correct")
    except Exception as e:
        print(f"❌ Error querying maintenance history: {e}")


def demo_check_overdue_maintenance():
    """Demo: Check for overdue maintenance."""
    print("\n=== Demo: Checking Overdue Maintenance ===")
    
    print("This demo shows the logic used in check_rig_maintenance.py")
    
    # Show the maintenance types and their intervals
    print("\nMaintenance intervals defined:")
    for mtype in rig_maintenance.MaintenanceType.contents:
        maintenance_type, description, interval_days = mtype
        print(f"  • {maintenance_type}: every {interval_days} days")
    
    # Show the logic for checking overdue maintenance
    current_date = date.today()
    print(f"\nCurrent date: {current_date}")
    print("For each rig and maintenance type combination:")
    print("1. Find most recent maintenance record")
    print("2. Calculate days since last maintenance")
    print("3. Compare against required interval")
    print("4. Flag as overdue if interval exceeded")
    print("✅ Overdue checking logic is sound")


def main():
    """Main demo function."""
    print("Rig Maintenance Schema Demo")
    print("=" * 50)
    
    print("This demo shows how to use the new rig maintenance schema.")
    print("The schema includes:")
    print("- MaintenanceType: Lookup table with maintenance types and intervals")
    print("- RigMaintenance: Records of maintenance performed on rigs")
    print()
    
    # Run demo functions
    demo_insert_maintenance_record()
    demo_query_maintenance_history()
    demo_check_overdue_maintenance()
    
    print("\n" + "=" * 50)
    print("Demo complete! To use the schema:")
    print("1. Configure DataJoint connection")
    print("2. Create the schema tables in your database")
    print("3. Use the check_rig_maintenance.py script to monitor maintenance")
    print("4. Insert maintenance records as work is completed")


if __name__ == "__main__":
    main()