#!/usr/bin/env python
"""
Script to fix database duplicate issues and run migrations automatically.
This script will:
1. Find all duplicate DataSource records (including soft-deleted ones)
2. Keep the most recent one and hard-delete the rest
3. Run the migration automatically
"""

import os
import sys
import django
from collections import defaultdict

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource
from django.core.management import execute_from_command_line


def fix_duplicates():
    """Fix all duplicate DataSource records by keeping the most recent and hard-deleting the rest"""
    print("=== FIXING DATABASE DUPLICATES ===")
    
    # Get ALL sources (including soft-deleted ones)
    all_sources = DataSource.objects.all().order_by('created_at')
    print(f"Total DataSource records found: {all_sources.count()}")
    
    # Group by (created_by, name)
    grouped = defaultdict(list)
    for ds in all_sources:
        key = (ds.created_by.pk, ds.name)
        grouped[key].append(ds)
    
    # Find duplicates
    duplicates = {k: v for k, v in grouped.items() if len(v) > 1}
    print(f"Found {len(duplicates)} duplicate groups")
    
    if not duplicates:
        print("✓ No duplicates found - database is clean!")
        return True
    
    # Process each duplicate group
    total_deleted = 0
    for (user_id, name), sources in duplicates.items():
        print(f"\nProcessing duplicates for user {user_id}, name '{name}':")
        
        # Sort by created_at descending to keep most recent
        sources_sorted = sorted(sources, key=lambda x: x.created_at, reverse=True)
        to_keep = sources_sorted[0]
        to_delete = sources_sorted[1:]
        
        print(f"  Keeping: {to_keep.id} (created: {to_keep.created_at}, deleted: {to_keep.is_deleted})")
        
        for ds in to_delete:
            print(f"  Deleting: {ds.id} (created: {ds.created_at}, deleted: {ds.is_deleted})")
            ds.delete()  # Hard delete
            total_deleted += 1
    
    print(f"\n✓ Deleted {total_deleted} duplicate records")
    return True


def run_migration():
    """Run the Django migration"""
    print("\n=== RUNNING MIGRATION ===")
    
    # Simulate running: python manage.py migrate
    try:
        # Set up the command line arguments
        sys.argv = ['manage.py', 'migrate']
        
        # Run the migration
        execute_from_command_line(sys.argv)
        print("✓ Migration completed successfully!")
        return True
        
    except Exception as e:
        print(f"✗ Migration failed: {e}")
        return False


def main():
    """Main function to fix duplicates and run migration"""
    print("Starting database fix and migration process...")
    
    # Step 1: Fix duplicates
    if not fix_duplicates():
        print("Failed to fix duplicates")
        return False
    
    # Step 2: Run migration
    if not run_migration():
        print("Failed to run migration")
        return False
    
    print("\n=== SUCCESS ===")
    print("✓ Database duplicates fixed")
    print("✓ Migration completed")
    print("✓ Your database is now ready!")
    
    return True


if __name__ == '__main__':
    success = main()
    if not success:
        sys.exit(1) 