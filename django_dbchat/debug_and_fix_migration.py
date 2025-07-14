import os
import django
from collections import defaultdict

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource
from django.db import connection


def debug_database_state():
    print("=== DATABASE STATE DEBUGGING ===")
    
    # Check total data sources
    total_sources = DataSource.objects.count()
    print(f"Total DataSource records: {total_sources}")
    
    # Check non-deleted sources
    active_sources = DataSource.objects.filter(is_deleted=False)
    print(f"Active (non-deleted) sources: {active_sources.count()}")
    
    # Check for duplicates by (created_by, name)
    grouped = defaultdict(list)
    for ds in active_sources:
        key = (ds.created_by.pk, ds.name)
        grouped[key].append(ds)
    
    duplicates = {k: v for k, v in grouped.items() if len(v) > 1}
    print(f"Duplicate groups found: {len(duplicates)}")
    
    if duplicates:
        print("\n=== DUPLICATES DETAILS ===")
        for (user_id, name), sources in duplicates.items():
            print(f"User {user_id}, Name '{name}': {len(sources)} sources")
            for ds in sources:
                print(f"  - ID: {ds.id}, Created: {ds.created_at}, Status: {ds.status}")
    
    # Check for any sources with NULL created_by
    null_user_sources = DataSource.objects.filter(created_by__isnull=True)
    print(f"\nSources with NULL created_by: {null_user_sources.count()}")
    
    # Check for any sources with NULL name
    null_name_sources = DataSource.objects.filter(name__isnull=True)
    print(f"Sources with NULL name: {null_name_sources.count()}")
    
    # Check for empty string names
    empty_name_sources = DataSource.objects.filter(name='')
    print(f"Sources with empty name: {empty_name_sources.count()}")
    
    return duplicates


def fix_database_issues():
    print("\n=== FIXING DATABASE ISSUES ===")
    
    # Fix 1: Remove sources with NULL created_by (they can't be part of unique constraint)
    null_user_sources = DataSource.objects.filter(created_by__isnull=True)
    if null_user_sources.exists():
        print(f"Removing {null_user_sources.count()} sources with NULL created_by")
        null_user_sources.delete()
    
    # Fix 2: Remove sources with NULL or empty names
    null_name_sources = DataSource.objects.filter(name__isnull=True)
    if null_name_sources.exists():
        print(f"Removing {null_name_sources.count()} sources with NULL name")
        null_name_sources.delete()
    
    empty_name_sources = DataSource.objects.filter(name='')
    if empty_name_sources.exists():
        print(f"Removing {empty_name_sources.count()} sources with empty name")
        empty_name_sources.delete()
    
    # Fix 3: Clean up duplicates
    duplicates = debug_database_state()
    if duplicates:
        print("\n=== CLEANING UP DUPLICATES ===")
        for (user_id, name), sources in duplicates.items():
            # Sort by created_at descending to keep most recent
            sources_sorted = sorted(sources, key=lambda x: x.created_at, reverse=True)
            to_keep = sources_sorted[0]
            to_delete = sources_sorted[1:]
            
            print(f"Keeping: {to_keep.id} ({to_keep.name}, user {to_keep.created_by.pk})")
            for ds in to_delete:
                print(f"  Soft deleting: {ds.id} ({ds.name}, user {ds.created_by.pk})")
                ds.is_deleted = True
                ds.status = 'inactive'
                ds.deleted_at = ds.created_at
                ds.save()
    
    # Fix 4: Check if there are any remaining issues
    print("\n=== FINAL STATE CHECK ===")
    active_sources = DataSource.objects.filter(is_deleted=False)
    grouped = defaultdict(list)
    for ds in active_sources:
        key = (ds.created_by.pk, ds.name)
        grouped[key].append(ds)
    
    remaining_duplicates = {k: v for k, v in grouped.items() if len(v) > 1}
    if remaining_duplicates:
        print(f"WARNING: {len(remaining_duplicates)} duplicate groups still exist!")
        return False
    else:
        print("✓ No duplicate groups remaining")
        return True


def test_unique_constraint():
    print("\n=== TESTING UNIQUE CONSTRAINT ===")
    
    # Try to create a test constraint to see if it works
    try:
        with connection.cursor() as cursor:
            # Check if the constraint already exists
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='data_sources'
            """)
            if cursor.fetchone():
                cursor.execute("PRAGMA table_info(data_sources)")
                columns = cursor.fetchall()
                print("Current table structure:")
                for col in columns:
                    print(f"  {col[1]} ({col[2]})")
                
                # Check for existing unique constraints
                cursor.execute("PRAGMA index_list(data_sources)")
                indexes = cursor.fetchall()
                print("\nExisting indexes:")
                for idx in indexes:
                    print(f"  {idx[1]} ({idx[2]})")
                
                # Check if there are any existing unique constraints on (created_by_id, name)
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='index' AND tbl_name='data_sources' 
                    AND sql LIKE '%created_by_id%' AND sql LIKE '%name%'
                """)
                existing_constraints = cursor.fetchall()
                if existing_constraints:
                    print(f"\nFound existing constraints: {existing_constraints}")
                
                # Try to add a test unique constraint
                print("\nAttempting to add test unique constraint...")
                try:
                    cursor.execute("""
                        CREATE UNIQUE INDEX test_unique_constraint 
                        ON data_sources (created_by_id, name)
                    """)
                    print("✓ Test unique constraint created successfully")
                    
                    # Clean up test constraint
                    cursor.execute("DROP INDEX test_unique_constraint")
                    print("✓ Test constraint cleaned up")
                    return True
                except Exception as constraint_error:
                    print(f"✗ Error creating test constraint: {constraint_error}")
                    
                    # Let's see what data might be causing the issue
                    cursor.execute("""
                        SELECT created_by_id, name, COUNT(*) as count
                        FROM data_sources 
                        WHERE is_deleted = 0
                        GROUP BY created_by_id, name
                        HAVING COUNT(*) > 1
                    """)
                    duplicates = cursor.fetchall()
                    if duplicates:
                        print(f"Found {len(duplicates)} duplicate groups in raw SQL:")
                        for dup in duplicates:
                            print(f"  created_by_id={dup[0]}, name='{dup[1]}', count={dup[2]}")
                    else:
                        print("No duplicates found in raw SQL query")
                    
                    return False
                
    except Exception as e:
        print(f"✗ Error testing unique constraint: {e}")
        return False


if __name__ == '__main__':
    print("Starting database debugging and fixing...")
    
    # Step 1: Debug current state
    duplicates = debug_database_state()
    
    # Step 2: Fix issues
    success = fix_database_issues()
    
    # Step 3: Test unique constraint
    constraint_works = test_unique_constraint()
    
    print("\n=== SUMMARY ===")
    if success and constraint_works:
        print("✓ Database is ready for migration")
        print("You can now run: python manage.py migrate")
    else:
        print("✗ Issues remain - manual intervention may be needed") 