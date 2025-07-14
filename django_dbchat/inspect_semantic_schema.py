#!/usr/bin/env python
"""
Script to inspect the semantic_columns table schema and identify issues.
"""

import os
import sys
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.db import connection


def inspect_semantic_schema():
    """Inspect the semantic_columns table schema"""
    print("=== INSPECTING SEMANTIC_COLUMNS TABLE SCHEMA ===")
    
    with connection.cursor() as cursor:
        # Get table info
        cursor.execute("PRAGMA table_info(semantic_columns)")
        columns = cursor.fetchall()
        
        print("Current semantic_columns table structure:")
        print("cid | name | type | notnull | dflt_value | pk")
        print("-" * 60)
        
        for col in columns:
            cid, name, type_name, notnull, dflt_value, pk = col
            print(f"{cid:3} | {name:20} | {type_name:15} | {notnull:7} | {str(dflt_value):10} | {pk}")
        
        # Check if semantic_table_id column exists
        column_names = [col[1] for col in columns]
        has_semantic_table_id = 'semantic_table_id' in column_names
        
        print(f"\nHas semantic_table_id column: {has_semantic_table_id}")
        
        if not has_semantic_table_id:
            print("❌ MISSING: semantic_table_id column")
            print("This is required by the Django model but doesn't exist in the database.")
            
            # Check what foreign key columns exist
            foreign_key_columns = [name for name in column_names if name.endswith('_id')]
            print(f"Foreign key columns found: {foreign_key_columns}")
            
            # Check if there's a table_name column (old schema)
            if 'table_name' in column_names:
                print("⚠️  Found 'table_name' column - this suggests old schema")
        
        # Check for any data
        cursor.execute("SELECT COUNT(*) FROM semantic_columns")
        count = cursor.fetchone()[0]
        print(f"\nTotal rows in semantic_columns: {count}")
        
        if count > 0:
            cursor.execute("SELECT * FROM semantic_columns LIMIT 3")
            sample_data = cursor.fetchall()
            print("\nSample data:")
            for row in sample_data:
                print(f"  {row}")


def check_semantic_tables_schema():
    """Check the semantic_tables table schema"""
    print("\n=== INSPECTING SEMANTIC_TABLES TABLE SCHEMA ===")
    
    with connection.cursor() as cursor:
        # Get table info
        cursor.execute("PRAGMA table_info(semantic_tables)")
        columns = cursor.fetchall()
        
        print("Current semantic_tables table structure:")
        print("cid | name | type | notnull | dflt_value | pk")
        print("-" * 60)
        
        for col in columns:
            cid, name, type_name, notnull, dflt_value, pk = col
            print(f"{cid:3} | {name:20} | {type_name:15} | {notnull:7} | {str(dflt_value):10} | {pk}")
        
        # Check for data
        cursor.execute("SELECT COUNT(*) FROM semantic_tables")
        count = cursor.fetchone()[0]
        print(f"\nTotal rows in semantic_tables: {count}")


def main():
    """Main function"""
    print("Starting schema inspection...")
    
    inspect_semantic_schema()
    check_semantic_tables_schema()
    
    print("\n=== RECOMMENDATION ===")
    print("The issue is that the semantic_columns table is missing the semantic_table_id column")
    print("that the Django model expects. This needs to be fixed with a migration.")


if __name__ == '__main__':
    main() 