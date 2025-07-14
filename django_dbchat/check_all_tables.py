#!/usr/bin/env python
"""
Script to check what tables exist in the database.
"""

import os
import sys
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.db import connection


def check_existing_tables():
    """Check what tables exist in the database"""
    print("=== CHECKING ALL TABLES IN DATABASE ===")
    
    with connection.cursor() as cursor:
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("Existing tables:")
        for table in tables:
            table_name = table[0]
            print(f"  - {table_name}")
            
            # Check if it's a semantic-related table
            if 'semantic' in table_name.lower():
                print(f"    *** SEMANTIC TABLE FOUND: {table_name} ***")
                
                # Get table info
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                print(f"    Columns in {table_name}:")
                for col in columns:
                    cid, name, type_name, notnull, dflt_value, pk = col
                    print(f"      {name} ({type_name})")


def main():
    """Main function"""
    print("Checking database tables...")
    check_existing_tables()


if __name__ == '__main__':
    main() 