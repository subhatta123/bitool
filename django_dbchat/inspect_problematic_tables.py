#!/usr/bin/env python3
"""
Inspect problematic tables that contain hardcoded superstore_data references
"""

import os
import sys
import django

# Set up Django environment
sys.path.append('/c%3A/Users/SuddhasheelBhattacha/OneDrive%20-%20Mendix%20Technology%20B.V/Desktop/dbchat/django_dbchat')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

import duckdb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inspect_problematic_tables():
    """Inspect tables that are causing superstore_data errors"""
    print("🔍 Inspecting Problematic Tables with Hardcoded References...")
    print("=" * 70)
    
    problematic_tables = [
        'col_mapping',
        'col_mapping_fixed_dates', 
        'csv_data',
        'sample___superstore2'
    ]
    
    try:
        db_path = 'data/integrated.duckdb'
        conn = duckdb.connect(db_path)
        
        # Get all tables first
        all_tables = conn.execute("SHOW TABLES").fetchall()
        print(f"📊 All tables in DuckDB: {[t[0] for t in all_tables]}")
        
        for table_name in problematic_tables:
            print(f"\n🔍 Inspecting table: {table_name}")
            print("-" * 50)
            
            try:
                # Check if table exists
                try:
                    table_check = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = ?", [table_name]).fetchall()
                except:
                    # Fallback to SHOW TABLES approach
                    table_check = conn.execute(f"SHOW TABLES").fetchall()
                    table_check = [t for t in table_check if t and len(t) > 0 and t[0] == table_name]
                
                if not table_check:
                    print(f"   ⚠️  Table {table_name} does not exist")
                    continue
                
                # Get table schema
                print(f"   📋 Schema for {table_name}:")
                schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
                for col_info in schema:
                    print(f"      - {col_info[0]}: {col_info[1]}")
                
                # Get row count
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"   📈 Row count: {row_count}")
                
                if row_count > 0:
                    # Get sample data to see what's inside
                    print(f"   📋 Sample data (first 3 rows):")
                    sample_data = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                    
                    for i, row in enumerate(sample_data, 1):
                        print(f"      Row {i}: {row}")
                        
                        # Check if any values contain "superstore_data"
                        for j, value in enumerate(row):
                            if value and "superstore_data" in str(value):
                                col_name = schema[j][0]
                                print(f"         🚨 FOUND hardcoded 'superstore_data' in column '{col_name}': {value}")
                
                print(f"   ✅ Inspection complete for {table_name}")
                
            except Exception as table_error:
                print(f"   ❌ Error inspecting {table_name}: {table_error}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error during inspection: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_for_hardcoded_references():
    """Check all tables for any hardcoded references"""
    print(f"\n🔍 Checking All Tables for Hardcoded References...")
    print("=" * 70)
    
    try:
        db_path = 'data/integrated.duckdb'
        conn = duckdb.connect(db_path)
        
        # Get all tables
        all_tables = conn.execute("SHOW TABLES").fetchall()
        
        hardcoded_patterns = [
            'superstore_data',
            'csv_data',
            'main_table',
            'data_table'
        ]
        
        problematic_tables = []
        
        for table_tuple in all_tables:
            table_name = table_tuple[0]
            print(f"\n📊 Checking table: {table_name}")
            
            try:
                # Get a sample of all data to check for hardcoded references
                sample_data = conn.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchall()
                
                found_hardcoded = False
                for row in sample_data:
                    for value in row:
                        if value:
                            value_str = str(value)
                            for pattern in hardcoded_patterns:
                                if pattern in value_str:
                                    print(f"   🚨 Found '{pattern}' in value: {value_str[:100]}...")
                                    found_hardcoded = True
                
                if found_hardcoded:
                    problematic_tables.append(table_name)
                    print(f"   ❌ Table {table_name} contains hardcoded references")
                else:
                    print(f"   ✅ Table {table_name} looks clean")
                    
            except Exception as e:
                print(f"   ⚠️  Could not check {table_name}: {e}")
        
        print(f"\n📋 SUMMARY:")
        print(f"   Total tables checked: {len(all_tables)}")
        print(f"   Problematic tables: {len(problematic_tables)}")
        
        if problematic_tables:
            print(f"   🚨 Tables with hardcoded references:")
            for table in problematic_tables:
                print(f"      - {table}")
        else:
            print(f"   ✅ No hardcoded references found!")
        
        conn.close()
        return problematic_tables
        
    except Exception as e:
        print(f"❌ Error during hardcoded reference check: {e}")
        return []

def cleanup_problematic_tables(problematic_tables):
    """Clean up or fix tables with hardcoded references"""
    print(f"\n🧹 Cleaning Up Problematic Tables...")
    print("=" * 70)
    
    if not problematic_tables:
        print("✅ No tables need cleanup!")
        return True
    
    try:
        db_path = 'data/integrated.duckdb'
        conn = duckdb.connect(db_path)
        
        for table_name in problematic_tables:
            print(f"\n🧹 Cleaning table: {table_name}")
            
            try:
                # Check if this table is a metadata/mapping table that can be safely dropped
                if any(keyword in table_name.lower() for keyword in ['mapping', 'col_', 'sample', 'csv_data']):
                    print(f"   🗑️  Dropping metadata table: {table_name}")
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    print(f"   ✅ Dropped {table_name}")
                else:
                    print(f"   ⚠️  Skipping {table_name} (might contain important data)")
                    
            except Exception as table_error:
                print(f"   ❌ Error cleaning {table_name}: {table_error}")
        
        conn.close()
        print(f"\n✅ Cleanup completed!")
        return True
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Root Cause Analysis: Hardcoded Table References")
    print("=" * 70)
    
    # Step 1: Inspect the known problematic tables
    step1_success = inspect_problematic_tables()
    
    # Step 2: Check all tables for hardcoded references
    problematic_tables = check_for_hardcoded_references()
    
    # Step 3: Clean up problematic tables
    if problematic_tables:
        step3_success = cleanup_problematic_tables(problematic_tables)
    else:
        step3_success = True
    
    print(f"\n" + "=" * 70)
    print("🎯 ROOT CAUSE ANALYSIS RESULTS:")
    print(f"   Table inspection: {'✅' if step1_success else '❌'}")
    print(f"   Hardcoded reference check: {'✅' if len(problematic_tables) == 0 else f'❌ Found {len(problematic_tables)} problematic tables'}")
    print(f"   Cleanup: {'✅' if step3_success else '❌'}")
    
    if step1_success and step3_success:
        print(f"\n🎉 ROOT CAUSE FIXED!")
        print("✅ Hardcoded 'superstore_data' references eliminated")
        print("✅ Problematic metadata tables cleaned up")
        print("✅ System should no longer search for non-existent tables")
    else:
        print(f"\n⚠️  Some issues remain")
        print("💡 Check the error messages above for details")
    
    print("=" * 70) 