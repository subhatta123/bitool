#!/usr/bin/env python3
"""
Fix DuckDB connection and date column types
"""

import os
import sys
import django
import duckdb
import pandas as pd
from pathlib import Path

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

def fix_duckdb_issues():
    """Fix DuckDB connection and date column type issues"""
    
    print("🔧 Starting DuckDB and date type fixes...")
    
    # 1. Check and fix DuckDB database
    duckdb_path = "/app/data/integrated.duckdb"
    print(f"📂 DuckDB path: {duckdb_path}")
    
    try:
        # Create data directory if it doesn't exist
        os.makedirs("/app/data", exist_ok=True)
        
        # Connect to DuckDB
        conn = duckdb.connect(duckdb_path)
        print("✅ Connected to DuckDB successfully")
        
        # Check existing tables
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"📋 Existing tables: {[table[0] for table in tables]}")
        
        # Find the source table
        source_table = None
        for table in tables:
            if table[0].startswith('source_id_4e09d61d'):
                source_table = table[0]
                break
        
        if not source_table:
            print("❌ Source table not found")
            return False
        
        print(f"📊 Found source table: {source_table}")
        
        # Check current schema
        schema = conn.execute(f"DESCRIBE {source_table}").fetchall()
        print("📋 Current schema:")
        for col in schema:
            print(f"   {col[0]}: {col[1]}")
        
        # Check if Order Date is VARCHAR
        order_date_type = None
        ship_date_type = None
        for col in schema:
            if col[0] == 'Order Date':
                order_date_type = col[1]
            elif col[0] == 'Ship Date':
                ship_date_type = col[1]
        
        print(f"📅 Order Date type: {order_date_type}")
        print(f"📅 Ship Date type: {ship_date_type}")
        
        # If dates are VARCHAR, convert them
        if order_date_type == 'VARCHAR' or ship_date_type == 'VARCHAR':
            print("🔄 Converting date columns to TIMESTAMP...")
            
            # Create a new table with proper date types
            new_table = f"{source_table}_fixed"
            
            # Get sample data to check date format
            sample = conn.execute(f'SELECT "Order Date", "Ship Date" FROM {source_table} LIMIT 5').fetchall()
            print("📊 Sample date values:")
            for row in sample:
                print(f"   Order: {row[0]}, Ship: {row[1]}")
            
            # Try to detect date format and convert
            try:
                # First, let's try different date formats
                conversion_sql = f"""
                CREATE TABLE {new_table} AS 
                SELECT 
                    *,
                    TRY_STRPTIME("Order Date", '%d/%m/%Y') AS "Order Date_new",
                    TRY_STRPTIME("Ship Date", '%d/%m/%Y') AS "Ship Date_new"
                FROM {source_table}
                LIMIT 10
                """
                
                conn.execute(conversion_sql)
                test_result = conn.execute(f'SELECT "Order Date_new", "Ship Date_new" FROM {new_table} WHERE "Order Date_new" IS NOT NULL LIMIT 3').fetchall()
                
                if test_result:
                    print("✅ DD/MM/YYYY format detected")
                    date_format = '%d/%m/%Y'
                else:
                    # Try MM/DD/YYYY format
                    conn.execute(f"DROP TABLE {new_table}")
                    conversion_sql = f"""
                    CREATE TABLE {new_table} AS 
                    SELECT 
                        *,
                        TRY_STRPTIME("Order Date", '%m/%d/%Y') AS "Order Date_new",
                        TRY_STRPTIME("Ship Date", '%m/%d/%Y') AS "Ship Date_new"
                    FROM {source_table}
                    LIMIT 10
                    """
                    conn.execute(conversion_sql)
                    test_result = conn.execute(f'SELECT "Order Date_new", "Ship Date_new" FROM {new_table} WHERE "Order Date_new" IS NOT NULL LIMIT 3').fetchall()
                    
                    if test_result:
                        print("✅ MM/DD/YYYY format detected")
                        date_format = '%m/%d/%Y'
                    else:
                        print("❌ Could not detect date format")
                        conn.execute(f"DROP TABLE {new_table}")
                        return False
                
                # Clean up test table
                conn.execute(f"DROP TABLE {new_table}")
                
                # Now create the final fixed table with all columns
                print(f"🔄 Creating fixed table with {date_format} format...")
                
                # Get all column names except the date columns
                all_columns = [col[0] for col in schema if col[0] not in ['Order Date', 'Ship Date']]
                column_list = ', '.join([f'"{col}"' for col in all_columns])
                
                final_conversion_sql = f"""
                CREATE TABLE {new_table} AS 
                SELECT 
                    {column_list},
                    STRPTIME("Order Date", '{date_format}') AS "Order Date",
                    STRPTIME("Ship Date", '{date_format}') AS "Ship Date"
                FROM {source_table}
                """
                
                conn.execute(final_conversion_sql)
                
                # Verify the conversion
                count = conn.execute(f"SELECT COUNT(*) FROM {new_table}").fetchone()[0]
                print(f"✅ Fixed table created with {count} rows")
                
                # Check new schema
                new_schema = conn.execute(f"DESCRIBE {new_table}").fetchall()
                for col in new_schema:
                    if 'Date' in col[0]:
                        print(f"   {col[0]}: {col[1]}")
                
                # Test the problematic query
                test_query = f'''
                SELECT "Customer Name", SUM("Sales") AS total_sales
                FROM {new_table} 
                WHERE YEAR("Order Date") = 2015
                GROUP BY "Customer Name"
                ORDER BY total_sales DESC
                LIMIT 3
                '''
                
                print("🧪 Testing the fixed query...")
                result = conn.execute(test_query).fetchall()
                
                if result:
                    print("✅ Query successful! Results:")
                    for i, row in enumerate(result, 1):
                        print(f"   {i}. {row[0]}: ${row[1]:,.2f}")
                    
                    # Replace the original table
                    conn.execute(f"DROP TABLE {source_table}")
                    conn.execute(f"ALTER TABLE {new_table} RENAME TO {source_table}")
                    print(f"✅ Original table replaced with fixed version")
                    
                else:
                    print("❌ Query returned no results")
                
            except Exception as e:
                print(f"❌ Date conversion failed: {e}")
                return False
        
        else:
            print("✅ Date columns already have correct types")
            
            # Test the query anyway
            test_query = f'''
            SELECT "Customer Name", SUM("Sales") AS total_sales
            FROM {source_table} 
            WHERE YEAR("Order Date") = 2015
            GROUP BY "Customer Name"
            ORDER BY total_sales DESC
            LIMIT 3
            '''
            
            print("🧪 Testing query with existing data...")
            try:
                result = conn.execute(test_query).fetchall()
                if result:
                    print("✅ Query successful! Results:")
                    for i, row in enumerate(result, 1):
                        print(f"   {i}. {row[0]}: ${row[1]:,.2f}")
                else:
                    print("❌ Query returned no results")
            except Exception as e:
                print(f"❌ Query failed: {e}")
        
        conn.close()
        print("✅ DuckDB fixes completed")
        return True
        
    except Exception as e:
        print(f"❌ DuckDB fix failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    fix_duckdb_issues() 