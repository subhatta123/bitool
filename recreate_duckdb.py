#!/usr/bin/env python3
"""
Recreate DuckDB database from scratch with proper date types
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

def recreate_duckdb_with_dates():
    """Recreate DuckDB database from CSV with proper date types"""
    
    print("🔄 Recreating DuckDB database from scratch...")
    
    # 1. Remove corrupted database
    duckdb_path = "/app/data/integrated.duckdb"
    if os.path.exists(duckdb_path):
        os.remove(duckdb_path)
        print(f"🗑️ Removed corrupted database: {duckdb_path}")
    
    # 2. Find the CSV file
    csv_path = "/app/media/csv_files/Sample - Superstore2.csv"
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return False
    
    print(f"📄 Found CSV file: {csv_path}")
    
    try:
        # 3. Create data directory if it doesn't exist
        os.makedirs("/app/data", exist_ok=True)
        
        # 4. Connect to new DuckDB database
        conn = duckdb.connect(duckdb_path)
        print("✅ Created new DuckDB database")
        
        # 5. Load CSV and detect date format
        print("📊 Loading CSV data...")
        df = pd.read_csv(csv_path)
        print(f"✅ Loaded CSV: {len(df)} rows, {len(df.columns)} columns")
        
        # Check sample date values to detect format
        print("📅 Sample date values:")
        print(f"   Order Date: {df['Order Date'].head(3).tolist()}")
        print(f"   Ship Date: {df['Ship Date'].head(3).tolist()}")
        
        # Try to detect date format
        sample_date = str(df['Order Date'].iloc[0])
        if '/' in sample_date:
            parts = sample_date.split('/')
            if len(parts[0]) == 1 or len(parts[0]) == 2:  # Day or Month first
                if int(parts[0]) > 12:  # Must be day first (DD/MM/YYYY)
                    date_format = '%d/%m/%Y'
                    print("✅ Detected DD/MM/YYYY format")
                else:  # Could be MM/DD/YYYY
                    date_format = '%m/%d/%Y'
                    print("✅ Detected MM/DD/YYYY format")
            else:
                date_format = '%Y/%m/%d'
                print("✅ Detected YYYY/MM/DD format")
        elif '-' in sample_date:
            parts = sample_date.split('-')
            if len(parts[0]) == 1 or len(parts[0]) == 2:  # Day or Month first
                if int(parts[0]) > 12:  # Must be day first (DD-MM-YYYY)
                    date_format = '%d-%m-%Y'
                    print("✅ Detected DD-MM-YYYY format")
                else:  # Could be MM-DD-YYYY
                    date_format = '%m-%d-%Y'
                    print("✅ Detected MM-DD-YYYY format")
            else:
                date_format = '%Y-%m-%d'
                print("✅ Detected YYYY-MM-DD format")
        else:
            print("❌ Unknown date format")
            return False
        
        # 6. Create table with proper date conversion in DuckDB
        table_name = "source_id_4e09d61d_3bb6_4184_a9cc_703d6daf4d43"
        
        print(f"🔄 Creating table '{table_name}' with date conversion...")
        
        # First, create a temporary table to load the CSV data
        temp_table = "temp_csv_data"
        
        # Load CSV into DuckDB directly
        conn.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM read_csv_auto('{csv_path}')")
        
        # Check what columns we have
        columns = conn.execute(f"DESCRIBE {temp_table}").fetchall()
        print("📋 Loaded columns:")
        for col in columns:
            print(f"   {col[0]}: {col[1]}")
        
        # Get all columns except date columns for the final table creation
        regular_columns = []
        for col in columns:
            col_name = col[0]
            if col_name not in ['Order Date', 'Ship Date']:
                regular_columns.append(f'"{col_name}"')
        
        # Create the final table with proper date conversion
        create_table_sql = f"""
        CREATE TABLE {table_name} AS 
        SELECT 
            {', '.join(regular_columns)},
            STRPTIME("Order Date", '{date_format}') AS "Order Date",
            STRPTIME("Ship Date", '{date_format}') AS "Ship Date"
        FROM {temp_table}
        """
        
        print("🔄 Converting date columns...")
        conn.execute(create_table_sql)
        
        # Drop temp table
        conn.execute(f"DROP TABLE {temp_table}")
        
        # 7. Verify the table
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"✅ Created table with {count} rows")
        
        # Check the schema
        schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
        print("📋 Final schema:")
        for col in schema:
            if 'Date' in col[0]:
                print(f"   {col[0]}: {col[1]} ⭐")
            else:
                print(f"   {col[0]}: {col[1]}")
        
        # 8. Test the problematic query
        test_query = f'''
        SELECT "Customer Name", SUM("Sales") AS total_sales
        FROM {table_name} 
        WHERE YEAR("Order Date") = 2015
        GROUP BY "Customer Name"
        ORDER BY total_sales DESC
        LIMIT 3
        '''
        
        print("🧪 Testing the query that was failing...")
        result = conn.execute(test_query).fetchall()
        
        if result:
            print("🎉 SUCCESS! Query results:")
            for i, row in enumerate(result, 1):
                print(f"   {i}. {row[0]}: ${row[1]:,.2f}")
        else:
            print("⚠️ Query returned no results (might be no data for 2015)")
        
        # Also test with a different year to make sure the data is there
        test_query_2014 = f'''
        SELECT "Customer Name", SUM("Sales") AS total_sales
        FROM {table_name} 
        WHERE YEAR("Order Date") = 2014
        GROUP BY "Customer Name"
        ORDER BY total_sales DESC
        LIMIT 3
        '''
        
        print("🧪 Testing with year 2014...")
        result_2014 = conn.execute(test_query_2014).fetchall()
        
        if result_2014:
            print("✅ 2014 data found:")
            for i, row in enumerate(result_2014, 1):
                print(f"   {i}. {row[0]}: ${row[1]:,.2f}")
        
        # Check available years
        years_query = f'SELECT DISTINCT YEAR("Order Date") as year FROM {table_name} ORDER BY year'
        years = conn.execute(years_query).fetchall()
        print(f"📅 Available years: {[year[0] for year in years]}")
        
        conn.close()
        print("✅ DuckDB recreation completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ DuckDB recreation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    recreate_duckdb_with_dates() 