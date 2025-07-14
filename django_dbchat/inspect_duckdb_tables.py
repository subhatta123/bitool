#!/usr/bin/env python3
"""
Inspect DuckDB Tables to Find Data
"""

import os
import sys
import django

# Setup Django environment
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

import duckdb

def inspect_duckdb_tables():
    """Inspect all tables in DuckDB to find the data"""
    
    duckdb_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_integration_storage', 'integrated_data.db')
    
    print("🔍 INSPECTING DUCKDB TABLES")
    print("=" * 50)
    print(f"DuckDB path: {duckdb_path}")
    print(f"File exists: {os.path.exists(duckdb_path)}")
    print("")
    
    if not os.path.exists(duckdb_path):
        print("❌ DuckDB file not found!")
        return
    
    conn = duckdb.connect(duckdb_path)
    
    try:
        # Get all tables
        tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        tables = conn.execute(tables_query).fetchall()
        table_names = [table[0] for table in tables]
        
        print(f"📊 Found {len(table_names)} tables:")
        for table_name in table_names:
            print(f"   - {table_name}")
        print("")
        
        # Inspect each table
        for table_name in table_names:
            print(f"🔍 TABLE: {table_name}")
            print("-" * 40)
            
            try:
                # Get row count
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                count_result = conn.execute(count_query).fetchone()
                row_count = count_result[0] if count_result else 0
                
                print(f"   Rows: {row_count}")
                
                if row_count > 0:
                    # Get column info
                    describe_query = f"DESCRIBE {table_name}"
                    columns = conn.execute(describe_query).fetchall()
                    
                    print(f"   Columns ({len(columns)}):")
                    for col in columns[:10]:  # Show first 10 columns
                        print(f"     - {col[0]} ({col[1]})")
                    
                    if len(columns) > 10:
                        print(f"     ... and {len(columns) - 10} more")
                    
                    # Get sample data
                    sample_query = f"SELECT * FROM {table_name} LIMIT 2"
                    sample_data = conn.execute(sample_query).fetchall()
                    
                    if sample_data:
                        print(f"   Sample data (first 2 rows):")
                        for i, row in enumerate(sample_data):
                            print(f"     Row {i+1}: {str(row)[:100]}{'...' if len(str(row)) > 100 else ''}")
                else:
                    print("   (Empty table)")
                
                print("")
                
            except Exception as e:
                print(f"   ❌ Error inspecting {table_name}: {e}")
                print("")
    
    finally:
        conn.close()

def find_titanic_data():
    """Look specifically for Titanic data (PassengerId, Survived, etc.)"""
    
    duckdb_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_integration_storage', 'integrated_data.db')
    conn = duckdb.connect(duckdb_path)
    
    print("🚢 LOOKING FOR TITANIC DATA")
    print("=" * 50)
    
    try:
        # Get all tables
        tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        tables = conn.execute(tables_query).fetchall()
        table_names = [table[0] for table in tables]
        
        for table_name in table_names:
            try:
                # Check if this table has Titanic-like columns
                describe_query = f"DESCRIBE {table_name}"
                columns = conn.execute(describe_query).fetchall()
                column_names = [col[0].lower() for col in columns]
                
                # Look for Titanic indicators
                titanic_indicators = ['passengerid', 'survived', 'pclass', 'name', 'sex', 'age']
                
                if any(indicator in column_names for indicator in titanic_indicators):
                    print(f"🎯 FOUND TITANIC DATA in table: {table_name}")
                    
                    # Get row count
                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                    count_result = conn.execute(count_query).fetchone()
                    row_count = count_result[0] if count_result else 0
                    
                    print(f"   Rows: {row_count}")
                    print(f"   Columns: {[col[0] for col in columns]}")
                    
                    # Sample data
                    if row_count > 0:
                        sample_query = f"SELECT * FROM {table_name} LIMIT 3"
                        sample = conn.execute(sample_query).fetchall()
                        print(f"   First 3 rows:")
                        for i, row in enumerate(sample):
                            print(f"     {i+1}: {row}")
                    
                    return table_name
                    
            except Exception as e:
                continue
        
        print("❌ No Titanic data found in any table")
        return None
        
    finally:
        conn.close()

if __name__ == "__main__":
    inspect_duckdb_tables()
    print("\n" + "=" * 50)
    titanic_table = find_titanic_data()
    
    if titanic_table:
        print(f"\n✅ Found your data in table: {titanic_table}")
        print("This is the table that should be used for semantic layer generation!")
    else:
        print("\n⚠️ Your Titanic data may be in a different DuckDB file or location") 