#!/usr/bin/env python
"""
Check what tables exist in the integrated database
"""

import os
import duckdb

# Database path
db_path = '../data_integration_storage/integrated_data.db'

if os.path.exists(db_path):
    print(f"Database found at: {db_path}")
    print(f"Database size: {os.path.getsize(db_path)} bytes")
    
    try:
        # Connect to database
        conn = duckdb.connect(db_path)
        
        # List all tables
        print("\n=== TABLES IN DATABASE ===")
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Found {len(tables)} tables:")
        for table in tables:
            table_name = table[0]
            print(f"  - {table_name}")
            
            # Get table info
            try:
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                if count_result:
                    count = count_result[0]
                    print(f"    Rows: {count}")
                else:
                    print(f"    Rows: Unable to get count")
                
                # Get column info
                columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
                print(f"    Columns: {[col[0] for col in columns]}")
                
                # Show sample data
                sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                if sample:
                    print(f"    Sample data: {sample[0]}")
                print()
                
            except Exception as e:
                print(f"    Error querying table: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error connecting to database: {e}")
        
else:
    print(f"Database not found at: {db_path}")
    print("Checking other possible locations...")
    
    other_paths = [
        'data/integrated.duckdb',
        'data_integration_storage/integrated_data.db',
        '../dbchat/data_integration_storage/integrated_data.db'
    ]
    
    for path in other_paths:
        if os.path.exists(path):
            print(f"  Found alternative at: {path}")
        else:
            print(f"  Not found: {path}") 