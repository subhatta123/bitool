#!/usr/bin/env python
"""
Debug script to check g7 data source issue
"""
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource
import duckdb

def debug_g7_issue():
    print("=== DEBUGGING G7 TABLE ISSUE ===")
    
    # Check the g7 data source
    try:
        ds = DataSource.objects.get(name='g7')
        print(f"✅ Found data source: {ds.name}")
        print(f"   Type: {ds.source_type}")
        print(f"   Status: {ds.status}")
        print(f"   ID: {ds.id}")
        
        if ds.connection_info:
            print(f"   Connection info keys: {list(ds.connection_info.keys())}")
            if 'file_path' in ds.connection_info:
                file_path = ds.connection_info['file_path']
                print(f"   File path: {file_path}")
                
                # Check if file exists
                if os.path.exists(file_path):
                    print(f"   ✅ CSV file exists")
                else:
                    print(f"   ❌ CSV file missing: {file_path}")
        
    except DataSource.DoesNotExist:
        print("❌ Data source 'g7' not found")
        return
    except Exception as e:
        print(f"❌ Error checking data source: {e}")
        return
    
    # Check integrated database
    print("\n=== CHECKING INTEGRATED DATABASE ===")
    try:
        db_path = 'data_integration_storage/integrated_data.db'
        print(f"Database path: {db_path}")
        
        if os.path.exists(db_path):
            print("✅ Integrated database file exists")
            
            conn = duckdb.connect(db_path)
            tables = conn.execute("SHOW TABLES").fetchall()
            
            print(f"Found {len(tables)} tables in integrated database:")
            for table in tables:
                table_name = table[0]
                print(f"  - {table_name}")
                
                # Check if this is our g7 table
                if 'g7' in table_name.lower():
                    print(f"    🎯 This looks like our g7 table!")
                    
                    # Check row count
                    try:
                        count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                        print(f"    📊 Row count: {count}")
                    except Exception as e:
                        print(f"    ❌ Error counting rows: {e}")
            
            conn.close()
            
            # Check if any table matches g7
            g7_tables = [t[0] for t in tables if 'g7' in str(t[0]).lower()]
            if g7_tables:
                print(f"\n✅ Found potential g7 tables: {g7_tables}")
            else:
                print(f"\n❌ No g7 tables found in integrated database")
                print("🔧 SOLUTION: You need to run ETL integration for the g7 CSV file")
                
        else:
            print("❌ Integrated database file does not exist")
            print("🔧 SOLUTION: You need to create the integrated database first")
            
    except Exception as e:
        print(f"❌ Error checking integrated database: {e}")
    
    print("\n=== RECOMMENDATIONS ===")
    print("1. Go to the ETL Operations page: /datasets/integration/")
    print("2. Find the 'g7' data source")
    print("3. Click 'Integrate' to load it into the database")
    print("4. After integration, try your query again")

if __name__ == '__main__':
    debug_g7_issue() 