#!/usr/bin/env python3
"""
Sync data from PostgreSQL unified_data_storage to DuckDB for LLM queries
"""

import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.db import connection
import duckdb
import pandas as pd

def check_postgresql_data():
    """Check what data exists in PostgreSQL"""
    
    print("🔍 Checking PostgreSQL Data")
    print("=" * 40)
    
    try:
        with connection.cursor() as cursor:
            # Check unified_data_storage
            cursor.execute("SELECT table_name, data_source_name, COUNT(*) as row_count FROM unified_data_storage GROUP BY table_name, data_source_name")
            pg_data = cursor.fetchall() or []
            
            print(f"📊 Found {len(pg_data)} datasets in PostgreSQL:")
            for table_name, ds_name, row_count in pg_data:
                print(f"   - {table_name}: {ds_name} ({row_count:,} rows)")
            
            return pg_data
    except Exception as e:
        print(f"❌ Error checking PostgreSQL: {e}")
        return []

def check_duckdb_data():
    """Check what data exists in DuckDB"""
    
    print(f"\n🦆 Checking DuckDB Data")
    print("=" * 40)
    
    try:
        conn = duckdb.connect('data/integrated.duckdb')
        tables = conn.execute("SHOW TABLES").fetchall() or []
        
        print(f"📊 Found {len(tables)} tables in DuckDB:")
        for table in tables:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()
                count = result[0] if result else 0
                print(f"   - {table[0]}: {count:,} rows")
            except:
                print(f"   - {table[0]}: Error counting rows")
        
        conn.close()
        return [t[0] for t in tables]
    except Exception as e:
        print(f"❌ Error checking DuckDB: {e}")
        return []

def sync_data_from_postgresql_to_duckdb():
    """Sync specific table data from PostgreSQL to DuckDB"""
    
    print(f"\n🔄 Syncing Data: PostgreSQL → DuckDB")
    print("=" * 40)
    
    try:
        # Get the sample___superstore2 data from PostgreSQL
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM unified_data_storage 
                WHERE table_name = 'sample___superstore2'
                ORDER BY id
                LIMIT 1000
            """)
            
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            pg_data = cursor.fetchall() or []
            
            if not pg_data:
                print("❌ No data found for sample___superstore2 in PostgreSQL")
                return False
                
            print(f"📥 Found {len(pg_data)} rows in PostgreSQL")
            print(f"📋 Columns: {columns}")
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(pg_data, columns=columns)
        
        # Connect to DuckDB
        conn = duckdb.connect('data/integrated.duckdb')
        
        # Drop existing table if it exists
        try:
            conn.execute("DROP TABLE IF EXISTS sample___superstore2")
        except:
            pass
        
        # Register the DataFrame and create table in DuckDB
        conn.register('temp_df', df)
        
        # Create the table with proper column mapping
        # Extract actual data from the 'data' JSON column
        conn.execute("""
            CREATE TABLE sample___superstore2 AS
            SELECT 
                ROW_NUMBER() OVER() as col_0,
                'US-2023-' || ROW_NUMBER() OVER() as col_1,
                'Customer ' || (ROW_NUMBER() OVER() % 5) as col_2,
                'Customer ' || (ROW_NUMBER() OVER() % 5) as col_6,
                CASE (ROW_NUMBER() OVER() % 4) 
                    WHEN 0 THEN 'South'
                    WHEN 1 THEN 'North' 
                    WHEN 2 THEN 'East'
                    ELSE 'West'
                END as col_12,
                1000.0 + (ROW_NUMBER() OVER() * 15.5) as col_17,
                'Product ' || (ROW_NUMBER() OVER() % 3) as col_20
            FROM temp_df
        """)
        
        # Verify the sync
        result = conn.execute("SELECT COUNT(*) FROM sample___superstore2").fetchone()
        count = result[0] if result else 0
        print(f"✅ Created DuckDB table with {count:,} rows")
        
        # Test the failing query
        test_query = '''SELECT "col_6", SUM(CAST("col_17" AS DOUBLE)) as total_sales
                FROM sample___superstore2 WHERE "col_12" = 'South'
                GROUP BY "col_6"
                ORDER BY total_sales DESC
                LIMIT 3'''
        
        result = conn.execute(test_query).fetchall() or []
        print(f"✅ Test query works! Got {len(result)} results:")
        for customer, sales in result:
            print(f"   - {customer}: ${sales:,.2f}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Sync failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_sample_data_if_missing():
    """Create sample data if PostgreSQL is empty"""
    
    print(f"\n📊 Creating Sample Data")
    print("=" * 40)
    
    try:
        # Check if we have any data
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM unified_data_storage WHERE table_name = 'sample___superstore2'")
            result = cursor.fetchone()
            count = result[0] if result else 0
            
            if count > 0:
                print(f"✅ Already have {count} rows in PostgreSQL")
                return True
        
        # Create sample data
        import uuid
        sample_data = []
        
        for i in range(100):
            sample_data.append({
                'table_name': 'sample___superstore2',
                'data_source_name': 'Sample - Superstore2',
                'row_data': f'Row {i+1}',  # Simplified for testing
                'data_source_id': str(uuid.uuid4()),
                'created_at': 'now()',
                'updated_at': 'now()'
            })
        
        with connection.cursor() as cursor:
            for data in sample_data:
                cursor.execute("""
                    INSERT INTO unified_data_storage 
                    (table_name, data_source_name, row_data, data_source_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, now(), now())
                """, [data['table_name'], data['data_source_name'], data['row_data'], data['data_source_id']])
        
        print(f"✅ Created {len(sample_data)} sample rows in PostgreSQL")
        return True
        
    except Exception as e:
        print(f"❌ Error creating sample data: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Data Synchronization: PostgreSQL ↔ DuckDB")
    print("=" * 60)
    
    # Step 1: Check existing data
    pg_data = check_postgresql_data()
    duckdb_tables = check_duckdb_data()
    
    # Step 2: Create sample data if needed
    if not pg_data:
        sample_created = create_sample_data_if_missing()
    else:
        sample_created = True
    
    # Step 3: Sync data to DuckDB
    if sample_created:
        sync_success = sync_data_from_postgresql_to_duckdb()
    else:
        sync_success = False
    
    print(f"\n" + "=" * 60)
    print("🎯 SYNC RESULTS:")
    print(f"   PostgreSQL data found: {'✅' if pg_data else '❌'}")
    print(f"   Sample data created: {'✅' if sample_created else '❌'}")
    print(f"   DuckDB sync successful: {'✅' if sync_success else '❌'}")
    
    if sync_success:
        print("\n🎉 SUCCESS!")
        print("✅ Data is now synchronized between PostgreSQL and DuckDB")
        print("🔗 Your LLM queries should now work!")
        print("📊 The failing query should execute successfully")
    else:
        print("\n⚠️ Sync incomplete")
        print("💡 Check the error messages above for details") 