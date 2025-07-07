#!/usr/bin/env python3
"""
Debug Dataset Confusion Script
Helps identify and fix issues where wrong datasets are being loaded from DuckDB
"""

import os
import sys
import pandas as pd
import duckdb
from pathlib import Path

# Configure logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_django():
    """Setup Django environment"""
    try:
        django_path = Path(__file__).parent / 'django_dbchat'
        sys.path.insert(0, str(django_path))
        
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
        
        import django
        django.setup()
        
        return True
    except Exception as e:
        logger.error(f"Failed to setup Django: {e}")
        return False

def analyze_duckdb_tables():
    """Analyze all tables in DuckDB to identify confusion"""
    print("\n" + "="*60)
    print("DUCKDB TABLE ANALYSIS")
    print("="*60)
    
    try:
        # Connect to DuckDB
        db_path = 'data/integrated.duckdb'
        if not os.path.exists(db_path):
            print(f"[INFO] DuckDB file not found at: {db_path}")
            return {}
        
        conn = duckdb.connect(db_path)
        
        # Get all tables
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        
        print(f"[INFO] Found {len(tables)} tables in DuckDB")
        
        table_info = {}
        
        for (table_name,) in tables:
            try:
                # Get table info
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                row_count = count_result[0] if count_result else 0
                
                columns_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
                columns = [col[0] for col in columns_result]
                
                # Get sample data
                sample_result = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                
                table_info[table_name] = {
                    'row_count': row_count,
                    'column_count': len(columns),
                    'columns': columns,
                    'sample_data': sample_result
                }
                
                print(f"\n[TABLE] {table_name}:")
                print(f"  Rows: {row_count}")
                print(f"  Columns ({len(columns)}): {columns[:10]}{'...' if len(columns) > 10 else ''}")
                
                # Check if this looks like a specific dataset
                if 'Customer_Name' in columns and 'Sales' in columns:
                    print(f"  [DETECTED] Superstore-like dataset")
                elif 'Age' in columns and 'Sex' in columns and 'Survived' in columns:
                    print(f"  [DETECTED] Titanic-like dataset")
                else:
                    print(f"  [DETECTED] Unknown dataset type")
                
            except Exception as e:
                print(f"  [ERROR] Failed to analyze table {table_name}: {e}")
                table_info[table_name] = {'error': str(e)}
        
        conn.close()
        return table_info
        
    except Exception as e:
        print(f"[ERROR] Failed to analyze DuckDB: {e}")
        return {}

def analyze_django_data_sources():
    """Analyze Django data sources"""
    print("\n" + "="*60)
    print("DJANGO DATA SOURCES ANALYSIS")
    print("="*60)
    
    try:
        from datasets.models import DataSource
        
        data_sources = DataSource.objects.filter(is_deleted=False).order_by('-created_at')
        
        print(f"[INFO] Found {data_sources.count()} active data sources")
        
        for ds in data_sources:
            print(f"\n[DATA SOURCE] {ds.name} (ID: {ds.id})")
            print(f"  Type: {ds.source_type}")
            print(f"  Created: {ds.created_at}")
            print(f"  User: {ds.created_by.username if ds.created_by else 'Unknown'}")
            
            # Check connection info
            if ds.connection_info:
                print(f"  Connection Info:")
                for key, value in ds.connection_info.items():
                    if key == 'file_path':
                        print(f"    {key}: {value}")
                        # Check if file exists
                        full_path = Path('media') / value
                        if full_path.exists():
                            print(f"      [FILE EXISTS] Size: {full_path.stat().st_size} bytes")
                        else:
                            print(f"      [FILE MISSING] Expected at: {full_path}")
                    else:
                        print(f"    {key}: {value}")
            
            # Check schema info
            if ds.schema_info:
                columns = ds.schema_info.get('columns', [])
                print(f"  Schema: {len(columns)} columns")
                if columns:
                    column_names = [col.get('name', 'unknown') for col in columns[:5]]
                    print(f"    First 5 columns: {column_names}")
            
            # Check expected DuckDB table
            expected_table = f"ds_{ds.id.hex.replace('-', '_')}"
            print(f"  Expected DuckDB table: {expected_table}")
        
    except Exception as e:
        print(f"[ERROR] Failed to analyze Django data sources: {e}")

def identify_conflicts():
    """Identify potential conflicts and confusions"""
    print("\n" + "="*60)
    print("CONFLICT IDENTIFICATION")
    print("="*60)
    
    # Analyze both systems
    duckdb_info = analyze_duckdb_tables()
    
    if not setup_django():
        print("[ERROR] Cannot analyze Django data sources")
        return
    
    analyze_django_data_sources()
    
    # Identify potential issues
    print(f"\n[ANALYSIS] Potential Issues:")
    
    # Check for multiple similar datasets
    superstore_tables = []
    titanic_tables = []
    
    for table_name, info in duckdb_info.items():
        if 'error' not in info:
            columns = info.get('columns', [])
            if 'Customer_Name' in columns and 'Sales' in columns:
                superstore_tables.append(table_name)
            elif 'Age' in columns and 'Sex' in columns:
                titanic_tables.append(table_name)
    
    if len(superstore_tables) > 1:
        print(f"  [CONFLICT] Multiple Superstore-like tables: {superstore_tables}")
    
    if len(titanic_tables) > 1:
        print(f"  [CONFLICT] Multiple Titanic-like tables: {titanic_tables}")
    
    # Check for generic table names that might cause confusion
    generic_names = ['csv_data', 'superstore_data', 'data', 'main_table']
    for name in generic_names:
        if name in duckdb_info:
            print(f"  [WARNING] Generic table name found: {name} - might cause confusion")

def clear_duckdb_cache():
    """Clear DuckDB cache"""
    print("\n" + "="*60)
    print("CLEAR DUCKDB CACHE")
    print("="*60)
    
    try:
        db_path = 'data/integrated.duckdb'
        if not os.path.exists(db_path):
            print(f"[INFO] DuckDB file not found at: {db_path}")
            return
        
        conn = duckdb.connect(db_path)
        
        # Get all tables
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        
        print(f"[INFO] Found {len(tables)} tables to clear")
        
        for (table_name,) in tables:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"  [CLEARED] {table_name}")
            except Exception as e:
                print(f"  [ERROR] Failed to clear {table_name}: {e}")
        
        conn.close()
        print(f"[SUCCESS] Cleared all tables from DuckDB")
        
    except Exception as e:
        print(f"[ERROR] Failed to clear DuckDB cache: {e}")

def main():
    """Main debugging function"""
    print("Dataset Confusion Debugger")
    print("="*70)
    
    print("\nThis script helps identify why you're seeing:")
    print("- Wrong datasets (9994 rows instead of your 900 rows)")
    print("- Column mismatches (Titanic columns vs Superstore data)")
    print("- Duplicate data sources")
    
    while True:
        print("\n" + "="*50)
        print("OPTIONS:")
        print("1. Analyze DuckDB tables")
        print("2. Analyze Django data sources") 
        print("3. Identify conflicts")
        print("4. Clear DuckDB cache (CAUTION: removes all data)")
        print("5. Full analysis (options 1-3)")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            analyze_duckdb_tables()
        elif choice == '2':
            if setup_django():
                analyze_django_data_sources()
        elif choice == '3':
            identify_conflicts()
        elif choice == '4':
            confirm = input("Are you sure you want to clear ALL DuckDB data? (yes/no): ").strip().lower()
            if confirm == 'yes':
                clear_duckdb_cache()
            else:
                print("[CANCELLED] DuckDB cache not cleared")
        elif choice == '5':
            print("[RUNNING] Full analysis...")
            analyze_duckdb_tables()
            identify_conflicts()
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please enter 1-6.")

if __name__ == "__main__":
    main() 