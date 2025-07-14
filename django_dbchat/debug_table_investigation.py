#!/usr/bin/env python3
"""
Diagnostic tool to investigate table naming issues
"""

import os
import django
import sys
from pathlib import Path

# Add the project directory to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.data_access_layer import unified_data_access
from datasets.models import DataSource
from django.contrib.auth import get_user_model

User = get_user_model()

def investigate_missing_table():
    """Investigate why the right table cannot be found"""
    print("=== TABLE INVESTIGATION DIAGNOSTIC ===")
    
    # The problematic data source ID from the logs
    missing_table_id = "75779d7e-8232-4ffe-8462-97151c7e1ee5"
    print(f"Investigating missing table for data source: {missing_table_id}")
    
    # Check if data source exists
    try:
        data_source = DataSource.objects.get(id=missing_table_id)
        print(f"[SUCCESS] Data source found: {data_source.name}")
        print(f"  - Status: {data_source.status}")
        print(f"  - Source type: {data_source.source_type}")
        print(f"  - Created: {data_source.created_at}")
        print(f"  - Connection info: {data_source.connection_info}")
    except DataSource.DoesNotExist:
        print(f"[ERROR] Data source {missing_table_id} does not exist in database!")
        return
    
    # Get DuckDB connection
    unified_data_access._ensure_duckdb_connection()
    conn = unified_data_access.duckdb_connection
    
    if not conn:
        print("[ERROR] Could not connect to DuckDB")
        return
    
    print(f"\n[SUCCESS] Connected to DuckDB")
    
    # Get all available tables
    try:
        tables_result = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        available_tables = [table[0] for table in tables_result]
        print(f"\n[INFO] Found {len(available_tables)} total tables in database")
        
        print("\n=== ALL AVAILABLE TABLES ===")
        for i, table in enumerate(available_tables, 1):
            print(f"  {i:2d}. {table}")
        
    except Exception as e:
        print(f"[ERROR] Could not list tables: {e}")
        return
    
    # Generate all possible patterns for the missing table
    print(f"\n=== EXPECTED PATTERNS FOR {missing_table_id} ===")
    
    # Clean the UUID for different formats
    uuid_clean = missing_table_id.replace('-', '')
    uuid_underscores = missing_table_id.replace('-', '_')
    
    expected_patterns = [
        f"ds_{uuid_clean}",
        f"ds_{uuid_underscores}",
        f"source_id_{uuid_underscores}",
        f"source_{uuid_clean}",
        f"source_{uuid_underscores}",
        f"data_source_{uuid_clean}",
        f"data_source_{uuid_underscores}",
    ]
    
    print("Expected patterns:")
    for i, pattern in enumerate(expected_patterns, 1):
        exists = pattern in available_tables
        status = "[EXISTS]" if exists else "[MISSING]"
        print(f"  {i}. {pattern} {status}")
    
    # Check for partial matches
    print(f"\n=== PARTIAL MATCHES ===")
    partial_matches = []
    
    # Look for tables that contain parts of the UUID
    uuid_parts = [
        uuid_clean[:8],
        uuid_clean[8:16], 
        uuid_clean[16:24],
        uuid_clean[24:32],
        missing_table_id.split('-')[0],
        missing_table_id.split('-')[1],
    ]
    
    for table in available_tables:
        for part in uuid_parts:
            if part in table:
                partial_matches.append((table, part))
                break
    
    if partial_matches:
        print("Found partial matches:")
        for table, part in partial_matches:
            print(f"  - {table} (contains: {part})")
    else:
        print("No partial matches found")
    
    # Check similar data sources
    print(f"\n=== SIMILAR DATA SOURCES ===")
    try:
        similar_sources = DataSource.objects.filter(
            created_by=data_source.created_by,
            source_type=data_source.source_type,
            status='active'
        ).exclude(id=missing_table_id)[:5]
        
        print(f"Found {similar_sources.count()} similar data sources:")
        for source in similar_sources:
            # Check what table names exist for these sources
            source_patterns = [
                f"ds_{str(source.id).replace('-', '')}",
                f"source_id_{str(source.id).replace('-', '_')}"
            ]
            
            existing_patterns = [p for p in source_patterns if p in available_tables]
            print(f"  - {source.name} (ID: {source.id})")
            print(f"    Existing tables: {existing_patterns}")
            
    except Exception as e:
        print(f"Error checking similar sources: {e}")
    
    # Data source workflow analysis
    print(f"\n=== DATA SOURCE ANALYSIS ===")
    
    if hasattr(data_source, 'workflow_status'):
        workflow_status = data_source.workflow_status
        print(f"Workflow status: {workflow_status}")
    else:
        print("No workflow status found")
    
    # Check if any ETL operations exist for this data source
    from datasets.models import ETLOperation
    etl_operations = ETLOperation.objects.filter(
        source_tables__contains=[f"ds_{uuid_clean}"]
    )
    
    print(f"ETL operations referencing this data source: {etl_operations.count()}")
    
    # Recommendations
    print(f"\n=== RECOMMENDATIONS ===")
    
    if not any(pattern in available_tables for pattern in expected_patterns):
        print("1. [CRITICAL] No table found for this data source")
        print("   - Data source may not have been processed through ETL pipeline")
        print("   - Check data source upload/import status")
        print("   - Verify data source is marked as 'active'")
        
        if data_source.source_type == 'csv':
            print("   - For CSV sources: ensure file was uploaded and processed")
            
        print("\n2. [ACTION] Recommended fixes:")
        print("   a) Re-upload/re-process the data source")
        print("   b) Check data integration logs for errors")
        print("   c) Verify ETL pipeline completed successfully")
        print("   d) Consider using a different data source that has been processed")
        
    else:
        found_pattern = next(p for p in expected_patterns if p in available_tables)
        print(f"1. [SUCCESS] Found table: {found_pattern}")
        print("2. [INFO] The robust validation system should handle this")
        print("3. [DEBUG] Check for other issues like column validation")

def main():
    """Run the table investigation"""
    try:
        investigate_missing_table()
        print(f"\n=== INVESTIGATION COMPLETE ===")
        return True
    except Exception as e:
        print(f"\n[ERROR] Investigation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 