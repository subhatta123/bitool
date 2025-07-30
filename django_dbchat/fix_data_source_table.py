#!/usr/bin/env python3
"""
Fix data source table reference to use csv_data
"""
import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource

def fix_data_source_table():
    """Update data source to use csv_data table"""
    
    # Get the first data source
    ds = DataSource.objects.first()
    if not ds:
        print("No data source found")
        return
    
    print(f"Data source: {ds.name}")
    print(f"Before - Table: {ds.table_name}")
    
    # Update table name to csv_data
    ds.table_name = 'csv_data'
    
    # Update workflow status to enable queries
    workflow = ds.workflow_status or {}
    workflow['etl_completed'] = True
    workflow['query_enabled'] = True
    ds.workflow_status = workflow
    
    # Save changes
    ds.save()
    
    print(f"After - Table: {ds.table_name}")
    print(f"ETL completed: {workflow['etl_completed']}")
    print(f"Query enabled: {workflow['query_enabled']}")
    print("✅ Data source updated successfully!")

if __name__ == "__main__":
    fix_data_source_table() 