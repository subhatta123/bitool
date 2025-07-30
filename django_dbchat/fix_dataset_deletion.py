#!/usr/bin/env python3
"""
Fix Dataset Deletion Issues
Handles dataset deletion problems that occur in Docker containers
"""

import os
import sys
import django

# Set up Django environment
sys.path.append(os.path.dirname(__file__))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource, SemanticTable, SemanticColumn, SemanticMetric
from django.db import transaction
import shutil

def list_datasets():
    """List all datasets in the system"""
    
    print("📊 CURRENT DATASETS")
    print("=" * 50)
    
    data_sources = DataSource.objects.all()
    
    if not data_sources:
        print("No datasets found.")
        return []
    
    for i, ds in enumerate(data_sources, 1):
        print(f"{i}. {ds.name}")
        print(f"   ID: {ds.id}")
        print(f"   Type: {ds.source_type}")
        print(f"   Status: {ds.status}")
        print(f"   Created: {ds.created_at.strftime('%Y-%m-%d %H:%M')}")
        
        # Check related objects
        semantic_tables = SemanticTable.objects.filter(data_source=ds).count()
        semantic_columns = SemanticColumn.objects.filter(semantic_table__data_source=ds).count()
        semantic_metrics = SemanticMetric.objects.filter(base_table__data_source=ds).count()
        
        print(f"   Semantic Tables: {semantic_tables}")
        print(f"   Semantic Columns: {semantic_columns}")
        print(f"   Business Metrics: {semantic_metrics}")
        
        # Check file status
        if ds.source_type == 'csv':
            file_path = ds.connection_info.get('file_path', '')
            if file_path:
                from django.conf import settings
                full_path = os.path.join(settings.MEDIA_ROOT, file_path)
                file_exists = os.path.exists(full_path)
                print(f"   File exists: {file_exists}")
                if file_exists:
                    file_size = os.path.getsize(full_path)
                    print(f"   File size: {file_size:,} bytes")
        
        print()
    
    return list(data_sources)

def safe_delete_dataset(data_source_id, force=False):
    """Safely delete a dataset and all related data"""
    
    try:
        data_source = DataSource.objects.get(id=data_source_id)
    except DataSource.DoesNotExist:
        print(f"❌ Dataset with ID {data_source_id} not found")
        return False
    
    print(f"\n🗑️  DELETING DATASET: {data_source.name}")
    print("=" * 50)
    
    # Check what will be deleted
    semantic_tables = SemanticTable.objects.filter(data_source=data_source)
    semantic_columns = SemanticColumn.objects.filter(semantic_table__data_source=data_source)
    semantic_metrics = SemanticMetric.objects.filter(base_table__data_source=data_source)
    
    print(f"Will delete:")
    print(f"  - 1 Data Source: {data_source.name}")
    print(f"  - {semantic_tables.count()} Semantic Tables")
    print(f"  - {semantic_columns.count()} Semantic Columns")
    print(f"  - {semantic_metrics.count()} Business Metrics")
    
    # Check files
    files_to_delete = []
    if data_source.source_type == 'csv':
        file_path = data_source.connection_info.get('file_path', '')
        if file_path:
            from django.conf import settings
            full_path = os.path.join(settings.MEDIA_ROOT, file_path)
            if os.path.exists(full_path):
                files_to_delete.append(full_path)
                print(f"  - 1 CSV file: {file_path}")
    
    # Check DuckDB tables
    duckdb_tables = []
    try:
        from utils.table_name_helper import get_integrated_table_name
        table_name = get_integrated_table_name(data_source)
        
        from services.integration_service import DataIntegrationService
        integration_service = DataIntegrationService()
        
        if integration_service.check_table_exists(table_name):
            duckdb_tables.append(table_name)
            print(f"  - 1 DuckDB table: {table_name}")
    except Exception as e:
        print(f"  - DuckDB table check failed: {e}")
    
    if not force:
        print(f"\nThis will permanently delete all the above data.")
        response = input("Are you sure you want to continue? (yes/no): ").lower()
        if response != 'yes':
            print("Deletion cancelled.")
            return False
    
    # Perform deletion with transaction safety
    try:
        with transaction.atomic():
            print("\n🔄 Deleting database objects...")
            
            # Delete in correct order to avoid foreign key constraints
            deleted_metrics = semantic_metrics.count()
            semantic_metrics.delete()
            print(f"✅ Deleted {deleted_metrics} business metrics")
            
            deleted_columns = semantic_columns.count()
            semantic_columns.delete()
            print(f"✅ Deleted {deleted_columns} semantic columns")
            
            deleted_tables = semantic_tables.count()
            semantic_tables.delete()
            print(f"✅ Deleted {deleted_tables} semantic tables")
            
            data_source.delete()
            print(f"✅ Deleted data source: {data_source.name}")
        
        # Delete files (outside transaction)
        print("\n🔄 Deleting files...")
        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                print(f"✅ Deleted file: {file_path}")
            except Exception as e:
                print(f"⚠️  Could not delete file {file_path}: {e}")
        
        # Delete DuckDB tables
        print("\n🔄 Deleting DuckDB tables...")
        for table_name in duckdb_tables:
            try:
                from services.integration_service import DataIntegrationService
                integration_service = DataIntegrationService()
                integration_service.drop_table(table_name)
                print(f"✅ Deleted DuckDB table: {table_name}")
            except Exception as e:
                print(f"⚠️  Could not delete DuckDB table {table_name}: {e}")
        
        print(f"\n✅ Successfully deleted dataset: {data_source.name}")
        return True
        
    except Exception as e:
        print(f"\n❌ Error during deletion: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

def cleanup_orphaned_data():
    """Clean up orphaned semantic data without data sources"""
    
    print("\n🧹 CLEANING UP ORPHANED DATA")
    print("=" * 50)
    
    # Find semantic tables without data sources
    orphaned_tables = SemanticTable.objects.filter(data_source__isnull=True)
    orphaned_columns = SemanticColumn.objects.filter(semantic_table__data_source__isnull=True)
    orphaned_metrics = SemanticMetric.objects.filter(base_table__data_source__isnull=True)
    
    print(f"Found orphaned objects:")
    print(f"  - Semantic Tables: {orphaned_tables.count()}")
    print(f"  - Semantic Columns: {orphaned_columns.count()}")
    print(f"  - Business Metrics: {orphaned_metrics.count()}")
    
    if orphaned_tables.count() + orphaned_columns.count() + orphaned_metrics.count() == 0:
        print("✅ No orphaned data found")
        return
    
    response = input("Delete orphaned data? (y/N): ").lower()
    if response != 'y':
        print("Cleanup cancelled.")
        return
    
    try:
        with transaction.atomic():
            deleted_metrics = orphaned_metrics.count()
            orphaned_metrics.delete()
            
            deleted_columns = orphaned_columns.count()
            orphaned_columns.delete()
            
            deleted_tables = orphaned_tables.count()
            orphaned_tables.delete()
            
            print(f"✅ Cleaned up {deleted_tables} tables, {deleted_columns} columns, {deleted_metrics} metrics")
    
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")

def main():
    """Main function"""
    
    print("🗑️  CONVABI DATASET DELETION TOOL")
    print("=" * 60)
    print("This tool helps fix dataset deletion issues in Docker containers.")
    print()
    
    while True:
        print("\nChoose an option:")
        print("1. List all datasets")
        print("2. Delete a specific dataset")
        print("3. Cleanup orphaned semantic data")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            datasets = list_datasets()
            
        elif choice == '2':
            datasets = list_datasets()
            if not datasets:
                continue
                
            try:
                dataset_num = int(input(f"Enter dataset number to delete (1-{len(datasets)}): "))
                if 1 <= dataset_num <= len(datasets):
                    selected_dataset = datasets[dataset_num - 1]
                    safe_delete_dataset(selected_dataset.id)
                else:
                    print("Invalid dataset number")
            except ValueError:
                print("Please enter a valid number")
                
        elif choice == '3':
            cleanup_orphaned_data()
            
        elif choice == '4':
            print("Goodbye!")
            break
            
        else:
            print("Invalid choice. Please enter 1-4.")

if __name__ == "__main__":
    main() 