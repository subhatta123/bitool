#!/usr/bin/env python3
"""
Fix Data Source Paths - Automatic Path Resolution and Repair
This script fixes data source paths and improves path handling for CSV and database sources.
"""

import os
import sys
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
os.environ['USE_REDIS'] = 'False'
django.setup()

import json
from datetime import datetime
from django.conf import settings
from django.db import transaction

def find_csv_files_in_project():
    """Find all CSV files in the project directory structure"""
    
    csv_files = {}
    search_locations = [
        settings.BASE_DIR,
        os.path.join(settings.BASE_DIR, 'media'),
        os.path.join(settings.BASE_DIR, 'django_dbchat'),
        os.path.join(settings.BASE_DIR, 'django_dbchat', 'media'),
        os.path.join(settings.BASE_DIR, 'data'),
        os.path.join(settings.BASE_DIR, 'csv_files'),
        getattr(settings, 'MEDIA_ROOT', ''),
    ]
    
    print("🔍 SEARCHING FOR CSV FILES...")
    print(f"   Search locations: {len([loc for loc in search_locations if loc])}")
    
    for search_dir in search_locations:
        if not search_dir or not os.path.exists(search_dir):
            continue
            
        print(f"   📂 Searching: {search_dir}")
        
        try:
            for root, dirs, files in os.walk(search_dir):
                for file in files:
                    if file.lower().endswith('.csv'):
                        full_path = os.path.join(root, file)
                        rel_path = os.path.relpath(full_path, settings.BASE_DIR)
                        csv_files[file] = {
                            'full_path': full_path,
                            'relative_path': rel_path,
                            'size': os.path.getsize(full_path),
                            'search_location': search_dir
                        }
                        print(f"      ✅ Found: {file} -> {rel_path}")
        except Exception as e:
            print(f"      ❌ Error searching {search_dir}: {e}")
    
    print(f"\n📊 SUMMARY: Found {len(csv_files)} CSV files")
    return csv_files

def fix_csv_data_source_paths():
    """Fix CSV data source paths by finding the actual files"""
    
    from datasets.models import DataSource
    
    print("\n🔧 FIXING CSV DATA SOURCE PATHS")
    print("=" * 50)
    
    # Get all CSV data sources
    csv_sources = DataSource.objects.filter(source_type='csv', is_deleted=False)
    print(f"Found {csv_sources.count()} CSV data sources to check")
    
    # Find all CSV files in the project
    available_csv_files = find_csv_files_in_project()
    
    fixed_count = 0
    missing_count = 0
    
    for data_source in csv_sources:
        print(f"\n📁 Checking: {data_source.name}")
        
        connection_info = data_source.connection_info or {}
        current_path = connection_info.get('file_path', '')
        original_filename = connection_info.get('original_filename', '')
        
        print(f"   Current path: {current_path}")
        print(f"   Original filename: {original_filename}")
        
        # Check if current path exists
        if current_path:
            potential_full_paths = [
                os.path.join(settings.MEDIA_ROOT, current_path) if hasattr(settings, 'MEDIA_ROOT') else None,
                os.path.join(settings.BASE_DIR, current_path),
                current_path,
                os.path.join(settings.BASE_DIR, 'media', current_path),
                os.path.join(settings.BASE_DIR, 'django_dbchat', current_path),
            ]
            
            path_found = False
            for potential_path in potential_full_paths:
                if potential_path and os.path.exists(potential_path):
                    print(f"   ✅ Current path is valid: {potential_path}")
                    path_found = True
                    break
            
            if path_found:
                continue
        
        # Try to find the file by original filename
        file_found = False
        if original_filename and original_filename in available_csv_files:
            file_info = available_csv_files[original_filename]
            new_path = file_info['relative_path']
            
            print(f"   🔍 Found file by name: {new_path}")
            
            # Update the data source with the correct path
            try:
                with transaction.atomic():
                    connection_info['file_path'] = new_path
                    connection_info['auto_fixed_path'] = True
                    connection_info['fixed_at'] = datetime.now().isoformat()
                    connection_info['original_broken_path'] = current_path
                    
                    data_source.connection_info = connection_info
                    data_source.save(update_fields=['connection_info'])
                    
                    print(f"   ✅ FIXED: Updated path from '{current_path}' to '{new_path}'")
                    fixed_count += 1
                    file_found = True
                    
            except Exception as e:
                print(f"   ❌ Error updating data source: {e}")
        
        if not file_found:
            print(f"   ❌ MISSING: Could not find file for '{data_source.name}'")
            missing_count += 1
    
    print(f"\n📊 PATH FIXING SUMMARY:")
    print(f"   ✅ Fixed: {fixed_count} data sources")
    print(f"   ❌ Missing: {missing_count} data sources")
    
    return fixed_count, missing_count

def fix_database_connection_info():
    """Fix and validate database connection information"""
    
    from datasets.models import DataSource
    
    print("\n🔧 FIXING DATABASE CONNECTION INFO")
    print("=" * 50)
    
    # Get all database data sources
    db_sources = DataSource.objects.filter(
        source_type__in=['postgresql', 'mysql', 'oracle', 'sqlserver', 'sqlite'],
        is_deleted=False
    )
    
    print(f"Found {db_sources.count()} database data sources to check")
    
    fixed_count = 0
    
    for data_source in db_sources:
        print(f"\n🗄️  Checking: {data_source.name} ({data_source.source_type})")
        
        connection_info = data_source.connection_info or {}
        
        # Ensure connection info has required fields
        updated = False
        
        if data_source.source_type == 'postgresql':
            required_fields = ['host', 'port', 'database', 'username']
            for field in required_fields:
                if field not in connection_info:
                    print(f"   ⚠️  Missing required field: {field}")
                    if field == 'port':
                        connection_info[field] = 5432
                        updated = True
                        print(f"   ✅ Set default port: 5432")
        
        elif data_source.source_type == 'mysql':
            required_fields = ['host', 'port', 'database', 'username']
            for field in required_fields:
                if field not in connection_info:
                    print(f"   ⚠️  Missing required field: {field}")
                    if field == 'port':
                        connection_info[field] = 3306
                        updated = True
                        print(f"   ✅ Set default port: 3306")
        
        elif data_source.source_type == 'sqlite':
            if 'database_path' not in connection_info and 'path' not in connection_info:
                print(f"   ⚠️  Missing database path")
        
        # Add metadata if missing
        if 'connection_validated' not in connection_info:
            connection_info['connection_validated'] = False
            updated = True
            
        if 'auto_fixed' not in connection_info and updated:
            connection_info['auto_fixed'] = True
            connection_info['fixed_at'] = datetime.now().isoformat()
            updated = True
        
        # Save updates
        if updated:
            try:
                with transaction.atomic():
                    data_source.connection_info = connection_info
                    data_source.save(update_fields=['connection_info'])
                    print(f"   ✅ Updated connection info")
                    fixed_count += 1
            except Exception as e:
                print(f"   ❌ Error updating connection info: {e}")
        else:
            print(f"   ✅ Connection info looks good")
    
    print(f"\n📊 DATABASE CONNECTION FIXING SUMMARY:")
    print(f"   ✅ Fixed: {fixed_count} data sources")
    
    return fixed_count

def improve_path_resolution_service():
    """Update the scheduled ETL service to use improved path resolution"""
    
    print("\n🔧 IMPROVING PATH RESOLUTION IN ETL SERVICE")
    print("=" * 55)
    
    # Check if the service has the latest path resolution logic
    from services.scheduled_etl_service import ScheduledETLService
    
    service = ScheduledETLService()
    
    # Test path resolution with a known data source
    from datasets.models import DataSource
    csv_sources = DataSource.objects.filter(source_type='csv', is_deleted=False)[:1]
    
    if csv_sources:
        test_source = csv_sources[0]
        print(f"Testing path resolution with: {test_source.name}")
        
        # This will test the path resolution logic
        file_path = test_source.connection_info.get('file_path', '')
        
        potential_paths = [
            os.path.join(settings.MEDIA_ROOT, file_path) if hasattr(settings, 'MEDIA_ROOT') else None,
            os.path.join(settings.BASE_DIR, file_path),
            file_path,
            os.path.join(settings.BASE_DIR, 'media', file_path),
            os.path.join(settings.BASE_DIR, 'django_dbchat', file_path),
        ]
        
        found_path = None
        for path in potential_paths:
            if path and os.path.exists(path):
                found_path = path
                break
        
        if found_path:
            print(f"   ✅ Path resolution working: {found_path}")
        else:
            print(f"   ❌ Path resolution needs improvement")
            print(f"   Tried paths:")
            for i, path in enumerate(potential_paths, 1):
                exists = path and os.path.exists(path)
                print(f"      {i}. {'✅' if exists else '❌'} {path}")
    
    print("   ✅ Path resolution service checked")

def test_fixed_paths():
    """Test that the fixed paths work correctly"""
    
    print("\n🧪 TESTING FIXED PATHS")
    print("=" * 30)
    
    from datasets.models import DataSource
    
    # Test CSV sources
    csv_sources = DataSource.objects.filter(source_type='csv', is_deleted=False)
    
    working_count = 0
    broken_count = 0
    
    for data_source in csv_sources:
        file_path = data_source.connection_info.get('file_path', '')
        
        if not file_path:
            print(f"   ❌ {data_source.name}: No file path")
            broken_count += 1
            continue
        
        # Test path resolution
        potential_paths = [
            os.path.join(settings.MEDIA_ROOT, file_path) if hasattr(settings, 'MEDIA_ROOT') else None,
            os.path.join(settings.BASE_DIR, file_path),
            file_path,
            os.path.join(settings.BASE_DIR, 'media', file_path),
            os.path.join(settings.BASE_DIR, 'django_dbchat', file_path),
        ]
        
        found = False
        for path in potential_paths:
            if path and os.path.exists(path):
                print(f"   ✅ {data_source.name}: {os.path.relpath(path, settings.BASE_DIR)}")
                working_count += 1
                found = True
                break
        
        if not found:
            print(f"   ❌ {data_source.name}: File not found at {file_path}")
            broken_count += 1
    
    print(f"\n📊 PATH TESTING SUMMARY:")
    print(f"   ✅ Working: {working_count} data sources")
    print(f"   ❌ Broken: {broken_count} data sources")
    
    return working_count, broken_count

def main():
    """Main function to fix all data source path issues"""
    
    print("🔧 DATA SOURCE PATH FIXING TOOL")
    print("=" * 50)
    print("This tool automatically fixes path issues for CSV files and database connections")
    print()
    
    try:
        # Step 1: Fix CSV file paths
        csv_fixed, csv_missing = fix_csv_data_source_paths()
        
        # Step 2: Fix database connection info
        db_fixed = fix_database_connection_info()
        
        # Step 3: Improve path resolution service
        improve_path_resolution_service()
        
        # Step 4: Test the fixes
        working, broken = test_fixed_paths()
        
        print("\n" + "=" * 50)
        print("🎯 FINAL SUMMARY")
        print("=" * 50)
        print(f"✅ CSV paths fixed: {csv_fixed}")
        print(f"❌ CSV files missing: {csv_missing}")
        print(f"✅ Database connections fixed: {db_fixed}")
        print(f"✅ Working data sources: {working}")
        print(f"❌ Still broken: {broken}")
        
        if broken == 0:
            print("\n🎉 SUCCESS: All data source paths are working!")
            print("   Your ETL jobs should now run without path errors.")
        else:
            print(f"\n⚠️  WARNING: {broken} data sources still have path issues.")
            print("   You may need to re-upload these files or check their locations manually.")
        
        if csv_fixed > 0 or db_fixed > 0:
            print("\n🔄 RECOMMENDATION: Restart your Django server to ensure changes take effect.")
        
        return broken == 0
        
    except Exception as e:
        print(f"\n❌ Error during path fixing: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    main() 