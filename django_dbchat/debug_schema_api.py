#!/usr/bin/env python3
"""
Debug Schema API Issue
Investigates why schema information is not showing for ETL result sources
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

import json
from datasets.models import DataSource
from django.contrib.auth.models import User
from django.test import RequestFactory
from datasets.views import get_data_source_schema_api

def debug_schema_api():
    """Debug the schema API for ETL result sources"""
    print("=== DEBUGGING SCHEMA API ISSUE ===")
    
    # Find ETL result data source
    etl_result_sources = DataSource.objects.filter(source_type='etl_result')
    
    if not etl_result_sources.exists():
        print("❌ No ETL result data sources found")
        return
    
    etl_source = etl_result_sources.first()
    if not etl_source:
        print("❌ No ETL result data source found")
        return
        
    print(f"✅ Found ETL result source: {etl_source.name}")
    print(f"   - ID: {etl_source.id}")
    print(f"   - Source Type: {etl_source.source_type}")
    print(f"   - Connection Info: {etl_source.connection_info}")
    
    # Check current schema_info
    print(f"\n📋 Current schema_info: {etl_source.schema_info}")
    
    # Check what type of schema_info it is
    if etl_source.schema_info:
        print(f"   - Schema info type: {type(etl_source.schema_info)}")
        if isinstance(etl_source.schema_info, dict):
            print(f"   - Schema info keys: {list(etl_source.schema_info.keys())}")
    else:
        print("   - Schema info is empty/None")
    
    # Test the schema API directly
    print(f"\n🔍 Testing schema API directly...")
    try:
        # Create a mock request
        factory = RequestFactory()
        request = factory.get(f'/api/data-sources/{etl_source.id}/schema/')
        
        # Get the user who created this data source
        user = etl_source.created_by or User.objects.first()
        request.user = user
        
        # Call the schema API
        response = get_data_source_schema_api(request, etl_source.id)
        
        print(f"✅ Schema API called successfully")
        print(f"   - Status Code: {response.status_code}")
        
        if response.status_code == 200:
            response_data = json.loads(response.content.decode('utf-8'))
            print(f"   - Response Success: {response_data.get('success', 'N/A')}")
            print(f"   - Row Count: {response_data.get('row_count', 'N/A')}")
            print(f"   - Column Count: {response_data.get('column_count', 'N/A')}")
            print(f"   - Schema Keys: {list(response_data.get('schema', {}).keys())}")
            
            # Show first few columns
            schema = response_data.get('schema', {})
            for i, (col_name, col_info) in enumerate(schema.items()):
                if i < 3:  # Show first 3 columns
                    print(f"   - {col_name}: {col_info}")
                    
        else:
            print(f"❌ Schema API failed with error: {response.content.decode('utf-8')}")
            
    except Exception as e:
        print(f"❌ Error calling schema API: {e}")
    
    # Test schema generation functions directly
    print(f"\n🔧 Testing schema generation functions...")
    try:
        from datasets.views import _generate_schema_from_etl_result
        
        schema = _generate_schema_from_etl_result(etl_source)
        print(f"✅ Generated schema: {len(schema)} columns")
        
        for col_name, col_info in list(schema.items())[:3]:
            print(f"   - {col_name}: {col_info}")
            
        # Check if schema was saved
        etl_source.refresh_from_db()
        print(f"   - Schema saved to DB: {etl_source.schema_info is not None}")
        
    except Exception as e:
        print(f"❌ Error generating schema: {e}")

if __name__ == '__main__':
    debug_schema_api() 