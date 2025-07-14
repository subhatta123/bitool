#!/usr/bin/env python3
"""
Fix the table discovery issue where LLM receives wrong column names
"""

import os
import sys
import django

# Set up Django environment
sys.path.append('/c%3A/Users/SuddhasheelBhattacha/OneDrive%20-%20Mendix%20Technology%20B.V/Desktop/dbchat/django_dbchat')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from services.dynamic_llm_service import DynamicLLMService
from datasets.models import DataSource
import duckdb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_correct_table_for_data_source(data_source_id: str) -> str:
    """
    Find the correct table name for a data source based on query execution logs
    """
    
    # Priority order for table names (based on logs)
    table_patterns = [
        f"source_{data_source_id.replace('-', '')}",  # ETL transformed table
        f"ds_{data_source_id.replace('-', '')}",      # Original data table  
        f"source_id_{data_source_id.replace('-', '_')}",  # Alternative format
        f"ds_{data_source_id.replace('-', '_')}"      # Alternative format
    ]
    
    try:
        db_path = os.path.join(os.path.dirname(__file__), 'data', 'integrated.duckdb')
        conn = duckdb.connect(db_path)
        
        # Get all available tables
        tables = conn.execute("SHOW TABLES").fetchall()
        available_tables = [t[0] for t in tables]
        
        print(f"🔍 Looking for table for data source: {data_source_id}")
        print(f"📋 Available tables: {available_tables}")
        print(f"🎯 Checking patterns: {table_patterns}")
        
        # Find the best match
        for pattern in table_patterns:
            for table in available_tables:
                if pattern in table:
                    # Verify the table has data and columns
                    try:
                        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        schema = conn.execute(f"DESCRIBE {table}").fetchall()
                        
                        if count > 0 and len(schema) > 0:
                            print(f"✅ Found correct table: {table} ({count} rows, {len(schema)} columns)")
                            
                            # Show actual column names
                            print(f"📋 Actual columns:")
                            for col_name, col_type, *_ in schema:
                                print(f"   '{col_name}': {col_type}")
                            
                            conn.close()
                            return table
                    except Exception as e:
                        print(f"❌ Table {table} check failed: {e}")
                        continue
        
        conn.close()
        print(f"❌ No suitable table found for data source {data_source_id}")
        return None
        
    except Exception as e:
        print(f"❌ Error finding table: {e}")
        return None

def fix_dynamic_llm_table_discovery():
    """
    Apply fix to ensure LLM service uses the same table as query execution
    """
    
    print("🔧 Applying Dynamic LLM Table Discovery Fix")
    print("=" * 60)
    
    try:
        # Get current data sources
        data_sources = DataSource.objects.all()
        
        for ds in data_sources:
            print(f"\n📊 Fixing data source: {ds.name} (ID: {ds.id})")
            
            # Find the correct table
            correct_table = find_correct_table_for_data_source(str(ds.id))
            
            if correct_table:
                # Update the data source with correct table name
                ds.table_name = correct_table
                ds.save()
                print(f"✅ Updated data source table_name to: {correct_table}")
            else:
                print(f"❌ Could not find correct table for {ds.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error applying fix: {e}")
        return False

def test_fixed_llm_service():
    """
    Test the LLM service after applying the fix
    """
    
    print(f"\n🧪 Testing Fixed LLM Service")
    print("=" * 60)
    
    try:
        llm_service = DynamicLLMService()
        
        # Test environment discovery
        environment = llm_service.discover_data_environment()
        
        print(f"📊 Best table: {environment.get('best_table')}")
        
        if environment.get('best_table'):
            analysis = environment['table_analyses'][environment['best_table']]
            print(f"📋 Columns seen by LLM:")
            for col in analysis['columns'][:10]:
                print(f"   '{col}': {analysis['column_types'].get(col)}")
        
        # Test SQL generation
        success, sql = llm_service.generate_sql("top 3 customer names by sales in south region")
        
        print(f"\n🔧 SQL Generation:")
        print(f"Success: {success}")
        print(f"SQL: {sql}")
        
        # Extract table name from SQL
        import re
        table_match = re.search(r'FROM\s+["`]?([^"`\s]+)["`]?', sql, re.IGNORECASE)
        if table_match:
            sql_table = table_match.group(1)
            llm_table = environment.get('best_table')
            
            print(f"\n📊 Table Consistency Check:")
            print(f"   LLM analyzed table: {llm_table}")
            print(f"   SQL uses table: {sql_table}")
            
            if sql_table == llm_table:
                print(f"✅ FIXED: Tables match!")
                return True
            else:
                print(f"❌ STILL BROKEN: Table mismatch")
                return False
        else:
            print(f"❌ Could not extract table from SQL")
            return False
            
    except Exception as e:
        print(f"❌ Error testing fixed service: {e}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    print("🚀 Dynamic LLM Table Discovery Fix")
    print("=" * 70)
    
    # Step 1: Apply the fix
    fix_success = fix_dynamic_llm_table_discovery()
    
    if fix_success:
        # Step 2: Test the fix
        test_success = test_fixed_llm_service()
        
        print(f"\n" + "=" * 70)
        print("🎯 FIX RESULTS:")
        print(f"   Fix applied: {'✅' if fix_success else '❌'}")
        print(f"   LLM service working: {'✅' if test_success else '❌'}")
        
        if test_success:
            print(f"\n🎉 SUCCESS! LLM table discovery is now fixed!")
            print("✅ LLM receives correct column names")
            print("✅ SQL generation uses same table as schema analysis")
        else:
            print(f"\n⚠️  Fix applied but testing shows remaining issues")
    else:
        print(f"\n❌ Fix application failed")
    
    print("=" * 70) 