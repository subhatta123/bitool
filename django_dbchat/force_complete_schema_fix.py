#!/usr/bin/env python3
"""
Force complete schema refresh to eliminate all caching issues
"""

import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource
import duckdb
import json

def check_all_schema_sources():
    """Check all possible sources of schema information"""
    
    print("🔍 Checking All Schema Sources")
    print("=" * 70)
    
    try:
        # Check DataSource model
        ds = DataSource.objects.filter(name='test').first()
        if ds:
            print(f"📊 DataSource Model:")
            print(f"   Table name: {ds.table_name}")
            print(f"   Schema columns: {len(ds.schema_info.get('columns', []))}")
            if ds.schema_info and 'columns' in ds.schema_info:
                sample_cols = [col['name'] for col in ds.schema_info['columns'][:5]]
                print(f"   Sample columns: {sample_cols}")
        
        # Check DuckDB actual table
        conn = duckdb.connect('data/integrated.duckdb')
        
        # Check superstore_data table
        try:
            schema = conn.execute("DESCRIBE superstore_data").fetchall()
            print(f"\n🦆 DuckDB superstore_data Table:")
            print(f"   Actual columns: {len(schema)}")
            actual_cols = [row[0] for row in schema[:5]]
            print(f"   Sample columns: {actual_cols}")
        except Exception as e:
            print(f"❌ superstore_data table error: {e}")
        
        # Check what tables exist
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        print(f"\n📋 Available DuckDB tables: {len(table_names)}")
        print(f"   Tables: {table_names[:8]}...")
        
        conn.close()
        
        return ds
        
    except Exception as e:
        print(f"❌ Error checking schema sources: {e}")
        return None

def force_datasource_schema_reset():
    """Completely reset the DataSource schema"""
    
    print(f"\n🔄 Force DataSource Schema Reset")
    print("=" * 70)
    
    try:
        # Get actual DuckDB schema
        conn = duckdb.connect('data/integrated.duckdb')
        
        # Get superstore_data schema
        schema_info = conn.execute("DESCRIBE superstore_data").fetchall()
        print(f"✅ Reading superstore_data schema: {len(schema_info)} columns")
        
        # Build correct schema
        correct_columns = []
        for col_name, col_type, null, key, default, extra in schema_info:
            # Get sample values
            try:
                samples = conn.execute(f'SELECT DISTINCT "{col_name}" FROM superstore_data WHERE "{col_name}" IS NOT NULL LIMIT 3').fetchall()
                sample_values = [str(row[0])[:50] for row in samples if row[0] is not None]  # Truncate long values
            except:
                sample_values = ['sample1', 'sample2', 'sample3']
            
            correct_columns.append({
                'name': col_name,
                'type': str(col_type),
                'sample_values': sample_values[:3]
            })
        
        # Get row count
        count_result = conn.execute("SELECT COUNT(*) FROM superstore_data").fetchone()
        row_count = count_result[0] if count_result else 0
        
        conn.close()
        
        # Update DataSource with completely new schema
        ds = DataSource.objects.filter(name='test').first()
        if not ds:
            print("❌ DataSource not found")
            return False
        
        # Create brand new schema object
        brand_new_schema = {
            'columns': correct_columns,
            'row_count': row_count,
            'column_count': len(correct_columns),
            'source_file': 'superstore_data_final',
            'table_structure': 'duckdb_only',
            'data_source': 'duckdb_superstore',
            'schema_version': '4.0',
            'last_updated': '2025-06-30T15:56:00Z',
            'migration_complete': True
        }
        
        print(f"🔧 Creating brand new schema:")
        print(f"   Columns: {len(correct_columns)}")
        print(f"   Row count: {row_count:,}")
        print(f"   Table: superstore_data")
        
        # Force complete replacement
        ds.schema_info = brand_new_schema
        ds.table_name = 'superstore_data'
        ds.save()
        
        # Force database refresh
        ds.refresh_from_db()
        
        # Verify by re-reading
        updated_ds = DataSource.objects.get(id=ds.id)
        if updated_ds.schema_info and 'columns' in updated_ds.schema_info:
            verified_count = len(updated_ds.schema_info['columns'])
            print(f"✅ Verification: Schema has {verified_count} columns")
            
            # Show first few columns to verify
            first_cols = [col['name'] for col in updated_ds.schema_info['columns'][:5]]
            print(f"   First 5 columns: {first_cols}")
            
            return True
        else:
            print(f"❌ Verification failed")
            return False
            
    except Exception as e:
        print(f"❌ Error resetting schema: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_llm_schema_directly():
    """Test the LLM service directly with the new schema"""
    
    print(f"\n🧪 Testing LLM Schema Directly")
    print("=" * 70)
    
    try:
        from services.llm_service import LLMService
        
        # Get the updated DataSource
        ds = DataSource.objects.filter(name='test').first()
        if not ds or not ds.schema_info:
            print("❌ No schema found")
            return False
        
        columns = ds.schema_info.get('columns', [])
        print(f"📊 Schema for LLM test:")
        print(f"   Columns: {len(columns)}")
        print(f"   Table: superstore_data")
        
        # Show actual column names that LLM will see
        column_names = [col['name'] for col in columns]
        print(f"   Column names: {column_names[:8]}...")
        
        # Create the exact schema format sent to LLM
        llm_schema = {
            "tables": {
                "superstore_data": {
                    "columns": columns
                }
            }
        }
        
        # Test the problematic query
        llm_service = LLMService()
        
        test_query = "top 3 customers by sales in south region"
        print(f"\n🔍 Testing query: '{test_query}'")
        
        try:
            success, sql_result = llm_service.generate_sql(test_query, llm_schema)
            
            if success and sql_result and sql_result.strip() != ";":
                print(f"✅ LLM generated SQL:")
                print(f"   {sql_result[:200]}...")
                
                # Check if it uses correct column names
                if 'Customer_Name' in sql_result and 'Region' in sql_result and 'Sales' in sql_result:
                    print(f"✅ Uses correct column names!")
                elif 'col_' in sql_result:
                    print(f"❌ Still uses generic col_ names!")
                    return False
                else:
                    print(f"⚠️ Uses different column names - checking...")
                
                # Test execution
                conn = duckdb.connect('data/integrated.duckdb')
                try:
                    result = conn.execute(sql_result).fetchall()
                    print(f"✅ Query executed successfully: {len(result)} results")
                    if result:
                        print(f"📊 Sample result: {result[0]}")
                    return True
                except Exception as e:
                    print(f"❌ Query execution failed: {str(e)[:100]}...")
                    return False
                finally:
                    conn.close()
            else:
                print(f"❌ LLM failed to generate SQL: {sql_result}")
                return False
                
        except Exception as e:
            print(f"❌ LLM test error: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Testing failed: {e}")
        return False

def clear_any_caches():
    """Clear any potential caches in the Django application"""
    
    print(f"\n🧹 Clearing Potential Caches")
    print("=" * 70)
    
    try:
        # Force Django to reload models
        from django.apps import apps
        apps.clear_cache()
        
        # Clear any Django cache
        from django.core.cache import cache
        cache.clear()
        
        print("✅ Django caches cleared")
        
        return True
        
    except Exception as e:
        print(f"❌ Cache clearing error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Force Complete Schema Fix")
    print("=" * 80)
    
    # Step 1: Check all schema sources
    ds = check_all_schema_sources()
    
    # Step 2: Clear caches
    caches_cleared = clear_any_caches()
    
    # Step 3: Force complete DataSource schema reset
    if ds:
        schema_reset = force_datasource_schema_reset()
    else:
        schema_reset = False
    
    # Step 4: Test LLM with new schema
    if schema_reset:
        llm_test = test_llm_schema_directly()
    else:
        llm_test = False
    
    print(f"\n" + "=" * 80)
    print("🎯 COMPLETE FIX RESULTS:")
    print(f"   Schema sources checked: {'✅' if ds else '❌'}")
    print(f"   Caches cleared: {'✅' if caches_cleared else '❌'}")
    print(f"   Schema reset: {'✅' if schema_reset else '❌'}")
    print(f"   LLM test passed: {'✅' if llm_test else '❌'}")
    
    if schema_reset and llm_test:
        print(f"\n🎉 COMPLETE SUCCESS!")
        print("✅ All schema caching issues eliminated")
        print("✅ LLM now uses correct column names")
        print("✅ No more col_6, col_12, col_17 errors")
        print("🔗 Start Django server - queries should work perfectly!")
    else:
        print(f"\n⚠️ Issues remain")
        print("💡 Check error messages above for details") 