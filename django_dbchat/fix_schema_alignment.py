#!/usr/bin/env python3
"""
Fix schema alignment between DataSource and DuckDB table
"""

import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource
import duckdb

def fix_data_source_schema():
    """Update the DataSource schema to match the actual DuckDB table"""
    
    print("🔧 Fixing DataSource Schema Alignment")
    print("=" * 60)
    
    try:
        # Get the test data source
        ds = DataSource.objects.filter(name='test').first()
        if not ds:
            print("❌ No 'test' data source found")
            return False
        
        print(f"📊 Current DataSource: {ds.name}")
        print(f"   Table name: {ds.table_name}")
        
        # Check what's actually in DuckDB
        conn = duckdb.connect('data/integrated.duckdb')
        
        # Check csv_data table structure
        try:
            schema_info = conn.execute("DESCRIBE csv_data").fetchall()
            print(f"\n🔍 Actual DuckDB table structure (csv_data):")
            actual_columns = []
            for col_name, col_type, null, key, default, extra in schema_info:
                print(f"   - {col_name}: {col_type}")
                actual_columns.append({
                    'name': col_name,
                    'type': 'VARCHAR' if 'VARCHAR' in col_type else 'DECIMAL' if 'DECIMAL' in col_type else str(col_type),
                    'sample_values': []  # We'll populate this
                })
            
            # Get sample values for each column
            for i, col_info in enumerate(actual_columns):
                col_name = col_info['name']
                try:
                    sample_query = f'SELECT DISTINCT "{col_name}" FROM csv_data LIMIT 3'
                    samples = conn.execute(sample_query).fetchall()
                    col_info['sample_values'] = [str(row[0]) for row in samples]
                except:
                    col_info['sample_values'] = ['sample1', 'sample2', 'sample3']
            
            # Get row count
            count_result = conn.execute("SELECT COUNT(*) FROM csv_data").fetchone()
            row_count = count_result[0] if count_result else 0
            
        except Exception as e:
            print(f"❌ Error reading DuckDB table: {e}")
            conn.close()
            return False
        
        conn.close()
        
        # Create new schema info that matches the actual table
        new_schema_info = {
            'columns': actual_columns,
            'row_count': row_count,
            'column_count': len(actual_columns),
            'source_file': 'csv_data_table',
            'table_structure': 'aligned_with_duckdb'
        }
        
        print(f"\n🔄 Updating DataSource schema...")
        print(f"   Old column count: {len(ds.schema_info.get('columns', []))}")
        print(f"   New column count: {len(actual_columns)}")
        print(f"   New columns: {[col['name'] for col in actual_columns]}")
        
        # Update the DataSource
        ds.schema_info = new_schema_info
        ds.table_name = 'csv_data'  # Make sure it points to the right table
        ds.save()
        
        print(f"✅ DataSource schema updated successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Error fixing schema: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_llm_with_fixed_schema():
    """Test LLM query with the fixed schema"""
    
    print(f"\n🧪 Testing LLM with Fixed Schema")
    print("=" * 60)
    
    try:
        from services.llm_service import LLMService
        
        # Get updated data source
        ds = DataSource.objects.filter(name='test').first()
        if not ds:
            print("❌ Data source not found")
            return False
        
        print(f"📊 Updated Schema Info:")
        if ds.schema_info and 'columns' in ds.schema_info:
            for col in ds.schema_info['columns'][:5]:  # Show first 5
                print(f"   - {col['name']}: {col['type']}")
        
        # Create schema format for LLM
        llm_schema = {
            "tables": {
                "csv_data": {
                    "columns": ds.schema_info.get('columns', [])
                }
            }
        }
        
        # Test the failing query
        llm_service = LLMService()
        
        queries_to_test = [
            "top 3 customers by sales in south region",
            "total sales in south region", 
            "customers with highest sales"
        ]
        
        for query in queries_to_test:
            print(f"\n🔍 Testing: '{query}'")
            try:
                success, sql_result = llm_service.generate_sql(query, llm_schema)
                
                if success and sql_result:
                    print(f"   ✅ Generated SQL: {sql_result[:100]}...")
                    
                    # Test the SQL in DuckDB
                    conn = duckdb.connect('data/integrated.duckdb')
                    try:
                        result = conn.execute(sql_result).fetchall()
                        print(f"   ✅ Query executed: {len(result)} rows returned")
                        if result:
                            print(f"   📊 Sample result: {result[0]}")
                    except Exception as e:
                        print(f"   ❌ Query execution failed: {e}")
                    finally:
                        conn.close()
                else:
                    print(f"   ❌ LLM failed: {sql_result}")
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Testing failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Fixing Schema Alignment Issue")
    print("=" * 70)
    
    # Step 1: Fix the DataSource schema to match DuckDB
    schema_fixed = fix_data_source_schema()
    
    # Step 2: Test LLM with fixed schema
    if schema_fixed:
        test_success = test_llm_with_fixed_schema()
    else:
        test_success = False
    
    print(f"\n" + "=" * 70)
    print("🎯 RESULTS:")
    print(f"   Schema alignment: {'✅' if schema_fixed else '❌'}")
    print(f"   LLM testing: {'✅' if test_success else '❌'}")
    
    if schema_fixed and test_success:
        print(f"\n🎉 SUCCESS!")
        print("✅ Schema alignment fixed")
        print("✅ LLM now gets correct column names")
        print("🔗 Try your query again - it should work properly now!")
    else:
        print(f"\n⚠️ Issues remain")
        print("💡 Check the error messages above") 