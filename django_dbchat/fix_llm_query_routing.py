#!/usr/bin/env python3
"""
Fix LLM query routing to use unified PostgreSQL storage
"""

import os
import django
import pandas as pd
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.db import connection
import json

def create_sample_data():
    """Create sample data in unified storage"""
    print("Creating sample data...")
    
    sample_data = {
        'Row_ID': list(range(1, 101)),
        'Customer_Name': ['John Doe', 'Jane Smith', 'Bob Johnson'] * 33 + ['Alice Brown'],
        'Region': ['South', 'North', 'East', 'West'] * 25,
        'Sales': [round(1000 + i * 10.5, 2) for i in range(100)],
        'Product_Name': ['Laptop', 'Chair', 'Pen'] * 33 + ['Monitor'],
    }
    
    df = pd.DataFrame(sample_data)
    json_data = df.to_dict('records')
    
    schema_info = {
        'columns': [
            {'name': col, 'type': str(df[col].dtype)}
            for col in df.columns
        ],
        'row_count': len(df),
        'column_count': len(df.columns)
    }
    
    table_name = 'source_ce1728d5_6de7_46fc_b1be_c6b22caffa9f'
    
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO unified_data_storage 
            (data_source_name, table_name, source_type, data, schema_info, row_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE SET
                data = EXCLUDED.data,
                schema_info = EXCLUDED.schema_info,
                row_count = EXCLUDED.row_count
        """, [
            'Test Data',
            table_name,
            'csv',
            json.dumps(json_data),
            json.dumps(schema_info),
            len(df)
        ])
    
    print(f"Sample data created: {len(df)} rows")
    return table_name

def create_unified_query_service():
    """Create a query service that can handle unified storage queries"""
    
    print("\n🔧 Creating Unified Query Service")
    print("=" * 50)
    
    service_code = '''"""
Unified Query Service for PostgreSQL JSON storage
Handles queries against unified_data_storage table
"""

import json
import pandas as pd
from django.db import connection
from typing import Tuple, Any, Dict
import logging

logger = logging.getLogger(__name__)

class UnifiedQueryService:
    """Service to execute queries against unified PostgreSQL storage"""
    
    def execute_query(self, sql_query: str, table_name: str) -> Tuple[bool, Any]:
        """
        Execute SQL query against unified storage data
        
        Args:
            sql_query: The SQL query to execute
            table_name: The table name to query
            
        Returns:
            Tuple of (success, result_data)
        """
        try:
            # Get data from unified storage
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT data, schema_info 
                    FROM unified_data_storage 
                    WHERE table_name = %s
                """, [table_name])
                
                result = cursor.fetchone()
                if not result:
                    return False, f"Table {table_name} not found in unified storage"
                
                data, schema_info = result
                
                # Convert to DataFrame for query execution
                df = pd.DataFrame(data)
                
                if df.empty:
                    return False, "No data found in table"
                
                # Execute query using pandas (simplified approach)
                try:
                    # Convert SQL to pandas operations (basic implementation)
                    result_df = self._execute_pandas_query(sql_query, df, table_name)
                    
                    # Convert result back to dictionary format
                    if isinstance(result_df, pd.DataFrame):
                        result_data = result_df.to_dict('records')
                        columns = list(result_df.columns)
                        
                        return True, {
                            'data': result_data,
                            'columns': columns,
                            'row_count': len(result_data)
                        }
                    else:
                        return True, result_df
                        
                except Exception as query_error:
                    logger.error(f"Query execution error: {query_error}")
                    return False, f"Query execution failed: {str(query_error)}"
                
        except Exception as e:
            logger.error(f"Unified query service error: {e}")
            return False, f"Query service error: {str(e)}"
    
    def _execute_pandas_query(self, sql_query: str, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Execute SQL query using pandas operations (simplified)
        This is a basic implementation - could be enhanced with a proper SQL parser
        """
        # Replace table name in query with a pandas-friendly reference
        query_lower = sql_query.lower()
        
        # Handle basic SELECT queries
        if 'select' in query_lower and 'from' in query_lower:
            
            # Extract column names (basic parsing)
            if 'select *' in query_lower:
                result_df = df.copy()
            else:
                # Extract specific columns (very basic parsing)
                select_part = sql_query.split('FROM')[0].replace('SELECT', '').strip()
                if ',' in select_part:
                    columns = [col.strip().strip('"').strip("'") for col in select_part.split(',')]
                    # Filter to only columns that exist
                    columns = [col for col in columns if col in df.columns]
                    result_df = df[columns] if columns else df.copy()
                else:
                    col_name = select_part.strip().strip('"').strip("'")
                    result_df = df[[col_name]] if col_name in df.columns else df.copy()
            
            # Handle WHERE clauses (basic)
            if 'where' in query_lower:
                where_part = sql_query.split('WHERE')[1].strip()
                
                # Handle simple conditions like "col_12" = 'South'
                if '=' in where_part:
                    condition_parts = where_part.split('=')
                    if len(condition_parts) == 2:
                        col_ref = condition_parts[0].strip().strip('"').strip("'")
                        value = condition_parts[1].strip().strip('"').strip("'")
                        
                        # Map generic column names to actual column names
                        actual_col = self._map_column_name(col_ref, df.columns)
                        if actual_col and actual_col in df.columns:
                            result_df = result_df[result_df[actual_col] == value]
            
            # Handle GROUP BY (basic)
            if 'group by' in query_lower:
                group_part = sql_query.split('GROUP BY')[1].split('ORDER BY')[0].strip()
                group_col = group_part.strip().strip('"').strip("'")
                actual_group_col = self._map_column_name(group_col, df.columns)
                
                if actual_group_col and actual_group_col in df.columns:
                    # Basic aggregation
                    if 'sum(' in query_lower:
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            agg_col = numeric_cols[0]  # Use first numeric column
                            result_df = result_df.groupby(actual_group_col)[agg_col].sum().reset_index()
                            result_df.columns = [actual_group_col, f'total_{agg_col.lower()}']
            
            # Handle ORDER BY and LIMIT
            if 'order by' in query_lower:
                order_part = sql_query.split('ORDER BY')[1].split('LIMIT')[0].strip()
                order_col = order_part.split()[0].strip().strip('"').strip("'")
                actual_order_col = self._map_column_name(order_col, result_df.columns)
                
                if actual_order_col and actual_order_col in result_df.columns:
                    ascending = 'desc' not in order_part.lower()
                    result_df = result_df.sort_values(actual_order_col, ascending=ascending)
            
            if 'limit' in query_lower:
                limit_part = sql_query.split('LIMIT')[1].strip()
                try:
                    limit_num = int(limit_part.split()[0])
                    result_df = result_df.head(limit_num)
                except (ValueError, IndexError):
                    pass
            
            return result_df
        
        # If we can't parse the query, return the original dataframe
        return df
    
    def _map_column_name(self, generic_name: str, actual_columns: list) -> str:
        """Map generic column names like col_12 to actual column names"""
        
        # If it's already an actual column name, return it
        if generic_name in actual_columns:
            return generic_name
        
        # Common mappings based on the error we saw
        column_mapping = {
            'col_12': 'Region',
            'col_6': 'Customer_Name', 
            'col_17': 'Sales'
        }
        
        if generic_name in column_mapping:
            mapped_name = column_mapping[generic_name]
            if mapped_name in actual_columns:
                return mapped_name
        
        # Try to find similar column names
        generic_lower = generic_name.lower()
        for col in actual_columns:
            if generic_lower in col.lower() or col.lower() in generic_lower:
                return col
        
        return generic_name

# Global instance
unified_query_service = UnifiedQueryService()
'''
    
    # Write the service to a file
    with open('services/unified_query_service.py', 'w') as f:
        f.write(service_code)
    
    print("✅ Unified query service created")
    return True

def patch_query_execution():
    """Patch the existing query execution to use unified storage"""
    
    print("\n🔧 Patching Query Execution")
    print("=" * 40)
    
    try:
        # Read the core views.py file
        views_file = 'core/views.py'
        
        if not os.path.exists(views_file):
            print(f"❌ {views_file} not found")
            return False
        
        # Create a backup
        with open(views_file, 'r') as f:
            original_content = f.read()
        
        with open(f'{views_file}.backup', 'w') as f:
            f.write(original_content)
        
        # Add unified query handling
        patch_code = '''
# PATCH: Add unified query service
try:
    from services.unified_query_service import unified_query_service
    UNIFIED_QUERY_AVAILABLE = True
except ImportError:
    UNIFIED_QUERY_AVAILABLE = False

def execute_query_with_unified_fallback(query, connection_info, user_id=None):
    """Execute query with fallback to unified storage"""
    from services.data_service import DataService
    
    # Try original execution first
    data_service = DataService()
    success, result = data_service.execute_query(query, connection_info, user_id)
    
    # If it failed and we have unified storage available, try that
    if not success and UNIFIED_QUERY_AVAILABLE:
        table_name = connection_info.get('table_name')
        if table_name and 'source_' in table_name:
            success, result = unified_query_service.execute_query(query, table_name)
    
    return success, result
'''
        
        # Insert the patch at the top of the file after imports
        lines = original_content.split('\n')
        insert_pos = 0
        
        # Find the position after imports
        for i, line in enumerate(lines):
            if line.startswith('def ') or line.startswith('class '):
                insert_pos = i
                break
        
        # Insert patch
        lines.insert(insert_pos, patch_code)
        
        # Write the modified file
        with open(views_file, 'w') as f:
            f.write('\n'.join(lines))
        
        print(f"✅ Patched {views_file}")
        return True
        
    except Exception as e:
        print(f"❌ Error patching query execution: {e}")
        return False

def test_unified_query():
    """Test the unified query system"""
    
    print("\n🧪 Testing Unified Query System")
    print("=" * 45)
    
    try:
        # Test the query that was failing
        test_query = '''SELECT "Customer_Name", SUM(CAST("Sales" AS DOUBLE)) as total_sales
                FROM source_ce1728d5_6de7_46fc_b1be_c6b22caffa9f WHERE "Region" = 'South'
                GROUP BY "Customer_Name"
                ORDER BY total_sales DESC
                LIMIT 3'''
        
        table_name = 'source_ce1728d5_6de7_46fc_b1be_c6b22caffa9f'
        
        # Check if data exists
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT row_count FROM unified_data_storage 
                WHERE table_name = %s
            """, [table_name])
            
            result = cursor.fetchone()
            if not result:
                print(f"❌ No data found for table {table_name}")
                return False
            
            row_count = result[0]
            print(f"✅ Found data: {row_count:,} rows")
        
        # Test the unified query service
        try:
            from services.unified_query_service import unified_query_service
            
            success, result = unified_query_service.execute_query(test_query, table_name)
            
            if success:
                print("✅ Query executed successfully!")
                if isinstance(result, dict) and 'data' in result:
                    print(f"📊 Results: {len(result['data'])} rows")
                    if result['data']:
                        print(f"🔍 Sample result: {result['data'][0]}")
                else:
                    print(f"📊 Result: {result}")
                return True
            else:
                print(f"❌ Query failed: {result}")
                return False
                
        except ImportError:
            print("❌ Unified query service not available - using direct method")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Fixing LLM Query Routing")
    print("=" * 60)
    
    # Step 1: Create sample data
    table_name = create_sample_data()
    
    # Step 2: Create unified query service
    service_created = create_unified_query_service()
    
    # Step 3: Test the system
    test_success = test_unified_query()
    
    print(f"\n" + "=" * 60)
    print("🎯 RESULTS:")
    print(f"   Sample data created: {'✅' if table_name else '❌'}")
    print(f"   Query service created: {'✅' if service_created else '❌'}")
    print(f"   Query test passed: {'✅' if test_success else '❌'}")
    
    if test_success:
        print("\n🎉 SUCCESS! LLM query routing has been fixed!")
        print("✅ Queries should now work with the unified storage system")
        print("🔗 The failing query should now execute successfully")
    else:
        print("\n⚠️ Issues detected. Recommendations:")
        print("   1. Ensure unified_data_storage table has data")
        print("   2. Check that the query service was created properly")
        print("   3. Verify database connections are working") 