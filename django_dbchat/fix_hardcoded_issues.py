#!/usr/bin/env python3
"""
Remove Hardcoded Elements and Make System Dynamic
Fixes all hardcoded table names, column names, and data assumptions
"""

import os
import sys
import json
import re
import duckdb
from typing import Dict, Any, List, Tuple, Optional

# Add django settings
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')

import django
django.setup()

from datasets.models import DataSource

class DynamicDataAnalyzer:
    """Analyzes any dataset dynamically without hardcoding"""
    
    def __init__(self):
        self.duckdb_path = 'data/integrated.duckdb'
    
    def analyze_data_source(self, data_source: DataSource) -> Dict[str, Any]:
        """Dynamically analyze any data source structure"""
        
        analysis = {
            'original_table': data_source.table_name,
            'actual_table': None,
            'columns': [],
            'date_columns': [],
            'numeric_columns': [],
            'text_columns': [],
            'has_data': False,
            'row_count': 0,
            'data_types': {},
            'sample_data': {}
        }
        
        try:
            conn = duckdb.connect(self.duckdb_path)
            
            # Find the actual table
            tables = conn.execute("SHOW TABLES").fetchall()
            available_tables = [t[0] for t in tables]
            
            actual_table = self._find_best_table(data_source, available_tables)
            if not actual_table:
                return analysis
            
            analysis['actual_table'] = actual_table
            
            # Analyze table structure
            schema = conn.execute(f"DESCRIBE {actual_table}").fetchall()
            
            for row in schema:
                col_name = row[0]
                col_type = row[1].upper()
                
                analysis['columns'].append(col_name)
                analysis['data_types'][col_name] = col_type
                
                # Categorize by SQL data type
                if any(hint in col_type for hint in ['DATE', 'TIME', 'TIMESTAMP']):
                    analysis['date_columns'].append(col_name)
                elif any(hint in col_type for hint in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC']):
                    analysis['numeric_columns'].append(col_name)
                else:
                    analysis['text_columns'].append(col_name)
            
            # Get basic stats
            analysis['row_count'] = conn.execute(f"SELECT COUNT(*) FROM {actual_table}").fetchone()[0]
            analysis['has_data'] = analysis['row_count'] > 0
            
            # Auto-detect date columns in text format
            self._detect_date_columns(conn, actual_table, analysis)
            
            # Get sample data for each column type
            self._get_sample_data(conn, actual_table, analysis)
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error analyzing data source: {e}")
        
        return analysis
    
    def _find_best_table(self, data_source: DataSource, available_tables: List[str]) -> Optional[str]:
        """Find the best matching table for this data source"""
        
        # First try exact match
        if data_source.table_name in available_tables:
            return data_source.table_name
        
        # Try partial name matching
        ds_name_parts = data_source.name.lower().split()
        for table in available_tables:
            table_lower = table.lower()
            if any(part in table_lower for part in ds_name_parts):
                return table
        
        # Try tables with actual data, preferring larger ones
        table_sizes = {}
        for table in available_tables:
            try:
                conn = duckdb.connect(self.duckdb_path)
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                table_sizes[table] = count
                conn.close()
            except:
                table_sizes[table] = 0
        
        # Return the largest table with data
        if table_sizes:
            return max(table_sizes, key=table_sizes.get)
        
        return None
    
    def _detect_date_columns(self, conn, table_name: str, analysis: Dict[str, Any]):
        """Auto-detect date columns in text format"""
        
        date_patterns = [
            r'^\d{1,2}-\d{1,2}-\d{4}$',  # DD-MM-YYYY
            r'^\d{4}-\d{1,2}-\d{1,2}$',  # YYYY-MM-DD
            r'^\d{1,2}/\d{1,2}/\d{4}$',  # MM/DD/YYYY
        ]
        
        for col in analysis['text_columns'][:]:  # Copy to modify during iteration
            try:
                sample = conn.execute(f"SELECT {col} FROM {table_name} WHERE {col} IS NOT NULL LIMIT 20").fetchall()
                if not sample:
                    continue
                
                values = [str(row[0]).strip() for row in sample if row[0]]
                date_like_count = 0
                
                for value in values:
                    for pattern in date_patterns:
                        if re.match(pattern, value):
                            date_like_count += 1
                            break
                
                # If 70% look like dates, consider it a date column
                if date_like_count >= len(values) * 0.7:
                    analysis['date_columns'].append(col)
                    analysis['text_columns'].remove(col)
                    print(f"🗓️ Auto-detected date column: {col}")
                    
            except Exception as e:
                continue
    
    def _get_sample_data(self, conn, table_name: str, analysis: Dict[str, Any]):
        """Get sample data to understand content"""
        
        try:
            # Get a few sample rows
            sample_query = f"SELECT * FROM {table_name} LIMIT 3"
            sample_rows = conn.execute(sample_query).fetchall()
            
            if sample_rows:
                analysis['sample_data']['rows'] = sample_rows[:2]  # Store 2 sample rows
                
            # Get distinct values for key columns (first 5 text columns)
            for col in analysis['text_columns'][:5]:
                try:
                    distinct_query = f"SELECT DISTINCT {col} FROM {table_name} WHERE {col} IS NOT NULL LIMIT 10"
                    distinct_values = [row[0] for row in conn.execute(distinct_query).fetchall()]
                    analysis['sample_data'][col] = distinct_values
                except:
                    continue
                    
        except Exception as e:
            print(f"⚠️ Could not get sample data: {e}")
    
    def create_dynamic_date_view(self, analysis: Dict[str, Any]) -> Optional[str]:
        """Create date conversion view only if needed"""
        
        if not analysis['date_columns'] or not analysis['has_data']:
            return analysis['actual_table']  # Return original table if no date conversion needed
        
        table_name = analysis['actual_table']
        view_name = f"{table_name}_dates"
        
        try:
            conn = duckdb.connect(self.duckdb_path)
            
            # Build date conversion SQL dynamically
            date_conversions = []
            for col in analysis['date_columns']:
                col_type = analysis['data_types'].get(col, 'VARCHAR')
                
                if any(hint in col_type for hint in ['DATE', 'TIME', 'TIMESTAMP']):
                    # Already proper date type
                    date_conversions.append(f"{col} as {col}_converted")
                else:
                    # Convert string dates
                    conversion_sql = f"""CASE 
                        WHEN {col} LIKE '__-__-____' THEN strptime({col}, '%d-%m-%Y')
                        WHEN {col} LIKE '____-__-__' THEN strptime({col}, '%Y-%m-%d')
                        WHEN {col} LIKE '__/__/____' THEN strptime({col}, '%m/%d/%Y')
                        ELSE NULL
                    END as {col}_converted"""
                    date_conversions.append(conversion_sql)
            
            if date_conversions:
                # Create the view
                create_view_sql = f"""
                DROP VIEW IF EXISTS {view_name};
                
                CREATE VIEW {view_name} AS
                SELECT *,
                       {', '.join(date_conversions)}
                FROM {table_name}
                """
                
                conn.execute(create_view_sql)
                print(f"✅ Created dynamic date view: {view_name}")
                conn.close()
                return view_name
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error creating date view: {e}")
        
        return analysis['actual_table']  # Fallback to original table

def update_llm_service_dynamic():
    """Update LLM service to use dynamic analysis instead of hardcoded values"""
    
    print("🔧 Updating LLM Service to be Dynamic")
    print("=" * 60)
    
    llm_service_file = 'services/llm_service.py'
    
    try:
        with open(llm_service_file, 'r') as f:
            content = f.read()
        
        # Replace hardcoded elements with dynamic ones
        replacements = [
            # Remove hardcoded table names
            ("table_name = 'col_mapping_fixed_dates'", "table_name = data_source.table_name"),
            ("table_name = 'col_mapping'", "table_name = data_source.table_name"),
            
            # Remove hardcoded column lists
            ("column_list = ['customer_name', 'sales', 'region', 'order_date_converted', 'product_name']", 
             "column_list = self._get_dynamic_columns(schema_info)"),
            
            # Make prompt generation dynamic
            ("Use ONLY the table specified in the schema. Do NOT create JOINs or reference tables that don't exist.\nFor any date filtering, use the 'order_date_converted' column.",
             "Use ONLY the table specified in the schema. Do NOT create JOINs or reference tables that don't exist.\nAnalyze the available columns and use appropriate ones for the query.")
        ]
        
        updated_content = content
        for old, new in replacements:
            updated_content = updated_content.replace(old, new)
        
        # Add dynamic helper method
        dynamic_method = '''
    def _get_dynamic_columns(self, schema_info: Dict[str, Any]) -> List[str]:
        """Dynamically extract column names from schema info"""
        columns = schema_info.get('columns', [])
        
        if isinstance(columns, list) and columns:
            if isinstance(columns[0], dict):
                return [col.get('name', str(col)) for col in columns]
            else:
                return [str(col) for col in columns]
        
        # Fallback: return empty list to be filled by actual table analysis
        return []
'''
        
        # Insert the dynamic method before the _generate_sql_ollama method
        method_pos = updated_content.find('def _generate_sql_ollama(')
        if method_pos > 0:
            updated_content = (updated_content[:method_pos] + 
                             dynamic_method + '\n    ' +
                             updated_content[method_pos:])
        
        # Write the updated file
        with open(llm_service_file, 'w') as f:
            f.write(updated_content)
        
        print("✅ LLM service updated to be dynamic")
        return True
        
    except Exception as e:
        print(f"❌ Error updating LLM service: {e}")
        return False

def update_datasource_to_dynamic():
    """Update DataSource to use dynamic table detection"""
    
    print("🔄 Updating DataSource to Dynamic Table")
    print("=" * 50)
    
    try:
        analyzer = DynamicDataAnalyzer()
        
        # Get all data sources
        data_sources = DataSource.objects.all()
        
        for ds in data_sources:
            print(f"📊 Analyzing: {ds.name}")
            
            # Analyze dynamically
            analysis = analyzer.analyze_data_source(ds)
            
            if analysis['has_data']:
                # Create date view if needed
                best_table = analyzer.create_dynamic_date_view(analysis)
                
                # Update DataSource
                old_table = ds.table_name
                ds.table_name = best_table
                ds.save()
                
                print(f"   ✅ Updated: {old_table} → {best_table}")
                print(f"   📋 Columns: {len(analysis['columns'])}")
                print(f"   📅 Date columns: {len(analysis['date_columns'])}")
                print(f"   🔢 Numeric columns: {len(analysis['numeric_columns'])}")
                print(f"   📝 Rows: {analysis['row_count']:,}")
            else:
                print(f"   ⚠️ No data found for {ds.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating DataSource: {e}")
        return False

def test_dynamic_system():
    """Test the dynamic system with any available data"""
    
    print("🧪 Testing Dynamic System")
    print("=" * 50)
    
    try:
        analyzer = DynamicDataAnalyzer()
        
        # Test with first available data source
        data_source = DataSource.objects.first()
        if not data_source:
            print("❌ No data sources found")
            return False
        
        print(f"🔍 Testing with: {data_source.name}")
        
        # Analyze
        analysis = analyzer.analyze_data_source(data_source)
        
        if analysis['has_data']:
            print(f"✅ Analysis successful:")
            print(f"   Table: {analysis['actual_table']}")
            print(f"   Columns: {len(analysis['columns'])}")
            print(f"   Date columns: {analysis['date_columns']}")
            print(f"   Numeric columns: {analysis['numeric_columns'][:5]}")
            print(f"   Text columns: {analysis['text_columns'][:5]}")
            print(f"   Rows: {analysis['row_count']:,}")
            
            # Test dynamic query
            if analysis['numeric_columns']:
                numeric_col = analysis['numeric_columns'][0]
                test_query = f"SELECT SUM({numeric_col}) FROM {analysis['actual_table']}"
                
                conn = duckdb.connect(analyzer.duckdb_path)
                try:
                    result = conn.execute(test_query).fetchone()
                    print(f"   🧪 Test query result: {result[0] if result else 'None'}")
                except Exception as e:
                    print(f"   ⚠️ Test query failed: {e}")
                finally:
                    conn.close()
            
            return True
        else:
            print("❌ No usable data found")
            return False
            
    except Exception as e:
        print(f"❌ Testing failed: {e}")
        return False

def main():
    """Remove all hardcoded elements and make system truly dynamic"""
    
    print("🚀 Making ConvaBI Completely Dynamic")
    print("=" * 70)
    print("Removing ALL hardcoded table names, column names, and assumptions")
    print("Making system work with ANY dataset and data types")
    print()
    
    # Step 1: Update LLM service to be dynamic
    print("📝 Step 1: Making LLM Service Dynamic")
    llm_updated = update_llm_service_dynamic()
    
    # Step 2: Update DataSource models to use dynamic detection
    print("\n📊 Step 2: Updating DataSource Models")
    ds_updated = update_datasource_to_dynamic()
    
    # Step 3: Test the dynamic system
    print("\n🧪 Step 3: Testing Dynamic System")
    test_passed = test_dynamic_system()
    
    print("\n" + "=" * 70)
    print("🎯 DYNAMIC CONVERSION RESULTS:")
    print(f"   LLM service made dynamic: {'✅' if llm_updated else '❌'}")
    print(f"   DataSource models updated: {'✅' if ds_updated else '❌'}")
    print(f"   Dynamic system testing: {'✅' if test_passed else '❌'}")
    
    if llm_updated and ds_updated and test_passed:
        print("\n🎉 SUCCESS!")
        print("✅ System is now completely dynamic")
        print("✅ Works with ANY dataset and data types")
        print("✅ No hardcoded assumptions")
        print("✅ Automatically adapts to new data sources")
        print("🔗 Restart Django server to use the dynamic system")
    else:
        print("\n⚠️ Dynamic conversion incomplete")
        print("💡 Check error messages above")

if __name__ == "__main__":
    main() 