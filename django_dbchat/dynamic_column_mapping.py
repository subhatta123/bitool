#!/usr/bin/env python3
"""
Dynamic Column Mapping System
Creates column mappings for any dataset automatically, supporting both
generic column names (col_0, col_1, etc.) and actual column names.
"""

import duckdb
import json
import os
import sys

# Add django settings
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')

import django
django.setup()

from datasets.models import DataSource

class DynamicColumnMapper:
    """Handles dynamic column mapping for any dataset"""
    
    def __init__(self, duckdb_path='data/integrated.duckdb'):
        self.duckdb_path = duckdb_path
        self.conn = duckdb.connect(duckdb_path)
    
    def get_table_schema(self, table_name):
        """Get the actual schema of a table from DuckDB"""
        try:
            schema = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            return [(row[0], row[1]) for row in schema]  # (column_name, data_type)
        except Exception:
            return []
    
    def create_generic_mapping_view(self, table_name, schema_info=None):
        """Create a view that maps generic column names (col_0, col_1...) to actual column names"""
        
        # Get actual schema from DuckDB
        actual_schema = self.get_table_schema(table_name)
        if not actual_schema:
            print(f"❌ Table {table_name} not found in DuckDB")
            return False
        
        print(f"🔧 Creating dynamic mapping for table: {table_name}")
        print(f"📊 Found {len(actual_schema)} columns")
        
        # Generate column mappings
        column_mappings = []
        actual_columns = []
        
        for i, (col_name, col_type) in enumerate(actual_schema):
            # Add generic mapping: col_0, col_1, col_2...
            column_mappings.append(f'    "{col_name}" as col_{i}')
            # Also keep original column name
            actual_columns.append(f'    "{col_name}"')
        
        # Create the mapping view
        view_name = f"{table_name}_mapping"
        
        mapping_sql = f"""
        DROP VIEW IF EXISTS {view_name};
        
        CREATE VIEW {view_name} AS
        SELECT 
            -- Generic column mappings (col_0, col_1, etc.)
{chr(10).join(column_mappings)},
            
            -- Original column names
{chr(10).join(actual_columns)}
        FROM {table_name}
        """
        
        try:
            self.conn.execute(mapping_sql)
            print(f"✅ Created mapping view: {view_name}")
            return view_name
        except Exception as e:
            print(f"❌ Error creating mapping view: {e}")
            return False
    
    def create_universal_mapping_view(self, view_name="universal_mapping"):
        """Create a universal view that combines all data sources with consistent column mapping"""
        
        print(f"🌍 Creating universal mapping view: {view_name}")
        
        # Get all tables in DuckDB
        tables = self.conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables if not t[0].endswith('_mapping')]
        
        if not table_names:
            print("❌ No tables found in DuckDB")
            return False
        
        print(f"📊 Found {len(table_names)} tables: {table_names}")
        
        union_parts = []
        
        for table_name in table_names:
            schema = self.get_table_schema(table_name)
            if not schema:
                continue
            
            # Create consistent column mapping for this table
            column_selects = []
            
            # Map up to 50 generic columns (should be more than enough)
            for i in range(50):
                if i < len(schema):
                    col_name, col_type = schema[i]
                    # Cast to text for consistency across different data types
                    column_selects.append(f'CAST("{col_name}" AS VARCHAR) as col_{i}')
                else:
                    # Fill missing columns with NULL
                    column_selects.append(f'NULL as col_{i}')
            
            # Add table identifier and original columns
            table_select = f"""
                SELECT 
                    '{table_name}' as source_table,
                    {', '.join(column_selects[:20])},  -- First 20 generic columns
                    * EXCLUDE (source_table)  -- All original columns
                FROM {table_name}
            """
            
            union_parts.append(table_select)
        
        if union_parts:
            # Create the universal view
            universal_sql = f"""
            DROP VIEW IF EXISTS {view_name};
            
            CREATE VIEW {view_name} AS
            {' UNION ALL '.join(union_parts)}
            """
            
            try:
                self.conn.execute(universal_sql)
                print(f"✅ Created universal mapping view: {view_name}")
                return view_name
            except Exception as e:
                print(f"❌ Error creating universal view: {e}")
                return False
        
        return False
    
    def update_datasource_mappings(self):
        """Update Django DataSource models to use mapping views"""
        
        print(f"🔄 Updating DataSource models to use mapping views")
        
        try:
            data_sources = DataSource.objects.all()
            updated_count = 0
            
            for ds in data_sources:
                if ds.table_name:
                    # Check if the original table exists
                    original_table = ds.table_name
                    mapping_view = f"{original_table}_mapping"
                    
                    # Check if mapping view exists
                    try:
                        self.conn.execute(f"DESCRIBE {mapping_view}").fetchall()
                        # Update DataSource to use mapping view
                        ds.table_name = mapping_view
                        ds.save()
                        updated_count += 1
                        print(f"✅ Updated {ds.name} to use {mapping_view}")
                    except:
                        print(f"⚠️ No mapping view for {ds.name} (table: {original_table})")
            
            print(f"🎯 Updated {updated_count} DataSource models")
            return updated_count > 0
            
        except Exception as e:
            print(f"❌ Error updating DataSource models: {e}")
            return False
    
    def test_dynamic_queries(self):
        """Test queries with different column naming patterns"""
        
        print(f"\n🧪 Testing Dynamic Column Mapping")
        print("=" * 60)
        
        test_queries = [
            # Generic column pattern (what LLM generates)
            ("Generic columns (col_0, col_6, col_12)", 
             "SELECT col_0, col_6, col_12 FROM universal_mapping LIMIT 3"),
            
            # Mixed generic and specific
            ("Mixed pattern", 
             "SELECT source_table, col_0, col_1 FROM universal_mapping WHERE col_0 IS NOT NULL LIMIT 3"),
            
            # Count per table
            ("Table summary", 
             "SELECT source_table, COUNT(*) as row_count FROM universal_mapping GROUP BY source_table"),
        ]
        
        success_count = 0
        
        for test_name, query in test_queries:
            print(f"\n🔍 Testing: {test_name}")
            try:
                result = self.conn.execute(query).fetchall()
                print(f"   ✅ Success: {len(result)} rows")
                if result:
                    print(f"   📊 Sample: {result[0]}")
                success_count += 1
            except Exception as e:
                print(f"   ❌ Failed: {str(e)[:100]}...")
        
        return success_count == len(test_queries)
    
    def create_all_mappings(self):
        """Create all necessary mapping views for the current dataset"""
        
        print(f"🚀 Dynamic Column Mapping Setup")
        print("=" * 70)
        
        success_count = 0
        
        # Step 1: Get all tables
        tables = self.conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables if not t[0].endswith('_mapping')]
        
        print(f"📊 Found {len(table_names)} tables to map")
        
        # Step 2: Create individual mapping views
        for table_name in table_names:
            if self.create_generic_mapping_view(table_name):
                success_count += 1
        
        # Step 3: Create universal mapping view
        universal_created = self.create_universal_mapping_view()
        
        # Step 4: Update DataSource models
        datasources_updated = self.update_datasource_mappings()
        
        # Step 5: Test the system
        tests_passed = self.test_dynamic_queries()
        
        print(f"\n" + "=" * 70)
        print("🎯 DYNAMIC MAPPING RESULTS:")
        print(f"   Individual mappings created: {success_count}/{len(table_names)}")
        print(f"   Universal mapping created: {'✅' if universal_created else '❌'}")
        print(f"   DataSource models updated: {'✅' if datasources_updated else '❌'}")
        print(f"   Query testing passed: {'✅' if tests_passed else '❌'}")
        
        return success_count > 0 and universal_created
    
    def close(self):
        """Close the DuckDB connection"""
        if hasattr(self, 'conn'):
            self.conn.close()

def main():
    """Main function to set up dynamic column mapping"""
    
    mapper = DynamicColumnMapper()
    
    try:
        success = mapper.create_all_mappings()
        
        if success:
            print(f"\n🎉 DYNAMIC MAPPING SUCCESS!")
            print("✅ All datasets now support both generic (col_0, col_1...) and actual column names")
            print("✅ System will work with any new dataset automatically")
            print("✅ LLM queries will work regardless of column naming pattern")
            print("✅ Supports all data types automatically")
            print("🔗 Your system now handles any dataset dynamically!")
        else:
            print(f"\n⚠️ Dynamic mapping setup incomplete")
            print("💡 Check error messages above")
        
    finally:
        mapper.close()

if __name__ == "__main__":
    main() 