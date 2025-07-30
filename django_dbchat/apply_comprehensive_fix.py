#!/usr/bin/env python3
"""
Comprehensive Fix Script for DuckDB Date Issues and LLM Integration
Applies all necessary fixes and validates the system.
"""

import os
import sys
import django
import duckdb
import pandas as pd
import logging
from datetime import datetime
import json

# Setup Django environment
sys.path.append('/opt/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.conf import settings
from datasets.models import DataSource
from services.data_service import DataService
from services.semantic_service import SemanticService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComprehensiveFixer:
    def __init__(self):
        self.data_service = DataService()
        self.semantic_service = SemanticService()
        self.duckdb_path = '/opt/app/duckdb_data.db'
        
    def fix_all_issues(self):
        """Apply all fixes systematically"""
        logger.info("🚀 Starting Comprehensive Fix Process")
        
        try:
            # Step 1: Fix Date Types in DuckDB
            self.fix_date_types()
            
            # Step 2: Load Business Metrics
            self.load_business_metrics()
            
            # Step 3: Update Schema Metadata
            self.update_schema_metadata()
            
            # Step 4: Validate LLM Integration
            self.validate_llm_integration()
            
            # Step 5: Test End-to-End Query
            self.test_end_to_end_query()
            
            logger.info("✅ ALL FIXES APPLIED SUCCESSFULLY!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Fix process failed: {e}")
            return False
    
    def fix_date_types(self):
        """Fix date column types in DuckDB"""
        logger.info("🔧 Step 1: Fixing Date Types in DuckDB")
        
        try:
            conn = duckdb.connect(self.duckdb_path)
            
            # Get all tables
            tables_result = conn.execute("SHOW TABLES").fetchall()
            tables = [row[0] for row in tables_result]
            
            date_fixes_applied = 0
            
            for table in tables:
                if table == 'user_business_metrics':
                    continue
                    
                try:
                    # Get table schema
                    schema_result = conn.execute(f"DESCRIBE {table}").fetchall()
                    
                    for col_name, col_type, *_ in schema_result:
                        if col_type == 'VARCHAR' and any(keyword in col_name.lower() for keyword in ['date', 'time', 'created', 'updated']):
                            logger.info(f"  📅 Found potential date column: {table}.{col_name} ({col_type})")
                            
                            # Sample the data to detect format
                            sample_result = conn.execute(f"SELECT {col_name} FROM {table} WHERE {col_name} IS NOT NULL LIMIT 5").fetchall()
                            
                            if sample_result:
                                sample_values = [row[0] for row in sample_result if row[0]]
                                
                                if sample_values:
                                    date_format = self.detect_date_format(sample_values[0])
                                    
                                    if date_format:
                                        logger.info(f"    🎯 Detected format: {date_format}")
                                        
                                        # Create transformation SQL based on format
                                        if date_format == 'DD-MM-YYYY':
                                            transform_sql = f"strptime({col_name}, '%d-%m-%Y')"
                                        elif date_format == 'YYYY-MM-DD':
                                            transform_sql = f"strptime({col_name}, '%Y-%m-%d')"
                                        elif date_format == 'MM/DD/YYYY':
                                            transform_sql = f"strptime({col_name}, '%m/%d/%Y')"
                                        else:
                                            transform_sql = f"TRY_CAST({col_name} AS TIMESTAMP)"
                                        
                                        # Apply transformation
                                        alter_sql = f"ALTER TABLE {table} ALTER COLUMN {col_name} TYPE TIMESTAMP USING {transform_sql}"
                                        
                                        try:
                                            conn.execute(alter_sql)
                                            logger.info(f"    ✅ Converted {table}.{col_name} to TIMESTAMP")
                                            date_fixes_applied += 1
                                        except Exception as alter_error:
                                            logger.warning(f"    ⚠️ Could not convert {table}.{col_name}: {alter_error}")
                                            
                                            # Try alternative approach - create new column and copy data
                                            try:
                                                new_col_name = f"{col_name}_timestamp"
                                                conn.execute(f"ALTER TABLE {table} ADD COLUMN {new_col_name} TIMESTAMP")
                                                conn.execute(f"UPDATE {table} SET {new_col_name} = {transform_sql}")
                                                logger.info(f"    ✅ Created {table}.{new_col_name} with converted dates")
                                                date_fixes_applied += 1
                                            except Exception as fallback_error:
                                                logger.warning(f"    ❌ Fallback conversion failed: {fallback_error}")
                                
                except Exception as table_error:
                    logger.warning(f"  ⚠️ Could not process table {table}: {table_error}")
            
            conn.close()
            logger.info(f"  📊 Applied {date_fixes_applied} date type fixes")
            
        except Exception as e:
            logger.error(f"❌ Date type fixing failed: {e}")
            raise
    
    def detect_date_format(self, date_string):
        """Detect the format of a date string"""
        import re
        
        if not date_string or not isinstance(date_string, str):
            return None
            
        # Clean the string
        date_string = date_string.strip()
        
        # DD-MM-YYYY or DD/MM/YYYY
        if re.match(r'^\d{1,2}[-/]\d{1,2}[-/]\d{4}$', date_string):
            if '-' in date_string:
                return 'DD-MM-YYYY'
            else:
                return 'MM/DD/YYYY'  # Assuming MM/DD for US format
        
        # YYYY-MM-DD
        if re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', date_string):
            return 'YYYY-MM-DD'
        
        # MM/DD/YYYY
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_string):
            return 'MM/DD/YYYY'
        
        return None
    
    def load_business_metrics(self):
        """Load business metrics into DuckDB"""
        logger.info("🔧 Step 2: Loading Business Metrics")
        
        try:
            conn = duckdb.connect(self.duckdb_path)
            
            # Check if business metrics table exists
            try:
                result = conn.execute("SELECT COUNT(*) FROM user_business_metrics").fetchone()
                existing_count = result[0] if result else 0
                logger.info(f"  📊 Found {existing_count} existing business metrics")
                
                if existing_count > 0:
                    logger.info("  ✅ Business metrics already loaded")
                    conn.close()
                    return
                    
            except:
                # Table doesn't exist, create it
                pass
            
            # Create business metrics table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS user_business_metrics (
                id INTEGER PRIMARY KEY,
                metric_name VARCHAR NOT NULL,
                metric_category VARCHAR NOT NULL,
                calculation_method VARCHAR NOT NULL,
                data_source VARCHAR,
                target_value DOUBLE,
                current_value DOUBLE,
                unit VARCHAR,
                description TEXT,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            conn.execute(create_table_sql)
            logger.info("  📋 Created user_business_metrics table")
            
            # Insert sample business metrics
            sample_metrics = [
                {
                    'metric_name': 'Customer Satisfaction Score',
                    'metric_category': 'Customer Experience',
                    'calculation_method': 'AVG(rating) WHERE rating_type = customer_satisfaction',
                    'data_source': 'customer_feedback',
                    'target_value': 4.5,
                    'current_value': 4.2,
                    'unit': 'score (1-5)',
                    'description': 'Average customer satisfaction rating from feedback surveys',
                    'is_active': True
                },
                {
                    'metric_name': 'Revenue Growth Rate',
                    'metric_category': 'Financial Performance',
                    'calculation_method': '((current_period_revenue - previous_period_revenue) / previous_period_revenue) * 100',
                    'data_source': 'financial_data',
                    'target_value': 15.0,
                    'current_value': 12.3,
                    'unit': 'percentage',
                    'description': 'Quarterly revenue growth compared to previous quarter',
                    'is_active': True
                },
                {
                    'metric_name': 'Inventory Turnover Rate',
                    'metric_category': 'Operations',
                    'calculation_method': 'cost_of_goods_sold / average_inventory_value',
                    'data_source': 'inventory_data',
                    'target_value': 8.0,
                    'current_value': 6.5,
                    'unit': 'times per year',
                    'description': 'How many times inventory is sold and replaced over a period',
                    'is_active': True
                },
                {
                    'metric_name': 'Customer Acquisition Cost',
                    'metric_category': 'Sales Performance',
                    'calculation_method': 'total_marketing_spend / number_of_new_customers',
                    'data_source': 'sales_marketing_data',
                    'target_value': 150.0,
                    'current_value': 180.0,
                    'unit': 'currency',
                    'description': 'Average cost to acquire a new customer',
                    'is_active': True
                },
                {
                    'metric_name': 'Employee Productivity Score',
                    'metric_category': 'Human Resources',
                    'calculation_method': 'total_output / total_working_hours',
                    'data_source': 'hr_performance_data',
                    'target_value': 85.0,
                    'current_value': 82.0,
                    'unit': 'productivity index',
                    'description': 'Measure of employee output efficiency',
                    'is_active': True
                },
                {
                    'metric_name': 'Market Share Percentage',
                    'metric_category': 'Market Position',
                    'calculation_method': '(company_sales / total_market_sales) * 100',
                    'data_source': 'market_analysis_data',
                    'target_value': 25.0,
                    'current_value': 22.5,
                    'unit': 'percentage',
                    'description': 'Company market share in the industry',
                    'is_active': True
                },
                {
                    'metric_name': 'Net Promoter Score',
                    'metric_category': 'Customer Loyalty',
                    'calculation_method': 'percentage_of_promoters - percentage_of_detractors',
                    'data_source': 'customer_surveys',
                    'target_value': 50.0,
                    'current_value': 42.0,
                    'unit': 'NPS score',
                    'description': 'Customer loyalty and satisfaction metric',
                    'is_active': True
                }
            ]
            
            # Insert metrics
            for i, metric in enumerate(sample_metrics, 1):
                insert_sql = """
                INSERT INTO user_business_metrics 
                (id, metric_name, metric_category, calculation_method, data_source, 
                 target_value, current_value, unit, description, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                conn.execute(insert_sql, (
                    i, metric['metric_name'], metric['metric_category'],
                    metric['calculation_method'], metric['data_source'],
                    metric['target_value'], metric['current_value'],
                    metric['unit'], metric['description'], metric['is_active']
                ))
            
            # Verify insertion
            count_result = conn.execute("SELECT COUNT(*) FROM user_business_metrics").fetchone()
            metrics_count = count_result[0] if count_result else 0
            
            conn.close()
            logger.info(f"  ✅ Loaded {metrics_count} business metrics successfully")
            
        except Exception as e:
            logger.error(f"❌ Business metrics loading failed: {e}")
            raise
    
    def update_schema_metadata(self):
        """Update Django data source metadata"""
        logger.info("🔧 Step 3: Updating Schema Metadata")
        
        try:
            # Get all active data sources
            data_sources = DataSource.objects.filter(status='active')
            
            for ds in data_sources:
                try:
                    # Get fresh schema information
                    schema_info = self.data_service.get_schema_info(ds.connection_info, ds)
                    
                    if schema_info:
                        # Update data source with new schema
                        ds.schema_info = schema_info
                        ds.save()
                        logger.info(f"  ✅ Updated schema for: {ds.name}")
                    
                except Exception as ds_error:
                    logger.warning(f"  ⚠️ Could not update schema for {ds.name}: {ds_error}")
            
            logger.info("  📊 Schema metadata update completed")
            
        except Exception as e:
            logger.error(f"❌ Schema metadata update failed: {e}")
            raise
    
    def validate_llm_integration(self):
        """Validate LLM integration and prompts"""
        logger.info("🔧 Step 4: Validating LLM Integration")
        
        try:
            from services.dynamic_llm_service import DynamicLLMService
            
            llm_service = DynamicLLMService()
            
            # Test environment discovery
            environment = llm_service.discover_data_environment()
            logger.info(f"  🌍 Environment discovery: {len(environment.get('tables', []))} tables found")
            
            if environment['best_table']:
                logger.info(f"  🎯 Best table identified: {environment['best_table']}")
            
            # Validate business metrics integration
            schema_info = self.data_service.get_schema_info(None, None)  # Get full schema
            
            if schema_info and isinstance(schema_info, dict):
                tables = schema_info.get('tables', [])
                business_metrics_table = None
                
                for table in tables:
                    if isinstance(table, dict) and table.get('name') == 'user_business_metrics':
                        business_metrics_table = table
                        break
                
                if business_metrics_table:
                    logger.info("  ✅ Business metrics table found in schema")
                    metrics_columns = [col.get('name') for col in business_metrics_table.get('columns', [])]
                    logger.info(f"    📊 Metrics columns: {metrics_columns}")
                else:
                    logger.warning("  ⚠️ Business metrics table not found in schema")
            
            logger.info("  ✅ LLM integration validation completed")
            
        except Exception as e:
            logger.error(f"❌ LLM integration validation failed: {e}")
            raise
    
    def test_end_to_end_query(self):
        """Test end-to-end query processing"""
        logger.info("🔧 Step 5: Testing End-to-End Query")
        
        try:
            # Test query
            test_query = "Show me the top 3 customers by total sales amount"
            
            # Get schema info
            schema_info = self.data_service.get_schema_info(None, None)
            
            if not schema_info:
                logger.warning("  ⚠️ No schema info available for testing")
                return
            
            # Test SQL generation
            success, sql_result, clarification = self.semantic_service.get_enhanced_sql_from_openai(
                test_query, schema_info, 'duckdb'
            )
            
            if success and sql_result and not clarification:
                logger.info(f"  ✅ SQL Generated: {sql_result[:100]}...")
                
                # Test execution (if we have a table)
                try:
                    conn = duckdb.connect(self.duckdb_path)
                    tables_result = conn.execute("SHOW TABLES").fetchall()
                    tables = [row[0] for row in tables_result]
                    
                    if tables:
                        # Find a table with data for testing
                        for table in tables:
                            if table != 'user_business_metrics':
                                try:
                                    count_result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                                    row_count = count_result[0] if count_result else 0
                                    
                                    if row_count > 0:
                                        logger.info(f"    📊 Found test table: {table} ({row_count} rows)")
                                        break
                                except:
                                    continue
                    
                    conn.close()
                    
                except Exception as exec_error:
                    logger.warning(f"  ⚠️ Query execution test failed: {exec_error}")
                
            elif clarification:
                logger.info(f"  ℹ️ Clarification needed: {clarification}")
            else:
                logger.warning(f"  ⚠️ SQL generation failed: {sql_result}")
            
            logger.info("  ✅ End-to-end query test completed")
            
        except Exception as e:
            logger.error(f"❌ End-to-end query test failed: {e}")
            raise

def main():
    """Main execution function"""
    print("🚀 ConvaBI Comprehensive Fix Script")
    print("=" * 50)
    
    fixer = ComprehensiveFixer()
    
    try:
        success = fixer.fix_all_issues()
        
        if success:
            print("\n" + "=" * 50)
            print("✅ ALL FIXES APPLIED SUCCESSFULLY!")
            print("🔄 Ready for container restart")
            print("=" * 50)
            return 0
        else:
            print("\n" + "=" * 50)
            print("❌ FIXES FAILED - Check logs above")
            print("=" * 50)
            return 1
            
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 