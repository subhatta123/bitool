#!/usr/bin/env python3
"""
Quick Fix Script for Critical DuckDB Issues
This script applies essential fixes for date types and business metrics
"""

import os
import sys
import django
import duckdb
import logging

# Setup Django environment
sys.path.append('/opt/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from services.data_service import DataService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def quick_fix():
    """Apply critical fixes quickly"""
    logger.info("🚀 Applying Quick Fixes")
    
    try:
        duckdb_path = '/opt/app/duckdb_data.db'
        conn = duckdb.connect(duckdb_path)
        
        # Fix 1: Ensure business metrics table exists
        logger.info("📊 Creating business metrics table")
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
        
        # Check if metrics exist
        count_result = conn.execute("SELECT COUNT(*) FROM user_business_metrics").fetchone()
        metrics_count = count_result[0] if count_result else 0
        
        if metrics_count == 0:
            logger.info("📋 Adding sample business metrics")
            sample_metrics = [
                (1, 'Customer Satisfaction Score', 'Customer Experience', 'AVG(rating)', 'customer_feedback', 4.5, 4.2, 'score', 'Customer satisfaction rating', True),
                (2, 'Revenue Growth Rate', 'Financial Performance', 'revenue_calculation', 'financial_data', 15.0, 12.3, 'percentage', 'Revenue growth rate', True),
                (3, 'Inventory Turnover Rate', 'Operations', 'turnover_calculation', 'inventory_data', 8.0, 6.5, 'times per year', 'Inventory turnover', True),
            ]
            
            for metric in sample_metrics:
                insert_sql = """
                INSERT INTO user_business_metrics 
                (id, metric_name, metric_category, calculation_method, data_source, 
                 target_value, current_value, unit, description, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                conn.execute(insert_sql, metric)
        
        # Fix 2: Check and fix date columns
        logger.info("📅 Checking date columns")
        tables_result = conn.execute("SHOW TABLES").fetchall()
        tables = [row[0] for row in tables_result]
        
        for table in tables:
            if table == 'user_business_metrics':
                continue
                
            try:
                schema_result = conn.execute(f"DESCRIBE {table}").fetchall()
                for col_name, col_type, *_ in schema_result:
                    if col_type == 'VARCHAR' and any(keyword in col_name.lower() for keyword in ['date', 'time']):
                        logger.info(f"  Found potential date column: {table}.{col_name}")
                        
                        # Try to convert with common date format
                        try:
                            alter_sql = f"ALTER TABLE {table} ALTER COLUMN {col_name} TYPE TIMESTAMP USING strptime({col_name}, '%d-%m-%Y')"
                            conn.execute(alter_sql)
                            logger.info(f"  ✅ Converted {table}.{col_name} to TIMESTAMP")
                        except:
                            # Try alternative format
                            try:
                                alter_sql = f"ALTER TABLE {table} ALTER COLUMN {col_name} TYPE TIMESTAMP USING TRY_CAST({col_name} AS TIMESTAMP)"
                                conn.execute(alter_sql)
                                logger.info(f"  ✅ Converted {table}.{col_name} to TIMESTAMP (alternative)")
                            except Exception as e:
                                logger.info(f"  ⚠️ Could not convert {table}.{col_name}: {e}")
            except Exception as table_error:
                logger.info(f"  ⚠️ Could not process table {table}: {table_error}")
        
        conn.close()
        logger.info("✅ Quick fixes applied successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Quick fix failed: {e}")
        return False

if __name__ == '__main__':
    success = quick_fix()
    if success:
        print("✅ QUICK FIXES APPLIED SUCCESSFULLY!")
        sys.exit(0)
    else:
        print("❌ QUICK FIXES FAILED")
        sys.exit(1) 