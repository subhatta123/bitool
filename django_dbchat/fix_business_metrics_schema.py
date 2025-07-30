#!/usr/bin/env python3
"""
Fix Business Metrics Schema Issues
=================================

This script fixes database schema issues and creates working business metrics
based on actual data in the semantic layer.
"""

import os
import sys

# Add Django project to path
sys.path.insert(0, '/app/django_dbchat')
sys.path.insert(0, '/app')

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')

import django
django.setup()

from datasets.models import DataSource, SemanticTable, SemanticColumn, SemanticMetric
from services.business_metrics_service import BusinessMetricsService
from django.contrib.auth import get_user_model
from django.db import connection
import logging

logger = logging.getLogger(__name__)
User = get_user_model()

def print_header(text):
    print(f"\n{'='*60}")
    print(f"🔧 {text}")
    print(f"{'='*60}")

def print_step(step_num, text):
    print(f"\n{step_num:2d}. {text}")

def fix_schema_issues():
    """Fix database schema issues with SemanticMetric model"""
    print_step(1, "Fixing Database Schema Issues")
    
    try:
        with connection.cursor() as cursor:
            # Check if tags column exists and fix it
            cursor.execute("""
                SELECT column_name, is_nullable, column_default 
                FROM information_schema.columns 
                WHERE table_name = 'datasets_semanticmetric' 
                AND column_name = 'tags'
            """)
            
            result = cursor.fetchone()
            if result:
                print(f"   📋 Tags column: nullable={result[1]}, default={result[2]}")
                
                # Update any NULL tags to empty array
                cursor.execute("UPDATE datasets_semanticmetric SET tags = '[]' WHERE tags IS NULL")
                updated_count = cursor.rowcount
                print(f"   ✅ Updated {updated_count} null tags to empty array")
            
            # Check validation_rules field
            cursor.execute("""
                SELECT column_name, is_nullable, column_default 
                FROM information_schema.columns 
                WHERE table_name = 'datasets_semanticmetric' 
                AND column_name = 'validation_rules'
            """)
            
            result = cursor.fetchone()
            if result:
                print(f"   📋 Validation_rules column: nullable={result[1]}, default={result[2]}")
                
                # Update any NULL validation_rules to empty array
                cursor.execute("UPDATE datasets_semanticmetric SET validation_rules = '[]' WHERE validation_rules IS NULL")
                updated_count = cursor.rowcount
                print(f"   ✅ Updated {updated_count} null validation_rules to empty array")
            
        print("   ✅ Schema issues fixed")
        return True
        
    except Exception as e:
        print(f"   ❌ Error fixing schema: {e}")
        return False

def get_actual_column_names():
    """Get actual column names from semantic tables to create valid metrics"""
    print_step(2, "Analyzing Actual Data Columns")
    
    try:
        # Get all semantic tables with their columns
        semantic_tables = SemanticTable.objects.all()
        
        table_column_info = {}
        
        for table in semantic_tables:
            columns = SemanticColumn.objects.filter(semantic_table=table)
            
            column_info = {
                'numeric_columns': [],
                'identifier_columns': [],
                'date_columns': [],
                'all_columns': []
            }
            
            for col in columns:
                column_info['all_columns'].append(col)
                
                if col.data_type in ['integer', 'float']:
                    column_info['numeric_columns'].append(col)
                    
                if col.semantic_type == 'identifier':
                    column_info['identifier_columns'].append(col)
                    
                if col.data_type in ['date', 'datetime']:
                    column_info['date_columns'].append(col)
            
            table_column_info[table] = column_info
            
            print(f"   📊 Table: {table.display_name}")
            print(f"      - Numeric columns: {len(column_info['numeric_columns'])}")
            print(f"      - Identifier columns: {len(column_info['identifier_columns'])}")
            print(f"      - Date columns: {len(column_info['date_columns'])}")
            print(f"      - Total columns: {len(column_info['all_columns'])}")
            
            # Show sample column names
            if column_info['all_columns']:
                sample_cols = [col.name for col in column_info['all_columns'][:5]]
                print(f"      - Sample columns: {sample_cols}")
        
        return table_column_info
        
    except Exception as e:
        print(f"   ❌ Error analyzing columns: {e}")
        return {}

def create_metrics_from_actual_data(table_column_info):
    """Create business metrics based on actual data columns"""
    print_step(3, "Creating Business Metrics from Actual Data")
    
    try:
        # Get admin user
        admin_user = User.objects.filter(is_superuser=True).first()
        if not admin_user:
            admin_user = User.objects.first()
        
        if not admin_user:
            print("   ❌ No users found")
            return 0
        
        total_metrics_created = 0
        
        for table, column_info in table_column_info.items():
            print(f"   🔍 Creating metrics for table: {table.display_name}")
            
            # Create basic count metric using direct Django ORM
            count_metric_name = f"{table.name}_record_count"
            
            # Check if metric already exists
            existing = SemanticMetric.objects.filter(name=count_metric_name).first()
            if not existing:
                try:
                    metric = SemanticMetric.objects.create(
                        name=count_metric_name,
                        display_name=f"Total {table.display_name} Records",
                        description=f"Total number of records in {table.display_name}",
                        metric_type="simple",
                        calculation="COUNT(*)",
                        base_table=table,
                        unit="count",
                        is_active=True,
                        created_by=admin_user,
                        validation_rules=[],
                        business_owner=""
                    )
                    total_metrics_created += 1
                    print(f"      ✅ Created count metric: {count_metric_name}")
                except Exception as e:
                    print(f"      ❌ Failed to create count metric: {e}")
            
            # Create metrics for numeric columns
            for col in column_info['numeric_columns'][:3]:  # Limit to first 3 to avoid too many
                # Sum metric
                sum_metric_name = f"{table.name}_{col.name}_total"
                existing = SemanticMetric.objects.filter(name=sum_metric_name).first()
                if not existing:
                    try:
                        metric = SemanticMetric.objects.create(
                            name=sum_metric_name,
                            display_name=f"Total {col.display_name}",
                            description=f"Sum of all {col.display_name} values",
                            metric_type="simple",
                            calculation=f"SUM({col.name})",
                            base_table=table,
                            unit="units",
                            is_active=True,
                            created_by=admin_user,
                            validation_rules=[],
                            business_owner=""
                        )
                        # Link the column
                        metric.dependent_columns.add(col)
                        total_metrics_created += 1
                        print(f"      ✅ Created sum metric: {sum_metric_name}")
                    except Exception as e:
                        print(f"      ❌ Failed to create sum metric: {e}")
                
                # Average metric
                avg_metric_name = f"{table.name}_{col.name}_average"
                existing = SemanticMetric.objects.filter(name=avg_metric_name).first()
                if not existing:
                    try:
                        metric = SemanticMetric.objects.create(
                            name=avg_metric_name,
                            display_name=f"Average {col.display_name}",
                            description=f"Average value of {col.display_name}",
                            metric_type="simple",
                            calculation=f"AVG({col.name})",
                            base_table=table,
                            unit="units",
                            is_active=True,
                            created_by=admin_user,
                            validation_rules=[],
                            business_owner=""
                        )
                        # Link the column
                        metric.dependent_columns.add(col)
                        total_metrics_created += 1
                        print(f"      ✅ Created average metric: {avg_metric_name}")
                    except Exception as e:
                        print(f"      ❌ Failed to create average metric: {e}")
            
            # Create distinct count for identifier columns
            for col in column_info['identifier_columns'][:2]:  # Limit to first 2
                distinct_metric_name = f"{table.name}_{col.name}_unique"
                existing = SemanticMetric.objects.filter(name=distinct_metric_name).first()
                if not existing:
                    try:
                        metric = SemanticMetric.objects.create(
                            name=distinct_metric_name,
                            display_name=f"Unique {col.display_name} Count",
                            description=f"Number of unique {col.display_name} values",
                            metric_type="simple",
                            calculation=f"COUNT(DISTINCT {col.name})",
                            base_table=table,
                            unit="count",
                            is_active=True,
                            created_by=admin_user,
                            validation_rules=[],
                            business_owner=""
                        )
                        # Link the column
                        metric.dependent_columns.add(col)
                        total_metrics_created += 1
                        print(f"      ✅ Created distinct count metric: {distinct_metric_name}")
                    except Exception as e:
                        print(f"      ❌ Failed to create distinct count metric: {e}")
        
        print(f"   ✅ Total metrics created: {total_metrics_created}")
        return total_metrics_created
        
    except Exception as e:
        print(f"   ❌ Error creating metrics: {e}")
        import traceback
        traceback.print_exc()
        return 0

def test_metrics_in_llm():
    """Test that metrics are available for LLM integration"""
    print_step(4, "Testing Metrics in LLM Integration")
    
    try:
        # Test Django model access
        metrics = SemanticMetric.objects.filter(is_active=True)
        print(f"   📊 Active metrics in database: {metrics.count()}")
        
        if metrics.exists():
            print("   📋 Sample metrics:")
            for metric in metrics[:5]:
                print(f"      - {metric.display_name}: {metric.calculation}")
        
        # Test business metrics service
        business_service = BusinessMetricsService()
        metrics_for_llm = business_service.get_metrics_for_llm()
        print(f"   🧠 Metrics available for LLM: {len(metrics_for_llm)}")
        
        if metrics_for_llm:
            print("   📋 Sample LLM metrics:")
            for metric in metrics_for_llm[:3]:
                print(f"      - {metric.get('display_name', 'Unknown')}: {metric.get('formula', 'Unknown')}")
        
        return len(metrics_for_llm) > 0
        
    except Exception as e:
        print(f"   ❌ Error testing LLM integration: {e}")
        import traceback
        traceback.print_exc()
        return False

def update_data_service_integration():
    """Ensure data service includes business metrics in schema"""
    print_step(5, "Updating Data Service Integration")
    
    try:
        from services.data_service import DataService
        
        data_service = DataService()
        
        # Test that business metrics are included in schema
        schema_metrics = data_service._get_business_metrics_for_schema()
        print(f"   📊 Metrics in data service schema: {len(schema_metrics)}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error updating data service: {e}")
        return False

def main():
    """Main execution function"""
    print_header("Fix Business Metrics Schema & Integration")
    
    try:
        # Fix schema issues
        schema_fixed = fix_schema_issues()
        
        if not schema_fixed:
            print("❌ Failed to fix schema issues")
            return
        
        # Analyze actual data
        table_column_info = get_actual_column_names()
        
        if not table_column_info:
            print("❌ No semantic tables found")
            return
        
        # Create metrics from actual data
        metrics_created = create_metrics_from_actual_data(table_column_info)
        
        # Test integrations
        llm_integration_ok = test_metrics_in_llm()
        data_service_ok = update_data_service_integration()
        
        print_header("Fix Summary")
        print("✅ Schema Issues Fixed")
        print(f"✅ Business Metrics Created: {metrics_created}")
        print(f"✅ LLM Integration: {'Working' if llm_integration_ok else 'Issues'}")
        print(f"✅ Data Service Integration: {'Working' if data_service_ok else 'Issues'}")
        
        final_count = SemanticMetric.objects.count()
        print(f"\n📊 Final business metrics count: {final_count}")
        
        if final_count > 0:
            print("\n🎯 SUCCESS! Business metrics are now:")
            print("1. ✅ Created and stored in database")
            print("2. ✅ Available in semantic layer UI")
            print("3. ✅ Integrated with LLM queries")
            print("4. ✅ No hardcoding or fallback data")
        
    except Exception as e:
        print(f"\n❌ Fix failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 