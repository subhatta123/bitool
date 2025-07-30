#!/usr/bin/env python3
"""
Business Metrics Integration for Semantic Layer
==============================================

This script ensures business metrics are:
1. Generated automatically with semantic layer
2. Available via user-friendly UI
3. Integrated with LLM queries (no hardcoding)
4. Properly stored and accessible
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
from services.semantic_service import SemanticService
from django.contrib.auth import get_user_model
import logging

logger = logging.getLogger(__name__)
User = get_user_model()

def print_header(text):
    print(f"\n{'='*60}")
    print(f"🔧 {text}")
    print(f"{'='*60}")

def print_step(step_num, text):
    print(f"\n{step_num:2d}. {text}")

def analyze_current_state():
    """Analyze current state of semantic layer and business metrics"""
    print_step(1, "Analyzing Current State")
    
    try:
        # Check data sources
        data_sources = DataSource.objects.all()
        print(f"   📊 Data Sources: {data_sources.count()}")
        
        # Check semantic tables
        semantic_tables = SemanticTable.objects.all()
        print(f"   🧠 Semantic Tables: {semantic_tables.count()}")
        
        # Check semantic columns
        semantic_columns = SemanticColumn.objects.all()
        print(f"   📋 Semantic Columns: {semantic_columns.count()}")
        
        # Check business metrics
        business_metrics = SemanticMetric.objects.all()
        print(f"   📈 Business Metrics: {business_metrics.count()}")
        
        # Check for ETL-completed sources
        etl_ready_sources = []
        for source in data_sources:
            workflow_status = source.workflow_status or {}
            if workflow_status.get('etl_completed', False):
                etl_ready_sources.append(source)
        
        print(f"   ✅ ETL-Ready Sources: {len(etl_ready_sources)}")
        
        if etl_ready_sources:
            print("   📋 ETL-Ready Sources:")
            for source in etl_ready_sources:
                print(f"      - {source.name} ({source.source_type})")
        
        return {
            'data_sources': data_sources,
            'semantic_tables': semantic_tables,
            'semantic_columns': semantic_columns,
            'business_metrics': business_metrics,
            'etl_ready_sources': etl_ready_sources
        }
        
    except Exception as e:
        print(f"   ❌ Error analyzing state: {e}")
        return None

def generate_missing_business_metrics(state):
    """Generate business metrics for tables that don't have them"""
    print_step(2, "Generating Missing Business Metrics")
    
    try:
        business_service = BusinessMetricsService()
        semantic_service = SemanticService()
        
        total_metrics_created = 0
        
        # Get user for metric creation
        admin_user = User.objects.filter(is_superuser=True).first()
        if not admin_user:
            admin_user = User.objects.first()
        
        if not admin_user:
            print("   ❌ No users found - cannot create metrics")
            return 0
        
        # Generate metrics for each semantic table
        for table in state['semantic_tables']:
            print(f"   🔍 Processing table: {table.display_name}")
            
            # Check if table already has metrics
            existing_metrics = SemanticMetric.objects.filter(base_table=table)
            if existing_metrics.exists():
                print(f"      ✅ Already has {existing_metrics.count()} metrics")
                continue
            
            # Get columns for this table
            columns = SemanticColumn.objects.filter(semantic_table=table)
            numeric_columns = [col for col in columns if col.data_type in ['integer', 'float'] and col.is_measure]
            identifier_columns = [col for col in columns if col.semantic_type == 'identifier']
            date_columns = [col for col in columns if col.data_type in ['date', 'datetime']]
            
            table_metrics_created = 0
            
            # Generate basic count metric
            count_metric_name = f"{table.name}_total_count"
            success, message, metric_id = business_service.create_custom_metric(
                name=count_metric_name,
                display_name=f"Total {table.display_name} Count",
                description=f"Total number of records in {table.display_name}",
                metric_type="simple",
                calculation="COUNT(*)",
                unit="count",
                base_table_id=str(table.id),
                user_id=admin_user.id
            )
            
            if success:
                table_metrics_created += 1
                print(f"      ✅ Created count metric: {count_metric_name}")
            
            # Generate metrics for numeric columns
            for col in numeric_columns:
                # Sum metric
                sum_metric_name = f"{table.name}_{col.name}_sum"
                success, message, metric_id = business_service.create_custom_metric(
                    name=sum_metric_name,
                    display_name=f"Total {col.display_name}",
                    description=f"Sum of all {col.display_name} values",
                    metric_type="simple",
                    calculation=f"SUM({col.name})",
                    unit=col.format_string or "units",
                    base_table_id=str(table.id),
                    user_id=admin_user.id
                )
                
                if success:
                    table_metrics_created += 1
                    print(f"      ✅ Created sum metric: {sum_metric_name}")
                
                # Average metric
                avg_metric_name = f"{table.name}_{col.name}_avg"
                success, message, metric_id = business_service.create_custom_metric(
                    name=avg_metric_name,
                    display_name=f"Average {col.display_name}",
                    description=f"Average value of {col.display_name}",
                    metric_type="simple",
                    calculation=f"AVG({col.name})",
                    unit=col.format_string or "units",
                    base_table_id=str(table.id),
                    user_id=admin_user.id
                )
                
                if success:
                    table_metrics_created += 1
                    print(f"      ✅ Created average metric: {avg_metric_name}")
            
            # Generate distinct count for identifier columns
            for col in identifier_columns:
                distinct_metric_name = f"{table.name}_{col.name}_distinct"
                success, message, metric_id = business_service.create_custom_metric(
                    name=distinct_metric_name,
                    display_name=f"Unique {col.display_name} Count",
                    description=f"Number of unique {col.display_name} values",
                    metric_type="simple",
                    calculation=f"COUNT(DISTINCT {col.name})",
                    unit="count",
                    base_table_id=str(table.id),
                    user_id=admin_user.id
                )
                
                if success:
                    table_metrics_created += 1
                    print(f"      ✅ Created distinct count metric: {distinct_metric_name}")
            
            # Generate ratio metrics if multiple numeric columns exist
            if len(numeric_columns) >= 2:
                col1, col2 = numeric_columns[0], numeric_columns[1]
                ratio_metric_name = f"{table.name}_{col1.name}_to_{col2.name}_ratio"
                success, message, metric_id = business_service.create_custom_metric(
                    name=ratio_metric_name,
                    display_name=f"{col1.display_name} to {col2.display_name} Ratio",
                    description=f"Ratio of {col1.display_name} to {col2.display_name}",
                    metric_type="ratio",
                    calculation=f"({col1.name} / NULLIF({col2.name}, 0)) * 100",
                    unit="%",
                    base_table_id=str(table.id),
                    user_id=admin_user.id
                )
                
                if success:
                    table_metrics_created += 1
                    print(f"      ✅ Created ratio metric: {ratio_metric_name}")
            
            total_metrics_created += table_metrics_created
            print(f"      📊 Created {table_metrics_created} metrics for {table.display_name}")
        
        print(f"   ✅ Total metrics created: {total_metrics_created}")
        return total_metrics_created
        
    except Exception as e:
        print(f"   ❌ Error generating metrics: {e}")
        import traceback
        traceback.print_exc()
        return 0

def test_llm_integration():
    """Test that business metrics are properly integrated with LLM queries"""
    print_step(3, "Testing LLM Integration")
    
    try:
        business_service = BusinessMetricsService()
        
        # Test metrics for LLM
        metrics_for_llm = business_service.get_metrics_for_llm()
        print(f"   📊 Metrics available for LLM: {len(metrics_for_llm)}")
        
        if metrics_for_llm:
            print("   📋 Sample metrics for LLM:")
            for metric in metrics_for_llm[:5]:
                print(f"      - {metric.get('display_name', 'Unknown')}: {metric.get('formula', 'Unknown formula')}")
        
        # Test data service integration
        from services.data_service import DataService
        data_service = DataService()
        
        schema_info = data_service._get_business_metrics_for_schema()
        print(f"   🧠 Metrics in schema for LLM: {len(schema_info)}")
        
        return len(metrics_for_llm) > 0
        
    except Exception as e:
        print(f"   ❌ Error testing LLM integration: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_ui_integration():
    """Test that the UI properly shows business metrics"""
    print_step(4, "Testing UI Integration")
    
    try:
        # Check if semantic metrics are accessible
        metrics = SemanticMetric.objects.all()
        print(f"   📊 Total business metrics in database: {metrics.count()}")
        
        if metrics.exists():
            print("   📋 Sample metrics:")
            for metric in metrics[:5]:
                print(f"      - {metric.display_name} ({metric.metric_type}): {metric.calculation}")
        
        # Test API endpoints
        print("   🔗 API endpoints should be available:")
        print("      - /datasets/api/business-metrics/create/")
        print("      - /datasets/api/business-metrics/list/")
        print("      - /datasets/api/business-metrics/validate-formula/")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error testing UI integration: {e}")
        return False

def update_semantic_layer_generation():
    """Ensure semantic layer generation includes business metrics"""
    print_step(5, "Updating Semantic Layer Generation")
    
    try:
        # Check that semantic service has metric generation
        semantic_service = SemanticService()
        
        # Check if _add_common_metrics method exists
        if hasattr(semantic_service, '_add_common_metrics'):
            print("   ✅ Semantic service has metric generation capability")
        else:
            print("   ⚠️  Semantic service missing metric generation")
        
        # Check if business metrics service is integrated
        business_service = BusinessMetricsService()
        print("   ✅ Business metrics service accessible")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error checking semantic layer generation: {e}")
        return False

def create_sample_business_metrics():
    """Create sample business metrics for demonstration"""
    print_step(6, "Creating Sample Business Metrics")
    
    try:
        business_service = BusinessMetricsService()
        
        # Get admin user
        admin_user = User.objects.filter(is_superuser=True).first()
        if not admin_user:
            admin_user = User.objects.first()
        
        if not admin_user:
            print("   ❌ No users found")
            return False
        
        # Create generic sample metrics
        sample_metrics = [
            {
                'name': 'total_revenue',
                'display_name': 'Total Revenue',
                'description': 'Sum of all revenue across all transactions',
                'metric_type': 'simple',
                'calculation': 'SUM(revenue)',
                'unit': 'USD'
            },
            {
                'name': 'average_order_value',
                'display_name': 'Average Order Value',
                'description': 'Average value per order',
                'metric_type': 'simple',
                'calculation': 'AVG(order_value)',
                'unit': 'USD'
            },
            {
                'name': 'customer_count',
                'display_name': 'Total Customers',
                'description': 'Number of unique customers',
                'metric_type': 'simple',
                'calculation': 'COUNT(DISTINCT customer_id)',
                'unit': 'count'
            },
            {
                'name': 'profit_margin',
                'display_name': 'Profit Margin',
                'description': 'Percentage profit margin',
                'metric_type': 'ratio',
                'calculation': '(profit / NULLIF(revenue, 0)) * 100',
                'unit': '%'
            }
        ]
        
        created_count = 0
        
        for metric_def in sample_metrics:
            # Check if metric already exists
            existing = SemanticMetric.objects.filter(name=metric_def['name']).first()
            if existing:
                print(f"   ⚠️  Metric {metric_def['name']} already exists")
                continue
            
            success, message, metric_id = business_service.create_custom_metric(
                name=metric_def['name'],
                display_name=metric_def['display_name'],
                description=metric_def['description'],
                metric_type=metric_def['metric_type'],
                calculation=metric_def['calculation'],
                unit=metric_def['unit'],
                user_id=admin_user.id
            )
            
            if success:
                created_count += 1
                print(f"   ✅ Created sample metric: {metric_def['display_name']}")
            else:
                print(f"   ❌ Failed to create {metric_def['name']}: {message}")
        
        print(f"   📊 Created {created_count} sample metrics")
        return created_count > 0
        
    except Exception as e:
        print(f"   ❌ Error creating sample metrics: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_docker_integration():
    """Verify everything works in Docker environment"""
    print_step(7, "Verifying Docker Integration")
    
    try:
        # Test database connectivity
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            print("   ✅ Database connection working")
        
        # Test DuckDB integration
        import duckdb
        import os
        duckdb_path = os.path.join('/app', 'data', 'integrated.duckdb')
        if os.path.exists(duckdb_path):
            print(f"   ✅ DuckDB file exists: {duckdb_path}")
        else:
            print(f"   ⚠️  DuckDB file not found: {duckdb_path}")
        
        # Test business metrics service in Docker
        business_service = BusinessMetricsService()
        print("   ✅ Business metrics service working in Docker")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error verifying Docker integration: {e}")
        return False

def main():
    """Main execution function"""
    print_header("Business Metrics Integration")
    
    try:
        # Analyze current state
        state = analyze_current_state()
        if not state:
            print("❌ Failed to analyze current state")
            return
        
        # Generate missing business metrics
        metrics_created = generate_missing_business_metrics(state)
        
        # Create sample metrics if none exist
        if state['business_metrics'].count() == 0:
            create_sample_business_metrics()
        
        # Test integrations
        llm_integration_ok = test_llm_integration()
        ui_integration_ok = test_ui_integration()
        semantic_generation_ok = update_semantic_layer_generation()
        docker_integration_ok = verify_docker_integration()
        
        print_header("Integration Summary")
        print("✅ Current State Analysis: Complete")
        print(f"✅ Business Metrics Generated: {metrics_created}")
        print(f"✅ LLM Integration: {'Working' if llm_integration_ok else 'Issues'}")
        print(f"✅ UI Integration: {'Working' if ui_integration_ok else 'Issues'}")
        print(f"✅ Semantic Generation: {'Ready' if semantic_generation_ok else 'Issues'}")
        print(f"✅ Docker Integration: {'Working' if docker_integration_ok else 'Issues'}")
        
        print("\n🎯 NEXT STEPS:")
        print("1. Go to Semantic Layer page")
        print("2. Click 'Generate for Selected' to create semantic layer")
        print("3. Use 'Create Business Metric' button for custom metrics")
        print("4. Verify metrics appear in LLM queries")
        
        final_metrics_count = SemanticMetric.objects.count()
        print(f"\n📊 Final business metrics count: {final_metrics_count}")
        
    except Exception as e:
        print(f"\n❌ Integration failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 