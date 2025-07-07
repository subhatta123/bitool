#!/usr/bin/env python
"""
Fix Semantic Layer Column Issues
Repairs existing semantic columns with timestamp serialization problems
"""

import os
import sys
import django
import json
import pandas as pd
import logging

# Setup Django
sys.path.append('django_dbchat')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import SemanticColumn, SemanticTable, SemanticMetric
from django.contrib.auth import get_user_model

logger = logging.getLogger(__name__)

def fix_semantic_columns():
    """Fix existing semantic columns with timestamp serialization issues"""
    
    print("🔧 Fixing Semantic Layer Column Issues...")
    print("=" * 50)
    
    # Get all semantic columns
    columns = SemanticColumn.objects.all()
    fixed_count = 0
    error_count = 0
    
    print(f"📊 Found {len(columns)} semantic columns to check")
    
    for column in columns:
        try:
            # Check if sample_values has serialization issues
            if column.sample_values:
                if isinstance(column.sample_values, str):
                    try:
                        # Try to parse existing JSON
                        parsed_values = json.loads(column.sample_values)
                        # If it parses fine, continue
                        continue
                    except json.JSONDecodeError:
                        # If JSON is malformed, reset it
                        column.sample_values = []
                        column.save()
                        fixed_count += 1
                        print(f"   ✅ Fixed malformed JSON for column: {column.name}")
                
                elif isinstance(column.sample_values, list):
                    # Check if any values in the list cause JSON serialization issues
                    try:
                        json.dumps(column.sample_values)
                        # If it serializes fine, continue
                        continue
                    except (TypeError, ValueError) as e:
                        # Fix timestamp serialization issues
                        safe_values = []
                        for val in column.sample_values:
                            if hasattr(val, 'strftime') or 'Timestamp' in str(type(val)):
                                safe_values.append(str(val))
                            elif isinstance(val, (int, float, str, bool, type(None))):
                                safe_values.append(val)
                            else:
                                safe_values.append(str(val))
                        
                        column.sample_values = safe_values
                        column.save()
                        fixed_count += 1
                        print(f"   ✅ Fixed timestamp serialization for column: {column.name}")
            
        except Exception as e:
            print(f"   ❌ Error fixing column {column.name}: {e}")
            error_count += 1
    
    print(f"\n📋 Results:")
    print(f"   ✅ Fixed: {fixed_count} columns")
    print(f"   ❌ Errors: {error_count} columns")
    
    return fixed_count, error_count

def fix_semantic_metrics():
    """Fix semantic metrics with unit field issues"""
    
    print("\n🔧 Fixing Semantic Metrics Unit Issues...")
    print("=" * 50)
    
    # Get all semantic metrics
    metrics = SemanticMetric.objects.all()
    fixed_count = 0
    error_count = 0
    
    print(f"📊 Found {len(metrics)} semantic metrics to check")
    
    for metric in metrics:
        try:
            # Check if unit field is None (this causes NOT NULL constraint failures)
            if metric.unit is None:
                # Infer unit based on metric name/calculation
                metric_name_lower = metric.name.lower()
                
                if any(word in metric_name_lower for word in ['sales', 'revenue', 'price', 'cost', 'amount']):
                    metric.unit = 'USD'
                elif any(word in metric_name_lower for word in ['count', 'quantity', 'number']):
                    metric.unit = 'count'
                elif any(word in metric_name_lower for word in ['percent', 'rate', 'ratio']):
                    metric.unit = '%'
                else:
                    metric.unit = ''  # Empty string instead of None
                
                metric.save()
                fixed_count += 1
                print(f"   ✅ Fixed unit for metric: {metric.name} -> '{metric.unit}'")
            
        except Exception as e:
            print(f"   ❌ Error fixing metric {metric.name}: {e}")
            error_count += 1
    
    print(f"\n📋 Results:")
    print(f"   ✅ Fixed: {fixed_count} metrics")
    print(f"   ❌ Errors: {error_count} metrics")
    
    return fixed_count, error_count

def regenerate_missing_columns():
    """Regenerate the missing date columns for semantic layer"""
    
    print("\n🔧 Regenerating Missing Date Columns...")
    print("=" * 50)
    
    from datasets.models import DataSource
    from services.semantic_service import SemanticService
    from services.integration_service import DataIntegrationService
    
    # Find data sources with ETL completed but missing date columns
    data_sources = DataSource.objects.filter(
        workflow_status__etl_completed=True
    )
    
    regenerated_count = 0
    
    for data_source in data_sources:
        try:
            # Check if semantic table exists
            semantic_tables = SemanticTable.objects.filter(data_source=data_source)
            if not semantic_tables.exists():
                continue
            
            semantic_table = semantic_tables.first()
            
            # Check if Order Date and Ship Date columns exist
            existing_columns = SemanticColumn.objects.filter(
                semantic_table=semantic_table
            ).values_list('name', flat=True)
            
            missing_date_columns = []
            expected_date_columns = ['Order Date', 'Ship Date']
            
            for date_col in expected_date_columns:
                if date_col not in existing_columns:
                    missing_date_columns.append(date_col)
            
            if missing_date_columns:
                print(f"   📊 Data Source: {data_source.name}")
                print(f"      Missing date columns: {missing_date_columns}")
                
                # Regenerate missing columns
                from django.views import View
                from datasets.views import SemanticLayerView
                
                semantic_view = SemanticLayerView()
                semantic_service = SemanticService()
                integration_service = DataIntegrationService()
                
                # Use the enhanced semantic generation method
                result = semantic_view._generate_semantic_for_source(
                    data_source, semantic_service, integration_service
                )
                
                if result['success']:
                    regenerated_count += 1
                    print(f"      ✅ Regenerated semantic layer successfully")
                else:
                    print(f"      ❌ Failed to regenerate: {result.get('error', 'Unknown error')}")
            
        except Exception as e:
            print(f"   ❌ Error processing data source {data_source.name}: {e}")
    
    print(f"\n📋 Regeneration Results:")
    print(f"   ✅ Regenerated: {regenerated_count} data sources")
    
    return regenerated_count

if __name__ == "__main__":
    print("🚀 Semantic Layer Repair Tool")
    print("=" * 60)
    
    # Fix existing columns
    fixed_columns, column_errors = fix_semantic_columns()
    
    # Fix existing metrics
    fixed_metrics, metric_errors = fix_semantic_metrics()
    
    # Regenerate missing columns
    regenerated_sources = regenerate_missing_columns()
    
    print("\n" + "=" * 60)
    print("🎉 Repair Complete!")
    print(f"   📊 Fixed {fixed_columns} columns with serialization issues")
    print(f"   📈 Fixed {fixed_metrics} metrics with unit issues")  
    print(f"   🔄 Regenerated {regenerated_sources} data sources")
    
    total_fixes = fixed_columns + fixed_metrics + regenerated_sources
    total_errors = column_errors + metric_errors
    
    if total_fixes > 0:
        print(f"\n✅ SUCCESS: Applied {total_fixes} fixes")
    
    if total_errors > 0:
        print(f"\n⚠️  WARNING: {total_errors} errors encountered")
    
    print("\n💡 Next Steps:")
    print("1. Check semantic layer - should now show all 21 columns")
    print("2. Verify Order Date and Ship Date are present")
    print("3. Confirm metrics creation works without unit errors")
    print("4. Test date column functionality in queries") 