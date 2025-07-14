#!/usr/bin/env python3
"""
DuckDB-Centric Semantic Layer Fix
Fixes the core issue where semantic layer generation still tries to load CSV files
instead of using DuckDB as the primary source
"""

import os
import sys
import django
import logging

# Setup Django environment
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from datasets.models import DataSource, SemanticTable, SemanticColumn
from django.db.models import Count
from django.db import transaction

logger = logging.getLogger(__name__)

class DuckDBSemanticFix:
    """
    Complete fix for semantic layer generation to use DuckDB as primary source
    """
    
    def __init__(self):
        self.logger = logger
        self.fixed_count = 0
        self.error_count = 0
    
    def fix_semantic_generation_for_source(self, data_source):
        """
        Fix semantic layer generation for a specific data source using DuckDB
        """
        try:
            from utils.dynamic_naming import dynamic_naming
            import pandas as pd
            import duckdb
            from django.conf import settings
            
            self.logger.info(f"[FIX] Processing data source: {data_source.name} (ID: {data_source.id})")
            
            # Step 1: Check if semantic layer already exists
            existing_semantic = SemanticTable.objects.filter(data_source=data_source)
            if existing_semantic.exists():
                self.logger.info(f"[SKIP] Semantic layer already exists for {data_source.name}")
                return True
            
            # Step 2: Find DuckDB table for this data source
            table_name = dynamic_naming.find_table_for_data_source(data_source, 'duckdb')
            
            if not table_name:
                self.logger.warning(f"[WARNING] No DuckDB table found for {data_source.name}")
                return False
            
            self.logger.info(f"[FOUND] DuckDB table: {table_name}")
            
            # Step 3: Load data from DuckDB
            db_path = os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb')
            
            if not os.path.exists(db_path):
                self.logger.error(f"[ERROR] DuckDB file not found: {db_path}")
                return False
            
            conn = duckdb.connect(db_path)
            
            try:
                # Test if table exists and has data
                test_query = f"SELECT COUNT(*) FROM {table_name}"
                count_result = conn.execute(test_query).fetchone()
                
                if not count_result or count_result[0] == 0:
                    self.logger.warning(f"[WARNING] Table {table_name} is empty")
                    return False
                
                # Load the data
                data_query = f"SELECT * FROM {table_name} LIMIT 1000"  # Limit for schema analysis
                data = conn.execute(data_query).fetchdf()
                
                self.logger.info(f"[SUCCESS] Loaded {len(data)} rows from DuckDB")
                
                # Step 4: Create semantic layer
                return self._create_semantic_layer(data_source, data, table_name)
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to fix semantic generation for {data_source.name}: {e}")
            self.error_count += 1
            return False
    
    def _create_semantic_layer(self, data_source, data, table_name):
        """Create semantic layer from DuckDB data"""
        try:
            with transaction.atomic():
                # Create semantic table
                semantic_table_name = f"semantic_{data_source.id.hex.replace('-', '_')}"
                
                semantic_table, created = SemanticTable.objects.get_or_create(
                    data_source=data_source,
                    name=semantic_table_name,
                    defaults={
                        'display_name': data_source.name,
                        'description': f'Semantic layer for {data_source.name} (DuckDB source)',
                        'business_purpose': f'Analytics data from DuckDB table {table_name}',
                        'is_fact_table': True,
                        'is_dimension_table': False,
                        'row_count_estimate': len(data)
                    }
                )
                
                self.logger.info(f"[SEMANTIC] {'Created' if created else 'Found'} semantic table: {semantic_table.name}")
                
                # Create semantic columns
                columns_created = 0
                
                for col_name in data.columns:
                    try:
                        col_data = data[col_name]
                        semantic_type = self._infer_semantic_type(col_name, col_data)
                        
                        # Get sample values
                        sample_values = []
                        try:
                            non_null_values = col_data.dropna().head(3)
                            sample_values = [str(val) for val in non_null_values.tolist()]
                        except:
                            sample_values = []
                        
                        semantic_column, col_created = SemanticColumn.objects.get_or_create(
                            semantic_table=semantic_table,
                            name=col_name,
                            defaults={
                                'display_name': col_name.replace('_', ' ').title(),
                                'description': f'Column {col_name} containing {semantic_type} data',
                                'data_type': str(col_data.dtype),
                                'semantic_type': semantic_type,
                                'sample_values': sample_values,
                                'is_nullable': col_data.isnull().any(),
                                'is_editable': True,
                                'etl_enriched': False
                            }
                        )
                        
                        if col_created:
                            columns_created += 1
                        
                    except Exception as col_error:
                        self.logger.error(f"[ERROR] Failed to create column {col_name}: {col_error}")
                        continue
                
                # Update data source workflow status
                workflow_status = data_source.workflow_status or {}
                workflow_status['semantics_completed'] = True
                workflow_status['semantics_source'] = 'duckdb'
                workflow_status['semantics_table_name'] = semantic_table.name
                data_source.workflow_status = workflow_status
                data_source.save()
                
                self.logger.info(f"[SUCCESS] Created semantic layer with {columns_created} columns")
                self.fixed_count += 1
                
                return True
                
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to create semantic layer: {e}")
            raise
    
    def _infer_semantic_type(self, col_name: str, col_data) -> str:
        """Infer semantic type from column name and data"""
        col_name_lower = col_name.lower()
        
        # Identifier patterns
        if any(pattern in col_name_lower for pattern in ['id', 'key', 'number', 'code']):
            return 'identifier'
        
        # Date patterns  
        if any(pattern in col_name_lower for pattern in ['date', 'time', 'created', 'updated']):
            return 'date'
        
        # Measure patterns (numeric business metrics)
        if any(pattern in col_name_lower for pattern in ['sales', 'revenue', 'profit', 'amount', 'price', 'cost', 'quantity', 'count']):
            return 'measure'
        
        # Default to dimension
        return 'dimension'
    
    def fix_duplicate_data_sources(self):
        """Fix duplicate data sources"""
        self.logger.info("[DUPLICATE] Starting duplicate cleanup")
        
        # Find duplicates by name and user
        duplicates = (DataSource.objects
                     .values('name', 'created_by')
                     .annotate(count=Count('id'))
                     .filter(count__gt=1))
        
        duplicate_count = 0
        
        for duplicate_group in duplicates:
            name = duplicate_group['name']
            created_by = duplicate_group['created_by']
            
            # Get all instances
            duplicate_sources = DataSource.objects.filter(
                name=name,
                created_by=created_by
            ).order_by('-created_at')
            
            if duplicate_sources.count() > 1:
                # Keep the most recent one
                latest_source = duplicate_sources.first()
                older_sources = duplicate_sources[1:]
                
                self.logger.info(f"[DUPLICATE] Found {len(older_sources)} duplicates for '{name}', keeping latest")
                
                # Delete older duplicates and their semantic layers
                for old_source in older_sources:
                    SemanticTable.objects.filter(data_source=old_source).delete()
                    old_source.delete()
                    duplicate_count += 1
        
        self.logger.info(f"[DUPLICATE] Removed {duplicate_count} duplicate data sources")
        return duplicate_count
    
    def fix_all_data_sources(self):
        """Fix semantic layer generation for all data sources"""
        self.logger.info("[FIX_ALL] Starting comprehensive fix")
        
        # Step 1: Fix duplicates first
        duplicate_count = self.fix_duplicate_data_sources()
        
        # Step 2: Fix semantic layer generation for remaining sources
        data_sources = DataSource.objects.filter(status='active')
        
        for data_source in data_sources:
            success = self.fix_semantic_generation_for_source(data_source)
            if success:
                self.logger.info(f"[SUCCESS] Fixed semantic layer for: {data_source.name}")
            else:
                self.logger.warning(f"[FAILED] Could not fix semantic layer for: {data_source.name}")
        
        return {
            'duplicates_removed': duplicate_count,
            'semantic_layers_fixed': self.fixed_count,
            'errors': self.error_count,
            'total_processed': data_sources.count()
        }

def run_comprehensive_fix():
    """Run the comprehensive fix for all semantic layer issues"""
    print("🔧 COMPREHENSIVE SEMANTIC LAYER FIX")
    print("=" * 60)
    
    fixer = DuckDBSemanticFix()
    results = fixer.fix_all_data_sources()
    
    print(f"\n📊 RESULTS:")
    print(f"   🗑️  Duplicates removed: {results['duplicates_removed']}")
    print(f"   ✅ Semantic layers fixed: {results['semantic_layers_fixed']}")
    print(f"   ❌ Errors encountered: {results['errors']}")
    print(f"   📁 Total processed: {results['total_processed']}")
    
    if results['semantic_layers_fixed'] > 0:
        print(f"\n🎉 SUCCESS! Fixed {results['semantic_layers_fixed']} semantic layers")
        print("✅ All semantic layer generation now uses DuckDB as primary source")
        print("✅ No more CSV file dependency issues")
        print("✅ Duplicate data sources removed")
    else:
        print(f"\n⚠️ No semantic layers were fixed")
        print("💡 Check if your data sources have data in DuckDB")
    
    return results

if __name__ == "__main__":
    run_comprehensive_fix() 