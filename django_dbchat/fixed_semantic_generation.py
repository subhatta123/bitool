#!/usr/bin/env python3
"""
Fixed Semantic Layer Generation - DuckDB-Centric Approach
Completely refactored to use DuckDB as primary source instead of CSV files
Fixes duplicate data sources and semantic layer generation issues
"""

import os
import logging
from typing import Dict, Any, Optional, Tuple
from django.http import JsonResponse
from django.views import View
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.db import transaction

logger = logging.getLogger(__name__)

class FixedSemanticLayerGenerator:
    """
    Fixed semantic layer generation that uses DuckDB as the primary source
    Eliminates CSV file dependency and fixes duplicate issues
    """
    
    def __init__(self):
        self.logger = logger
    
    def generate_semantic_for_source(self, data_source, semantic_service=None, integration_service=None):
        """
        Generate semantic layer for data source using DuckDB as primary source
        COMPLETELY REFACTORED: No longer tries to load CSV files - uses DuckDB directly
        """
        try:
            from utils.dynamic_naming import dynamic_naming
            from datasets.models import SemanticTable, SemanticColumn, SemanticMetric
            from datasets.data_access_layer import unified_data_access
            import pandas as pd
            
            self.logger.info(f"[SEMANTIC] Starting semantic generation for: {data_source.name} (ID: {data_source.id})")
            
            # Step 1: Check for existing semantic layer (prevent duplicates)
            existing_semantic_tables = SemanticTable.objects.filter(data_source=data_source)
            if existing_semantic_tables.exists():
                self.logger.info(f"[SEMANTIC] Semantic layer already exists for {data_source.name}")
                
                # Count existing objects
                table_count = existing_semantic_tables.count()
                column_count = SemanticColumn.objects.filter(
                    semantic_table__data_source=data_source
                ).count()
                
                return {
                    'success': True,
                    'message': f'Semantic layer already exists! Found {table_count} tables and {column_count} columns.',
                    'tables': [{'name': table.name, 'display_name': table.display_name} for table in existing_semantic_tables],
                    'columns_created': column_count,
                    'metrics': [],
                    'already_existed': True
                }
            
            # Step 2: Load data from DuckDB (NOT from CSV files)
            self.logger.info(f"[DUCKDB] Loading data from DuckDB for {data_source.name}")
            
            data = None
            table_name = None
            
            # Method 1: Use dynamic naming to find the table
            try:
                table_name = dynamic_naming.find_table_for_data_source(data_source, 'duckdb')
                
                if table_name:
                    self.logger.info(f"[FOUND] Found DuckDB table: {table_name}")
                    
                    # Load data from DuckDB using unified data access layer
                    try:
                        success, df, message = unified_data_access.get_data_source_data(data_source)
                        
                        if success and df is not None and not df.empty:
                            data = df
                            self.logger.info(f"[SUCCESS] Loaded {len(data)} rows from DuckDB via unified access")
                        else:
                            raise Exception(f"No data returned from unified access: {message}")
                            
                    except Exception as unified_error:
                        self.logger.warning(f"[FALLBACK] Unified access failed: {unified_error}, trying direct DuckDB")
                        
                        # Fallback: Direct DuckDB connection
                        import duckdb
                        from django.conf import settings
                        
                        db_path = os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb')
                        
                        if os.path.exists(db_path):
                            conn = duckdb.connect(db_path)
                            
                            try:
                                # Test if table exists
                                test_query = f"SELECT COUNT(*) FROM {table_name}"
                                count_result = conn.execute(test_query).fetchone()
                                
                                if count_result and count_result[0] > 0:
                                    # Load the data
                                    data_query = f"SELECT * FROM {table_name}"
                                    data = conn.execute(data_query).fetchdf()
                                    self.logger.info(f"[SUCCESS] Loaded {len(data)} rows directly from DuckDB")
                                else:
                                    self.logger.warning(f"[WARNING] Table {table_name} exists but is empty")
                                    
                            except Exception as direct_error:
                                self.logger.error(f"[ERROR] Direct DuckDB access failed: {direct_error}")
                            finally:
                                conn.close()
                        else:
                            self.logger.error(f"[ERROR] DuckDB file not found: {db_path}")
                
                else:
                    self.logger.warning(f"[WARNING] No DuckDB table found for {data_source.name}")
                    
            except Exception as duckdb_error:
                self.logger.error(f"[ERROR] DuckDB data loading failed: {duckdb_error}")
            
            # Step 3: Fallback to schema-based approach if no data
            if data is None or data.empty:
                self.logger.info(f"[FALLBACK] Trying schema-based approach")
                
                try:
                    # Try to create semantic layer from stored schema info
                    schema_info = data_source.schema_info
                    
                    if schema_info and 'columns' in schema_info:
                        self.logger.info(f"[SCHEMA] Creating semantic layer from schema info")
                        return self._create_semantic_from_schema(data_source, schema_info)
                    else:
                        self.logger.warning(f"[SCHEMA] No schema info available")
                        
                except Exception as schema_error:
                    self.logger.error(f"[SCHEMA] Schema-based creation failed: {schema_error}")
            
            # Step 4: Final validation
            if data is None or data.empty:
                error_msg = f"No data available for semantic generation. DuckDB table: {table_name or 'not found'}"
                self.logger.error(f"[FAILED] {error_msg}")
                return {
                    'success': False, 
                    'error': error_msg,
                    'details': 'Data should be available in DuckDB after successful upload. Try re-uploading your data.',
                    'suggested_action': 'Please re-upload your CSV file to ensure data is properly stored in DuckDB'
                }
            
            self.logger.info(f"[DATA] Processing {len(data)} rows, {len(data.columns)} columns")
            self.logger.info(f"[COLUMNS] {list(data.columns)[:10]}...")  # Show first 10 columns
            
            # Step 5: Create semantic layer with transaction safety
            return self._create_semantic_from_data(data_source, data, table_name)
                
        except Exception as e:
            self.logger.error(f"[ERROR] Error generating semantic layer for {data_source.name}: {e}")
            import traceback
            self.logger.error(f"[TRACEBACK] {traceback.format_exc()}")
            
            return {
                'success': False, 
                'error': f'Error generating semantic layer: {str(e)}',
                'details': 'Check logs for detailed error information'
            }
    
    def _create_semantic_from_data(self, data_source, data, table_name=None):
        """Create semantic layer from actual data DataFrame"""
        from datasets.models import SemanticTable, SemanticColumn
        from django.db import transaction
        
        try:
            with transaction.atomic():
                # Create unique semantic table name
                semantic_table_name = f"semantic_{data_source.id.hex.replace('-', '_')}"
                
                semantic_table, created = SemanticTable.objects.get_or_create(
                    data_source=data_source,
                    name=semantic_table_name,
                    defaults={
                        'display_name': data_source.name,
                        'description': f'Semantic layer for {data_source.name}',
                        'business_purpose': f'Business analytics data from {data_source.source_type} source',
                        'is_fact_table': True,
                        'is_dimension_table': False,
                        'row_count_estimate': len(data)
                    }
                )
                
                self.logger.info(f"[SEMANTIC_TABLE] {'Created' if created else 'Found'} semantic table: {semantic_table.name}")
                
                # Create semantic columns based on actual data
                columns_created = 0
                
                for col_name in data.columns:
                    try:
                        # Analyze column data for semantic type
                        col_data = data[col_name]
                        
                        # Infer semantic type based on column name and data
                        semantic_type = self._infer_semantic_type(col_name, col_data)
                        
                        # Get sample values (non-null)
                        sample_values = []
                        try:
                            non_null_values = col_data.dropna().head(5)
                            sample_values = [str(val) for val in non_null_values.tolist()]
                        except:
                            sample_values = []
                        
                        # Create semantic column with duplicate prevention
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
                            self.logger.debug(f"[COLUMN] Created semantic column: {col_name} ({semantic_type})")
                        
                    except Exception as col_error:
                        self.logger.error(f"[ERROR] Failed to create semantic column {col_name}: {col_error}")
                        continue
                
                # Update workflow status
                workflow_status = data_source.workflow_status or {}
                workflow_status['semantics_completed'] = True
                workflow_status['semantics_table_name'] = semantic_table.name
                data_source.workflow_status = workflow_status
                data_source.save()
                
                self.logger.info(f"[SUCCESS] Semantic layer created: {columns_created} columns")
                
                return {
                    'success': True,
                    'message': f'Semantic layer generated successfully! Created {columns_created} columns.',
                    'tables': [{'name': semantic_table.name, 'display_name': semantic_table.display_name}],
                    'columns_created': columns_created,
                    'metrics': [],
                    'data_source_id': str(data_source.id),
                    'table_name': table_name
                }
                
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to create semantic layer from data: {e}")
            raise
    
    def _create_semantic_from_schema(self, data_source, schema_info):
        """Create semantic layer from schema information when data is not available"""
        from datasets.models import SemanticTable, SemanticColumn
        from django.db import transaction
        
        try:
            with transaction.atomic():
                semantic_table_name = f"semantic_{data_source.id.hex.replace('-', '_')}"
                
                semantic_table, created = SemanticTable.objects.get_or_create(
                    data_source=data_source,
                    name=semantic_table_name,
                    defaults={
                        'display_name': data_source.name,
                        'description': f'Semantic layer for {data_source.name} (schema-based)',
                        'business_purpose': f'Business analytics schema from {data_source.source_type} source',
                        'is_fact_table': True,
                        'is_dimension_table': False,
                        'row_count_estimate': 0
                    }
                )
                
                columns_created = 0
                columns_data = schema_info.get('columns', [])
                
                if isinstance(columns_data, list):
                    for col_info in columns_data:
                        if isinstance(col_info, dict) and 'name' in col_info:
                            col_name = col_info['name']
                            
                            semantic_column, col_created = SemanticColumn.objects.get_or_create(
                                semantic_table=semantic_table,
                                name=col_name,
                                defaults={
                                    'display_name': col_name.replace('_', ' ').title(),
                                    'description': f'Column {col_name} from schema',
                                    'data_type': col_info.get('type', 'string'),
                                    'semantic_type': 'dimension',
                                    'sample_values': col_info.get('sample_values', []),
                                    'is_nullable': True,
                                    'is_editable': True,
                                    'etl_enriched': False
                                }
                            )
                            
                            if col_created:
                                columns_created += 1
                
                return {
                    'success': True,
                    'message': f'Semantic layer generated from schema! Created {columns_created} columns.',
                    'tables': [{'name': semantic_table.name, 'display_name': semantic_table.display_name}],
                    'columns_created': columns_created,
                    'metrics': [],
                    'schema_based': True
                }
                
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to create semantic layer from schema: {e}")
            raise
    
    def _infer_semantic_type(self, col_name: str, col_data=None) -> str:
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

def fix_duplicate_data_sources():
    """Fix duplicate data sources to prevent confusion"""
    from datasets.models import DataSource
    from django.db.models import Count
    
    logger.info("[DUPLICATE_FIX] Starting duplicate data source cleanup")
    
    # Find duplicate data sources by name and user
    duplicates = (DataSource.objects
                 .values('name', 'created_by')
                 .annotate(count=Count('id'))
                 .filter(count__gt=1))
    
    fixed_count = 0
    
    for duplicate_group in duplicates:
        name = duplicate_group['name']
        created_by = duplicate_group['created_by']
        
        # Get all instances of this duplicate
        duplicate_sources = DataSource.objects.filter(
            name=name,
            created_by=created_by
        ).order_by('-created_at')
        
        if duplicate_sources.count() > 1:
            # Keep the most recent one
            latest_source = duplicate_sources.first()
            older_sources = duplicate_sources[1:]
            
            logger.info(f"[DUPLICATE_FIX] Found {len(older_sources)} duplicates for '{name}', keeping latest: {latest_source.id}")
            
            # Delete older duplicates
            for old_source in older_sources:
                # Clean up related semantic objects
                from datasets.models import SemanticTable
                SemanticTable.objects.filter(data_source=old_source).delete()
                
                old_source.delete()
                fixed_count += 1
                logger.info(f"[DUPLICATE_FIX] Deleted duplicate: {old_source.id}")
    
    logger.info(f"[DUPLICATE_FIX] Fixed {fixed_count} duplicate data sources")
    return fixed_count

# Global instance for easy access
fixed_semantic_generator = FixedSemanticLayerGenerator()

if __name__ == "__main__":
    print("🔧 FIXED SEMANTIC LAYER GENERATION")
    print("=" * 50)
    print("✅ DuckDB-centric semantic layer generation")
    print("✅ No more CSV file dependencies")
    print("✅ Duplicate prevention")
    print("✅ Robust error handling")
    print("✅ Schema-based fallback")
    print("\nUse: from fixed_semantic_generation import fixed_semantic_generator") 