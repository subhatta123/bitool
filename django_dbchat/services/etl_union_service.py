#!/usr/bin/env python3
"""
ETL Union Service
Handles union operations between data sources without external dependencies
"""

import logging
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from django.utils import timezone
from datasets.models import DataSource, ETLOperation
from datasets.data_access_layer import unified_data_access

logger = logging.getLogger(__name__)

class ETLUnionService:
    """Service for executing union operations on data sources"""
    
    def __init__(self):
        self.duckdb_conn = None
        self._init_connection()
    
    def _init_connection(self):
        """Initialize DuckDB connection"""
        try:
            unified_data_access._ensure_duckdb_connection()
            self.duckdb_conn = unified_data_access.duckdb_connection
            if self.duckdb_conn:
                logger.info("ETL Union Service initialized with DuckDB connection")
            else:
                logger.warning("ETL Union Service initialized without DuckDB connection")
        except Exception as e:
            logger.error(f"Failed to initialize ETL Union Service: {e}")
    
    def execute_union_operation(self, source_ids: List[str], operation_name: str, 
                               union_type: str = 'UNION ALL', user_id: Optional[int] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute union operation between multiple data sources
        
        Args:
            source_ids: List of data source IDs to union
            operation_name: Name for the operation
            union_type: Type of union ('UNION' or 'UNION ALL')
            user_id: User executing the operation
            
        Returns:
            Tuple[success, result_info]
        """
        try:
            # Validate inputs
            if len(source_ids) < 2:
                return False, {'error': 'Union operation requires at least 2 data sources'}
            
            if union_type not in ['UNION', 'UNION ALL']:
                union_type = 'UNION ALL'
            
            logger.info(f"Starting union operation: {operation_name} with {len(source_ids)} sources")
            
            # Get and validate data sources
            sources = []
            for source_id in source_ids:
                try:
                    source = DataSource.objects.get(id=source_id, status='active')
                    sources.append(source)
                except DataSource.DoesNotExist:
                    return False, {'error': f'Data source {source_id} not found or inactive'}
            
            # Load data from all sources
            dataframes = []
            source_info = []
            
            for source in sources:
                success, df, message = unified_data_access.get_data_source_data(source)
                if not success or df is None or df.empty:
                    return False, {'error': f'Failed to load data from {source.name}: {message}'}
                
                dataframes.append(df)
                source_info.append({
                    'name': source.name,
                    'id': str(source.id),
                    'rows': len(df),
                    'columns': list(df.columns)
                })
                logger.info(f"Loaded data from {source.name}: {len(df)} rows, {len(df.columns)} columns")
            
            # Perform schema alignment
            aligned_dataframes, schema_alignment = self._align_schemas(dataframes, source_info)
            
            # Execute union
            union_result = self._execute_union(aligned_dataframes, union_type)
            
            if union_result is None or union_result.empty:
                return False, {'error': 'Union operation produced no results'}
            
            # Generate output table name
            output_table_name = f"etl_union_{timezone.now().strftime('%Y%m%d_%H%M%S')}_" + \
                               "_".join([f"ds_{source.id.hex[:5]}" for source in sources])
            
            # Store result in DuckDB
            storage_success = self._store_union_result(union_result, output_table_name)
            if not storage_success:
                return False, {'error': 'Failed to store union results'}
            
            # Create ETL operation record
            etl_operation = ETLOperation.objects.create(
                name=operation_name,
                operation_type='union',
                source_tables=[str(source.id) for source in sources],
                parameters={
                    'union_type': union_type,
                    'source_ids': source_ids,
                    'schema_alignment': schema_alignment
                },
                output_table_name=output_table_name,
                status='completed',
                created_by_id=user_id or 1,
                row_count=len(union_result),
                result_summary={
                    'sources_count': len(sources),
                    'total_rows': len(union_result),
                    'total_columns': len(union_result.columns),
                    'union_type': union_type,
                    'schema_alignment_applied': bool(schema_alignment),
                    'source_breakdown': source_info
                },
                data_lineage={
                    'operation_type': 'union',
                    'source_data_sources': [str(source.id) for source in sources],
                    'union_type': union_type,
                    'created_at': timezone.now().isoformat()
                }
            )
            
            logger.info(f"Union operation completed successfully: {output_table_name} with {len(union_result)} rows")
            
            return True, {
                'operation_id': str(etl_operation.id),
                'operation_name': operation_name,
                'output_table': output_table_name,
                'row_count': len(union_result),
                'column_count': len(union_result.columns),
                'sources': [source.name for source in sources],
                'union_type': union_type,
                'schema_alignment': schema_alignment
            }
            
        except Exception as e:
            logger.error(f"Union operation failed: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False, {'error': str(e)}
    
    def _align_schemas(self, dataframes: List[pd.DataFrame], source_info: List[Dict]) -> Tuple[List[pd.DataFrame], Dict[str, Any]]:
        """
        Align schemas across multiple dataframes for union compatibility
        
        Returns:
            Tuple[aligned_dataframes, alignment_info]
        """
        if not dataframes:
            return [], {}
        
        # Get all unique columns across all dataframes
        all_columns = set()
        for df in dataframes:
            all_columns.update(df.columns)
        
        all_columns = sorted(list(all_columns))
        
        # Align each dataframe
        aligned_dataframes = []
        alignment_details = []
        
        for i, df in enumerate(dataframes):
            aligned_df = df.copy()
            source_alignment = {
                'source_name': source_info[i]['name'],
                'original_columns': list(df.columns),
                'added_columns': [],
                'column_count_before': len(df.columns),
                'column_count_after': 0
            }
            
            # Add missing columns with None values
            for col in all_columns:
                if col not in aligned_df.columns:
                    aligned_df[col] = None
                    source_alignment['added_columns'].append(col)
            
            # Reorder columns to match
            aligned_df = aligned_df[all_columns]
            source_alignment['column_count_after'] = len(aligned_df.columns)
            
            aligned_dataframes.append(aligned_df)
            alignment_details.append(source_alignment)
            
            logger.info(f"Aligned schema for {source_info[i]['name']}: {len(df.columns)} -> {len(aligned_df.columns)} columns")
        
        schema_alignment = {
            'total_unique_columns': len(all_columns),
            'alignment_required': any(details['added_columns'] for details in alignment_details),
            'alignment_details': alignment_details,
            'final_column_order': all_columns
        }
        
        return aligned_dataframes, schema_alignment
    
    def _execute_union(self, dataframes: List[pd.DataFrame], union_type: str) -> Optional[pd.DataFrame]:
        """
        Execute the actual union operation
        """
        try:
            if not dataframes:
                return None
            
            if len(dataframes) == 1:
                return dataframes[0]
            
            # Start with first dataframe
            result = dataframes[0].copy()
            
            # Union with remaining dataframes
            for df in dataframes[1:]:
                if union_type == 'UNION ALL':
                    # UNION ALL - simply concatenate
                    result = pd.concat([result, df], ignore_index=True)
                else:
                    # UNION - concatenate and remove duplicates
                    result = pd.concat([result, df], ignore_index=True).drop_duplicates()
            
            logger.info(f"Union completed: {len(result)} rows in result")
            return result
            
        except Exception as e:
            logger.error(f"Error executing union: {e}")
            return None
    
    def _store_union_result(self, result_df: pd.DataFrame, table_name: str) -> bool:
        """
        Store union result in DuckDB
        """
        try:
            if not self.duckdb_conn:
                logger.error("No DuckDB connection available")
                return False
            
            # Register DataFrame temporarily
            temp_name = f"temp_{table_name}"
            self.duckdb_conn.register(temp_name, result_df)
            
            # Create permanent table
            self.duckdb_conn.execute(f"""
                CREATE TABLE "{table_name}" AS 
                SELECT * FROM {temp_name}
            """)
            
            # Unregister temporary DataFrame
            self.duckdb_conn.unregister(temp_name)
            
            logger.info(f"Stored union result in table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing union result: {e}")
            return False

# Global instance
etl_union_service = ETLUnionService() 