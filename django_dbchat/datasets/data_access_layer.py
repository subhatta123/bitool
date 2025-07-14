"""
Unified Data Access Layer for ConvaBI
Handles data reading from multiple sources with DuckDB as the central source of truth
"""

import pandas as pd
import numpy as np
import json
import logging
import duckdb
from typing import Tuple, Optional, Dict, Any, List
from django.db import connection
from django.conf import settings
from django.core.files.storage import default_storage
import os

logger = logging.getLogger(__name__)


class UnifiedDataAccessLayer:
    """
    Unified data access that handles data reading from multiple sources
    with DuckDB as the central source of truth
    """
    
    def __init__(self):
        self.duckdb_connection = None
        self.diagnostic_info = {}
        self._ensure_duckdb_connection()
    
    def _ensure_duckdb_connection(self):
        """Ensure DuckDB connection is available"""
        try:
            if not self.duckdb_connection:
                # Use the integrated DuckDB database
                db_path = os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb')
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                self.duckdb_connection = duckdb.connect(db_path)
                logger.info(f"[DUCKDB] Connected to DuckDB at: {db_path}")
        except Exception as e:
            logger.error(f"[ERROR] Failed to connect to DuckDB: {e}")
            self.duckdb_connection = None
    
    def _safe_json_deserialize(self, data):
        """Safely deserialize JSON data back to pandas-compatible format"""
        if isinstance(data, list):
            # Convert list of dicts back to DataFrame
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            return pd.DataFrame()
    
    def get_data_source_data(self, data_source) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """
        Get data for a data source from the best available source
        Priority: DuckDB -> Original CSV file -> Schema-based sample
        
        Args:
            data_source: DataSource model instance
            
        Returns:
            Tuple of (success, dataframe, message)
        """
        try:
            # Initialize diagnostic information
            self.diagnostic_info = {
                'data_source_id': str(data_source.id),
                'data_source_name': data_source.name,
                'source_type': data_source.source_type,
                'attempts': [],
                'failure_reasons': [],
                'connection_info_available': bool(data_source.connection_info),
                'schema_info_available': bool(data_source.schema_info),
            }
            
            logger.info(f"[STARTING] Starting data access for data source: {data_source.name} (ID: {data_source.id})")
            logger.info(f"[INFO] Data source type: {data_source.source_type}")
            
            # Method 1: Try DuckDB integrated storage (preferred)
            logger.info("[ATTEMPT 1] DuckDB integrated storage")
            success, df, message = self._try_duckdb_storage(data_source)
            self.diagnostic_info['attempts'].append({
                'method': 'duckdb_storage',
                'success': success,
                'message': message,
                'rows_found': len(df) if df is not None else 0
            })
            
            if success and df is not None and not df.empty:
                logger.info(f"[SUCCESS] Successfully loaded data from DuckDB: {len(df)} rows")
                return True, df, f"Loaded from DuckDB storage: {message}"
            else:
                logger.warning(f"[FAILED] DuckDB storage failed: {message}")
                self.diagnostic_info['failure_reasons'].append(f"DuckDB storage: {message}")
            
            # Method 2: Try original CSV file (fallback)
            if data_source.source_type == 'csv':
                logger.info("[ATTEMPT 2] Original CSV file")
                success, df, message = self._try_original_csv_file(data_source)
                self.diagnostic_info['attempts'].append({
                    'method': 'original_csv_file',
                    'success': success,
                    'message': message,
                    'rows_found': len(df) if df is not None else 0,
                    'file_path': data_source.connection_info.get('file_path') if data_source.connection_info else None
                })
                
                if success and df is not None and not df.empty:
                    logger.info(f"[SUCCESS] Successfully loaded data from original CSV file: {len(df)} rows")
                    # Store in DuckDB for future access
                    self._store_in_duckdb(data_source, df)
                    return True, df, f"Loaded from original CSV file: {message}"
                else:
                    logger.warning(f"[FAILED] Original CSV file failed: {message}")
                    self.diagnostic_info['failure_reasons'].append(f"Original CSV file: {message}")
            
            # Method 3: Generate sample data from schema (last resort)
            logger.info("[ATTEMPT 3] Schema-based sample data generation")
            success, df, message = self._try_schema_based_sample(data_source)
            self.diagnostic_info['attempts'].append({
                'method': 'schema_based_sample',
                'success': success,
                'message': message,
                'rows_found': len(df) if df is not None else 0,
                'schema_columns': len(data_source.schema_info.get('columns', [])) if data_source.schema_info else 0
            })
            
            if success and df is not None and not df.empty:
                logger.warning(f"[WARNING] Using schema-based sample data: {len(df)} rows")
                return True, df, f"Generated sample data from schema: {message}"
            else:
                logger.error(f"[FAILED] Schema-based sample generation failed: {message}")
                self.diagnostic_info['failure_reasons'].append(f"Schema-based sample: {message}")
            
            # All methods failed
            failure_summary = self._generate_failure_summary()
            logger.error(f"[ERROR] All data access methods failed for {data_source.name}")
            logger.error(f"[SUMMARY] Failure summary: {failure_summary}")
            
            return False, None, failure_summary
            
        except Exception as e:
            logger.error(f"[ERROR] Critical error in unified data access for {data_source.name}: {e}")
            import traceback
            logger.error(f"[TRACEBACK] Full traceback: {traceback.format_exc()}")
            return False, None, f"Critical error accessing data: {str(e)}"
    
    def _try_duckdb_storage(self, data_source) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Try to load data from DuckDB integrated storage"""
        try:
            if not self.duckdb_connection:
                self._ensure_duckdb_connection()
            
            if not self.duckdb_connection:
                return False, None, "DuckDB connection not available"
            
            logger.info(f"[SEARCH] Searching DuckDB for data source: {data_source.name} (ID: {data_source.id})")
            
            # Use SPECIFIC table naming based on data source ID to prevent confusion
            primary_table_name = f"ds_{data_source.id.hex.replace('-', '_')}"
            
            # Try the specific table first (most reliable)
            try:
                logger.info(f"[CHECK] Checking primary DuckDB table: {primary_table_name}")
                
                # Check if table exists using proper DuckDB syntax
                result = self.duckdb_connection.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_name = ?", 
                    [primary_table_name]
                ).fetchone()
                
                if result:
                    # Table exists, try to read data
                    df = self.duckdb_connection.execute(f"SELECT * FROM {primary_table_name} LIMIT 10000").df()
                    
                    if not df.empty:
                        logger.info(f"[SUCCESS] Found data in primary DuckDB table '{primary_table_name}': {len(df)} rows, {len(df.columns)} columns")
                        logger.info(f"[INFO] Columns found: {list(df.columns)[:10]}...")
                        return True, df, f"Loaded {len(df)} rows from DuckDB table {primary_table_name}"
                    else:
                        logger.warning(f"[WARNING] Primary table '{primary_table_name}' exists but is empty")
                
            except Exception as primary_error:
                logger.debug(f"[DEBUG] Primary table check failed: {primary_error}")
            
            # Fallback: Try alternative naming patterns (but be more careful)
            fallback_table_names = [
                f"source_{data_source.id.hex.replace('-', '_')}",
                f"data_{data_source.name.lower().replace(' ', '_').replace('-', '_')}_{data_source.id.hex[:8]}"
            ]
            
            for table_name in fallback_table_names:
                try:
                    logger.info(f"[CHECK] Checking fallback DuckDB table: {table_name}")
                    
                    result = self.duckdb_connection.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_name = ?", 
                        [table_name]
                    ).fetchone()
                    
                    if result:
                        df = self.duckdb_connection.execute(f"SELECT * FROM {table_name} LIMIT 10000").df()
                        
                        if not df.empty:
                            logger.info(f"[SUCCESS] Found data in fallback table '{table_name}': {len(df)} rows, {len(df.columns)} columns")
                            return True, df, f"Loaded {len(df)} rows from DuckDB table {table_name}"
                    
                except Exception as fallback_error:
                    logger.debug(f"[DEBUG] Fallback table '{table_name}' check failed: {fallback_error}")
                    continue
            
            logger.warning(f"[FAILED] No data found for data source {data_source.id} in any DuckDB table")
            return False, None, f"No data found in DuckDB storage for data source {data_source.id}"
            
        except Exception as e:
            logger.error(f"[ERROR] Error accessing DuckDB storage: {e}")
            import traceback
            logger.error(f"[TRACEBACK] Traceback: {traceback.format_exc()}")
            return False, None, f"DuckDB error: {str(e)}"
    
    def _store_in_duckdb(self, data_source, df: pd.DataFrame):
        """Store data in DuckDB with unique table naming to prevent conflicts"""
        try:
            if not self.duckdb_connection:
                self._ensure_duckdb_connection()
            
            if not self.duckdb_connection:
                logger.warning("[WARNING] Cannot store in DuckDB - connection not available")
                return
            
            # Create UNIQUE table name based on data source ID
            table_name = f"ds_{data_source.id.hex.replace('-', '_')}"
            
            logger.info(f"[STORE] Storing data in DuckDB table: {table_name}")
            logger.info(f"[INFO] Data shape: {len(df)} rows, {len(df.columns)} columns")
            logger.info(f"[INFO] Columns: {list(df.columns)[:10]}...")
            
            # Clean up any existing table with the same name
            self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Store data with error handling
            self.duckdb_connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            
            # Verify storage
            verification = self.duckdb_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            if verification and verification[0] == len(df):
                logger.info(f"[SUCCESS] Successfully stored {verification[0]} rows in DuckDB table: {table_name}")
            else:
                logger.warning(f"[WARNING] Row count mismatch during storage verification")
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to store data in DuckDB: {e}")
            import traceback
            logger.error(f"[TRACEBACK] Full traceback: {traceback.format_exc()}")
    
    def clear_duckdb_cache(self, data_source_id=None):
        """Clear DuckDB cache for specific data source or all data"""
        try:
            if not self.duckdb_connection:
                self._ensure_duckdb_connection()
            
            if not self.duckdb_connection:
                logger.warning("[WARNING] Cannot clear cache - DuckDB connection not available")
                return False
            
            if data_source_id:
                # Clear specific data source
                table_name = f"ds_{data_source_id.hex.replace('-', '_')}"
                self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"[SUCCESS] Cleared DuckDB cache for table: {table_name}")
            else:
                # Clear all data source tables
                tables = self.duckdb_connection.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'ds_%' OR table_name LIKE 'source_%'"
                ).fetchall()
                
                for (table_name,) in tables:
                    self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                    logger.info(f"[SUCCESS] Dropped table: {table_name}")
                
                logger.info(f"[SUCCESS] Cleared {len(tables)} tables from DuckDB cache")
            
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to clear DuckDB cache: {e}")
            return False
    
    def check_for_duplicate_data_sources(self, data_source_name: str, user_id: int) -> bool:
        """
        Check if a data source with similar name already exists for the user
        
        Args:
            data_source_name: Name of the data source to check
            user_id: ID of the user
            
        Returns:
            True if duplicate exists, False otherwise
        """
        try:
            from datasets.models import DataSource
            
            # Check for exact name match
            exact_match = DataSource.objects.filter(
                name=data_source_name,
                created_by_id=user_id,
                is_deleted=False
            ).exists()
            
            if exact_match:
                logger.warning(f"[DUPLICATE] Exact duplicate found for data source: {data_source_name}")
                return True
            
            # Check for similar names (fuzzy match)
            similar_sources = DataSource.objects.filter(
                created_by_id=user_id,
                is_deleted=False
            ).values_list('name', flat=True)
            
            normalized_name = data_source_name.lower().replace(' ', '').replace('_', '').replace('-', '')
            
            for existing_name in similar_sources:
                normalized_existing = existing_name.lower().replace(' ', '').replace('_', '').replace('-', '')
                if normalized_name == normalized_existing:
                    logger.warning(f"[DUPLICATE] Similar data source found: '{existing_name}' vs '{data_source_name}'")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"[ERROR] Error checking for duplicates: {e}")
            return False
    
    def _generate_failure_summary(self) -> str:
        """Generate a comprehensive failure summary for debugging"""
        total_attempts = len(self.diagnostic_info['attempts'])
        failed_methods = [attempt['method'] for attempt in self.diagnostic_info['attempts'] if not attempt['success']]
        
        summary = f"No data could be loaded from any source after {total_attempts} attempts. "
        summary += f"Failed methods: {', '.join(failed_methods)}. "
        
        # Add specific guidance based on failure patterns
        if 'original_csv_file' in failed_methods and self.diagnostic_info.get('source_type') == 'csv':
            summary += "CSV file appears to be missing or inaccessible. "
        
        if all('duckdb' in method for method in failed_methods):
            summary += "No DuckDB data found. "
        
        summary += f"Detailed reasons: {'; '.join(self.diagnostic_info['failure_reasons'])}"
        
        return summary
    
    def _try_original_csv_file(self, data_source) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Try to load data from original CSV file"""
        try:
            connection_info = data_source.connection_info or {}
            file_path = connection_info.get('file_path')
            
            logger.info(f"[ATTEMPT] Attempting to load CSV file: {file_path}")
            
            if not file_path:
                logger.warning("[FAILED] No file path in connection info")
                return False, None, "No file path in connection info"
            
            # Try different path resolutions
            potential_paths = [
                os.path.join(settings.MEDIA_ROOT, file_path),
                os.path.join(settings.BASE_DIR, file_path),
                file_path,  # Absolute path
                os.path.join(settings.BASE_DIR, 'media', file_path),
            ]
            
            logger.info(f"[SEARCH] Trying {len(potential_paths)} potential file paths:")
            for i, path in enumerate(potential_paths, 1):
                logger.info(f"  {i}. {path}")
            
            for i, full_path in enumerate(potential_paths, 1):
                if os.path.exists(full_path):
                    logger.info(f"[SUCCESS] Found file at path {i}: {full_path}")
                    
                    delimiter = connection_info.get('delimiter', ',')
                    has_header = connection_info.get('has_header', True)
                    
                    logger.info(f"[INFO] CSV parameters: delimiter='{delimiter}', has_header={has_header}")
                    
                    df = pd.read_csv(
                        full_path, 
                        delimiter=delimiter, 
                        header=0 if has_header else None
                    )
                    
                    if not has_header:
                        df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
                        logger.info(f"[INFO] Generated column names for headerless file: {list(df.columns)}")
                    
                    logger.info(f"[SUCCESS] Successfully loaded CSV: {len(df)} rows, {len(df.columns)} columns")
                    return True, df, f"Loaded {len(df)} rows from {full_path}"
                else:
                    logger.warning(f"[FAILED] File not found at path {i}: {full_path}")
            
            logger.error("[ERROR] CSV file not found at any expected location")
            return False, None, f"CSV file not found at any expected location"
            
        except Exception as e:
            logger.error(f"[ERROR] Error loading original CSV file: {e}")
            import traceback
            logger.error(f"[TRACEBACK] Traceback: {traceback.format_exc()}")
            return False, None, f"CSV file error: {str(e)}"
    
    def _try_schema_based_sample(self, data_source) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Generate sample data based on schema information"""
        try:
            schema_info = data_source.schema_info or {}
            columns_info = schema_info.get('columns', [])
            
            logger.info(f"[ATTEMPT] Attempting schema-based sample generation with {len(columns_info)} columns")
            
            if not columns_info:
                logger.warning("[FAILED] No schema information available")
                return False, None, "No schema information available"
            
            # Generate sample data based on column types
            sample_data = {}
            sample_size = 10  # Generate 10 sample rows
            
            logger.info(f"[INFO] Generating {sample_size} sample rows for {len(columns_info)} columns")
            
            for i, col_info in enumerate(columns_info, 1):
                col_name = col_info.get('name', f'Column_{len(sample_data)}')
                col_type = col_info.get('type', 'string')
                sample_values = col_info.get('sample_values', [])
                
                logger.info(f"  {i}. Column '{col_name}' (type: {col_type}) with {len(sample_values)} sample values")
                
                # Generate appropriate sample data based on type
                if col_type in ['integer', 'int']:
                    if sample_values:
                        # Use actual samples if available
                        sample_data[col_name] = [int(v) if str(v).isdigit() else 1 for v in sample_values[:sample_size]]
                    else:
                        sample_data[col_name] = list(range(1, sample_size + 1))
                        
                elif col_type in ['float', 'decimal']:
                    if sample_values:
                        sample_data[col_name] = [float(v) if str(v).replace('.', '').isdigit() else 1.0 for v in sample_values[:sample_size]]
                    else:
                        sample_data[col_name] = [i * 1.5 for i in range(1, sample_size + 1)]
                        
                elif col_type in ['date', 'datetime']:
                    sample_data[col_name] = ['2024-01-01'] * sample_size
                    
                elif col_type == 'boolean':
                    sample_data[col_name] = [True, False] * (sample_size // 2) + [True] * (sample_size % 2)
                    
                else:  # string or other
                    if sample_values:
                        sample_data[col_name] = sample_values[:sample_size] + ['Sample'] * (sample_size - len(sample_values))
                    else:
                        sample_data[col_name] = [f'Sample_{col_name}_{i}' for i in range(1, sample_size + 1)]
                
                # Ensure all columns have the same length
                if len(sample_data[col_name]) < sample_size:
                    last_value = sample_data[col_name][-1] if sample_data[col_name] else 'Sample'
                    sample_data[col_name].extend([last_value] * (sample_size - len(sample_data[col_name])))
                elif len(sample_data[col_name]) > sample_size:
                    sample_data[col_name] = sample_data[col_name][:sample_size]
            
            if sample_data:
                df = pd.DataFrame(sample_data)
                logger.info(f"[SUCCESS] Generated sample DataFrame: {len(df)} rows, {len(df.columns)} columns")
                logger.warning("[WARNING] This is synthetic sample data - not real data from your source")
                return True, df, f"Generated {len(df)} sample rows from schema"
            
            logger.error("[ERROR] Could not generate sample data from schema")
            return False, None, "Could not generate sample data"
            
        except Exception as e:
            logger.error(f"[ERROR] Error generating schema-based sample: {e}")
            import traceback
            logger.error(f"[TRACEBACK] Traceback: {traceback.format_exc()}")
            return False, None, f"Sample generation error: {str(e)}"
    
    def get_data_source_diagnostics(self, data_source) -> Dict[str, Any]:
        """
        Get detailed diagnostic information about why a data source failed to load
        
        Args:
            data_source: DataSource model instance
            
        Returns:
            Dictionary with detailed diagnostic information
        """
        # First attempt to load data to populate diagnostic info
        success, df, message = self.get_data_source_data(data_source)
        
        diagnostics = {
            'data_source_info': {
                'id': str(data_source.id),
                'name': data_source.name,
                'source_type': data_source.source_type,
                'status': getattr(data_source, 'status', 'unknown'),
                'created_at': str(getattr(data_source, 'created_at', 'unknown')),
            },
            'data_access_results': {
                'success': success,
                'final_message': message,
                'rows_loaded': len(df) if df is not None else 0,
                'columns_loaded': len(df.columns) if df is not None else 0,
            },
            'attempts_made': self.diagnostic_info.get('attempts', []),
            'failure_reasons': self.diagnostic_info.get('failure_reasons', []),
            'configuration_analysis': self._analyze_configuration(data_source),
            'recommendations': self._generate_recommendations(data_source),
        }
        
        return diagnostics
    
    def _analyze_configuration(self, data_source) -> Dict[str, Any]:
        """Analyze data source configuration for potential issues"""
        analysis = {
            'connection_info_present': bool(data_source.connection_info),
            'schema_info_present': bool(data_source.schema_info),
            'table_name_present': bool(getattr(data_source, 'table_name', None)),
            'issues_found': [],
            'configuration_details': {}
        }
        
        # Analyze connection info
        if data_source.connection_info:
            if data_source.source_type == 'csv':
                file_path = data_source.connection_info.get('file_path')
                analysis['configuration_details']['file_path'] = file_path
                if not file_path:
                    analysis['issues_found'].append("Missing file_path in connection_info")
                elif not any(os.path.exists(os.path.join(base, file_path)) for base in [settings.MEDIA_ROOT, settings.BASE_DIR]):
                    analysis['issues_found'].append(f"File not found at path: {file_path}")
        else:
            analysis['issues_found'].append("No connection_info available")
        
        # Analyze schema info
        if data_source.schema_info:
            columns = data_source.schema_info.get('columns', [])
            analysis['configuration_details']['schema_columns_count'] = len(columns)
            if not columns:
                analysis['issues_found'].append("Schema info present but no columns defined")
        else:
            analysis['issues_found'].append("No schema_info available")
        
        return analysis
    
    def _generate_recommendations(self, data_source) -> List[str]:
        """Generate actionable recommendations for fixing data source issues"""
        recommendations = []
        
        # Check data source type specific recommendations
        if data_source.source_type == 'csv':
            if not data_source.connection_info or not data_source.connection_info.get('file_path'):
                recommendations.append("Upload a CSV file and ensure the file path is properly stored in connection_info")
            else:
                recommendations.append("Re-upload the CSV file - the original file may have been moved or deleted")
                recommendations.append("Check that the file path in connection_info is correct")
        
        # Check for DuckDB data
        attempts = self.diagnostic_info.get('attempts', [])
        attempts = attempts if attempts is not None else []
        if not any(attempt['success'] for attempt in attempts if 'duckdb' in attempt.get('method', '')):
            recommendations.append("Run ETL operations to process and store your data in the integrated database")
            recommendations.append("Ensure your data has been successfully imported through the data integration workflow")
        
        # Schema-related recommendations
        if not data_source.schema_info:
            recommendations.append("Generate schema information by re-analyzing your data source")
        
        # General recommendations
        recommendations.extend([
            "Check the data source status and ensure it's marked as 'active'",
            "Verify that the data source hasn't been soft-deleted",
            "Try refreshing the data source connection",
            "Contact support if the issue persists with detailed error information"
        ])
        
        return recommendations
    
    def get_data_source_summary(self, data_source) -> Dict[str, Any]:
        """Get summary information about data source data availability"""
        summary = {
            'data_source_name': data_source.name,
            'source_type': data_source.source_type,
            'methods_tried': [],
            'successful_method': None,
            'row_count': 0,
            'column_count': 0,
            'columns': [],
            'has_data': False
        }
        
        try:
            success, df, message = self.get_data_source_data(data_source)
            
            # Add information about methods tried
            summary['methods_tried'] = [attempt['method'] for attempt in self.diagnostic_info.get('attempts', [])]
            
            if success and df is not None:
                summary.update({
                    'successful_method': message,
                    'row_count': len(df),
                    'column_count': len(df.columns),
                    'columns': list(df.columns),
                    'has_data': True
                })
            else:
                summary['error'] = message
                summary['diagnostic_info'] = self.diagnostic_info
                
        except Exception as e:
            summary['error'] = str(e)
        
        return summary


# Global instance for easy access
unified_data_access = UnifiedDataAccessLayer() 