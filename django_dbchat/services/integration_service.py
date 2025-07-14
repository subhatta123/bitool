"""
Data Integration Service for ConvaBI Application
Enhanced with DuckDB, security, and proper error handling
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import re
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import logging
from celery import shared_task
from django.db import transaction
from django.core.cache import cache
from django.conf import settings
from datasets.models import DataSource, ETLOperation, DataIntegrationJob
from utils.type_helpers import (
    map_pandas_dtype_to_standard,
    get_column_type_info,
    ensure_no_object_types,
    validate_semantic_data_type
)
from utils.data_contracts import DataType
from utils.table_name_helper import validate_table_name, TableNameManager

logger = logging.getLogger(__name__)

@dataclass
class DataRelationship:
    """Represents a detected relationship between data sources"""
    source1_id: str
    source1_table: str
    source1_column: str
    source2_id: str
    source2_table: str
    source2_column: str
    relationship_type: str
    confidence_score: float
    suggested_join_type: str

class DataIntegrationService:
    """Enhanced data integration service with DuckDB and security features"""
    
    def __init__(self):
        self.integrated_db: Optional[Any] = None
        self._init_integrated_database()
    
    def _init_integrated_database(self):
        """Initialize DuckDB database for better performance and persistence with enhanced logging"""
        try:
            import duckdb
            from django.conf import settings
            
            # Use settings for consistent database path
            db_path = getattr(settings, 'INTEGRATED_DB_PATH', getattr(settings, 'DUCKDB_PATH', ':memory:'))
            
            # Log connection details for debugging
            if db_path == ':memory:':
                logger.warning("Using in-memory DuckDB database - data will not persist between restarts")
            else:
                logger.info(f"Using persistent DuckDB database at: {db_path}")
                # Ensure directory exists
                import os
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            self.integrated_db = duckdb.connect(db_path)
            logger.info(f"DuckDB integrated database initialized successfully at: {db_path}")
            
            # Log current connection status for debugging
            try:
                version_result = self.integrated_db.execute("SELECT version()").fetchone()
                logger.info(f"DuckDB version: {version_result[0] if version_result else 'Unknown'}")
            except Exception as version_error:
                logger.warning(f"Could not retrieve DuckDB version: {version_error}")
                
        except ImportError:
            logger.warning("DuckDB not available, falling back to SQLite")
            try:
                self.integrated_db = sqlite3.connect(':memory:', check_same_thread=False)
                logger.info("SQLite integrated database initialized")
            except Exception as e:
                logger.error(f"Failed to initialize integrated database: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB: {e}")
            # Fallback to SQLite
            try:
                self.integrated_db = sqlite3.connect(':memory:', check_same_thread=False)
                logger.info("Fallback SQLite integrated database initialized")
            except Exception as fallback_e:
                logger.error(f"Failed to initialize fallback database: {fallback_e}")
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the integrated database before attempting to query it
        """
        try:
            if not self.integrated_db:
                logger.error("Integrated database not connected")
                return False
            
            return TableNameManager.check_table_exists(self.integrated_db, table_name)
            
        except Exception as e:
            logger.error(f"Error checking table existence for '{table_name}': {e}")
            return False
    
    def process_existing_data_source(self, data_source, data=None):
        """
        Process an existing DataSource object without creating duplicates
        This method works with an already-created DataSource object
        """
        try:
            source_id = str(data_source.id)
            
            if data is not None:
                # Process the data without creating new DataSource
                data, column_mapping = self._clean_column_names(data)
                schema = self._analyze_schema(data, data_source.source_type, 
                                            data_source.connection_info, column_mapping)
                
                # Update existing DataSource with processed schema
                data_source.schema_info = schema
                data_source.table_name = self._get_safe_table_name(source_id)
                data_source.save()
                
                # Load data to integrated DB
                success = self._load_cleaned_data_to_integrated_db(source_id, data)
                if success:
                    # Update workflow status
                    workflow_status = data_source.workflow_status or {}
                    workflow_status['etl_completed'] = True
                    data_source.workflow_status = workflow_status
                    data_source.save()
                    
                    # Detect relationships with existing sources
                    self._detect_relationships(source_id)
                
                return success
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process existing data source {data_source.id}: {e}")
            return False

    def add_data_source(self, name: str, source_type: str, 
                       connection_info: Dict, data: Optional[pd.DataFrame] = None,
                       user_id: Optional[int] = None) -> Optional[str]:
        """
        Add data source with duplicate prevention using get_or_create
        FIXED: Use get_or_create to prevent duplicate DataSource creation
        """
        try:
            with transaction.atomic():
                # Use get_or_create to prevent duplicates
                data_source, created = DataSource.objects.get_or_create(
                    name=name,
                    created_by_id=user_id or 1,
                    defaults={
                        'source_type': source_type,
                        'connection_info': connection_info,
                        'status': 'active',
                        'workflow_status': {
                            'data_loaded': True,
                            'etl_completed': False,
                            'semantics_completed': False,
                            'query_enabled': False,
                            'dashboard_enabled': False
                        }
                    }
                )
                
                if not created:
                    # DataSource already exists, just update if needed
                    logger.info(f"Data source '{name}' already exists for user {user_id}")
                    if data is not None:
                        # Process the data for existing source
                        success = self.process_existing_data_source(data_source, data)
                        return str(data_source.id) if success else None
                    return str(data_source.id)
                
                # New data source created, process it
                source_id = str(data_source.id)
                
                # Process data if provided
                if data is not None:
                    success = self.process_existing_data_source(data_source, data)
                    if not success:
                        # Clean up the created data source on failure
                        data_source.delete()
                        return None
                
                logger.info(f"Added data source: {name} ({source_id})")
                return source_id
                
        except Exception as e:
            logger.error(f"Failed to add data source {name}: {e}")
            return None
    
    def remove_data_source(self, source_id: str) -> bool:
        """Remove a data source with proper cleanup and transaction support"""
        try:
            with transaction.atomic():
                # Remove from database
                data_source = DataSource.objects.filter(id=source_id).first()
                if data_source:
                    data_source.delete()
                
                # Remove from integrated database with error handling
                if self.integrated_db:
                    table_name = self._get_safe_table_name(source_id)
                    try:
                        if hasattr(self.integrated_db, 'execute'):
                            # DuckDB syntax
                            self.integrated_db.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")
                        else:
                            # SQLite syntax
                            cursor = self.integrated_db.cursor()
                            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                            self.integrated_db.commit()
                    except Exception as db_error:
                        logger.warning(f"Failed to drop table {table_name}: {db_error}")
                
                # Remove related ETL operations
                ETLOperation.objects.filter(
                    source_tables__contains=f"source_{source_id}"
                ).delete()
                
                logger.info(f"Removed data source: {source_id}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to remove data source {source_id}: {e}")
            return False
    
    def _clean_column_names(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """Clean column names to be SQL-safe with enhanced validation"""
        cleaned_data = data.copy()
        original_columns = cleaned_data.columns.tolist()
        cleaned_columns = []
        column_counter = {}
        
        for col in original_columns:
            # Clean the column name
            cleaned_col = re.sub(r'[^a-zA-Z0-9_]', '_', str(col))
            
            # Ensure it starts with a letter or underscore
            if cleaned_col and cleaned_col[0].isdigit():
                cleaned_col = f"col_{cleaned_col}"
            
            # Handle empty or invalid names
            if not cleaned_col or cleaned_col == '_' or cleaned_col == '':
                cleaned_col = f"column_{len(cleaned_columns)}"
            
            # Handle duplicates
            if cleaned_col in column_counter:
                column_counter[cleaned_col] += 1
                cleaned_col = f"{cleaned_col}_{column_counter[cleaned_col]}"
            else:
                column_counter[cleaned_col] = 0
            
            cleaned_columns.append(cleaned_col)
        
        column_mapping = dict(zip(original_columns, cleaned_columns))
        if cleaned_columns != original_columns:
            cleaned_data.rename(columns=column_mapping, inplace=True)
            logger.info(f"Cleaned {len(column_mapping)} column names for SQL safety")
        
        return cleaned_data, column_mapping
    
    def _get_safe_table_name(self, source_id: str) -> str:
        """Get safe table name for DuckDB - centralized through table name helper"""
        from utils.table_name_helper import generate_safe_table_name
        return generate_safe_table_name(source_id)
    
    def _load_cleaned_data_to_integrated_db(self, source_id: str, data: pd.DataFrame) -> bool:
        """Load cleaned data into integrated database with proper type conversions"""
        if not self.integrated_db:
            logger.error("Integrated database not connected.")
            return False
        
        try:
            # ENHANCED: Apply ETL type conversions before loading
            converted_data, transformation_log = self._apply_etl_type_conversions(data, source_id)
            
            # Log transformations applied
            if transformation_log:
                logger.info(f"Applied {len(transformation_log)} type conversions for source {source_id}")
                for col, conversion in transformation_log.items():
                    logger.info(f"  {col}: {conversion['from']} -> {conversion['to']}")
            
            # Use safe table name without hyphens
            table_name = self._get_safe_table_name(source_id)
            
            if hasattr(self.integrated_db, 'register'):
                # DuckDB approach - use safe names throughout
                safe_df_name = f"df_{str(source_id).replace('-', '_')}"
                self.integrated_db.register(safe_df_name, converted_data)
                self.integrated_db.execute(f"""
                    CREATE TABLE "{table_name}" AS 
                    SELECT * FROM {safe_df_name}
                """)
                self.integrated_db.unregister(safe_df_name)
                
                # Store transformation metadata in DuckDB for future reference
                self._store_transformation_metadata(source_id, table_name, transformation_log)
            else:
                # SQLite approach
                converted_data.to_sql(table_name, self.integrated_db, if_exists='replace', index=False)
            
            logger.info(f"Loaded {len(converted_data)} rows into table {table_name} with proper type conversions")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data for source {source_id}: {e}")
            return False
    
    def _apply_etl_type_conversions(self, data: pd.DataFrame, source_id: str) -> Tuple[pd.DataFrame, Dict[str, Dict[str, str]]]:
        """
        Apply ETL type conversions based on detected data types
        CRITICAL: Convert date strings to proper datetime objects for DuckDB DATE storage
        """
        converted_data = data.copy()
        transformation_log = {}
        
        try:
            # Analyze each column for potential type conversions
            for col in data.columns:
                original_dtype = str(data[col].dtype)
                
                # Check if this looks like a date column
                if self._is_date_column(col, data[col]):
                    try:
                        # Attempt date conversion with multiple format attempts
                        converted_series, date_format = self._convert_to_date(data[col])
                        if converted_series is not None:
                            converted_data[col] = converted_series
                            transformation_log[col] = {
                                'from': original_dtype,
                                'to': 'datetime64[ns]',
                                'format_detected': date_format,
                                'transformation_type': 'date_conversion',
                                'etl_enriched': True
                            }
                            logger.info(f"Converted {col} from {original_dtype} to datetime using format: {date_format}")
                        else:
                            logger.warning(f"Could not convert {col} to date - keeping as {original_dtype}")
                    except Exception as date_error:
                        logger.warning(f"Date conversion failed for {col}: {date_error}")
                
                # Check for numeric conversions
                elif self._is_numeric_column(col, data[col]) and original_dtype == 'object':
                    try:
                        # Attempt numeric conversion
                        converted_series = pd.to_numeric(data[col], errors='coerce')
                        # Only apply if we don't lose too much data
                        non_null_original = data[col].notna().sum()
                        non_null_converted = converted_series.notna().sum()
                        if non_null_converted >= non_null_original * 0.9:  # 90% conversion success
                            converted_data[col] = converted_series
                            final_dtype = str(converted_series.dtype)
                            transformation_log[col] = {
                                'from': original_dtype,
                                'to': final_dtype,
                                'transformation_type': 'numeric_conversion',
                                'etl_enriched': True
                            }
                            logger.info(f"Converted {col} from {original_dtype} to {final_dtype}")
                    except Exception as num_error:
                        logger.warning(f"Numeric conversion failed for {col}: {num_error}")
                
                # Check for boolean conversions
                elif self._is_boolean_column(col, data[col]):
                    try:
                        converted_series = self._convert_to_boolean(data[col])
                        if converted_series is not None:
                            converted_data[col] = converted_series
                            transformation_log[col] = {
                                'from': original_dtype,
                                'to': 'bool',
                                'transformation_type': 'boolean_conversion',
                                'etl_enriched': True
                            }
                            logger.info(f"Converted {col} from {original_dtype} to bool")
                    except Exception as bool_error:
                        logger.warning(f"Boolean conversion failed for {col}: {bool_error}")
            
            return converted_data, transformation_log
            
        except Exception as e:
            logger.error(f"Error during ETL type conversions: {e}")
            return data, {}
    
    def _is_date_column(self, col_name: str, series: pd.Series) -> bool:
        """
        Detect if a column should be treated as a date column
        """
        col_name_lower = col_name.lower()
        
        # Check column name patterns
        date_patterns = ['date', 'time', 'timestamp', 'created', 'updated', 'modified', 'ship', 'order']
        name_indicates_date = any(pattern in col_name_lower for pattern in date_patterns)
        
        if not name_indicates_date:
            return False
        
        # Check if current type is object (string-like)
        if series.dtype != 'object':
            return False
        
        # Check sample values for date-like patterns
        try:
            sample_values = series.dropna().head(10)
            if len(sample_values) == 0:
                return False
            
            date_like_count = 0
            for value in sample_values:
                if self._looks_like_date(str(value)):
                    date_like_count += 1
            
            # If 60% or more look like dates, consider it a date column
            return date_like_count >= len(sample_values) * 0.6
            
        except Exception:
            return False
    
    def _looks_like_date(self, value: str) -> bool:
        """
        Check if a string value looks like a date
        """
        import re
        
        # Common date patterns
        date_patterns = [
            r'^\d{1,2}[-/]\d{1,2}[-/]\d{4}$',      # DD-MM-YYYY or MM-DD-YYYY
            r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$',      # YYYY-MM-DD
            r'^\d{1,2}[-/]\d{1,2}[-/]\d{2}$',       # DD-MM-YY or MM-DD-YY
            r'^\d{4}\d{2}\d{2}$',                   # YYYYMMDD
            r'^\w{3}\s+\d{1,2},?\s+\d{4}$',        # Jan 15, 2023
            r'^\d{1,2}\s+\w{3}\s+\d{4}$',          # 15 Jan 2023
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, value.strip()):
                return True
        
        return False
    
    def _convert_to_date(self, series: pd.Series) -> Tuple[Optional[pd.Series], Optional[str]]:
        """
        Convert a series to datetime with multiple format attempts
        """
        # Common date formats to try
        date_formats = [
            '%d-%m-%Y',    # 08-11-2016
            '%m-%d-%Y',    # 11-08-2016
            '%Y-%m-%d',    # 2016-11-08
            '%d/%m/%Y',    # 08/11/2016
            '%m/%d/%Y',    # 11/08/2016
            '%Y/%m/%d',    # 2016/11/08
            '%d-%m-%y',    # 08-11-16
            '%m-%d-%y',    # 11-08-16
            '%Y%m%d',      # 20161108
            '%b %d, %Y',   # Nov 08, 2016
            '%d %b %Y',    # 08 Nov 2016
        ]
        
        # First try pandas' automatic parsing
        try:
            converted = pd.to_datetime(series, errors='coerce', infer_datetime_format=True)
            if converted.notna().sum() >= len(series) * 0.8:  # 80% success rate
                return converted, 'auto_detected'
        except Exception:
            pass
        
        # Try specific formats
        for date_format in date_formats:
            try:
                converted = pd.to_datetime(series, format=date_format, errors='coerce')
                success_rate = converted.notna().sum() / len(series)
                if success_rate >= 0.8:  # 80% success rate
                    return converted, date_format
            except Exception:
                continue
        
        # If no format works well, return None
        return None, None
    
    def _is_numeric_column(self, col_name: str, series: pd.Series) -> bool:
        """
        Detect if a column should be treated as numeric
        """
        if series.dtype != 'object':
            return False
        
        # Check if values can be converted to numeric
        try:
            converted = pd.to_numeric(series, errors='coerce')
            success_rate = converted.notna().sum() / len(series)
            return success_rate >= 0.9  # 90% success rate
        except Exception:
            return False
    
    def _is_boolean_column(self, col_name: str, series: pd.Series) -> bool:
        """
        Detect if a column should be treated as boolean
        """
        if series.dtype == 'bool':
            return False  # Already boolean
        
        unique_values = set(str(v).lower().strip() for v in series.dropna().unique())
        boolean_values = {'true', 'false', 'yes', 'no', '1', '0', 'y', 'n'}
        
        return len(unique_values) <= 2 and unique_values.issubset(boolean_values)
    
    def _convert_to_boolean(self, series: pd.Series) -> Optional[pd.Series]:
        """
        Convert a series to boolean
        """
        try:
            # Create mapping for boolean conversion
            true_values = {'true', 'yes', '1', 'y'}
            
            def convert_value(val):
                if pd.isna(val):
                    return None
                return str(val).lower().strip() in true_values
            
            converted = series.apply(convert_value)
            return converted
            
        except Exception:
            return None
    
    def _store_transformation_metadata(self, source_id: str, table_name: str, transformation_log: Dict[str, Dict[str, str]]):
        """
        Store transformation metadata in DuckDB for future reference
        """
        try:
            if not transformation_log or not self.integrated_db or not hasattr(self.integrated_db, 'execute'):
                return
            
            # Create transformation metadata table if it doesn't exist
            self.integrated_db.execute("""
                CREATE TABLE IF NOT EXISTS transformation_metadata (
                    source_id VARCHAR,
                    table_name VARCHAR,
                    column_name VARCHAR,
                    original_type VARCHAR,
                    transformed_type VARCHAR,
                    transformation_applied BOOLEAN,
                    format_detected VARCHAR,
                    transformation_type VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert transformation records
            for col, transformation in transformation_log.items():
                self.integrated_db.execute("""
                    INSERT INTO transformation_metadata 
                    (source_id, table_name, column_name, original_type, transformed_type, 
                     transformation_applied, format_detected, transformation_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    source_id,
                    table_name,
                    col,
                    transformation['from'],
                    transformation['to'],
                    True,
                    transformation.get('format_detected', ''),
                    transformation.get('transformation_type', '')
                ))
            
            logger.info(f"Stored transformation metadata for {len(transformation_log)} columns")
            
        except Exception as e:
            logger.warning(f"Failed to store transformation metadata: {e}")
    
    def _analyze_schema(self, data: pd.DataFrame, source_type: str, 
                       connection_info: Dict, column_mapping: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Enhanced schema analysis using centralized type mapping utilities"""
        if column_mapping is None:
            column_mapping = {}
            
        schema = {
            'tables': {
                'main_table': {
                    'columns': {},
                    'row_count': len(data),
                    'sample_data': [],
                    'original_column_mapping': column_mapping,
                    'data_quality': self._assess_data_quality(data)
                }
            }
        }
        
        # Add sample data with error handling
        try:
            if len(data) > 0:
                sample_size = min(3, len(data))
                schema['tables']['main_table']['sample_data'] = self._safe_json_serialize(data.head(sample_size))
        except Exception as e:
            logger.warning(f"Failed to add sample data: {e}")
        
        # Use centralized type analysis
        try:
            type_info = get_column_type_info(data)
        except Exception as e:
            logger.error(f"Failed to get column type info: {e}")
            type_info = {}
        
        for col in data.columns:
            try:
                # Get pandas dtype
                pandas_dtype = str(data[col].dtype)
                
                # Use centralized type mapping for semantic type with enhanced error handling
                try:
                    sample_values = self._get_safe_sample_values(data[col])
                    semantic_type = map_pandas_dtype_to_standard(pandas_dtype, sample_values)
                except Exception as e:
                    logger.warning(f"Error getting sample values or mapping type for column {col}: {e}")
                    sample_values = []
                    semantic_type = 'string'  # Safe fallback
                
                # Validate semantic type
                if not validate_semantic_data_type(semantic_type):
                    logger.warning(f"Invalid semantic type '{semantic_type}' for column {col}, defaulting to 'string'")
                    semantic_type = 'string'
                
                col_info = {
                    'type': semantic_type,  # Use semantic type, not pandas dtype
                    'pandas_type': pandas_dtype,  # Store pandas type for debugging/traceability
                    'null_count': int(data[col].isnull().sum()),
                    'unique_count': int(data[col].nunique()),
                    'sample_values': sample_values
                }
                
                # Enhanced key detection
                if col_info['unique_count'] == len(data) and col_info['null_count'] == 0:
                    col_info['potential_key'] = True
                
                # Enhanced foreign key detection
                original_col = next((orig for orig, cleaned in column_mapping.items() if cleaned == col), col)
                check_name = original_col.lower()
                if any(keyword in check_name for keyword in ['id', 'key', 'ref', 'fk']):
                    col_info['potential_foreign_key'] = True
                
                # Data quality metrics
                col_info['completeness'] = (len(data) - col_info['null_count']) / len(data) if len(data) > 0 else 0
                col_info['uniqueness'] = col_info['unique_count'] / len(data) if len(data) > 0 else 0
                
                schema['tables']['main_table']['columns'][col] = col_info
                
            except AttributeError as e:
                # Specific handling for AttributeError (like 'int' object has no attribute 'lower')
                logger.warning(f"AttributeError during column analysis for {col}: {e}. This may be due to type conversion issues.")
                # Add complete fallback column info
                schema['tables']['main_table']['columns'][col] = {
                    'type': 'string',  # Default to string instead of 'unknown'
                    'pandas_type': str(data[col].dtype) if col in data.columns else 'unknown',
                    'null_count': int(data[col].isnull().sum()) if col in data.columns else 0,
                    'unique_count': int(data[col].nunique()) if col in data.columns else 0,
                    'sample_values': [],
                    'error_type': 'AttributeError',
                    'error': str(e)
                }
            except Exception as e:
                logger.warning(f"Failed to analyze column {col}: {e}")
                # Add complete fallback column info for any other errors
                schema['tables']['main_table']['columns'][col] = {
                    'type': 'string',  # Default to string instead of 'unknown'
                    'pandas_type': str(data[col].dtype) if col in data.columns else 'unknown',
                    'null_count': int(data[col].isnull().sum()) if col in data.columns else 0,
                    'unique_count': int(data[col].nunique()) if col in data.columns else 0,
                    'sample_values': [],
                    'error_type': type(e).__name__,
                    'error': str(e)
                }
        
        # Final validation to ensure no object types
        schema = ensure_no_object_types(schema)
            
        return schema
    
    def _get_safe_sample_values(self, series: pd.Series, max_samples: int = 5) -> List[str]:
        """Safely extract sample values from a pandas Series with enhanced error handling"""
        try:
            # Get non-null values first
            non_null_series = series.dropna().head(max_samples)
            
            # Convert to strings with proper error handling
            sample_values = []
            for value in non_null_series:
                try:
                    # Defensive programming: ensure all values are converted to strings safely
                    if value is None or pd.isna(value):
                        continue
                    str_value = str(value)
                    sample_values.append(str_value)
                except Exception as e:
                    logger.warning(f"Failed to convert individual sample value to string: {value}, error: {e}")
                    # Use empty string as fallback rather than failing completely
                    sample_values.append('')
            
            return sample_values
            
        except Exception as e:
            logger.warning(f"Failed to get sample values from series: {e}")
            return []
    
    def _assess_data_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Assess data quality metrics"""
        try:
            total_cells = len(data) * len(data.columns)
            null_cells = data.isnull().sum().sum()
            
            return {
                'completeness': (total_cells - null_cells) / total_cells if total_cells > 0 else 0,
                'total_rows': len(data),
                'total_columns': len(data.columns),
                'null_percentage': (null_cells / total_cells * 100) if total_cells > 0 else 0
            }
        except Exception as e:
            logger.warning(f"Failed to assess data quality: {e}")
            return {'error': str(e)}
    
    def _detect_relationships(self, new_source_id: str):
        """Detect relationships between new source and existing sources"""
        try:
            new_source = DataSource.objects.get(id=new_source_id)
            if not new_source.schema_info:
                return
            
            # Get existing data sources
            existing_sources = DataSource.objects.filter(
                status='active'
            ).exclude(id=new_source_id)
            
            for existing_source in existing_sources:
                if existing_source.schema_info:
                    relationships = self._find_column_relationships(
                        new_source_id, new_source.schema_info,
                        str(existing_source.id), existing_source.schema_info
                    )
                    
                    # Save relationships to database
                    for relationship in relationships:
                        self._save_relationship(relationship)
                        
        except Exception as e:
            logger.error(f"Failed to detect relationships for source {new_source_id}: {e}")
    
    def _find_column_relationships(self, source1_id: str, schema1: Dict, 
                                  source2_id: str, schema2: Dict) -> List[DataRelationship]:
        """Find potential relationships between columns in two data sources"""
        relationships = []
        
        # Get main table columns from both schemas
        table1_columns = schema1.get('tables', {}).get('main_table', {}).get('columns', {})
        table2_columns = schema2.get('tables', {}).get('main_table', {}).get('columns', {})
        
        for col1_name, col1_info in table1_columns.items():
            for col2_name, col2_info in table2_columns.items():
                confidence = self._calculate_relationship_confidence(
                    col1_name, col1_info, col2_name, col2_info
                )
                
                if confidence > 0.6:  # Threshold for potential relationship
                    relationship_type = self._determine_relationship_type(col1_info, col2_info)
                    suggested_join = self._suggest_join_type(relationship_type, col1_info, col2_info)
                    
                    relationship = DataRelationship(
                        source1_id=source1_id,
                        source1_table='main_table',
                        source1_column=col1_name,
                        source2_id=source2_id,
                        source2_table='main_table',
                        source2_column=col2_name,
                        relationship_type=relationship_type,
                        confidence_score=confidence,
                        suggested_join_type=suggested_join
                    )
                    relationships.append(relationship)
                    
        return relationships
    
    def _calculate_relationship_confidence(self, col1_name: str, col1_info: Dict, 
                                         col2_name: str, col2_info: Dict) -> float:
        """Calculate confidence score for potential relationship between two columns"""
        confidence = 0.0
        
        # Name similarity (most important factor)
        name_similarity = self._calculate_name_similarity(col1_name, col2_name)
        confidence += name_similarity * 0.5
        
        # Type compatibility
        if self._are_types_compatible(col1_info.get('type', ''), col2_info.get('type', '')):
            confidence += 0.3
        
        # Check for key indicators
        if col1_info.get('potential_key') or col1_info.get('potential_foreign_key'):
            confidence += 0.1
        if col2_info.get('potential_key') or col2_info.get('potential_foreign_key'):
            confidence += 0.1
        
        # Unique value ratio similarity
        unique1 = col1_info.get('unique_count', 0)
        unique2 = col2_info.get('unique_count', 0)
        if unique1 > 0 and unique2 > 0:
            ratio_diff = abs(unique1 - unique2) / max(unique1, unique2)
            confidence += (1 - ratio_diff) * 0.1
        
        return min(confidence, 1.0)
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between column names"""
        name1_lower = name1.lower()
        name2_lower = name2.lower()
        
        # Exact match
        if name1_lower == name2_lower:
            return 1.0
        
        # Common key patterns
        key_patterns = ['id', 'key', 'ref', 'code', 'num', 'number']
        for pattern in key_patterns:
            if pattern in name1_lower and pattern in name2_lower:
                return 0.8
        
        # Substring match
        if name1_lower in name2_lower or name2_lower in name1_lower:
            return 0.7
        
        # Common prefixes/suffixes
        for i in range(3, min(len(name1_lower), len(name2_lower)) + 1):
            if name1_lower[:i] == name2_lower[:i] or name1_lower[-i:] == name2_lower[-i:]:
                return 0.6
        
        return 0.0
    
    def _are_types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two column types are compatible for joining"""
        # Normalize type names
        type1_norm = type1.lower()
        type2_norm = type2.lower()
        
        # Exact match
        if type1_norm == type2_norm:
            return True
        
        # Integer types
        int_types = ['int', 'integer', 'bigint', 'smallint']
        if any(t in type1_norm for t in int_types) and any(t in type2_norm for t in int_types):
            return True
        
        # String types
        str_types = ['str', 'string', 'varchar', 'char', 'text', 'object']
        if any(t in type1_norm for t in str_types) and any(t in type2_norm for t in str_types):
            return True
        
        # Float types
        float_types = ['float', 'double', 'decimal', 'numeric']
        if any(t in type1_norm for t in float_types) and any(t in type2_norm for t in float_types):
            return True
        
        return False
    
    def _determine_relationship_type(self, col1_info: Dict, col2_info: Dict) -> str:
        """Determine the type of relationship between two columns"""
        unique1 = col1_info.get('unique_count', 0)
        unique2 = col2_info.get('unique_count', 0)
        
        # Check if either column is a potential key
        is_key1 = col1_info.get('potential_key', False)
        is_key2 = col2_info.get('potential_key', False)
        
        if is_key1 and is_key2:
            return 'one_to_one'
        elif is_key1 and not is_key2:
            return 'one_to_many'
        elif not is_key1 and is_key2:
            return 'many_to_one'
        else:
            # Use unique count ratio as heuristic
            if unique1 == unique2:
                return 'one_to_one'
            elif unique1 > unique2 * 0.8:
                return 'one_to_many'
            elif unique2 > unique1 * 0.8:
                return 'many_to_one'
            else:
                return 'many_to_many'
    
    def _suggest_join_type(self, relationship_type: str, col1_info: Dict, col2_info: Dict) -> str:
        """Suggest appropriate join type based on relationship"""
        null_count1 = col1_info.get('null_count', 0)
        null_count2 = col2_info.get('null_count', 0)
        
        # If either column has many nulls, suggest LEFT or RIGHT join
        if null_count1 > 0 and null_count2 == 0:
            return 'RIGHT'
        elif null_count2 > 0 and null_count1 == 0:
            return 'LEFT'
        elif null_count1 > 0 or null_count2 > 0:
            return 'FULL'
        else:
            return 'INNER'
    
    def _save_relationship(self, relationship: DataRelationship):
        """Save relationship to database (placeholder for future implementation)"""
        # This would save to a relationships table in the future
        logger.info(f"Detected relationship: {relationship.source1_id}.{relationship.source1_column} -> "
                   f"{relationship.source2_id}.{relationship.source2_column} "
                   f"(confidence: {relationship.confidence_score:.2f})")
    
    def get_suggested_joins(self) -> List[Dict[str, Any]]:
        """Get AI-suggested joins between data sources"""
        suggestions = []
        
        try:
            # Get all active data sources
            data_sources = DataSource.objects.filter(status='active')
            source_dict = {str(ds.id): ds for ds in data_sources}
            
            # Find relationships between all pairs
            for i, source1 in enumerate(data_sources):
                for source2 in data_sources[i+1:]:
                    if source1.schema_info and source2.schema_info:
                        relationships = self._find_column_relationships(
                            str(source1.id), source1.schema_info,
                            str(source2.id), source2.schema_info
                        )
                        
                        for relationship in relationships:
                            suggestion = {
                                'relationship': relationship,
                                'source1_name': source1.name,
                                'source2_name': source2.name,
                                'confidence': relationship.confidence_score,
                                'join_type': relationship.suggested_join_type,
                                'suggestion_text': f"Join {source1.name}.{relationship.source1_column} with {source2.name}.{relationship.source2_column}"
                            }
                            suggestions.append(suggestion)
            
            # Sort by confidence score
            suggestions.sort(key=lambda x: x['confidence'], reverse=True)
            return suggestions[:10]  # Return top 10 suggestions
            
        except Exception as e:
            logger.error(f"Failed to get suggested joins: {e}")
            return []
    
    def create_etl_operation(self, name: str, operation_type: str, 
                           source_tables: List[str], parameters: Dict[str, Any],
                           user_id: Optional[int] = None) -> Optional[str]:
        """
        Create and execute ETL operation with enhanced security and error handling
        """
        try:
            # Validate input parameters
            if not source_tables:
                raise ValueError("Source tables cannot be empty")
            
            # Generate secure SQL query
            sql_query = self._generate_etl_sql(operation_type, source_tables, parameters)
            
            # Create ETL operation record
            etl_operation = ETLOperation.objects.create(
                name=name,
                operation_type=operation_type,
                source_tables=source_tables,
                parameters=parameters,
                output_table_name=f"etl_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                sql_query=sql_query,
                created_by_id=user_id or 1,
                status='pending'
            )
            
            # Execute operation asynchronously
            execute_etl_operation.delay(str(etl_operation.id))
            
            return str(etl_operation.id)
            
        except Exception as e:
            logger.error(f"Failed to create ETL operation {name}: {e}")
            return None
    
    def _generate_etl_sql(self, operation_type: str, source_tables: List[str], 
                         parameters: Dict[str, Any]) -> str:
        """Generate secure SQL for ETL operations with injection prevention"""
        # Validate table names to prevent SQL injection
        safe_tables = []
        for table in source_tables:
            if not re.match(r'^[a-zA-Z0-9_]+$', table):
                raise ValueError(f"Invalid table name: {table}")
            safe_tables.append(table)
        
        if operation_type.lower() == 'join':
            return self._generate_join_sql(safe_tables, parameters)
        elif operation_type.lower() == 'union':
            return self._generate_union_sql(safe_tables, parameters)
        elif operation_type.lower() == 'aggregate':
            return self._generate_aggregate_sql(safe_tables, parameters)
        else:
            raise ValueError(f"Unsupported ETL operation type: {operation_type}")
    
    def _generate_join_sql(self, source_tables: List[str], parameters: Dict[str, Any]) -> str:
        """Generate secure SQL for JOIN operation using validator"""
        if len(source_tables) < 2:
            raise ValueError("JOIN requires at least 2 tables")
            
        left_table, right_table = source_tables[0], source_tables[1]
        left_column = parameters.get('left_column')
        right_column = parameters.get('right_column')
        join_type = parameters.get('join_type', 'INNER')
        
        if not left_column or not right_column:
            raise ValueError("JOIN requires left and right columns")
        
        # Use the validator to generate proper SQL
        from utils.join_validator import JoinSQLValidator
        
        output_table = f"etl_join_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        join_result = JoinSQLValidator.generate_join_sql(
            left_table=left_table,
            right_table=right_table,
            left_column=left_column,
            right_column=right_column,
            join_type=join_type,
            output_table=output_table
        )
        
        if not join_result.is_valid:
            raise ValueError(f"Invalid join configuration: {join_result.error_message}")
        
        if not join_result.corrected_sql:
            raise ValueError("Validator did not generate SQL")
        
        # Log warnings
        for warning in join_result.warnings:
            logger.warning(f"Join warning: {warning}")
        
        return join_result.corrected_sql
    
    def _generate_union_sql(self, source_tables: List[str], parameters: Dict[str, Any]) -> str:
        """Generate secure SQL for UNION operation"""
        if len(source_tables) < 2:
            raise ValueError("UNION requires at least 2 tables")
        
        union_type = parameters.get('union_type', 'UNION')
        if union_type not in ['UNION', 'UNION ALL']:
            union_type = 'UNION'
        
        return f" {union_type} ".join([f"SELECT * FROM {table}" for table in source_tables])
    
    def _generate_aggregate_sql(self, source_tables: List[str], parameters: Dict[str, Any]) -> str:
        """Generate secure SQL for aggregation operation"""
        if len(source_tables) != 1:
            raise ValueError("AGGREGATE requires exactly 1 table")
        
        table = source_tables[0]
        group_by = parameters.get('group_by', [])
        aggregations = parameters.get('aggregations', [])
        
        if not aggregations:
            raise ValueError("Aggregations must be specified")
        
        # Validate aggregation functions and columns
        safe_aggregations = []
        allowed_functions = ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX']
        
        for agg in aggregations:
            func = agg.get('function', '').upper()
            column = agg.get('column', '')
            
            if func not in allowed_functions:
                raise ValueError(f"Invalid aggregation function: {func}")
            
            if not re.match(r'^[a-zA-Z0-9_]+$', column):
                raise ValueError(f"Invalid column name: {column}")
            
            safe_aggregations.append(f"{func}({column}) as {func.lower()}_{column}")
        
        # Validate GROUP BY columns
        safe_group_by = []
        for col in group_by:
            if not re.match(r'^[a-zA-Z0-9_]+$', col):
                raise ValueError(f"Invalid GROUP BY column: {col}")
            safe_group_by.append(col)
        
        select_clause = ", ".join(safe_group_by + safe_aggregations)
        group_clause = f"GROUP BY {', '.join(safe_group_by)}" if safe_group_by else ""
        
        return f"SELECT {select_clause} FROM {table} {group_clause}"
    
    def get_integrated_data(self, table_name: Optional[str] = None) -> pd.DataFrame:
        """Get integrated data from a specific table or list all tables with enhanced error handling"""
        if not self.integrated_db:
            logger.error("Integrated database not connected")
            return pd.DataFrame()
            
        try:
            if table_name:
                # Check if table exists before attempting to query it
                if not self.check_table_exists(table_name):
                    logger.warning(f"Table '{table_name}' does not exist in integrated database")
                    return pd.DataFrame()
                
                # FIXED: Remove double prefixing bug - use table name directly
                # Don't apply _get_safe_table_name again if already safe
                safe_table_name = table_name
                
                # Only sanitize if the table name contains problematic characters
                # This prevents the double-prefix issue (source_source_...)
                
                if not validate_table_name(table_name):
                    # Apply sanitization only if needed
                    safe_table_name = TableNameManager.generate_safe_table_name(table_name)
                    logger.info(f"Sanitized problematic table name from '{table_name}' to '{safe_table_name}' for DuckDB query")
                else:
                    logger.debug(f"Using table name as-is: '{table_name}' (already valid)")
                
                # Use quoted table names for all scenarios to handle any special characters
                quoted_table_name = f'"{safe_table_name}"'
                logger.info(f"Executing query with table name: {quoted_table_name}")
                return pd.read_sql_query(f"SELECT * FROM {quoted_table_name}", self.integrated_db)
            else:
                # Get list of tables - handle both DuckDB and SQLite
                if hasattr(self.integrated_db, 'execute'):
                    # DuckDB approach
                    try:
                        result = self.integrated_db.execute("SHOW TABLES").fetchdf()
                        return pd.DataFrame({'available_tables': result['name'].tolist()})
                    except:
                        # Fallback to information schema
                        result = self.integrated_db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchdf()
                        return pd.DataFrame({'available_tables': result['table_name'].tolist()})
                else:
                    # SQLite approach
                    cursor = self.integrated_db.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    return pd.DataFrame({'available_tables': [row[0] for row in cursor.fetchall()]})
                
        except Exception as e:
            logger.error(f"Failed to get integrated data for table '{table_name}': {e}")
            return pd.DataFrame()
    
    def get_data_sources_summary(self) -> Dict[str, Any]:
        """Get summary of all integrated data sources"""
        try:
            summary = {
                'total_sources': 0,
                'sources': [],
                'total_tables': 0,
                'total_relationships': 0
            }
            
            # Get Django data sources instead of internal ones
            from datasets.models import DataSource
            
            data_sources = DataSource.objects.all()
            summary['total_sources'] = data_sources.count()
            
            for ds in data_sources:
                source_info = {
                    'id': str(ds.id),
                    'name': ds.name,
                    'type': ds.source_type,
                    'status': 'active',  # Assume active if in database
                    'created_at': ds.created_at.isoformat() if hasattr(ds, 'created_at') else None,
                    'schema_info': getattr(ds, 'schema_info', {}),
                    'connection_info': getattr(ds, 'connection_info', {})
                }
                summary['sources'].append(source_info)
                
                # Count tables (each data source represents a table)
                summary['total_tables'] += 1
            
            logger.info(f"Generated summary: {summary['total_sources']} sources, {summary['total_tables']} tables")
            return summary
            
        except Exception as e:
            logger.error(f"Error getting data sources summary: {e}")
            return {
                'total_sources': 0,
                'sources': [],
                'total_tables': 0,
                'total_relationships': 0
            }

    def store_transformed_data(self, table_name: str, data: 'pd.DataFrame', 
                             transformations: Dict[str, str], source_id: str) -> bool:
        """Store transformed data to integrated database"""
        try:
            import duckdb
            from django.conf import settings
            import os
            
            # Create/connect to integrated database
            db_path = os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb')
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            conn = duckdb.connect(db_path)
            
            # Drop existing table if it exists
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table from DataFrame with proper types
            conn.register('temp_df', data)
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
            
            # Add metadata about transformations
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transformation_metadata (
                    table_name VARCHAR,
                    source_id VARCHAR,
                    transformations JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Store transformation metadata
            import json
            conn.execute("""
                INSERT INTO transformation_metadata (table_name, source_id, transformations)
                VALUES (?, ?, ?)
            """, (table_name, source_id, json.dumps(transformations)))
            
            conn.close()
            
            logger.info(f"Successfully stored transformed data: {table_name} with {len(data)} rows")
            return True
            
        except Exception as e:
            logger.error(f"Error storing transformed data: {e}")
            return False

    def _safe_json_serialize(self, obj):
        """
        Safely serialize pandas data to JSON, handling NaN, inf, and other edge cases
        """
        if isinstance(obj, (pd.DataFrame, pd.Series)):
            # Convert pandas objects to Python objects first
            obj = obj.to_dict('records') if isinstance(obj, pd.DataFrame) else obj.tolist()
        
        def convert_value(value):
            """Convert individual values to JSON-safe format"""
            if pd.isna(value) or value is None:
                return None
            elif isinstance(value, (np.integer, int)):
                return int(value)
            elif isinstance(value, (np.floating, float)):
                if np.isnan(value) or np.isinf(value):
                    return None
                return float(value)
            elif isinstance(value, (np.bool_, bool)):
                return bool(value)
            elif isinstance(value, (np.str_, str)):
                return str(value)
            elif isinstance(value, (list, tuple)):
                return [convert_value(item) for item in value]
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            else:
                # For any other type, convert to string
                return str(value)
        
        if isinstance(obj, list):
            return [convert_value(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: convert_value(v) for k, v in obj.items()}
        else:
            return convert_value(obj)

    def get_integrated_data_for_source(self, source_id: str) -> Optional[pd.DataFrame]:
        """
        Get integrated data for a specific data source by source ID
        
        Args:
            source_id: The data source ID
            
        Returns:
            DataFrame with the data or None if not found
        """
        try:
            # Get the table name for this source
            table_name = self._get_safe_table_name(source_id)
            
            # Check if table exists
            if not self.check_table_exists(table_name):
                logger.warning(f"[NOT_FOUND] Table {table_name} not found for source {source_id}")
                return None
            
            # Get the data
            data = self.get_integrated_data(table_name)
            
            if data is not None and not data.empty:
                logger.info(f"[SUCCESS] Retrieved {len(data)} rows for source {source_id}")
                return data
            else:
                logger.warning(f"[EMPTY] Table {table_name} exists but contains no data")
                return None
                
        except Exception as e:
            logger.error(f"[ERROR] Failed to get integrated data for source {source_id}: {e}")
            return None


@shared_task
def execute_etl_operation(etl_operation_id: str):
    """
    Enhanced Celery task for executing ETL operations with proper error handling
    """
    try:
        from datasets.models import ETLOperation
        
        # Get ETL operation
        etl_operation = ETLOperation.objects.get(id=etl_operation_id)
        integration_service = DataIntegrationService()
        
        # Update status to running
        etl_operation.status = 'running'
        etl_operation.save()
        
        # Execute the SQL operation
        try:
            if integration_service.integrated_db:
                start_time = datetime.now()
                
                if hasattr(integration_service.integrated_db, 'execute'):
                    # DuckDB execution
                    result = integration_service.integrated_db.execute(etl_operation.sql_query).fetchdf()
                else:
                    # SQLite execution
                    result = pd.read_sql(etl_operation.sql_query, integration_service.integrated_db)
                
                # Store results
                output_table = etl_operation.output_table_name
                if hasattr(integration_service.integrated_db, 'register'):
                    integration_service.integrated_db.register(f"temp_{output_table}", result)
                    integration_service.integrated_db.execute(f"CREATE TABLE {output_table} AS SELECT * FROM temp_{output_table}")
                    integration_service.integrated_db.unregister(f"temp_{output_table}")
                else:
                    result.to_sql(output_table, integration_service.integrated_db, if_exists='replace', index=False)
                
                # Update operation with results - Fixed: use result_summary not result_info
                execution_time = (datetime.now() - start_time).total_seconds()
                etl_operation.status = 'completed'
                etl_operation.last_run = datetime.now()
                etl_operation.execution_time = execution_time
                etl_operation.row_count = len(result) if hasattr(result, '__len__') else 0
                etl_operation.save()
                
                logger.info(f"ETL operation {etl_operation_id} completed successfully")
                return {'success': True, 'operation_id': etl_operation_id, 'row_count': len(result)}
                
            else:
                raise Exception("Integrated database not available")
                
        except Exception as exec_error:
            etl_operation.status = 'failed'
            etl_operation.error_message = str(exec_error)
            etl_operation.save()
            logger.error(f"ETL operation {etl_operation_id} execution failed: {exec_error}")
            return {'success': False, 'operation_id': etl_operation_id, 'error': str(exec_error)}
        
    except Exception as exc:
        logger.error(f"Failed to execute ETL operation {etl_operation_id}: {exc}")
        try:
            etl_operation = ETLOperation.objects.get(id=etl_operation_id)
            etl_operation.status = 'failed'
            etl_operation.error_message = str(exc)
            etl_operation.save()
        except:
            pass
        return {'success': False, 'operation_id': etl_operation_id, 'error': str(exc)} 