"""
Type Helpers for ConvaBI Application
Provides centralized data type mapping and conversion utilities for the ETL pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
import logging
import re
from datetime import datetime, date

logger = logging.getLogger(__name__)


def map_pandas_dtype_to_standard(pandas_dtype: str, sample_values: Optional[List[Any]] = None) -> str:
    """
    Map pandas dtype to standardized semantic type.
    
    Args:
        pandas_dtype: String representation of pandas dtype
        sample_values: Optional sample values to help with type inference
        
    Returns:
        Standardized semantic type ('string', 'integer', 'float', 'boolean', 'date', etc.)
    """
    if not pandas_dtype:
        return 'string'
    
    dtype_str = str(pandas_dtype).lower()
    
    # Handle pandas object types - always default to string unless sample data suggests otherwise
    if dtype_str == 'object':
        if sample_values:
            inferred_type = _infer_type_from_samples(sample_values)
            if inferred_type != 'string':
                logger.info(f"Object type inferred as {inferred_type} based on sample values")
            return inferred_type
        else:
            logger.debug("Object type defaulting to 'string' - no sample values provided")
            return 'string'
    
    # Integer types
    if any(int_type in dtype_str for int_type in ['int', 'integer']):
        return 'integer'
    
    # Float types
    if any(float_type in dtype_str for float_type in ['float', 'double', 'decimal', 'numeric']):
        return 'float'
    
    # Boolean types
    if 'bool' in dtype_str:
        return 'boolean'
    
    # Date/time types
    if any(date_type in dtype_str for date_type in ['datetime', 'timestamp']):
        return 'datetime'
    if 'date' in dtype_str and 'datetime' not in dtype_str:
        return 'date'
    if 'time' in dtype_str and 'datetime' not in dtype_str:
        return 'time'
    
    # String types
    if any(str_type in dtype_str for str_type in ['str', 'string', 'varchar', 'char', 'text']):
        return 'string'
    
    # Category types (pandas specific)
    if 'category' in dtype_str:
        return 'string'
    
    # Default to string for unknown types
    logger.warning(f"Unknown pandas dtype '{pandas_dtype}', defaulting to 'string'")
    return 'string'


def _infer_type_from_samples(sample_values: List[Any]) -> str:
    """
    Infer semantic type from sample values.
    
    Args:
        sample_values: List of sample values (will be converted to strings internally)
        
    Returns:
        Inferred semantic type
    """
    if not sample_values:
        return 'string'
    
    # Filter out None/NaN values and ensure all are strings with defensive programming
    valid_samples = []
    for v in sample_values:
        if v is not None and pd.notna(v):
            try:
                # Safely convert to string, handling any type
                str_v = str(v).strip()
                if str_v and str_v.lower() not in ['none', 'nan', 'null', '']:
                    valid_samples.append(str_v)
            except Exception as e:
                logger.warning(f"Failed to convert value to string: {v}, error: {e}")
                # Skip problematic values rather than failing
                continue
    
    if not valid_samples:
        return 'string'
    
    # Check for boolean values (check string representations)
    boolean_values = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n'}
    try:
        if all(str(v).lower() in boolean_values for v in valid_samples):
            return 'boolean'
    except (AttributeError, TypeError) as e:
        logger.warning(f"Error checking boolean values: {e}")
        # Continue with other type checks
    
    # Check for numeric values with enhanced error handling
    try:
        numeric_values = []
        for v in valid_samples:
            try:
                # Ensure v is a string before processing
                str_v = str(v).strip()
                # Remove common numeric formatting
                clean_v = str_v.replace(',', '').replace('$', '').replace('%', '')
                
                try:
                    if '.' in clean_v:
                        float_val = float(clean_v)
                        numeric_values.append(float_val)
                    else:
                        int_val = int(clean_v)
                        numeric_values.append(int_val)
                except (ValueError, TypeError):
                    break
            except Exception as e:
                logger.debug(f"Error processing numeric value {v}: {e}")
                break
        
        if len(numeric_values) == len(valid_samples):
            # All values are numeric
            if all(isinstance(v, int) or (isinstance(v, float) and v.is_integer()) for v in numeric_values):
                return 'integer'
            else:
                return 'float'
    except (ValueError, AttributeError, TypeError) as e:
        logger.debug(f"Error in numeric type inference: {e}")
        # Continue with other type checks
    
    # Check for date values with enhanced error handling
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
        r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
    ]
    
    try:
        for pattern in date_patterns:
            if all(re.match(pattern, str(v).strip()) for v in valid_samples):
                return 'date'
    except (AttributeError, TypeError) as e:
        logger.debug(f"Error in date pattern matching: {e}")
    
    # Check for datetime values with enhanced error handling
    datetime_patterns = [
        r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
    ]
    
    try:
        for pattern in datetime_patterns:
            if all(re.match(pattern, str(v).strip()) for v in valid_samples):
                return 'datetime'
    except (AttributeError, TypeError) as e:
        logger.debug(f"Error in datetime pattern matching: {e}")
    
    # Default to string
    return 'string'


def infer_semantic_type_from_series(series: pd.Series) -> str:
    """
    Infer semantic type from a pandas Series.
    
    Args:
        series: Pandas Series to analyze
        
    Returns:
        Inferred semantic type
    """
    if series.empty:
        return 'string'
    
    # Get dtype and sample values
    pandas_dtype = str(series.dtype)
    sample_values = series.dropna().head(10).tolist()
    
    return map_pandas_dtype_to_standard(pandas_dtype, sample_values)


def validate_semantic_data_type(data_type: str) -> bool:
    """
    Validate that a data type is a valid semantic type.
    
    Args:
        data_type: Data type string to validate
        
    Returns:
        True if valid, False otherwise
    """
    valid_types = {
        'string', 'integer', 'float', 'boolean', 'date', 'datetime', 'time', 'json', 'binary'
    }
    return data_type in valid_types


def normalize_data_type(data_type: str) -> str:
    """
    Normalize various data type representations to canonical form.
    
    Args:
        data_type: Data type string to normalize
        
    Returns:
        Normalized data type
    """
    if not data_type:
        return 'string'
    
    dtype_lower = data_type.lower().strip()
    
    # String variations
    if dtype_lower in ['str', 'string', 'varchar', 'char', 'text', 'object']:
        return 'string'
    
    # Integer variations
    if dtype_lower in ['int', 'integer', 'int64', 'int32', 'bigint', 'smallint']:
        return 'integer'
    
    # Float variations
    if dtype_lower in ['float', 'double', 'float64', 'float32', 'decimal', 'numeric', 'real']:
        return 'float'
    
    # Boolean variations
    if dtype_lower in ['bool', 'boolean', 'bit']:
        return 'boolean'
    
    # Date variations
    if dtype_lower in ['date']:
        return 'date'
    
    # DateTime variations
    if dtype_lower in ['datetime', 'timestamp', 'datetime64', 'datetime64[ns]']:
        return 'datetime'
    
    # Time variations
    if dtype_lower in ['time']:
        return 'time'
    
    # JSON variations
    if dtype_lower in ['json', 'jsonb']:
        return 'json'
    
    # Binary variations
    if dtype_lower in ['binary', 'blob', 'bytes']:
        return 'binary'
    
    # Default to string for unknown types
    logger.warning(f"Unknown data type '{data_type}', normalizing to 'string'")
    return 'string'


def get_pandas_dtype_mapping() -> Dict[str, str]:
    """
    Get a mapping of pandas dtypes to semantic types.
    
    Returns:
        Dictionary mapping pandas dtypes to semantic types
    """
    return {
        'object': 'string',
        'int64': 'integer',
        'int32': 'integer',
        'int16': 'integer',
        'int8': 'integer',
        'float64': 'float',
        'float32': 'float',
        'bool': 'boolean',
        'datetime64[ns]': 'datetime',
        'datetime64': 'datetime',
        'timedelta64[ns]': 'time',
        'category': 'string',
        'string': 'string',
    }


def convert_object_columns_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all 'object' dtype columns to string type.
    
    Args:
        df: DataFrame to process
        
    Returns:
        DataFrame with object columns converted to string
    """
    df_copy = df.copy()
    object_columns = df_copy.select_dtypes(include=['object']).columns
    
    for col in object_columns:
        try:
            df_copy[col] = df_copy[col].astype(str)
            logger.debug(f"Converted column '{col}' from object to string")
        except Exception as e:
            logger.warning(f"Failed to convert column '{col}' to string: {e}")
    
    return df_copy


def _validate_and_clean_dataframes_recursively(data: Any) -> Any:
    """
    ENHANCED: Recursively validate and clean DataFrame objects from data structures.
    This prevents DataFrame boolean context ambiguity errors during serialization.
    
    Args:
        data: Data structure to clean
        
    Returns:
        Cleaned data structure with DataFrame objects converted to safe representations
    """
    try:
        if data is None:
            return None
        elif isinstance(data, pd.DataFrame):
            # FIXED: Handle DataFrame objects by converting to safe representation
            try:
                if not data.empty:
                    return {
                        '__dataframe_shape__': data.shape,
                        '__dataframe_columns__': list(data.columns),
                        '__dataframe_info__': f"DataFrame with {data.shape[0]} rows, {data.shape[1]} columns"
                    }
                else:
                    return {
                        '__dataframe_shape__': (0, 0),
                        '__dataframe_columns__': [],
                        '__dataframe_info__': "Empty DataFrame"
                    }
            except ValueError:
                # Handle "The truth value of a DataFrame is ambiguous" error
                return {
                    '__dataframe_shape__': data.shape,
                    '__dataframe_columns__': list(data.columns) if hasattr(data, 'columns') else [],
                    '__dataframe_info__': f"DataFrame with shape {data.shape}",
                    '__dataframe_ambiguous_handled__': True
                }
        elif isinstance(data, pd.Series):
            # Handle Series objects
            try:
                return {
                    '__series_length__': len(data),
                    '__series_info__': f"Series with {len(data)} elements"
                }
            except Exception:
                return str(data)
        elif isinstance(data, dict):
            # Recursively clean dictionary values
            cleaned_dict = {}
            for key, value in data.items():
                try:
                    cleaned_dict[key] = _validate_and_clean_dataframes_recursively(value)
                except Exception as e:
                    logger.warning(f"Error cleaning dict key {key}: {e}")
                    cleaned_dict[key] = str(value) if value is not None else None
            return cleaned_dict
        elif isinstance(data, (list, tuple)):
            # Recursively clean list/tuple items
            cleaned_items = []
            for item in data:
                try:
                    cleaned_items.append(_validate_and_clean_dataframes_recursively(item))
                except Exception as e:
                    logger.warning(f"Error cleaning list item: {e}")
                    cleaned_items.append(str(item) if item is not None else None)
            return cleaned_items if isinstance(data, list) else tuple(cleaned_items)
        else:
            # For other types, return as-is
            return data
    except Exception as e:
        logger.error(f"Error in _validate_and_clean_dataframes_recursively: {e}")
        return str(data) if data is not None else None


def get_column_type_info(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Get comprehensive type information for all columns in a DataFrame.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with column type information
    """
    type_info = {}
    
    for col in df.columns:
        # Ensure column name is string
        col_name = str(col)
        
        try:
            pandas_dtype = str(df[col].dtype)
            
            # Safely get sample values and convert to strings with enhanced error handling
            try:
                sample_values = df[col].dropna().head(5).tolist()
                # Ensure all sample values are strings for consistent processing
                sample_values_str = []
                for v in sample_values:
                    try:
                        # Defensive programming: convert any type to string safely
                        if v is None or pd.isna(v):
                            continue
                        sample_values_str.append(str(v))
                    except Exception as e:
                        logger.warning(f"Error converting sample value to string for column {col_name}: {v}, error: {e}")
                        # Use empty string as fallback
                        sample_values_str.append('')
            except Exception as e:
                logger.warning(f"Error getting sample values for column {col_name}: {e}")
                sample_values_str = []
            
            # Use string sample values for type inference
            semantic_type = map_pandas_dtype_to_standard(pandas_dtype, sample_values_str)
            
            type_info[col_name] = {
                'pandas_type': pandas_dtype,
                'semantic_type': semantic_type,
                'null_count': int(df[col].isnull().sum()),
                'unique_count': int(df[col].nunique()),
                'sample_values': sample_values_str,
                'is_object_type': pandas_dtype == 'object'
            }
            
        except Exception as e:
            logger.warning(f"Error analyzing column {col_name}: {e}")
            # Provide fallback information
            type_info[col_name] = {
                'pandas_type': 'object',
                'semantic_type': 'string',
                'null_count': 0,
                'unique_count': 0,
                'sample_values': [],
                'is_object_type': True
            }
    
    return type_info


def ensure_no_object_types(schema_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure schema information contains no 'object' types in the semantic type field.
    Enhanced to handle edge cases and DataFrame operations more robustly.
    FIXED: Added comprehensive DataFrame boolean context safety checks.
    
    Args:
        schema_info: Schema information dictionary
        
    Returns:
        Updated schema information with no object types
    """
    if not isinstance(schema_info, dict):
        return schema_info
    
    try:
        # ENHANCED: Recursive DataFrame detection and safe handling
        updated_schema = _validate_and_clean_dataframes_recursively(schema_info)
        
        # Handle different schema structures with enhanced error handling
        if 'tables' in updated_schema:
            for table_name, table_info in updated_schema['tables'].items():
                if isinstance(table_info, dict) and 'columns' in table_info:
                    for col_name, col_info in table_info['columns'].items():
                        if isinstance(col_info, dict) and 'type' in col_info:
                            if col_info['type'] == 'object':
                                col_info['type'] = 'string'
                                logger.info(f"Fixed object type to string for column {col_name}")
                            
                            # Enhanced validation for nested structures
                            if 'pandas_type' in col_info and col_info['pandas_type'] == 'object':
                                col_info['pandas_type'] = 'string'
                                logger.debug(f"Fixed pandas object type for column {col_name}")
        
        # ENHANCED: Handle direct columns list (non-table format)
        elif 'columns' in updated_schema and isinstance(updated_schema['columns'], list):
            for col_info in updated_schema['columns']:
                if isinstance(col_info, dict) and 'type' in col_info:
                    if col_info['type'] == 'object':
                        col_info['type'] = 'string'
                        logger.info(f"Fixed object type to string for column {col_info.get('name', 'unknown')}")
                    
                    # Enhanced validation for nested structures
                    if 'pandas_type' in col_info and col_info['pandas_type'] == 'object':
                        col_info['pandas_type'] = 'string'
                        logger.debug(f"Fixed pandas object type for column {col_info.get('name', 'unknown')}")
        
        # ENHANCED: Handle DataFrame truth value ambiguity issues with comprehensive safety checks
        if 'data_quality' in updated_schema:
            # Safely handle data quality metadata that might contain DataFrame references
            try:
                data_quality = updated_schema['data_quality']
                if isinstance(data_quality, dict):
                    # ENHANCED: Remove or convert problematic DataFrame references with better error handling
                    for key, value in list(data_quality.items()):  # Use list() to avoid dict modification during iteration
                        try:
                            # FIXED: More comprehensive DataFrame detection
                            if hasattr(value, 'empty') and hasattr(value, 'shape') and hasattr(value, 'columns'):
                                # This looks like a DataFrame, convert to summary safely
                                try:
                                    # Use .empty check instead of boolean context
                                    if not value.empty:
                                        data_quality[key] = f"DataFrame with {value.shape[0]} rows, {value.shape[1]} columns"
                                    else:
                                        data_quality[key] = "Empty DataFrame"
                                except ValueError as ambiguous_error:
                                    # Handle "The truth value of a DataFrame is ambiguous" error
                                    if 'ambiguous' in str(ambiguous_error).lower():
                                        data_quality[key] = f"DataFrame with shape {value.shape} (ambiguous boolean context handled)"
                                        logger.debug(f"Handled ambiguous DataFrame for {key}")
                                    else:
                                        data_quality[key] = f"DataFrame conversion error: {str(ambiguous_error)}"
                            elif hasattr(value, 'shape') and hasattr(value, '__len__'):
                                # This might be a numpy array or pandas Series
                                try:
                                    data_quality[key] = f"Array/Series with shape {getattr(value, 'shape', len(value))}"
                                except Exception:
                                    data_quality[key] = f"Array/Series (shape unknown)"
                        except Exception as value_error:
                            logger.warning(f"Error processing data quality value for {key}: {value_error}")
                            # Convert problematic values to string representation
                            try:
                                data_quality[key] = str(value)[:100]  # Limit length
                            except Exception:
                                data_quality[key] = "[Unprocessable data quality value]"
            except Exception as dq_error:
                logger.warning(f"Error processing data quality metadata: {dq_error}")
                # Remove problematic data quality info rather than failing
                updated_schema.pop('data_quality', None)
        
        return updated_schema
        
    except Exception as e:
        logger.warning(f"Error in ensure_no_object_types: {e}")
        # Return original schema if processing fails
        return schema_info 