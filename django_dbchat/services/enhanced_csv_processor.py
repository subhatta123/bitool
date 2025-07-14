#!/usr/bin/env python3
"""
Enhanced CSV Processor Service
Handles complex CSV parsing scenarios including:
- Auto-detection of column separators within cells
- Smart column splitting for comma-separated values
- Advanced parsing options for different CSV formats
- Preview functionality for users to validate parsing
"""

import pandas as pd
import numpy as np
import re
import logging
import json
from typing import Dict, List, Tuple, Any, Optional
from io import StringIO
import csv
from django.core.files.storage import default_storage
from django.conf import settings
import os

logger = logging.getLogger(__name__)

class EnhancedCSVProcessor:
    """Enhanced CSV processor with advanced parsing capabilities"""
    
    def __init__(self):
        self.supported_delimiters = [',', ';', '|', '\t', ':', ' ']
        self.supported_encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    
    def _safe_json_serialize(self, obj):
        """Safely serialize objects for JSON, handling NaN and other problematic values"""
        if isinstance(obj, (list, tuple)):
            return [self._safe_json_serialize(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._safe_json_serialize(v) for k, v in obj.items()}
        elif pd.isna(obj):
            return None
        elif isinstance(obj, (np.integer, np.floating)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif obj is np.nan:
            return None
        else:
            return obj
    
    def detect_csv_structure(self, file_path: str) -> Dict[str, Any]:
        """
        Detect CSV structure and recommend parsing options
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dictionary with detected structure information
        """
        try:
            # Try different encodings
            content = None
            detected_encoding = 'utf-8'
            
            for encoding in self.supported_encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                        detected_encoding = encoding
                        break
                except (UnicodeDecodeError, UnicodeError):
                    continue
            
            if not content:
                raise ValueError("Could not read file with any supported encoding")
            
            # Sample first few lines for analysis
            lines = content.split('\n')[:20]  # Analyze first 20 lines
            
            # Detect delimiter
            delimiter = self._detect_delimiter(lines)
            
            # Detect if file has header
            has_header = self._detect_header(lines, delimiter)
            
            # Detect if columns contain nested comma-separated values
            nested_columns = self._detect_nested_columns(lines, delimiter)
            
            # Get sample data preview
            sample_df = self._create_sample_dataframe(file_path, delimiter, has_header, detected_encoding)
            
            # Clean data for JSON serialization
            sample_preview = []
            column_names = []
            
            if sample_df is not None:
                # Replace NaN values with None for JSON compatibility
                sample_df_clean = sample_df.copy()
                sample_df_clean = sample_df_clean.where(pd.notnull(sample_df_clean), None)
                sample_preview = sample_df_clean.head(5).to_dict('records')
                column_names = list(sample_df.columns)
            
            # Create response with safe JSON serialization
            response = {
                'delimiter': delimiter,
                'has_header': has_header,
                'encoding': detected_encoding,
                'estimated_rows': len(lines),
                'estimated_columns': len(lines[0].split(delimiter)) if lines else 0,
                'nested_columns': nested_columns,
                'sample_preview': sample_preview,
                'column_names': column_names,
                'parsing_suggestions': self._generate_parsing_suggestions(nested_columns, sample_df)
            }
            
            # Apply safe JSON serialization to entire response
            return self._safe_json_serialize(response)
            
        except Exception as e:
            logger.error(f"Error detecting CSV structure: {e}")
            error_response = {
                'error': str(e),
                'delimiter': ',',
                'has_header': True,
                'encoding': 'utf-8',
                'nested_columns': {},
                'parsing_suggestions': []
            }
            return self._safe_json_serialize(error_response)
    
    def _detect_delimiter(self, lines: List[str]) -> str:
        """Detect the most likely delimiter"""
        if not lines:
            return ','
        
        # Count occurrences of each delimiter
        delimiter_counts = {}
        
        for delimiter in self.supported_delimiters:
            count = 0
            for line in lines[:5]:  # Check first 5 lines
                count += line.count(delimiter)
            delimiter_counts[delimiter] = count
        
        # Return delimiter with highest count
        detected_delimiter = max(delimiter_counts, key=delimiter_counts.get)
        
        # Validate by checking consistency across lines
        if self._validate_delimiter_consistency(lines, detected_delimiter):
            return detected_delimiter
        
        return ','  # Default fallback
    
    def _validate_delimiter_consistency(self, lines: List[str], delimiter: str) -> bool:
        """Validate that delimiter is consistent across lines"""
        if not lines:
            return False
        
        # Count columns in each line
        column_counts = []
        for line in lines[:10]:  # Check first 10 lines
            if line.strip():
                column_counts.append(line.count(delimiter) + 1)
        
        if not column_counts:
            return False
        
        # Check if most lines have the same number of columns
        most_common_count = max(set(column_counts), key=column_counts.count)
        matching_lines = sum(1 for count in column_counts if count == most_common_count)
        
        return matching_lines >= len(column_counts) * 0.8  # 80% consistency threshold
    
    def _detect_header(self, lines: List[str], delimiter: str) -> bool:
        """Detect if the first line contains headers"""
        if len(lines) < 2:
            return True  # Default to true if not enough data
        
        try:
            # Split first two lines
            first_line = lines[0].split(delimiter)
            second_line = lines[1].split(delimiter)
            
            if len(first_line) != len(second_line):
                return True  # Different column counts, likely has header
            
            # Check if first line contains non-numeric values while second line contains numeric
            numeric_pattern = re.compile(r'^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$')
            
            first_numeric = sum(1 for cell in first_line if numeric_pattern.match(cell.strip()))
            second_numeric = sum(1 for cell in second_line if numeric_pattern.match(cell.strip()))
            
            # If first line has fewer numeric values, it's likely a header
            return first_numeric < second_numeric
            
        except Exception:
            return True  # Default to true on error
    
    def _detect_nested_columns(self, lines: List[str], delimiter: str) -> Dict[str, Any]:
        """Detect columns that contain comma-separated values"""
        nested_columns = {}
        
        if not lines:
            return nested_columns
        
        try:
            # Sample a few lines to analyze
            sample_lines = lines[1:6] if len(lines) > 1 else lines[:5]
            
            for line_idx, line in enumerate(sample_lines):
                if not line.strip():
                    continue
                    
                cells = line.split(delimiter)
                
                for col_idx, cell in enumerate(cells):
                    cell = cell.strip().strip('"\'')  # Remove quotes
                    
                    # Check if cell contains comma-separated values
                    if ',' in cell and cell != '':
                        # Split by comma and check if it looks like separate values
                        parts = [part.strip() for part in cell.split(',')]
                        
                        # Skip if it's likely a decimal number or single value
                        if len(parts) > 1 and not self._is_decimal_number(cell):
                            col_name = f"Column_{col_idx}"
                            
                            if col_name not in nested_columns:
                                nested_columns[col_name] = {
                                    'index': col_idx,
                                    'sample_values': [],
                                    'separator': ',',
                                    'suggested_split': True,
                                    'estimated_sub_columns': len(parts)
                                }
                            
                            nested_columns[col_name]['sample_values'].append(cell)
                            
                            # Update estimated sub-columns based on max parts seen
                            if len(parts) > nested_columns[col_name]['estimated_sub_columns']:
                                nested_columns[col_name]['estimated_sub_columns'] = len(parts)
            
            return nested_columns
            
        except Exception as e:
            logger.error(f"Error detecting nested columns: {e}")
            return {}
    
    def _is_decimal_number(self, value: str) -> bool:
        """Check if a string represents a decimal number"""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def _create_sample_dataframe(self, file_path: str, delimiter: str, has_header: bool, encoding: str) -> Optional[pd.DataFrame]:
        """Create a sample DataFrame for preview with robust error handling"""
        try:
            # Try different strategies for malformed CSV files
            # Check pandas version to determine which parameters to use
            import pandas as pd
            pandas_version = pd.__version__
            major_version = int(pandas_version.split('.')[0])
            minor_version = int(pandas_version.split('.')[1])
            
            # Use appropriate parameters based on pandas version
            if major_version > 1 or (major_version == 1 and minor_version >= 3):
                # Newer pandas versions (1.3+) use on_bad_lines
                strategies = [
                    # Strategy 1: Standard parsing with skip bad lines
                    {
                        'on_bad_lines': 'skip'
                    },
                    # Strategy 2: Force consistent columns by filling missing values
                    {
                        'on_bad_lines': 'skip',
                        'skipinitialspace': True
                    },
                    # Strategy 3: Use Python engine with more flexibility
                    {
                        'engine': 'python',
                        'on_bad_lines': 'skip'
                    }
                ]
            else:
                # Older pandas versions use error_bad_lines and warn_bad_lines
                strategies = [
                    # Strategy 1: Standard parsing
                    {
                        'error_bad_lines': False,
                        'warn_bad_lines': False
                    },
                    # Strategy 2: Force consistent columns by filling missing values
                    {
                        'error_bad_lines': False,
                        'warn_bad_lines': False,
                        'skipinitialspace': True
                    },
                    # Strategy 3: Use Python engine with more flexibility
                    {
                        'engine': 'python',
                        'error_bad_lines': False,
                        'warn_bad_lines': False
                    }
                ]
            
            for i, strategy in enumerate(strategies):
                try:
                    logger.info(f"Attempting CSV parsing strategy {i+1}")
                    
                    # Combine base parameters with strategy
                    params = {
                        'delimiter': delimiter,
                        'header': 0 if has_header else None,
                        'encoding': encoding,
                        'nrows': 10,
                        **strategy
                    }
                    
                    # Use version-appropriate parameters
                    df = pd.read_csv(file_path, **params)
                    
                    if not df.empty:
                        logger.info(f"Successfully parsed CSV with strategy {i+1}: {len(df)} rows, {len(df.columns)} columns")
                        return df
                        
                except Exception as strategy_error:
                    logger.warning(f"Strategy {i+1} failed: {strategy_error}")
                    continue
            
            # If all strategies fail, try reading just the first few lines manually
            logger.warning("All automated strategies failed, attempting manual parsing")
            return self._manual_csv_parse(file_path, delimiter, has_header, encoding)
            
        except Exception as e:
            logger.error(f"Error creating sample DataFrame: {e}")
            return None
    
    def _manual_csv_parse(self, file_path: str, delimiter: str, has_header: bool, encoding: str) -> Optional[pd.DataFrame]:
        """Manually parse CSV when automated methods fail"""
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                lines = []
                for i, line in enumerate(f):
                    if i >= 10:  # Only read first 10 lines
                        break
                    lines.append(line.strip())
            
            if not lines:
                return None
            
            # Find the most common number of columns
            column_counts = []
            for line in lines:
                if line:
                    column_counts.append(len(line.split(delimiter)))
            
            if not column_counts:
                return None
            
            # Use the most frequent column count
            most_common_count = max(set(column_counts), key=column_counts.count)
            
            # Parse lines and pad/truncate to consistent length
            data_rows = []
            headers = None
            
            for i, line in enumerate(lines):
                if not line:
                    continue
                    
                parts = line.split(delimiter)
                
                # Pad or truncate to consistent length
                if len(parts) < most_common_count:
                    parts.extend([''] * (most_common_count - len(parts)))
                elif len(parts) > most_common_count:
                    parts = parts[:most_common_count]
                
                if i == 0 and has_header:
                    headers = [f"col_{j}" if not part.strip() else part.strip() for j, part in enumerate(parts)]
                else:
                    data_rows.append(parts)
            
            if not data_rows:
                return None
            
            # Create DataFrame
            if headers:
                df = pd.DataFrame(data_rows, columns=headers)
            else:
                df = pd.DataFrame(data_rows, columns=[f"Column_{i+1}" for i in range(most_common_count)])
            
            # Replace empty strings with None to avoid conversion issues
            df = df.replace('', None)
            
            logger.info(f"Successfully manually parsed CSV: {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Manual CSV parsing failed: {e}")
            return None
    
    def _generate_parsing_suggestions(self, nested_columns: Dict[str, Any], sample_df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
        """Generate parsing suggestions based on analysis"""
        suggestions = []
        
        if nested_columns:
            suggestions.append({
                'type': 'column_splitting',
                'message': f"Found {len(nested_columns)} columns with comma-separated values",
                'columns': list(nested_columns.keys()),
                'action': 'split_columns'
            })
        
        if sample_df is not None:
            # Check for columns that might be dates
            for col in sample_df.columns:
                sample_values = sample_df[col].dropna().astype(str).head(3).tolist()
                if self._might_be_date_column(sample_values):
                    suggestions.append({
                        'type': 'date_parsing',
                        'message': f"Column '{col}' might contain dates",
                        'column': col,
                        'action': 'parse_dates'
                    })
        
        return suggestions
    
    def _might_be_date_column(self, sample_values: List[str]) -> bool:
        """Check if sample values might represent dates"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'\d{2}/\d{2}/\d{2}',  # MM/DD/YY
        ]
        
        for value in sample_values:
            for pattern in date_patterns:
                if re.search(pattern, str(value)):
                    return True
        return False
    
    def process_csv_with_options(self, file_path: str, parsing_options: Dict[str, Any]) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """
        Process CSV file with advanced parsing options
        
        Args:
            file_path: Path to the CSV file
            parsing_options: Dictionary with parsing options
            
        Returns:
            Tuple of (success, dataframe, message)
        """
        try:
            # Extract parsing options
            delimiter = parsing_options.get('delimiter', ',')
            has_header = parsing_options.get('has_header', True)
            encoding = parsing_options.get('encoding', 'utf-8')
            split_columns = parsing_options.get('split_columns', {})
            parse_dates = parsing_options.get('parse_dates', [])
            
            # Read the CSV file
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                header=0 if has_header else None,
                encoding=encoding
            )
            
            logger.info(f"Successfully loaded CSV with {len(df)} rows and {len(df.columns)} columns")
            
            # Apply column splitting if requested
            if split_columns:
                df = self._split_columns(df, split_columns)
                logger.info(f"Applied column splitting, now have {len(df.columns)} columns")
            
            # Apply date parsing if requested
            if parse_dates:
                df = self._parse_dates(df, parse_dates)
                logger.info(f"Applied date parsing to {len(parse_dates)} columns")
            
            # Clean column names
            df.columns = [self._clean_column_name(col) for col in df.columns]
            
            return True, df, f"Successfully processed CSV with {len(df)} rows and {len(df.columns)} columns"
            
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            return False, None, f"Error processing CSV: {str(e)}"
    
    def _split_columns(self, df: pd.DataFrame, split_columns: Dict[str, Any]) -> pd.DataFrame:
        """Split columns that contain comma-separated values"""
        for col_name, split_options in split_columns.items():
            if col_name not in df.columns:
                continue
            
            separator = split_options.get('separator', ',')
            max_splits = split_options.get('max_splits', None)
            
            # Split the column
            split_data = df[col_name].astype(str).str.split(separator, expand=True)
            
            # Limit splits if specified
            if max_splits and split_data.shape[1] > max_splits:
                split_data = split_data.iloc[:, :max_splits]
            
            # Generate new column names
            new_col_names = []
            for i in range(split_data.shape[1]):
                new_col_names.append(f"{col_name}_{i+1}")
            
            split_data.columns = new_col_names
            
            # Remove original column and add new ones
            df = df.drop(columns=[col_name])
            df = pd.concat([df, split_data], axis=1)
        
        return df
    
    def _parse_dates(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """Parse specified columns as dates"""
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logger.info(f"Successfully parsed column '{col}' as datetime")
                except Exception as e:
                    logger.warning(f"Could not parse column '{col}' as datetime: {e}")
        
        return df
    
    def _clean_column_name(self, col_name: str) -> str:
        """Clean column name to be SQL-friendly"""
        # Convert to string and strip
        col_str = str(col_name).strip()
        
        # Replace spaces and special characters with underscores
        col_str = re.sub(r'[^a-zA-Z0-9_]', '_', col_str)
        
        # Remove multiple consecutive underscores
        col_str = re.sub(r'_+', '_', col_str)
        
        # Remove leading/trailing underscores
        col_str = col_str.strip('_')
        
        # Ensure it doesn't start with a number
        if col_str and col_str[0].isdigit():
            col_str = f"col_{col_str}"
        
        # Ensure it's not empty
        if not col_str:
            col_str = "unnamed_column"
        
        return col_str
    
    def create_parsing_preview(self, file_path: str, parsing_options: Dict[str, Any]) -> Dict[str, Any]:
        """Create a preview of how the CSV will be parsed"""
        try:
            # Process with options but limit to first 20 rows
            success, df, message = self.process_csv_with_options(file_path, parsing_options)
            
            if not success:
                return {
                    'success': False,
                    'error': message
                }
            
            # Create preview data with NaN handling
            df_clean = df.copy()
            df_clean = df_clean.where(pd.notnull(df_clean), None)
            preview_data = df_clean.head(20).to_dict('records')
            
            # Generate column information
            column_info = []
            for col in df.columns:
                col_data = df[col]
                sample_values = col_data.dropna().head(3).tolist()
                # Clean sample values for JSON serialization
                sample_values = [None if pd.isna(val) else val for val in sample_values]
                
                column_info.append({
                    'name': col,
                    'type': str(col_data.dtype),
                    'non_null_count': int(col_data.count()),
                    'null_count': int(col_data.isnull().sum()),
                    'unique_count': int(col_data.nunique()),
                    'sample_values': sample_values
                })
            
            response = {
                'success': True,
                'preview_data': preview_data,
                'column_info': column_info,
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'message': message
            }
            
            return self._safe_json_serialize(response)
            
        except Exception as e:
            logger.error(f"Error creating parsing preview: {e}")
            error_response = {
                'success': False,
                'error': str(e)
            }
            return self._safe_json_serialize(error_response) 