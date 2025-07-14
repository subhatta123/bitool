"""
PostgreSQL Data Service for Unified Storage
Handles CSV uploads and data operations directly in PostgreSQL
"""

import json
import pandas as pd
import numpy as np
import uuid
from django.db import connection
from django.utils import timezone
from typing import Dict, List, Any, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class PostgreSQLDataService:
    """Service for managing data in PostgreSQL unified storage"""
    
    def __init__(self):
        self.table_name = 'unified_data_storage'
    
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
    
    def upload_csv_data(self, file_data: bytes, filename: str, user_id: int) -> Tuple[bool, str, Optional[Dict]]:
        """
        Upload CSV data directly to PostgreSQL unified storage
        
        Args:
            file_data: CSV file bytes
            filename: Original filename
            user_id: User ID
            
        Returns:
            Tuple of (success, message, data_info)
        """
        try:
            # Read CSV data
            from io import StringIO
            df = pd.read_csv(StringIO(file_data.decode('utf-8')))
            
            if df.empty:
                return False, "CSV file is empty", None
            
            # Clean data thoroughly (handle NaN, inf, and other edge cases)
            df_cleaned = df.copy()
            
            # Replace NaN, inf, -inf with None/null values
            df_cleaned = df_cleaned.replace([np.nan, np.inf, -np.inf], None)
            
            # Ensure all object columns are strings (handle mixed types)
            for col in df_cleaned.columns:
                if df_cleaned[col].dtype == 'object':
                    df_cleaned[col] = df_cleaned[col].astype(str)
                    # Replace 'None' strings back to actual None
                    df_cleaned[col] = df_cleaned[col].replace('None', None)
            
            # Create data source name from filename
            data_source_name = filename.replace('.csv', '').replace('_', ' ').replace('-', ' ').title()
            
            # Create table name
            table_name = f'csv_{filename.lower().replace(".csv", "").replace(" ", "_").replace("-", "_")}'
            
            # Prepare JSON data using safe serialization
            json_data = self._safe_json_serialize(df_cleaned)
            
            # Create schema info with safe serialization
            schema_info = {
                'columns': [
                    {
                        'name': col,
                        'type': str(df_cleaned[col].dtype),
                        'sample_values': self._safe_json_serialize(
                            df_cleaned[col].dropna().head(3).tolist()
                        )
                    }
                    for col in df_cleaned.columns
                ],
                'row_count': len(df_cleaned),
                'column_count': len(df_cleaned.columns),
                'source_file': filename,
                'uploaded_at': timezone.now().isoformat()
            }
            
            # Insert into PostgreSQL unified storage
            with connection.cursor() as cursor:
                # Check if table already exists
                cursor.execute('SELECT id FROM unified_data_storage WHERE table_name = %s', [table_name])
                exists = cursor.fetchone()
                
                if exists:
                    # Update existing entry
                    cursor.execute("""
                        UPDATE unified_data_storage 
                        SET data = %s, schema_info = %s, row_count = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE table_name = %s
                    """, [
                        json.dumps(json_data),
                        json.dumps(schema_info),
                        len(df_cleaned),
                        table_name
                    ])
                    message = f"Updated existing dataset '{data_source_name}' with {len(df_cleaned):,} rows"
                else:
                    # Insert new entry
                    cursor.execute("""
                        INSERT INTO unified_data_storage 
                        (data_source_name, table_name, source_type, data, schema_info, row_count)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, [
                        data_source_name,
                        table_name,
                        'csv',
                        json.dumps(json_data),
                        json.dumps(schema_info),
                        len(df_cleaned)
                    ])
                    message = f"Uploaded '{data_source_name}' with {len(df_cleaned):,} rows"
            
            data_info = {
                'data_source_name': data_source_name,
                'table_name': table_name,
                'row_count': len(df_cleaned),
                'column_count': len(df_cleaned.columns),
                'columns': list(df_cleaned.columns),
                'sample_data': self._safe_json_serialize(df_cleaned.head(5))
            }
            
            logger.info(f"Successfully uploaded CSV '{filename}' to PostgreSQL: {len(df_cleaned)} rows")
            return True, message, data_info
            
        except Exception as e:
            logger.error(f"Error uploading CSV to PostgreSQL: {e}")
            return False, f"Failed to upload CSV: {str(e)}", None
    
    def get_all_datasets(self, user_id: Optional[int] = None) -> List[Dict]:
        """Get all datasets from unified storage"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT data_source_name, table_name, source_type, row_count, 
                           schema_info, created_at, updated_at
                    FROM unified_data_storage
                    ORDER BY created_at DESC
                """)
                
                datasets = []
                for row in cursor.fetchall():
                    datasets.append({
                        'data_source_name': row[0],
                        'table_name': row[1],
                        'source_type': row[2],
                        'row_count': row[3],
                        'schema_info': row[4],
                        'created_at': row[5],
                        'updated_at': row[6]
                    })
                
                return datasets
                
        except Exception as e:
            logger.error(f"Error getting datasets: {e}")
            return []
    
    def get_dataset_preview(self, table_name: str, limit: int = 100) -> Tuple[bool, Any]:
        """Get preview data for a dataset"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT data, schema_info FROM unified_data_storage 
                    WHERE table_name = %s
                """, [table_name])
                
                result = cursor.fetchone()
                if not result:
                    return False, "Dataset not found"
                
                data, schema_info = result
                
                # Return limited preview
                preview_data = data[:limit] if isinstance(data, list) else data
                
                return True, {
                    'data': preview_data,
                    'schema_info': schema_info,
                    'total_rows': len(data) if isinstance(data, list) else 0
                }
                
        except Exception as e:
            logger.error(f"Error getting dataset preview: {e}")
            return False, f"Error: {str(e)}"
    
    def delete_dataset(self, table_name: str) -> Tuple[bool, str]:
        """Delete a dataset from unified storage"""
        try:
            with connection.cursor() as cursor:
                cursor.execute('DELETE FROM unified_data_storage WHERE table_name = %s', [table_name])
                
                if cursor.rowcount > 0:
                    logger.info(f"Deleted dataset with table_name: {table_name}")
                    return True, "Dataset deleted successfully"
                else:
                    return False, "Dataset not found"
                    
        except Exception as e:
            logger.error(f"Error deleting dataset: {e}")
            return False, f"Failed to delete dataset: {str(e)}"
    
    def query_dataset(self, table_name: str, sql_query: str) -> Tuple[bool, Any]:
        """Execute a query against a dataset"""
        try:
            with connection.cursor() as cursor:
                # First get the data
                cursor.execute("""
                    SELECT data FROM unified_data_storage 
                    WHERE table_name = %s
                """, [table_name])
                
                result = cursor.fetchone()
                if not result:
                    return False, "Dataset not found"
                
                data = result[0]
                
                # Convert to DataFrame for querying
                df = pd.DataFrame(data)
                
                # This is a simplified query execution
                # In a real implementation, you'd want proper SQL parsing
                # For now, return the full dataset
                return True, df.to_dict('records')
                
        except Exception as e:
            logger.error(f"Error querying dataset: {e}")
            return False, f"Query error: {str(e)}"
    
    def get_dataset_stats(self) -> Dict[str, Any]:
        """Get statistics about all datasets"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_datasets,
                        SUM(row_count) as total_rows,
                        COUNT(DISTINCT source_type) as source_types
                    FROM unified_data_storage
                """)
                
                result = cursor.fetchone()
                
                if result:
                    return {
                        'total_datasets': result[0] or 0,
                        'total_rows': result[1] or 0,
                        'source_types': result[2] or 0
                    }
                else:
                    return {'total_datasets': 0, 'total_rows': 0, 'source_types': 0}
                
        except Exception as e:
            logger.error(f"Error getting dataset stats: {e}")
            return {'total_datasets': 0, 'total_rows': 0, 'source_types': 0} 