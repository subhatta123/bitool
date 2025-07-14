"""
Table Name Helper Utility for ConvaBI
Centralized table name management to ensure consistency across all services
"""

import re
import logging
from typing import Optional, Dict, Any, List
from django.utils.text import slugify

logger = logging.getLogger(__name__)

class TableNameManager:
    """Centralized table name management for consistent naming across services"""
    
    @staticmethod
    def generate_safe_table_name(source_id: str) -> str:
        """
        Generate a safe table name from source ID
        Always applies the 'source_' prefix exactly once
        """
        try:
            # Clean the source ID to be DuckDB-safe
            cleaned_id = re.sub(r'[^a-zA-Z0-9_]', '_', str(source_id))
            
            # Ensure it doesn't start with a number
            if cleaned_id and cleaned_id[0].isdigit():
                cleaned_id = f"id_{cleaned_id}"
            
            # Apply prefix exactly once
            safe_name = f"source_{cleaned_id}"
            
            logger.debug(f"Generated safe table name: {source_id} -> {safe_name}")
            return safe_name
            
        except Exception as e:
            logger.error(f"Error generating safe table name for {source_id}: {e}")
            return f"source_unknown_{hash(str(source_id)) % 10000}"
    
    @staticmethod
    def validate_table_name(name: str) -> bool:
        """
        Validate that a table name is safe for use in SQL queries
        """
        if not name:
            return False
        
        # Check for SQL injection patterns
        dangerous_patterns = [
            r'drop\s+table', r'delete\s+from', r'insert\s+into',
            r'update\s+set', r'alter\s+table', r'create\s+table',
            r'--', r'/\*', r'\*/', r';'
        ]
        
        name_lower = name.lower()
        for pattern in dangerous_patterns:
            if re.search(pattern, name_lower):
                return False
        
        # Check for valid identifier format
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            return False
        
        return True
    
    @staticmethod
    def get_integrated_table_name(data_source) -> str:
        """
        Get the correct table name for a data source in the integrated database
        Uses the table_name field if available, otherwise generates from ID
        """
        try:
            # First try to use the stored table_name field
            if hasattr(data_source, 'table_name') and data_source.table_name:
                stored_name = data_source.table_name
                if TableNameManager.validate_table_name(stored_name):
                    logger.debug(f"Using stored table name: {stored_name}")
                    return stored_name
            
            # Fallback to generating from source ID
            generated_name = TableNameManager.generate_safe_table_name(str(data_source.id))
            logger.debug(f"Generated table name from ID: {generated_name}")
            return generated_name
            
        except Exception as e:
            logger.error(f"Error getting integrated table name: {e}")
            return f"source_error_{hash(str(data_source.id)) % 10000}"
    
    @staticmethod
    def normalize_column_name(column_name: str) -> str:
        """
        Normalize column names to be SQL-safe
        """
        try:
            # Convert to string and clean
            col_str = str(column_name).strip()
            
            # Handle empty or invalid names
            if not col_str or col_str.lower() in ['unnamed', 'unnamed:']:
                return "unnamed_column"
            
            # Make SQL-safe by replacing spaces and special characters
            col_str = re.sub(r'[^a-zA-Z0-9_]', '_', col_str)
            
            # Ensure it doesn't start with a number
            if col_str and col_str[0].isdigit():
                col_str = f"col_{col_str}"
            
            # Ensure minimum length
            if not col_str:
                col_str = "unknown_column"
            
            return col_str
            
        except Exception as e:
            logger.error(f"Error normalizing column name '{column_name}': {e}")
            return "error_column"
    
    @staticmethod
    def check_table_exists(connection, table_name: str) -> bool:
        """
        Check if a table exists in the database with enhanced error information
        """
        try:
            if not TableNameManager.validate_table_name(table_name):
                logger.warning(f"Invalid table name format: {table_name}")
                return False
            
            # For DuckDB
            if hasattr(connection, 'execute'):
                try:
                    # Use quoted table name to handle any special characters
                    quoted_name = f'"{table_name}"'
                    result = connection.execute(f"SELECT 1 FROM {quoted_name} LIMIT 1")
                    logger.debug(f"DuckDB table exists check successful for: {table_name}")
                    return True
                except Exception as duck_error:
                    logger.debug(f"DuckDB table does not exist: {table_name} - {duck_error}")
                    return False
            
            # For SQLite
            elif hasattr(connection, 'cursor'):
                cursor = connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                exists = cursor.fetchone() is not None
                logger.debug(f"SQLite table exists check for {table_name}: {exists}")
                return exists
            
            logger.warning(f"Unknown connection type for table existence check: {type(connection)}")
            return False
            
        except Exception as e:
            logger.error(f"Error checking if table exists '{table_name}': {e}")
            return False
    
    @staticmethod
    def list_all_tables(connection) -> List[str]:
        """
        Get a list of all tables in the integrated database for debugging purposes
        """
        try:
            tables = []
            
            # For DuckDB
            if hasattr(connection, 'execute'):
                try:
                    result = connection.execute("SHOW TABLES").fetchdf()
                    tables = result['name'].tolist() if 'name' in result.columns else []
                    logger.debug(f"DuckDB tables found: {tables}")
                except Exception as duck_error:
                    logger.debug(f"DuckDB SHOW TABLES failed, trying information_schema: {duck_error}")
                    try:
                        result = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchdf()
                        tables = result['table_name'].tolist() if 'table_name' in result.columns else []
                    except Exception as schema_error:
                        logger.warning(f"Both DuckDB table listing methods failed: {schema_error}")
            
            # For SQLite
            elif hasattr(connection, 'cursor'):
                cursor = connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                logger.debug(f"SQLite tables found: {tables}")
            
            return tables
            
        except Exception as e:
            logger.error(f"Error listing all tables: {e}")
            return []
    
    @staticmethod
    def get_connection_type(connection) -> str:
        """
        Identify whether the connection is DuckDB or SQLite and return appropriate metadata
        """
        try:
            if hasattr(connection, 'execute'):
                # Try DuckDB-specific query
                try:
                    connection.execute("SELECT version()").fetchone()
                    return "DuckDB"
                except:
                    return "DuckDB (assumed)"
            elif hasattr(connection, 'cursor'):
                return "SQLite"
            else:
                return f"Unknown ({type(connection).__name__})"
        except Exception as e:
            logger.error(f"Error detecting connection type: {e}")
            return f"Error detecting type: {str(e)}"
    
    @staticmethod
    def get_table_info(connection, table_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a table including structure and row count
        """
        try:
            if not TableNameManager.validate_table_name(table_name):
                return {'exists': False, 'error': 'Invalid table name format'}
            
            info = {'exists': False, 'connection_type': TableNameManager.get_connection_type(connection)}
            
            # For DuckDB
            if hasattr(connection, 'execute'):
                try:
                    # Check if table exists and get structure
                    quoted_name = f'"{table_name}"'
                    
                    # Get table structure
                    result = connection.execute(f"DESCRIBE {quoted_name}").fetchdf()
                    info.update({
                        'exists': True,
                        'columns': result['column_name'].tolist(),
                        'types': dict(zip(result['column_name'], result['column_type']))
                    })
                    
                    # Get row count
                    try:
                        count_result = connection.execute(f"SELECT COUNT(*) as row_count FROM {quoted_name}").fetchone()
                        info['row_count'] = count_result[0] if count_result else 0
                    except Exception as count_error:
                        logger.warning(f"Could not get row count for {table_name}: {count_error}")
                        info['row_count'] = -1
                        
                except Exception as duck_error:
                    logger.debug(f"DuckDB table info failed for {table_name}: {duck_error}")
                    return {'exists': False, 'error': str(duck_error), 'connection_type': 'DuckDB'}
            
            # For SQLite
            elif hasattr(connection, 'cursor'):
                cursor = connection.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                if columns:
                    info.update({
                        'exists': True,
                        'columns': [col[1] for col in columns],
                        'types': {col[1]: col[2] for col in columns}
                    })
                    
                    # Get row count
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count_result = cursor.fetchone()
                        info['row_count'] = count_result[0] if count_result else 0
                    except Exception as count_error:
                        logger.warning(f"Could not get row count for {table_name}: {count_error}")
                        info['row_count'] = -1
                else:
                    return {'exists': False, 'connection_type': 'SQLite'}
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting table info for '{table_name}': {e}")
            return {'exists': False, 'error': str(e)}
    
    @staticmethod
    def get_debugging_info(connection) -> Dict[str, Any]:
        """
        Get comprehensive debugging information about the database connection
        """
        try:
            debug_info = {
                'connection_type': TableNameManager.get_connection_type(connection),
                'tables': TableNameManager.list_all_tables(connection),
                'total_tables': 0,
                'table_details': {}
            }
            
            debug_info['total_tables'] = len(debug_info['tables'])
            
            # Get details for each table (limit to first 10 to avoid overwhelming output)
            for table_name in debug_info['tables'][:10]:
                debug_info['table_details'][table_name] = TableNameManager.get_table_info(connection, table_name)
            
            return debug_info
            
        except Exception as e:
            logger.error(f"Error getting debugging info: {e}")
            return {'error': str(e), 'connection_type': 'unknown'}

# Convenience functions for backward compatibility
def generate_safe_table_name(source_id: str) -> str:
    """Generate safe table name - convenience function"""
    return TableNameManager.generate_safe_table_name(source_id)

def validate_table_name(name: str) -> bool:
    """Validate table name - convenience function"""
    return TableNameManager.validate_table_name(name)

def get_integrated_table_name(data_source) -> str:
    """Get integrated table name - convenience function"""
    return TableNameManager.get_integrated_table_name(data_source)

def normalize_column_name(column_name: str) -> str:
    """Normalize column name - convenience function"""
    return TableNameManager.normalize_column_name(column_name) 