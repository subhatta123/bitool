"""
Dynamic Naming System for ConvaBI
Replaces all hardcoded table and column names with dynamic, configurable naming
"""

import os
import uuid
from typing import Dict, List, Optional, Any
from django.conf import settings
from django.db import connection
import logging

logger = logging.getLogger(__name__)


class DynamicNamingManager:
    """
    Manages dynamic table and column naming throughout the application
    Eliminates hardcoded references like 'csv_data', 'superstore_data', etc.
    """
    
    def __init__(self):
        self.table_prefix = getattr(settings, 'TABLE_PREFIX', 'ds')
        self.unified_table_name = getattr(settings, 'UNIFIED_TABLE_NAME', 'unified_data_storage')
        self._schema_cache = {}
    
    def generate_table_name(self, data_source_id: str, table_type: str = 'main') -> str:
        """
        Generate dynamic table name based on data source ID
        
        Args:
            data_source_id: Unique identifier for data source
            table_type: Type of table (main, temp, backup, etc.)
            
        Returns:
            Dynamic table name
        """
        if isinstance(data_source_id, uuid.UUID):
            data_source_id = str(data_source_id)
        
        # Clean the ID for table name usage
        clean_id = data_source_id.replace('-', '_').replace(' ', '_').lower()
        
        if table_type == 'main':
            return f"{self.table_prefix}_{clean_id}"
        else:
            return f"{self.table_prefix}_{table_type}_{clean_id}"
    
    def get_data_source_table_name(self, data_source) -> str:
        """Get the main table name for a data source"""
        return self.generate_table_name(data_source.id)
    
    def get_unified_table_name(self) -> str:
        """Get the unified data storage table name"""
        return self.unified_table_name
    
    def get_table_schema(self, table_name: str, connection_type: str = 'duckdb') -> Optional[Dict[str, Any]]:
        """
        Get dynamic schema information for a table
        
        Args:
            table_name: Name of the table
            connection_type: Type of connection (duckdb, postgresql)
            
        Returns:
            Schema information with column names and types
        """
        cache_key = f"{connection_type}_{table_name}"
        
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]
        
        try:
            if connection_type == 'duckdb':
                schema_info = self._get_duckdb_schema(table_name)
            elif connection_type == 'postgresql':
                schema_info = self._get_postgresql_schema(table_name)
            else:
                logger.error(f"[ERROR] Unsupported connection type: {connection_type}")
                return None
            
            self._schema_cache[cache_key] = schema_info
            return schema_info
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to get schema for {table_name}: {e}")
            return None
    
    def _get_duckdb_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get schema from DuckDB"""
        import duckdb
        
        # Use INTEGRATED_DB_PATH if available, otherwise construct from DUCKDB_PATH
        db_path = getattr(settings, 'INTEGRATED_DB_PATH', None)
        if not db_path:
            duckdb_path = getattr(settings, 'DUCKDB_PATH', 'data')
            if duckdb_path == ':memory:':
                db_path = ':memory:'
            else:
                db_path = os.path.join(settings.BASE_DIR, duckdb_path, 'integrated.duckdb')
        
        try:
            conn = duckdb.connect(db_path)
            
            # Check if table exists
            tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name = ?", [table_name]).fetchall()
            
            if not tables:
                logger.warning(f"[WARNING] Table {table_name} not found in DuckDB")
                return None
            
            # Get column information
            columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
            
            schema_info = {
                'table_name': table_name,
                'columns': {},
                'column_list': [],
                'column_types': {},
            }
            
            for col_info in columns:
                col_name = col_info[0]
                col_type = col_info[1]
                
                schema_info['columns'][col_name] = {
                    'name': col_name,
                    'type': col_type,
                    'nullable': True  # DuckDB default
                }
                schema_info['column_list'].append(col_name)
                schema_info['column_types'][col_name] = col_type
            
            conn.close()
            return schema_info
            
        except Exception as e:
            logger.error(f"[ERROR] DuckDB schema error: {e}")
            return None
    
    def _get_postgresql_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get schema from PostgreSQL"""
        try:
            with connection.cursor() as cursor:
                # Check if table exists
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_name = %s AND table_schema = 'public'
                """, [table_name])
                
                if not cursor.fetchone():
                    logger.warning(f"[WARNING] Table {table_name} not found in PostgreSQL")
                    return None
                
                # Get column information
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position
                """, [table_name])
                
                columns = cursor.fetchall()
                
                schema_info = {
                    'table_name': table_name,
                    'columns': {},
                    'column_list': [],
                    'column_types': {},
                }
                
                for col_name, col_type, is_nullable in columns:
                    schema_info['columns'][col_name] = {
                        'name': col_name,
                        'type': col_type,
                        'nullable': is_nullable == 'YES'
                    }
                    schema_info['column_list'].append(col_name)
                    schema_info['column_types'][col_name] = col_type
                
                return schema_info
                
        except Exception as e:
            logger.error(f"[ERROR] PostgreSQL schema error: {e}")
            return None
    
    def get_available_tables(self, connection_type: str = 'duckdb') -> List[str]:
        """
        Get list of all available tables
        
        Args:
            connection_type: Type of connection (duckdb, postgresql)
            
        Returns:
            List of table names
        """
        try:
            if connection_type == 'duckdb':
                return self._get_duckdb_tables()
            elif connection_type == 'postgresql':
                return self._get_postgresql_tables()
            else:
                logger.error(f"[ERROR] Unsupported connection type: {connection_type}")
                return []
                
        except Exception as e:
            logger.error(f"[ERROR] Failed to get tables: {e}")
            return []
    
    def _get_duckdb_tables(self) -> List[str]:
        """Get tables from DuckDB"""
        import duckdb
        
        # Use INTEGRATED_DB_PATH if available, otherwise construct from DUCKDB_PATH
        db_path = getattr(settings, 'INTEGRATED_DB_PATH', None)
        if not db_path:
            duckdb_path = getattr(settings, 'DUCKDB_PATH', 'data')
            if duckdb_path == ':memory:':
                db_path = ':memory:'
            else:
                db_path = os.path.join(settings.BASE_DIR, duckdb_path, 'integrated.duckdb')
        
        try:
            conn = duckdb.connect(db_path)
            tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
            conn.close()
            
            return [table[0] for table in tables]
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to get DuckDB tables: {e}")
            return []
    
    def _get_postgresql_tables(self) -> List[str]:
        """Get tables from PostgreSQL"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """)
                
                tables = cursor.fetchall()
                return [table[0] for table in tables]
                
        except Exception as e:
            logger.error(f"[ERROR] Failed to get PostgreSQL tables: {e}")
            return []
    
    def find_table_for_data_source(self, data_source, connection_type: str = 'duckdb') -> Optional[str]:
        """
        Find the actual table name for a data source by trying multiple strategies
        
        Args:
            data_source: DataSource model instance
            connection_type: Type of connection
            
        Returns:
            Actual table name if found, None otherwise
        """
        # Strategy 1: Use the expected table name
        expected_table = self.get_data_source_table_name(data_source)
        
        available_tables = self.get_available_tables(connection_type)
        
        if expected_table in available_tables:
            return expected_table
        
        # Strategy 2: Try legacy naming patterns
        legacy_patterns = [
            f"source_{data_source.id.hex.replace('-', '_')}",
            f"data_{data_source.name.lower().replace(' ', '_').replace('-', '_')}",
            data_source.name.lower().replace(' ', '_').replace('-', '_'),
        ]
        
        for pattern in legacy_patterns:
            if pattern in available_tables:
                logger.info(f"[FOUND] Using legacy table name: {pattern}")
                return pattern
        
        # Strategy 3: Try to find by data source name similarity
        data_source_name_clean = data_source.name.lower().replace(' ', '').replace('_', '').replace('-', '')
        
        for table_name in available_tables:
            table_clean = table_name.lower().replace(' ', '').replace('_', '').replace('-', '')
            if data_source_name_clean in table_clean or table_clean in data_source_name_clean:
                logger.info(f"[FOUND] Using similar table name: {table_name}")
                return table_name
        
        logger.warning(f"[NOT_FOUND] No table found for data source: {data_source.name}")
        return None
    
    def create_column_mapping(self, table_schema: Dict[str, Any]) -> Dict[str, str]:
        """
        Create intelligent column mapping for queries
        
        Args:
            table_schema: Schema information from get_table_schema
            
        Returns:
            Dictionary mapping semantic names to actual column names
        """
        if not table_schema or 'column_list' not in table_schema:
            return {}
        
        column_list = table_schema['column_list']
        mapping = {}
        
        # Common semantic mappings
        semantic_patterns = {
            'customer_name': ['customer_name', 'customer', 'name', 'customer_id'],
            'sales': ['sales', 'amount', 'revenue', 'total', 'value'],
            'region': ['region', 'area', 'location', 'territory'],
            'product_name': ['product_name', 'product', 'item', 'product_id'],
            'order_id': ['order_id', 'order', 'id', 'transaction_id'],
            'quantity': ['quantity', 'qty', 'count', 'amount'],
            'profit': ['profit', 'margin', 'earnings'],
            'category': ['category', 'type', 'class', 'group'],
            'date': ['date', 'order_date', 'created_at', 'timestamp'],
            'price': ['price', 'cost', 'unit_price'],
        }
        
        # Map columns based on patterns
        for semantic_name, patterns in semantic_patterns.items():
            for column in column_list:
                column_lower = column.lower()
                for pattern in patterns:
                    if pattern in column_lower:
                        mapping[semantic_name] = column
                        break
                if semantic_name in mapping:
                    break
        
        # Add direct mappings for all columns
        for column in column_list:
            mapping[column.lower()] = column
            mapping[column] = column
        
        return mapping
    
    def clear_cache(self):
        """Clear the schema cache"""
        self._schema_cache = {}
        logger.info("[INFO] Schema cache cleared")
    
    def find_column_by_pattern(self, column_list: List[str], patterns: List[str]) -> Optional[str]:
        """
        Find column that matches any of the given patterns
        
        Args:
            column_list: List of column names to search
            patterns: List of patterns to match against
            
        Returns:
            First matching column name or None
        """
        for col in column_list:
            col_lower = col.lower()
            for pattern in patterns:
                if pattern.lower() in col_lower:
                    return col
        return None


# Global instance
dynamic_naming = DynamicNamingManager() 