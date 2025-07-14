#!/usr/bin/env python3
"""
Schema-Aware Table Validation Service for DuckDB
Handles schema qualification and multiple naming patterns
"""

import re
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class SchemaAwareTableResult:
    """Result of schema-aware table validation"""
    is_valid: bool
    qualified_table_name: Optional[str] = None  # e.g., "integrated.ds_abc123"
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    error_message: Optional[str] = None
    alternative_names: List[str] = field(default_factory=list)
    column_info: Dict[str, Any] = field(default_factory=dict)
    row_count: int = 0
    warnings: List[str] = field(default_factory=list)

class SchemaAwareTableService:
    """Service for schema-aware table validation and resolution"""
    
    def __init__(self):
        self.connection = None
        self.cache = {}
        self.discovered_schemas = None
        
    def set_connection(self, connection):
        """Set the database connection"""
        self.connection = connection
        # Clear cache when connection changes
        self.cache.clear()
        self.discovered_schemas = None
        
    def get_available_schemas(self) -> List[str]:
        """Get list of available schemas in the database"""
        if self.discovered_schemas is not None:
            return self.discovered_schemas
        
        if not self.connection:
            return ['main']  # Default fallback
        
        try:
            schemas_result = self.connection.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
            self.discovered_schemas = [schema[0] for schema in schemas_result]
            logger.info(f"Discovered schemas: {self.discovered_schemas}")
            return self.discovered_schemas
        except Exception as e:
            logger.warning(f"Could not discover schemas, using defaults: {e}")
            self.discovered_schemas = ['main', 'integrated']  # Common defaults
            return self.discovered_schemas
    
    def get_all_table_patterns_with_schemas(self, data_source_id: str) -> List[str]:
        """Generate all possible table patterns with schema qualification"""
        patterns = []
        
        # Base patterns without schema
        uuid_clean = str(data_source_id).replace('-', '')
        uuid_underscores = str(data_source_id).replace('-', '_')
        
        base_patterns = [
            f"ds_{uuid_clean}",
            f"ds_{uuid_underscores}",
            f"source_id_{uuid_underscores}",
            f"source_{uuid_clean}",
            f"source_{uuid_underscores}",
        ]
        
        # Get available schemas
        schemas = self.get_available_schemas()
        
        # Create fully qualified patterns for each schema
        for schema in schemas:
            for base_pattern in base_patterns:
                patterns.append(f"{schema}.{base_pattern}")
        
        # Also include unqualified patterns (for backward compatibility)
        patterns.extend(base_patterns)
        
        return patterns
    
    def validate_table_with_schema_resolution(self, data_source_id: str) -> SchemaAwareTableResult:
        """Validate table existence with automatic schema resolution"""
        
        cache_key = f"schema_table_{data_source_id}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        if not self.connection:
            return SchemaAwareTableResult(
                is_valid=False,
                error_message="Database connection not available"
            )
        
        try:
            logger.info(f"Validating table for data source {data_source_id} with schema resolution")
            
            # Get all possible patterns with schemas
            patterns = self.get_all_table_patterns_with_schemas(data_source_id)
            logger.info(f"Checking {len(patterns)} schema-qualified patterns")
            
            # Try each pattern
            for pattern in patterns:
                try:
                    # Check if table exists using information_schema
                    schema_name = None
                    table_name = None
                    
                    if '.' in pattern:
                        schema_name, table_name = pattern.split('.', 1)
                    else:
                        table_name = pattern
                        schema_name = 'main'  # Default schema
                    
                    # Check existence using information_schema
                    result = self.connection.execute("""
                        SELECT table_name FROM information_schema.tables 
                        WHERE table_schema = ? AND table_name = ?
                    """, [schema_name, table_name]).fetchone()
                    
                    if result:
                        logger.info(f"[SUCCESS] Found table: {pattern}")
                        
                        # Get table metadata
                        column_info = self._get_table_metadata_qualified(pattern)
                        row_count = self._get_table_row_count_qualified(pattern)
                        
                        validation_result = SchemaAwareTableResult(
                            is_valid=True,
                            qualified_table_name=pattern,
                            schema_name=schema_name,
                            table_name=table_name,
                            column_info=column_info,
                            row_count=row_count,
                            alternative_names=[p for p in patterns if p != pattern]
                        )
                        
                        # Cache the result
                        self.cache[cache_key] = validation_result
                        return validation_result
                        
                except Exception as e:
                    logger.debug(f"Error checking pattern {pattern}: {e}")
                    continue
            
            # No table found - get available tables for debugging
            available_tables = self._get_available_tables_all_schemas()
            
            validation_result = SchemaAwareTableResult(
                is_valid=False,
                error_message=f"No table found for data source {data_source_id} in any schema. Checked {len(patterns)} patterns.",
                alternative_names=available_tables[:10]  # Show first 10 available tables
            )
            
            # Cache the result
            self.cache[cache_key] = validation_result
            return validation_result
            
        except Exception as e:
            logger.error(f"Error in schema-aware table validation: {e}")
            return SchemaAwareTableResult(
                is_valid=False,
                error_message=f"Validation error: {str(e)}"
            )
    
    def validate_column_with_schema(self, qualified_table_name: str, column_name: str) -> Dict[str, Any]:
        """Validate column existence in a schema-qualified table"""
        if not self.connection:
            return {
                'exists': False,
                'error': 'Database connection not available'
            }
        
        try:
            # Use the qualified table name directly
            columns_result = self.connection.execute(f"DESCRIBE {qualified_table_name}").fetchall()
            columns = [col[0] for col in columns_result]
            
            # Check exact match
            if column_name in columns:
                return {
                    'exists': True,
                    'column_name': column_name,
                    'qualified_table_name': qualified_table_name
                }
            
            # Check case-insensitive match
            column_lower = column_name.lower()
            for col in columns:
                if col.lower() == column_lower:
                    return {
                        'exists': True,
                        'column_name': col,  # Return the actual column name
                        'qualified_table_name': qualified_table_name,
                        'case_mismatch': True
                    }
            
            return {
                'exists': False,
                'error': f"Column '{column_name}' not found in table '{qualified_table_name}'",
                'available_columns': columns[:10]  # Show first 10 columns
            }
            
        except Exception as e:
            logger.error(f"Error validating column in {qualified_table_name}: {e}")
            return {
                'exists': False,
                'error': f"Column validation error: {str(e)}"
            }
    
    def _get_table_metadata_qualified(self, qualified_table_name: str) -> Dict[str, Any]:
        """Get table metadata using qualified table name"""
        try:
            if not self.connection:
                return {}
            columns_result = self.connection.execute(f"DESCRIBE {qualified_table_name}").fetchall()
            columns = [{'name': col[0], 'type': col[1]} for col in columns_result]
            
            return {
                'columns': columns,
                'column_count': len(columns),
                'column_names': [col['name'] for col in columns]
            }
        except Exception as e:
            logger.error(f"Error getting metadata for {qualified_table_name}: {e}")
            return {}
    
    def _get_table_row_count_qualified(self, qualified_table_name: str) -> int:
        """Get table row count using qualified table name"""
        try:
            if not self.connection:
                return 0
            result = self.connection.execute(f"SELECT COUNT(*) FROM {qualified_table_name}").fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting row count for {qualified_table_name}: {e}")
            return 0
    
    def _get_available_tables_all_schemas(self) -> List[str]:
        """Get list of available tables from all schemas"""
        try:
            if not self.connection:
                return []
            
            # Get tables from all schemas with schema qualification
            tables_result = self.connection.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                ORDER BY table_schema, table_name
            """).fetchall()
            
            qualified_tables = [f"{schema}.{table}" for schema, table in tables_result]
            return qualified_tables
            
        except Exception as e:
            logger.error(f"Error getting available tables: {e}")
            return []
    
    def generate_qualified_table_name(self, data_source_id: str, preferred_schema: str = 'integrated') -> str:
        """Generate a qualified table name for new table creation"""
        uuid_clean = str(data_source_id).replace('-', '')
        base_name = f"ds_{uuid_clean}"
        return f"{preferred_schema}.{base_name}"
    
    def clear_cache(self):
        """Clear the validation cache"""
        self.cache.clear()
        self.discovered_schemas = None
        logger.info("Schema-aware table validation cache cleared")

# Global instance
schema_aware_table_service = SchemaAwareTableService() 