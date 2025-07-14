#!/usr/bin/env python3
"""
Join validation and SQL generation utility for ETL operations
"""

import re
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class JoinValidationResult:
    """Result of join validation"""
    is_valid: bool
    error_message: Optional[str] = None
    corrected_sql: Optional[str] = None
    warnings: Optional[List[str]] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []

class JoinSQLValidator:
    """Validates and generates proper JOIN SQL for DuckDB"""
    
    # Mapping from UI join types to correct SQL syntax
    JOIN_TYPE_MAPPING = {
        'inner': 'INNER JOIN',
        'left': 'LEFT JOIN', 
        'right': 'RIGHT JOIN',
        'outer': 'FULL OUTER JOIN',  # FIX: correct syntax for outer join
        'full': 'FULL OUTER JOIN',
        'full_outer': 'FULL OUTER JOIN',
        
        # Handle case variations
        'INNER': 'INNER JOIN',
        'LEFT': 'LEFT JOIN',
        'RIGHT': 'RIGHT JOIN', 
        'OUTER': 'FULL OUTER JOIN',
        'FULL': 'FULL OUTER JOIN',
        'FULL_OUTER': 'FULL OUTER JOIN'
    }
    
    # Valid join types for validation
    VALID_JOIN_TYPES = list(JOIN_TYPE_MAPPING.keys())
    
    @classmethod
    def normalize_join_type(cls, join_type: str) -> str:
        """
        Normalize join type to correct SQL syntax
        
        Args:
            join_type: Raw join type from UI (e.g., 'outer', 'INNER', etc.)
            
        Returns:
            Correct SQL join syntax (e.g., 'FULL OUTER JOIN', 'INNER JOIN')
        """
        if not join_type:
            return 'INNER JOIN'  # Default
        
        join_type_clean = join_type.strip().lower()
        
        if join_type_clean in cls.JOIN_TYPE_MAPPING:
            return cls.JOIN_TYPE_MAPPING[join_type_clean]
        
        # Try case-insensitive lookup
        for key, value in cls.JOIN_TYPE_MAPPING.items():
            if key.lower() == join_type_clean:
                return value
        
        # Default fallback
        logger.warning(f"Unknown join type '{join_type}', defaulting to INNER JOIN")
        return 'INNER JOIN'
    
    @classmethod
    def validate_table_name(cls, table_name: str) -> bool:
        """Validate table name for SQL injection prevention"""
        if not table_name:
            return False
        
        # Allow alphanumeric, underscores, and limited special characters
        pattern = r'^[a-zA-Z][a-zA-Z0-9_]*$'
        return bool(re.match(pattern, table_name))
    
    @classmethod
    def validate_column_name(cls, column_name: str) -> bool:
        """Validate column name for SQL injection prevention"""
        if not column_name:
            return False
        
        # Allow alphanumeric, underscores, spaces (will be quoted)
        # More permissive for column names as they'll be quoted
        return len(column_name) <= 255 and not any(char in column_name for char in [';', '--', '/*', '*/'])
    
    @classmethod
    def validate_table_exists(cls, table_name: str, connection) -> bool:
        """Check if table exists in DuckDB"""
        try:
            # Check if table exists
            result = connection.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """).fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False
    
    @classmethod
    def get_available_tables(cls, connection) -> List[str]:
        """Get list of available tables in DuckDB"""
        try:
            result = connection.execute("SHOW TABLES").fetchall()
            return [row[0] for row in result]
        except Exception:
            return []
    
    @classmethod
    def generate_join_sql(cls, 
                         left_table: str,
                         right_table: str, 
                         left_column: str,
                         right_column: str,
                         join_type: str,
                         output_table: str,
                         select_columns: Optional[str] = None,
                         connection=None) -> JoinValidationResult:
        """
        Generate validated JOIN SQL
        
        Args:
            left_table: Name of left table
            right_table: Name of right table  
            left_column: Join column in left table
            right_column: Join column in right table
            join_type: Type of join (inner, left, right, outer, etc.)
            output_table: Name for output table
            select_columns: Custom SELECT clause (default: SELECT *)
            
        Returns:
            JoinValidationResult with validation status and SQL
        """
        warnings = []
        
        # Validate table names
        if not cls.validate_table_name(left_table):
            return JoinValidationResult(
                is_valid=False,
                error_message=f"Invalid left table name: {left_table}"
            )
        
        if not cls.validate_table_name(right_table):
            return JoinValidationResult(
                is_valid=False,
                error_message=f"Invalid right table name: {right_table}"
            )
        
        if not cls.validate_table_name(output_table):
            return JoinValidationResult(
                is_valid=False,
                error_message=f"Invalid output table name: {output_table}"
            )
        
        # Validate table existence if connection provided
        if connection:
            if not cls.validate_table_exists(left_table, connection):
                available_tables = cls.get_available_tables(connection)
                return JoinValidationResult(
                    is_valid=False,
                    error_message=f"Left table '{left_table}' does not exist. Available tables: {', '.join(available_tables[:5])}"
                )
            
            if not cls.validate_table_exists(right_table, connection):
                available_tables = cls.get_available_tables(connection)
                return JoinValidationResult(
                    is_valid=False,
                    error_message=f"Right table '{right_table}' does not exist. Available tables: {', '.join(available_tables[:5])}"
                )
            
            # Check if output table already exists and warn
            if cls.validate_table_exists(output_table, connection):
                warnings.append(f"Output table '{output_table}' already exists and will be replaced")
        
        # Validate column names
        if not cls.validate_column_name(left_column):
            return JoinValidationResult(
                is_valid=False,
                error_message=f"Invalid left column name: {left_column}"
            )
        
        if not cls.validate_column_name(right_column):
            return JoinValidationResult(
                is_valid=False,
                error_message=f"Invalid right column name: {right_column}"
            )
        
        # Check for same table join
        if left_table == right_table:
            return JoinValidationResult(
                is_valid=False,
                error_message="Cannot join table with itself"
            )
        
        # Normalize join type
        sql_join_type = cls.normalize_join_type(join_type)
        
        # Add warning for potentially risky joins
        if sql_join_type == 'FULL OUTER JOIN':
            warnings.append("FULL OUTER JOIN may produce large result sets")
        
        # Generate SELECT clause
        if select_columns:
            select_clause = select_columns
        else:
            select_clause = "t1.*, t2.*"
        
        # Generate SQL with proper quoting
        sql = f"""CREATE OR REPLACE TABLE "{output_table}" AS
SELECT {select_clause}
FROM "{left_table}" t1
{sql_join_type} "{right_table}" t2
ON t1."{left_column}" = t2."{right_column}" """
        
        return JoinValidationResult(
            is_valid=True,
            corrected_sql=sql.strip(),
            warnings=warnings
        )
    
    @classmethod
    def test_join_syntax(cls) -> Dict[str, bool]:
        """Test all join type mappings for correctness"""
        test_results = {}
        
        for join_type in cls.VALID_JOIN_TYPES:
            try:
                result = cls.generate_join_sql(
                    left_table="test_left",
                    right_table="test_right", 
                    left_column="id",
                    right_column="ref_id",
                    join_type=join_type,
                    output_table="test_output"
                )
                test_results[join_type] = result.is_valid
                
                if result.is_valid:
                    logger.info(f"✅ JOIN type '{join_type}' -> '{cls.normalize_join_type(join_type)}'")
                else:
                    logger.error(f"❌ JOIN type '{join_type}' failed: {result.error_message}")
                    
            except Exception as e:
                test_results[join_type] = False
                logger.error(f"❌ JOIN type '{join_type}' exception: {e}")
        
        return test_results

def validate_etl_table_for_further_operations(table_name: str, connection) -> Dict[str, any]:
    """
    Validate that an ETL result table is ready for further operations
    
    Args:
        table_name: Name of ETL result table
        connection: DuckDB connection
        
    Returns:
        Dict with validation results and metadata
    """
    try:
        # Check table exists
        table_exists = connection.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0]
        
        if not table_exists:
            return {
                'valid': False,
                'error': f'Table {table_name} does not exist',
                'ready_for_operations': False
            }
        
        # Get table metadata
        columns_result = connection.execute(f"DESCRIBE {table_name}").fetchall()
        row_count = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        columns = [{'name': col[0], 'type': col[1]} for col in columns_result]
        
        # Check if table is ready for further operations
        ready_for_operations = (
            len(columns) > 0 and  # Has columns
            row_count > 0         # Has data
        )
        
        return {
            'valid': True,
            'table_name': table_name,
            'row_count': row_count,
            'column_count': len(columns),
            'columns': columns,
            'ready_for_operations': ready_for_operations,
            'can_save_to_duckdb': True  # Always true if table exists
        }
        
    except Exception as e:
        return {
            'valid': False,
            'error': str(e),
            'ready_for_operations': False
        } 