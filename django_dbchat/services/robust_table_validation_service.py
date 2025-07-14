#!/usr/bin/env python3
"""
Robust Table Validation Service for ETL Operations
Handles multiple table naming patterns and provides comprehensive validation
"""

import re
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from django.contrib.auth.models import User
from datasets.models import DataSource
from utils.join_validator import JoinValidationResult

logger = logging.getLogger(__name__)

@dataclass
class TableValidationResult:
    """Result of table validation with detailed information"""
    is_valid: bool
    table_name: Optional[str] = None
    error_message: Optional[str] = None
    alternative_names: List[str] = field(default_factory=list)
    column_info: Dict[str, Any] = field(default_factory=dict)
    row_count: int = 0
    warnings: List[str] = field(default_factory=list)

@dataclass
class JoinPreValidationResult:
    """Result of comprehensive join pre-validation"""
    is_ready: bool
    left_table_result: TableValidationResult
    right_table_result: TableValidationResult
    column_validation: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    root_cause_analysis: Dict[str, Any] = field(default_factory=dict)

class RobustTableValidationService:
    """Service for robust table validation and name resolution"""
    
    def __init__(self):
        self.connection = None
        self.cache = {}
        
    def set_connection(self, connection):
        """Set the database connection"""
        self.connection = connection
        
    def get_all_table_patterns(self, data_source_id: str) -> List[str]:
        """Generate all possible table naming patterns for a data source"""
        patterns = []
        
        # Clean the UUID for different formats
        uuid_clean = str(data_source_id).replace('-', '')
        uuid_underscores = str(data_source_id).replace('-', '_')
        
        # Common patterns found in the codebase
        patterns.extend([
            f"ds_{uuid_clean}",
            f"ds_{uuid_underscores}",
            f"source_id_{uuid_underscores}",
            f"source_{uuid_clean}",
            f"source_{uuid_underscores}",
            f"data_source_{uuid_clean}",
            f"data_source_{uuid_underscores}",
        ])
        
        # Add patterns based on DataSource name if available
        try:
            data_source = DataSource.objects.get(id=data_source_id)
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', data_source.name.lower())
            patterns.extend([
                f"data_{safe_name}_{uuid_clean[:8]}",
                f"{safe_name}_{uuid_clean[:8]}",
                f"ds_{safe_name}_{uuid_clean[:8]}",
            ])
        except DataSource.DoesNotExist:
            logger.warning(f"DataSource {data_source_id} not found for name-based patterns")
        
        # Remove duplicates while preserving order
        unique_patterns = []
        seen = set()
        for pattern in patterns:
            if pattern not in seen:
                unique_patterns.append(pattern)
                seen.add(pattern)
                
        return unique_patterns
    
    def validate_table_existence(self, data_source_id: str, user: User) -> TableValidationResult:
        """Validate that a table exists and get its metadata"""
        if not self.connection:
            return TableValidationResult(
                is_valid=False,
                error_message="Database connection not available"
            )
        
        cache_key = f"table_validation_{data_source_id}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # Get all possible table patterns
            patterns = self.get_all_table_patterns(data_source_id)
            
            logger.info(f"Validating table existence for data source {data_source_id}")
            logger.info(f"Checking {len(patterns)} possible table patterns")
            
            # Check each pattern
            for pattern in patterns:
                try:
                    # Check if table exists
                    result = self.connection.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
                        [pattern]
                    ).fetchone()
                    
                    if result:
                        logger.info(f"[SUCCESS] Found table: {pattern}")
                        
                        # Get table metadata
                        column_info = self._get_table_metadata(pattern)
                        row_count = self._get_table_row_count(pattern)
                        
                        validation_result = TableValidationResult(
                            is_valid=True,
                            table_name=pattern,
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
            available_tables = self._get_available_tables()
            
            validation_result = TableValidationResult(
                is_valid=False,
                error_message=f"No table found for data source {data_source_id}. Checked patterns: {patterns[:5]}...",
                alternative_names=available_tables[:10]  # Show first 10 available tables
            )
            
            # Cache the result
            self.cache[cache_key] = validation_result
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validating table existence: {e}")
            return TableValidationResult(
                is_valid=False,
                error_message=f"Validation error: {str(e)}"
            )
    
    def validate_column_existence(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Validate that a column exists in a table"""
        if not self.connection:
            return {
                'exists': False,
                'error': 'Database connection not available'
            }
        
        try:
            # Get table columns
            columns_result = self.connection.execute(f"DESCRIBE {table_name}").fetchall()
            columns = [col[0] for col in columns_result]
            
            # Check exact match
            if column_name in columns:
                return {
                    'exists': True,
                    'column_name': column_name,
                    'table_name': table_name
                }
            
            # Check case-insensitive match
            column_lower = column_name.lower()
            for col in columns:
                if col.lower() == column_lower:
                    return {
                        'exists': True,
                        'column_name': col,  # Return the actual column name
                        'table_name': table_name,
                        'case_mismatch': True
                    }
            
            return {
                'exists': False,
                'error': f"Column '{column_name}' not found in table '{table_name}'",
                'available_columns': columns[:10]  # Show first 10 columns
            }
            
        except Exception as e:
            logger.error(f"Error validating column existence: {e}")
            return {
                'exists': False,
                'error': f"Column validation error: {str(e)}"
            }
    
    def pre_validate_join_operation(self, left_source_id: str, right_source_id: str, 
                                   left_column: str, right_column: str, 
                                   user: User) -> JoinPreValidationResult:
        """Comprehensive pre-validation for join operations"""
        logger.info(f"Pre-validating join operation: {left_source_id} + {right_source_id}")
        
        # Validate left table
        left_result = self.validate_table_existence(left_source_id, user)
        
        # Validate right table
        right_result = self.validate_table_existence(right_source_id, user)
        
        # Check if both tables are valid
        if not left_result.is_valid or not right_result.is_valid:
            return JoinPreValidationResult(
                is_ready=False,
                left_table_result=left_result,
                right_table_result=right_result,
                root_cause_analysis=self._analyze_table_validation_failures(left_result, right_result),
                recommendations=self._generate_fix_recommendations(left_result, right_result)
            )
        
        # Validate columns
        if left_result.table_name and right_result.table_name:
            column_validation = self._validate_join_columns(
                left_result.table_name, left_column,
                right_result.table_name, right_column
            )
        else:
            column_validation = {
                'left_column_valid': False,
                'right_column_valid': False,
                'left_column_info': {'exists': False, 'error': 'Table not found'},
                'right_column_info': {'exists': False, 'error': 'Table not found'},
                'left_column_issues': ['Table validation failed'],
                'right_column_issues': ['Table validation failed']
            }
        
        # Generate recommendations
        recommendations = []
        if column_validation.get('left_column_issues'):
            recommendations.extend(column_validation['left_column_issues'])
        if column_validation.get('right_column_issues'):
            recommendations.extend(column_validation['right_column_issues'])
        
        is_ready = (left_result.is_valid and right_result.is_valid and 
                   column_validation.get('left_column_valid', False) and 
                   column_validation.get('right_column_valid', False))
        
        return JoinPreValidationResult(
            is_ready=is_ready,
            left_table_result=left_result,
            right_table_result=right_result,
            column_validation=column_validation,
            recommendations=recommendations,
            root_cause_analysis=self._analyze_join_readiness(left_result, right_result, column_validation)
        )
    
    def _get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive table metadata"""
        try:
            if not self.connection:
                return {}
            columns_result = self.connection.execute(f"DESCRIBE {table_name}").fetchall()
            columns = [{'name': col[0], 'type': col[1]} for col in columns_result]
            
            return {
                'columns': columns,
                'column_count': len(columns),
                'column_names': [col['name'] for col in columns]
            }
        except Exception as e:
            logger.error(f"Error getting table metadata for {table_name}: {e}")
            return {}
    
    def _get_table_row_count(self, table_name: str) -> int:
        """Get table row count safely"""
        try:
            if not self.connection:
                return 0
            result = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
    
    def _get_available_tables(self) -> List[str]:
        """Get list of available tables"""
        try:
            if not self.connection:
                return []
            tables_result = self.connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            return [table[0] for table in tables_result]
        except Exception as e:
            logger.error(f"Error getting available tables: {e}")
            return []
    
    def _validate_join_columns(self, left_table: str, left_column: str, 
                              right_table: str, right_column: str) -> Dict[str, Any]:
        """Validate join columns exist and are compatible"""
        left_validation = self.validate_column_existence(left_table, left_column)
        right_validation = self.validate_column_existence(right_table, right_column)
        
        return {
            'left_column_valid': left_validation['exists'],
            'right_column_valid': right_validation['exists'],
            'left_column_info': left_validation,
            'right_column_info': right_validation,
            'left_column_issues': [] if left_validation['exists'] else [left_validation.get('error', 'Column validation failed')],
            'right_column_issues': [] if right_validation['exists'] else [right_validation.get('error', 'Column validation failed')]
        }
    
    def _analyze_table_validation_failures(self, left_result: TableValidationResult, 
                                         right_result: TableValidationResult) -> Dict[str, Any]:
        """Analyze why table validation failed"""
        analysis = {
            'left_table_issues': [],
            'right_table_issues': [],
            'common_issues': [],
            'data_pipeline_status': 'unknown'
        }
        
        if not left_result.is_valid:
            analysis['left_table_issues'].append(left_result.error_message)
            if left_result.alternative_names:
                analysis['left_table_issues'].append(f"Available alternatives: {left_result.alternative_names[:3]}")
        
        if not right_result.is_valid:
            analysis['right_table_issues'].append(right_result.error_message)
            if right_result.alternative_names:
                analysis['right_table_issues'].append(f"Available alternatives: {right_result.alternative_names[:3]}")
        
        # Check if it's a data pipeline issue
        available_tables = self._get_available_tables()
        if not available_tables:
            analysis['common_issues'].append("No tables found in database - data pipeline may not be initialized")
            analysis['data_pipeline_status'] = 'not_initialized'
        elif len(available_tables) < 3:
            analysis['common_issues'].append("Very few tables found - data pipeline may be incomplete")
            analysis['data_pipeline_status'] = 'incomplete'
        else:
            analysis['data_pipeline_status'] = 'tables_exist'
        
        return analysis
    
    def _generate_fix_recommendations(self, left_result: TableValidationResult, 
                                    right_result: TableValidationResult) -> List[str]:
        """Generate actionable recommendations to fix table validation issues"""
        recommendations = []
        
        if not left_result.is_valid:
            recommendations.append("Left table validation failed:")
            recommendations.append(f"  - {left_result.error_message}")
            if left_result.alternative_names:
                recommendations.append(f"  - Check if any of these tables exist: {left_result.alternative_names[:3]}")
        
        if not right_result.is_valid:
            recommendations.append("Right table validation failed:")
            recommendations.append(f"  - {right_result.error_message}")
            if right_result.alternative_names:
                recommendations.append(f"  - Check if any of these tables exist: {right_result.alternative_names[:3]}")
        
        # General recommendations
        recommendations.extend([
            "Ensure both data sources have completed ETL processing",
            "Check that data sources are marked as 'active' status",
            "Verify that data has been successfully imported and stored",
            "Try refreshing the data source connections",
            "Check the data integration logs for any processing errors"
        ])
        
        return recommendations
    
    def _analyze_join_readiness(self, left_result: TableValidationResult, 
                               right_result: TableValidationResult, 
                               column_validation: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze overall join readiness"""
        analysis = {
            'tables_ready': left_result.is_valid and right_result.is_valid,
            'columns_ready': (column_validation.get('left_column_valid', False) and 
                            column_validation.get('right_column_valid', False)),
            'estimated_result_size': 0,
            'join_complexity': 'simple',
            'performance_notes': []
        }
        
        if analysis['tables_ready']:
            # Estimate result size
            left_rows = left_result.row_count
            right_rows = right_result.row_count
            
            if left_rows > 0 and right_rows > 0:
                # Simple estimation - actual will depend on join selectivity
                analysis['estimated_result_size'] = min(left_rows, right_rows)
                
                if left_rows > 100000 or right_rows > 100000:
                    analysis['join_complexity'] = 'large'
                    analysis['performance_notes'].append("Large table join - may take longer to execute")
                elif left_rows > 10000 or right_rows > 10000:
                    analysis['join_complexity'] = 'medium'
                    analysis['performance_notes'].append("Medium table join - should execute quickly")
                else:
                    analysis['performance_notes'].append("Small table join - should execute very quickly")
        
        return analysis
    
    def clear_cache(self):
        """Clear the validation cache"""
        self.cache.clear()
        logger.info("Table validation cache cleared")

# Global instance
robust_table_validator = RobustTableValidationService() 