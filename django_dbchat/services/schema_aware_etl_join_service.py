#!/usr/bin/env python3
"""
Schema-Aware ETL Join Service - Fixes table qualification issues
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from django.utils import timezone
from django.contrib.auth.models import User
from django.db import transaction
from datasets.models import DataSource, ETLOperation
from services.schema_aware_table_service import schema_aware_table_service, SchemaAwareTableResult
from utils.join_validator import JoinSQLValidator, JoinValidationResult

logger = logging.getLogger(__name__)

@dataclass
class SchemaAwareJoinResult:
    """Result of schema-aware join operation"""
    success: bool
    etl_operation_id: Optional[str] = None
    error_message: Optional[str] = None
    left_table_result: Optional[SchemaAwareTableResult] = None
    right_table_result: Optional[SchemaAwareTableResult] = None
    execution_details: Optional[Dict[str, Any]] = None
    recommendations: List[str] = None
    root_cause_analysis: Optional[Dict[str, Any]] = None
    validation_details: Optional[Any] = None  # Added to fix the AttributeError

@dataclass
class SimpleValidationDetails:
    """Simple validation details structure to match expected interface"""
    left_table_result: Optional[Any] = None
    right_table_result: Optional[Any] = None
    column_validation: Optional[Dict[str, Any]] = None

class SchemaAwareETLJoinService:
    """Enhanced ETL Join Service with schema-aware validation"""
    
    def __init__(self):
        self.connection = None
        
    def set_connection(self, connection):
        """Set the database connection"""
        self.connection = connection
        schema_aware_table_service.set_connection(connection)
    
    def execute_join_with_schema_validation(self, left_source_id: str, right_source_id: str,
                                          left_column: str, right_column: str,
                                          join_type: str, operation_name: str,
                                          user: User) -> SchemaAwareJoinResult:
        """
        Execute join operation with schema-aware validation
        
        Args:
            left_source_id: ID of left data source
            right_source_id: ID of right data source
            left_column: Column name for left table
            right_column: Column name for right table
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)
            operation_name: Name for the ETL operation
            user: User performing the operation
            
        Returns:
            SchemaAwareJoinResult with comprehensive details
        """
        logger.info(f"Starting schema-aware join operation: {operation_name}")
        
        # Step 1: Schema-aware table validation
        logger.info("Step 1: Schema-aware table validation")
        
        left_result = schema_aware_table_service.validate_table_with_schema_resolution(left_source_id)
        right_result = schema_aware_table_service.validate_table_with_schema_resolution(right_source_id)
        
        # Check if both tables are found
        if not left_result.is_valid or not right_result.is_valid:
            logger.warning("Schema-aware validation failed - join operation cannot proceed")
            
            validation_details = SimpleValidationDetails(
                left_table_result=left_result,
                right_table_result=right_result,
                column_validation={'left_column_valid': False, 'right_column_valid': False}
            )
            
            return SchemaAwareJoinResult(
                success=False,
                error_message="Schema validation failed - tables not found or not accessible",
                left_table_result=left_result,
                right_table_result=right_result,
                recommendations=self._generate_table_validation_recommendations(left_result, right_result),
                root_cause_analysis=self._analyze_table_validation_failure(left_result, right_result),
                validation_details=validation_details
            )
        
        logger.info(f"[SUCCESS] Schema validation passed")
        logger.info(f"Left table: {left_result.qualified_table_name}")
        logger.info(f"Right table: {right_result.qualified_table_name}")
        
        # Step 2: Column validation with schema-qualified tables
        logger.info("Step 2: Column validation")
        
        left_column_validation = schema_aware_table_service.validate_column_with_schema(
            left_result.qualified_table_name, left_column
        )
        right_column_validation = schema_aware_table_service.validate_column_with_schema(
            right_result.qualified_table_name, right_column
        )
        
        if not left_column_validation['exists'] or not right_column_validation['exists']:
            logger.warning("Column validation failed")
            
            return SchemaAwareJoinResult(
                success=False,
                error_message="Column validation failed - join columns not found",
                left_table_result=left_result,
                right_table_result=right_result,
                recommendations=self._generate_column_validation_recommendations(
                    left_column_validation, right_column_validation, left_column, right_column
                ),
                root_cause_analysis={
                    'error_type': 'column_validation_failure',
                    'left_column_valid': left_column_validation['exists'],
                    'right_column_valid': right_column_validation['exists'],
                    'left_column_info': left_column_validation,
                    'right_column_info': right_column_validation
                },
                validation_details=SimpleValidationDetails(
                    left_table_result=left_result,
                    right_table_result=right_result,
                    column_validation={'left_column_valid': left_column_validation['exists'], 'right_column_valid': right_column_validation['exists']}
                )
            )
        
        logger.info("[SUCCESS] Column validation passed")
        
        # Step 3: Create ETL operation record
        try:
            with transaction.atomic():
                etl_operation = self._create_etl_operation_with_schema(
                    operation_name, left_result, right_result,
                    left_column, right_column, join_type, user
                )
                
                logger.info(f"Created ETL operation: {etl_operation.id}")
                
                # Step 4: Generate and execute SQL with qualified table names
                execution_result = self._execute_schema_aware_join_sql(
                    etl_operation, left_result, right_result,
                    left_column, right_column, join_type
                )
                
                if execution_result['success']:
                    logger.info(f"[SUCCESS] Join operation completed: {execution_result['row_count']} rows")
                    
                    validation_details = SimpleValidationDetails(
                        left_table_result=left_result,
                        right_table_result=right_result,
                        column_validation={'left_column_valid': True, 'right_column_valid': True}
                    )
                    
                    return SchemaAwareJoinResult(
                        success=True,
                        etl_operation_id=str(etl_operation.id),
                        left_table_result=left_result,
                        right_table_result=right_result,
                        execution_details=execution_result,
                        recommendations=["Join operation completed successfully!"],
                        validation_details=validation_details
                    )
                else:
                    logger.error(f"Join execution failed: {execution_result['error']}")
                    return self._handle_execution_failure(etl_operation, execution_result, left_result, right_result)
                    
        except Exception as e:
            logger.error(f"Error in schema-aware join operation: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            return SchemaAwareJoinResult(
                success=False,
                error_message=f"Join operation failed: {str(e)}",
                left_table_result=left_result,
                right_table_result=right_result,
                recommendations=self._generate_error_recommendations(str(e)),
                root_cause_analysis={'error_type': 'execution_error', 'error_details': str(e)},
                validation_details=SimpleValidationDetails(
                    left_table_result=left_result,
                    right_table_result=right_result,
                    column_validation={'left_column_valid': False, 'right_column_valid': False}
                )
            )
    
    def _create_etl_operation_with_schema(self, operation_name: str, 
                                        left_result: SchemaAwareTableResult, 
                                        right_result: SchemaAwareTableResult,
                                        left_column: str, right_column: str, 
                                        join_type: str, user: User) -> ETLOperation:
        """Create ETL operation record with schema-qualified table names"""
        
        # Generate unique output table name
        output_table = f"etl_join_{timezone.now().strftime('%Y%m%d_%H%M%S')}_{left_result.table_name[:8]}_{right_result.table_name[:8]}"
        
        etl_params = {
            'join_type': join_type.upper(),
            'left_column': left_column,
            'right_column': right_column,
            'left_table': left_result.qualified_table_name,
            'right_table': right_result.qualified_table_name,
            'left_schema': left_result.schema_name,
            'right_schema': right_result.schema_name,
            'schema_aware': True,
            'validation_performed': True,
            'pre_validation_results': {
                'left_table_row_count': left_result.row_count,
                'right_table_row_count': right_result.row_count,
                'left_qualified_name': left_result.qualified_table_name,
                'right_qualified_name': right_result.qualified_table_name
            }
        }
        
        return ETLOperation.objects.create(
            name=operation_name,
            operation_type='join',
            source_tables=[left_result.qualified_table_name, right_result.qualified_table_name],
            parameters=etl_params,
            output_table_name=output_table,
            status='pending',
            created_by=user,
            result_summary={
                'operation_type': 'join',
                'status': 'pending',
                'left_table': left_result.qualified_table_name,
                'right_table': right_result.qualified_table_name,
                'left_rows': left_result.row_count,
                'right_rows': right_result.row_count,
                'join_column_left': left_column,
                'join_column_right': right_column,
                'join_type': join_type,
                'created_at': timezone.now().isoformat()
            },
            data_lineage={
                'left_source_table': left_result.qualified_table_name,
                'right_source_table': right_result.qualified_table_name,
                'left_column': left_column,
                'right_column': right_column,
                'join_type': join_type,
                'schema_aware': True,
                'validation_timestamp': timezone.now().isoformat(),
                'created_at': timezone.now().isoformat()
            }
        )
    
    def _execute_schema_aware_join_sql(self, etl_operation: ETLOperation, 
                                     left_result: SchemaAwareTableResult, 
                                     right_result: SchemaAwareTableResult,
                                     left_column: str, right_column: str, 
                                     join_type: str) -> Dict[str, Any]:
        """Execute join SQL with schema-qualified table names"""
        
        start_time = timezone.now()
        
        try:
            # Generate SQL with qualified table names
            join_sql = f"""
            CREATE TABLE {etl_operation.output_table_name} AS
            SELECT * FROM {left_result.qualified_table_name} t1
            {join_type.upper()} JOIN {right_result.qualified_table_name} t2
            ON t1."{left_column}" = t2."{right_column}"
            """
            
            logger.info(f"Executing schema-aware SQL: {join_sql}")
            
            # Execute the join SQL
            self.connection.execute(join_sql)
            
            # Get result statistics
            result_stats = self.connection.execute(
                f"SELECT COUNT(*) as row_count FROM {etl_operation.output_table_name}"
            ).fetchone()
            
            row_count = result_stats[0] if result_stats else 0
            execution_time = (timezone.now() - start_time).total_seconds()
            
            # Update ETL operation with comprehensive result summary
            etl_operation.status = 'completed'
            etl_operation.row_count = row_count
            etl_operation.execution_time = execution_time
            etl_operation.result_summary = {
                'operation_type': 'join',
                'status': 'completed',
                'success': True,
                'row_count': row_count,
                'execution_time': execution_time,
                'left_table': left_result.qualified_table_name,
                'right_table': right_result.qualified_table_name,
                'left_rows': left_result.row_count,
                'right_rows': right_result.row_count,
                'join_column_left': left_column,
                'join_column_right': right_column,
                'join_type': join_type,
                'output_table': etl_operation.output_table_name,
                'join_efficiency': self._calculate_join_efficiency(row_count, left_result.row_count, right_result.row_count),
                'completed_at': timezone.now().isoformat()
            }
            etl_operation.save()
            
            return {
                'success': True,
                'row_count': row_count,
                'execution_time': execution_time,
                'output_table': etl_operation.output_table_name,
                'sql_executed': join_sql
            }
            
        except Exception as e:
            logger.error(f"Error executing schema-aware join SQL: {e}")
            
            # Update ETL operation as failed with result summary
            etl_operation.status = 'failed'
            etl_operation.error_message = str(e)
            etl_operation.result_summary = {
                'operation_type': 'join',
                'status': 'failed',
                'success': False,
                'error': str(e),
                'execution_time': (timezone.now() - start_time).total_seconds(),
                'left_table': left_result.qualified_table_name,
                'right_table': right_result.qualified_table_name,
                'join_column_left': left_column,
                'join_column_right': right_column,
                'join_type': join_type,
                'failed_at': timezone.now().isoformat()
            }
            etl_operation.save()
            
            return {
                'success': False,
                'error': str(e),
                'execution_time': (timezone.now() - start_time).total_seconds()
            }
    
    def _calculate_join_efficiency(self, result_rows: int, left_rows: int, right_rows: int) -> float:
        """Calculate join efficiency as a percentage"""
        try:
            if left_rows == 0 or right_rows == 0:
                return 0.0
            
            # Calculate the theoretical maximum (Cartesian product)
            max_possible = left_rows * right_rows
            
            # Calculate efficiency as result/max ratio
            efficiency = (result_rows / max_possible) * 100
            
            return round(efficiency, 2)
        except:
            return 0.0

    def _generate_table_validation_recommendations(self, left_result: SchemaAwareTableResult, 
                                                 right_result: SchemaAwareTableResult) -> List[str]:
        """Generate recommendations for table validation failures"""
        recommendations = []
        
        if not left_result.is_valid:
            recommendations.append(f"Left table validation failed: {left_result.error_message}")
            if left_result.alternative_names:
                recommendations.append(f"Available tables: {left_result.alternative_names[:3]}")
        
        if not right_result.is_valid:
            recommendations.append(f"Right table validation failed: {right_result.error_message}")
            if right_result.alternative_names:
                recommendations.append(f"Available tables: {right_result.alternative_names[:3]}")
        
        recommendations.extend([
            "Check that both data sources have been processed through ETL pipeline",
            "Verify that data sources are marked as 'active' status",
            "Consider using different data sources that have been processed"
        ])
        
        return recommendations
    
    def _generate_column_validation_recommendations(self, left_col_result: Dict, right_col_result: Dict,
                                                   left_column: str, right_column: str) -> List[str]:
        """Generate recommendations for column validation failures"""
        recommendations = []
        
        if not left_col_result['exists']:
            recommendations.append(f"Left column '{left_column}' not found")
            if 'available_columns' in left_col_result:
                recommendations.append(f"Available columns: {left_col_result['available_columns'][:5]}")
        
        if not right_col_result['exists']:
            recommendations.append(f"Right column '{right_column}' not found")
            if 'available_columns' in right_col_result:
                recommendations.append(f"Available columns: {right_col_result['available_columns'][:5]}")
        
        recommendations.extend([
            "Check column names for typos or case sensitivity",
            "Verify that the selected columns exist in both tables",
            "Consider using different columns that exist in both tables"
        ])
        
        return recommendations
    
    def _analyze_table_validation_failure(self, left_result: SchemaAwareTableResult, 
                                        right_result: SchemaAwareTableResult) -> Dict[str, Any]:
        """Analyze table validation failures"""
        return {
            'error_type': 'table_validation_failure',
            'left_table_valid': left_result.is_valid,
            'right_table_valid': right_result.is_valid,
            'left_table_details': {
                'qualified_name': left_result.qualified_table_name,
                'schema': left_result.schema_name,
                'error': left_result.error_message
            },
            'right_table_details': {
                'qualified_name': right_result.qualified_table_name,
                'schema': right_result.schema_name,
                'error': right_result.error_message
            },
            'schemas_checked': schema_aware_table_service.get_available_schemas()
        }
    
    def _handle_execution_failure(self, etl_operation: ETLOperation, 
                                execution_result: Dict[str, Any],
                                left_result: SchemaAwareTableResult,
                                right_result: SchemaAwareTableResult) -> SchemaAwareJoinResult:
        """Handle join execution failure"""
        
        return SchemaAwareJoinResult(
            success=False,
            etl_operation_id=str(etl_operation.id),
            error_message=f"Join execution failed: {execution_result['error']}",
            left_table_result=left_result,
            right_table_result=right_result,
            execution_details=execution_result,
            recommendations=self._generate_execution_failure_recommendations(execution_result['error']),
            root_cause_analysis={
                'error_type': 'execution_failure',
                'error_details': execution_result['error'],
                'execution_time': execution_result.get('execution_time', 0)
            },
            validation_details=SimpleValidationDetails(
                left_table_result=left_result,
                right_table_result=right_result,
                column_validation={'left_column_valid': False, 'right_column_valid': False}
            )
        )
    
    def _generate_error_recommendations(self, error_message: str) -> List[str]:
        """Generate recommendations based on error message"""
        recommendations = []
        
        error_lower = error_message.lower()
        
        if 'schema' in error_lower:
            recommendations.extend([
                "Schema-related error - table may be in different schema",
                "Check that tables exist in the expected schema",
                "Verify schema permissions and access rights"
            ])
        elif 'table' in error_lower and 'not found' in error_lower:
            recommendations.extend([
                "Table not found - may be in different schema",
                "Check that data sources have been processed",
                "Verify table names and schema qualification"
            ])
        else:
            recommendations.extend([
                "Unexpected error during join operation",
                "Check system logs for detailed error information",
                "Try refreshing and attempting the operation again"
            ])
        
        recommendations.append("Contact support if the issue persists")
        return recommendations
    
    def _generate_execution_failure_recommendations(self, error_message: str) -> List[str]:
        """Generate specific recommendations for execution failures"""
        recommendations = self._generate_error_recommendations(error_message)
        
        recommendations.extend([
            "Join execution failed after validation passed",
            "This may indicate a system or schema permission issue",
            "Check database logs for more detailed error information"
        ])
        
        return recommendations

# Global instance
schema_aware_etl_join_service = SchemaAwareETLJoinService() 