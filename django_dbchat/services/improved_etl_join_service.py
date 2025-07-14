#!/usr/bin/env python3
"""
Improved ETL Join Service with comprehensive validation
"""

import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from django.utils import timezone
from django.contrib.auth.models import User
from django.db import transaction
from datasets.models import DataSource, ETLOperation
from services.robust_table_validation_service import robust_table_validator, JoinPreValidationResult
from utils.join_validator import JoinSQLValidator, JoinValidationResult

logger = logging.getLogger(__name__)

@dataclass
class ImprovedJoinResult:
    """Result of improved join operation"""
    success: bool
    etl_operation_id: Optional[str] = None
    error_message: Optional[str] = None
    validation_details: Optional[JoinPreValidationResult] = None
    execution_details: Optional[Dict[str, Any]] = None
    recommendations: List[str] = None
    root_cause_analysis: Optional[Dict[str, Any]] = None

class ImprovedETLJoinService:
    """Enhanced ETL Join Service with comprehensive validation"""
    
    def __init__(self):
        self.connection = None
        
    def set_connection(self, connection):
        """Set the database connection"""
        self.connection = connection
        robust_table_validator.set_connection(connection)
    
    def execute_join_with_validation(self, left_source_id: str, right_source_id: str,
                                   left_column: str, right_column: str,
                                   join_type: str, operation_name: str,
                                   user: User) -> ImprovedJoinResult:
        """
        Execute join operation with comprehensive pre-validation
        
        Args:
            left_source_id: ID of left data source
            right_source_id: ID of right data source
            left_column: Column name for left table
            right_column: Column name for right table
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)
            operation_name: Name for the ETL operation
            user: User performing the operation
            
        Returns:
            ImprovedJoinResult with comprehensive details
        """
        logger.info(f"Starting improved join operation: {operation_name}")
        
        # Step 1: Pre-validate the join operation
        logger.info("Step 1: Pre-validating join operation")
        pre_validation = robust_table_validator.pre_validate_join_operation(
            left_source_id, right_source_id, left_column, right_column, user
        )
        
        if not pre_validation.is_ready:
            logger.warning("Pre-validation failed - join operation cannot proceed")
            return ImprovedJoinResult(
                success=False,
                error_message="Join validation failed - tables or columns not ready",
                validation_details=pre_validation,
                recommendations=pre_validation.recommendations,
                root_cause_analysis=pre_validation.root_cause_analysis
            )
        
        logger.info("[SUCCESS] Pre-validation passed - proceeding with join")
        
        # Step 2: Get validated table names
        left_table = pre_validation.left_table_result.table_name
        right_table = pre_validation.right_table_result.table_name
        
        # Update column names if case mismatch was detected
        validated_left_column = (pre_validation.column_validation['left_column_info']
                               .get('column_name', left_column))
        validated_right_column = (pre_validation.column_validation['right_column_info']
                                .get('column_name', right_column))
        
        logger.info(f"Validated table names: {left_table} + {right_table}")
        logger.info(f"Validated columns: {validated_left_column} + {validated_right_column}")
        
        # Step 3: Create ETL operation record
        try:
            with transaction.atomic():
                etl_operation = self._create_etl_operation(
                    operation_name, left_table, right_table,
                    validated_left_column, validated_right_column,
                    join_type, user, pre_validation
                )
                
                logger.info(f"Created ETL operation: {etl_operation.id}")
                
                # Step 4: Generate and validate SQL
                sql_result = self._generate_validated_sql(
                    left_table, right_table, validated_left_column, 
                    validated_right_column, join_type, etl_operation.output_table_name
                )
                
                if not sql_result.is_valid:
                    logger.error(f"SQL generation failed: {sql_result.error_message}")
                    return self._handle_sql_generation_failure(etl_operation, sql_result)
                
                # Step 5: Execute the join
                execution_result = self._execute_join_sql(
                    etl_operation, sql_result.corrected_sql, pre_validation
                )
                
                if execution_result['success']:
                    logger.info(f"[SUCCESS] Join operation completed successfully: {execution_result['row_count']} rows")
                    return ImprovedJoinResult(
                        success=True,
                        etl_operation_id=str(etl_operation.id),
                        validation_details=pre_validation,
                        execution_details=execution_result,
                        recommendations=["Join operation completed successfully!"]
                    )
                else:
                    logger.error(f"Join execution failed: {execution_result['error']}")
                    return self._handle_execution_failure(etl_operation, execution_result, pre_validation)
                    
        except Exception as e:
            logger.error(f"Error in join operation: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            return ImprovedJoinResult(
                success=False,
                error_message=f"Join operation failed: {str(e)}",
                validation_details=pre_validation,
                recommendations=self._generate_error_recommendations(str(e)),
                root_cause_analysis={'error_type': 'execution_error', 'error_details': str(e)}
            )
    
    def _create_etl_operation(self, operation_name: str, left_table: str, right_table: str,
                             left_column: str, right_column: str, join_type: str,
                             user: User, pre_validation: JoinPreValidationResult) -> ETLOperation:
        """Create ETL operation record with validation details"""
        
        # Generate unique output table name
        output_table = f"etl_join_{timezone.now().strftime('%Y%m%d_%H%M%S')}_{left_table[:8]}_{right_table[:8]}"
        
        etl_params = {
            'join_type': join_type.upper(),
            'left_column': left_column,
            'right_column': right_column,
            'left_table': left_table,
            'right_table': right_table,
            'validated_left_column': left_column,
            'validated_right_column': right_column,
            'validation_performed': True,
            'pre_validation_results': {
                'left_table_row_count': pre_validation.left_table_result.row_count,
                'right_table_row_count': pre_validation.right_table_result.row_count,
                'estimated_result_size': pre_validation.root_cause_analysis.get('estimated_result_size', 0),
                'join_complexity': pre_validation.root_cause_analysis.get('join_complexity', 'simple')
            }
        }
        
        return ETLOperation.objects.create(
            name=operation_name,
            operation_type='join',
            source_tables=[left_table, right_table],
            parameters=etl_params,
            output_table_name=output_table,
            status='pending',
            created_by=user,
            data_lineage={
                'left_source_table': left_table,
                'right_source_table': right_table,
                'left_column': left_column,
                'right_column': right_column,
                'join_type': join_type,
                'validation_timestamp': timezone.now().isoformat(),
                'created_at': timezone.now().isoformat()
            }
        )
    
    def _generate_validated_sql(self, left_table: str, right_table: str,
                               left_column: str, right_column: str,
                               join_type: str, output_table: str) -> JoinValidationResult:
        """Generate validated SQL for join operation"""
        
        return JoinSQLValidator.generate_join_sql(
            left_table=left_table,
            right_table=right_table,
            left_column=left_column,
            right_column=right_column,
            join_type=join_type,
            output_table=output_table,
            connection=self.connection
        )
    
    def _execute_join_sql(self, etl_operation: ETLOperation, sql: str,
                         pre_validation: JoinPreValidationResult) -> Dict[str, Any]:
        """Execute the validated join SQL"""
        
        start_time = timezone.now()
        
        try:
            logger.info(f"Executing SQL: {sql}")
            
            # Execute the join SQL
            self.connection.execute(sql)
            
            # Get result statistics
            result_stats = self.connection.execute(
                f"SELECT COUNT(*) as row_count FROM {etl_operation.output_table_name}"
            ).fetchone()
            
            row_count = result_stats[0] if result_stats else 0
            execution_time = (timezone.now() - start_time).total_seconds()
            
            # Update ETL operation
            etl_operation.status = 'completed'
            etl_operation.row_count = row_count
            etl_operation.execution_time = execution_time
            etl_operation.result_summary = {
                'success': True,
                'row_count': row_count,
                'execution_time': execution_time,
                'left_table_rows': pre_validation.left_table_result.row_count,
                'right_table_rows': pre_validation.right_table_result.row_count,
                'join_efficiency': self._calculate_join_efficiency(
                    row_count, 
                    pre_validation.left_table_result.row_count,
                    pre_validation.right_table_result.row_count
                ),
                'execution_timestamp': timezone.now().isoformat()
            }
            etl_operation.save()
            
            return {
                'success': True,
                'row_count': row_count,
                'execution_time': execution_time,
                'output_table': etl_operation.output_table_name
            }
            
        except Exception as e:
            logger.error(f"Error executing join SQL: {e}")
            
            # Update ETL operation as failed
            etl_operation.status = 'failed'
            etl_operation.error_message = str(e)
            etl_operation.result_summary = {
                'success': False,
                'error': str(e),
                'execution_time': (timezone.now() - start_time).total_seconds(),
                'execution_timestamp': timezone.now().isoformat()
            }
            etl_operation.save()
            
            return {
                'success': False,
                'error': str(e),
                'execution_time': (timezone.now() - start_time).total_seconds()
            }
    
    def _calculate_join_efficiency(self, result_rows: int, left_rows: int, right_rows: int) -> float:
        """Calculate join efficiency as a percentage"""
        if left_rows == 0 or right_rows == 0:
            return 0.0
        
        expected_max = min(left_rows, right_rows)
        if expected_max == 0:
            return 0.0
        
        return (result_rows / expected_max) * 100.0
    
    def _handle_sql_generation_failure(self, etl_operation: ETLOperation, 
                                      sql_result: JoinValidationResult) -> ImprovedJoinResult:
        """Handle SQL generation failure"""
        
        etl_operation.status = 'failed'
        etl_operation.error_message = sql_result.error_message
        etl_operation.result_summary = {
            'success': False,
            'error_type': 'sql_generation_failure',
            'error': sql_result.error_message,
            'execution_timestamp': timezone.now().isoformat()
        }
        etl_operation.save()
        
        return ImprovedJoinResult(
            success=False,
            etl_operation_id=str(etl_operation.id),
            error_message=f"SQL generation failed: {sql_result.error_message}",
            recommendations=[
                "SQL generation failed - this indicates a system issue",
                "Please check the join parameters and try again",
                "Contact support if the issue persists"
            ],
            root_cause_analysis={
                'error_type': 'sql_generation_failure',
                'error_details': sql_result.error_message
            }
        )
    
    def _handle_execution_failure(self, etl_operation: ETLOperation, 
                                 execution_result: Dict[str, Any],
                                 pre_validation: JoinPreValidationResult) -> ImprovedJoinResult:
        """Handle join execution failure"""
        
        return ImprovedJoinResult(
            success=False,
            etl_operation_id=str(etl_operation.id),
            error_message=f"Join execution failed: {execution_result['error']}",
            validation_details=pre_validation,
            execution_details=execution_result,
            recommendations=self._generate_execution_failure_recommendations(execution_result['error']),
            root_cause_analysis={
                'error_type': 'execution_failure',
                'error_details': execution_result['error'],
                'execution_time': execution_result.get('execution_time', 0)
            }
        )
    
    def _generate_error_recommendations(self, error_message: str) -> List[str]:
        """Generate recommendations based on error message"""
        recommendations = []
        
        error_lower = error_message.lower()
        
        if 'table' in error_lower and 'not found' in error_lower:
            recommendations.extend([
                "Table not found error - data may not be properly imported",
                "Check that both data sources have completed ETL processing",
                "Verify that data sources are active and not deleted"
            ])
        elif 'column' in error_lower and 'not found' in error_lower:
            recommendations.extend([
                "Column not found error - check column names",
                "Verify that the selected columns exist in both tables",
                "Column names may be case-sensitive"
            ])
        elif 'permission' in error_lower or 'access' in error_lower:
            recommendations.extend([
                "Permission error - check database access rights",
                "Ensure you have permission to access the data sources"
            ])
        else:
            recommendations.extend([
                "Unexpected error occurred during join operation",
                "Try refreshing the page and attempting the operation again",
                "Check the system logs for more detailed error information"
            ])
        
        recommendations.append("Contact support if the issue persists")
        return recommendations
    
    def _generate_execution_failure_recommendations(self, error_message: str) -> List[str]:
        """Generate specific recommendations for execution failures"""
        recommendations = self._generate_error_recommendations(error_message)
        
        recommendations.extend([
            "Join execution failed after validation passed",
            "This may indicate a temporary system issue",
            "Try the operation again in a few minutes"
        ])
        
        return recommendations
    
    def get_join_operation_status(self, operation_id: str) -> Dict[str, Any]:
        """Get detailed status of a join operation"""
        try:
            operation = ETLOperation.objects.get(id=operation_id)
            
            return {
                'operation_id': str(operation.id),
                'name': operation.name,
                'status': operation.status,
                'operation_type': operation.operation_type,
                'created_at': operation.created_at.isoformat(),
                'updated_at': operation.updated_at.isoformat(),
                'row_count': operation.row_count,
                'execution_time': operation.execution_time,
                'error_message': operation.error_message,
                'result_summary': operation.result_summary,
                'output_table_name': operation.output_table_name,
                'parameters': operation.parameters
            }
            
        except ETLOperation.DoesNotExist:
            return {
                'error': f'ETL operation {operation_id} not found'
            }
        except Exception as e:
            return {
                'error': f'Error retrieving operation status: {str(e)}'
            }

# Global instance
improved_etl_join_service = ImprovedETLJoinService() 