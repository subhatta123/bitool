"""
Workflow Manager for ConvaBI Application
Handles the mandatory ETL → Semantics → Query → Dashboard flow
"""

from typing import Dict, Any, Optional, List, Tuple
from enum import Enum
from dataclasses import dataclass
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

class WorkflowStep(Enum):
    """Workflow steps in order"""
    DATA_LOADED = "data_loaded"
    ETL_COMPLETED = "etl_completed"
    SEMANTICS_COMPLETED = "semantics_completed"
    QUERY_ENABLED = "query_enabled"
    DASHBOARD_ENABLED = "dashboard_enabled"

class WorkflowState(Enum):
    """Workflow states"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class WorkflowTransition:
    """Represents a workflow transition"""
    from_step: Optional[WorkflowStep]
    to_step: WorkflowStep
    required_conditions: List[str]
    validation_function: Optional[str] = None

class WorkflowManager:
    """Manages workflow state transitions and validation"""
    
    # Define valid transitions
    TRANSITIONS = [
        WorkflowTransition(None, WorkflowStep.DATA_LOADED, ["data_source_created"]),
        WorkflowTransition(WorkflowStep.DATA_LOADED, WorkflowStep.ETL_COMPLETED, ["etl_operation_success"]),
        WorkflowTransition(WorkflowStep.ETL_COMPLETED, WorkflowStep.SEMANTICS_COMPLETED, ["semantic_metadata_generated"]),
        WorkflowTransition(WorkflowStep.SEMANTICS_COMPLETED, WorkflowStep.QUERY_ENABLED, ["semantic_validation_passed"]),
        WorkflowTransition(WorkflowStep.QUERY_ENABLED, WorkflowStep.DASHBOARD_ENABLED, ["first_query_executed"])
    ]
    
    @staticmethod
    def get_default_status() -> Dict[str, Any]:
        """Get default workflow status"""
        return {
            'data_loaded': False,
            'etl_completed': False,
            'semantics_completed': False,
            'query_enabled': False,
            'dashboard_enabled': False,
            'current_step': WorkflowStep.DATA_LOADED.value,
            'last_updated': timezone.now().isoformat(),
            'progress_percentage': 0
        }
    
    @staticmethod
    def update_workflow_step(current_status: Dict[str, Any], step: WorkflowStep, success: bool = True) -> Dict[str, Any]:
        """Update workflow step status"""
        try:
            updated_status = current_status.copy()
            
            if success:
                updated_status[step.value] = True
                next_step = WorkflowManager._get_next_step(step)
                updated_status['current_step'] = next_step.value if next_step else step.value
                updated_status['progress_percentage'] = WorkflowManager._calculate_progress(updated_status)
            else:
                updated_status[step.value] = False
            
            updated_status['last_updated'] = timezone.now().isoformat()
            
            logger.info(f"Workflow step {step.value} updated: {success}")
            return updated_status
            
        except Exception as e:
            logger.error(f"Failed to update workflow step {step}: {e}")
            return current_status
    
    @staticmethod
    def can_transition_to(current_status: Dict[str, Any], target_step: WorkflowStep) -> bool:
        """Check if transition to target step is allowed"""
        try:
            step_order = [
                WorkflowStep.DATA_LOADED,
                WorkflowStep.ETL_COMPLETED,
                WorkflowStep.SEMANTICS_COMPLETED,
                WorkflowStep.QUERY_ENABLED,
                WorkflowStep.DASHBOARD_ENABLED
            ]
            
            target_index = step_order.index(target_step)
            
            # Check if all previous steps are completed
            for i in range(target_index):
                step = step_order[i]
                if not current_status.get(step.value, False):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to check transition to {target_step}: {e}")
            return False
    
    @staticmethod
    def _get_next_step(current_step: WorkflowStep) -> Optional[WorkflowStep]:
        """Get next workflow step"""
        step_order = [
            WorkflowStep.DATA_LOADED,
            WorkflowStep.ETL_COMPLETED,
            WorkflowStep.SEMANTICS_COMPLETED,
            WorkflowStep.QUERY_ENABLED,
            WorkflowStep.DASHBOARD_ENABLED
        ]
        
        try:
            current_index = step_order.index(current_step)
            if current_index < len(step_order) - 1:
                return step_order[current_index + 1]
        except (ValueError, IndexError):
            pass
        
        return None
    
    @staticmethod
    def _calculate_progress(status: Dict[str, Any]) -> int:
        """Calculate workflow progress percentage"""
        steps = [
            'data_loaded',
            'etl_completed', 
            'semantics_completed',
            'query_enabled',
            'dashboard_enabled'
        ]
        
        completed_steps = sum(1 for step in steps if status.get(step, False))
        return int((completed_steps / len(steps)) * 100)
    
    @staticmethod
    def get_workflow_summary(status: Dict[str, Any]) -> Dict[str, Any]:
        """Get workflow summary information"""
        return {
            'current_step': status.get('current_step', 'data_loaded'),
            'progress_percentage': WorkflowManager._calculate_progress(status),
            'completed_steps': [step for step in status.keys() if step != 'current_step' and step != 'last_updated' and step != 'progress_percentage' and status.get(step, False)],
            'is_complete': WorkflowManager._calculate_progress(status) == 100,
            'last_updated': status.get('last_updated')
        }

    @staticmethod
    def validate_etl_transformation_results(transformation_results: List[Dict]) -> bool:
        """Validate that ETL transformations were successful"""
        if not transformation_results:
            return False
        
        failed_transformations = [r for r in transformation_results if not r.get('success', False)]
        
        # Check for critical failures
        if failed_transformations:
            logger.error(f"ETL validation failed: {len(failed_transformations)} transformations failed")
            return False
        
        # ENHANCED: More lenient validation for real-world data quality
        # Check for extremely high null rates (95%+) only in critical columns
        critical_failures = 0
        
        for result in transformation_results:
            column_name = result.get('column', '').lower()
            target_type = result.get('target_type', '')
            null_percentage = result.get('null_percentage', 0)
            
            # Only flag as critical failure if:
            # 1. NULL rate > 95% AND it's an ID/key column, OR
            # 2. NULL rate = 100% (completely empty column)
            is_id_column = any(keyword in column_name for keyword in ['id', 'key', 'pk'])
            
            if null_percentage >= 100:
                logger.warning(f"Column '{column_name}' is completely empty (100% nulls)")
                critical_failures += 1
            elif null_percentage > 95 and is_id_column:
                logger.warning(f"Critical column '{column_name}' has very high null rate ({null_percentage}%)")
                critical_failures += 1
            elif null_percentage > 80:
                # Just log high null rates for awareness, don't fail validation
                logger.warning(f"High null rate ({null_percentage}%) in {column_name} transformation")
        
        # Only fail if we have multiple critical failures or all columns are empty
        if critical_failures > len(transformation_results) * 0.5:  # More than half are critically bad
            logger.error(f"ETL validation failed: {critical_failures} critical failures out of {len(transformation_results)} columns")
            return False
        
        logger.info(f"ETL validation passed: {len(transformation_results)} transformations successful ({critical_failures} warnings)")
        return True

    @staticmethod
    def validate_semantic_layer_creation(semantic_result: Dict) -> bool:
        """Validate that semantic layer creation was successful"""
        if not semantic_result.get('success', False):
            logger.error(f"Semantic layer validation failed: {semantic_result.get('error', 'Unknown error')}")
            return False
        
        # Check that columns were created
        columns_created = semantic_result.get('columns_created', 0)
        if columns_created == 0:
            logger.error("Semantic layer validation failed: No columns were created")
            return False
        
        # Check for ETL enrichment if expected
        etl_enriched = semantic_result.get('etl_enriched', False)
        if etl_enriched:
            logger.info(f"Semantic layer validation passed: {columns_created} columns created with ETL enrichment")
        else:
            logger.info(f"Semantic layer validation passed: {columns_created} columns created (no ETL enrichment)")
        
        return True

    @staticmethod
    def validate_data_consistency(data_source, integration_service) -> bool:
        """Validate data consistency between ETL and semantic layers"""
        try:
            from utils.table_name_helper import get_integrated_table_name
            
            # Check if integrated data exists
            table_name = get_integrated_table_name(data_source)
            
            if not integration_service.check_table_exists(table_name):
                logger.error(f"Data consistency validation failed: Table {table_name} does not exist")
                return False
            
            # Check if data is not empty
            data = integration_service.get_integrated_data(table_name)
            if data.empty:
                logger.error(f"Data consistency validation failed: Table {table_name} is empty")
                return False
            
            # Check if semantic metadata exists
            from datasets.models import SemanticTable, SemanticColumn
            semantic_tables = SemanticTable.objects.filter(data_source=data_source)
            
            if not semantic_tables.exists():
                logger.error(f"Data consistency validation failed: No semantic tables found for {data_source.name}")
                return False
            
            semantic_table = semantic_tables.first()
            semantic_columns = SemanticColumn.objects.filter(semantic_table=semantic_table)
            
            # Validate column count consistency
            data_columns = len(data.columns)
            semantic_column_count = semantic_columns.count()
            
            if abs(data_columns - semantic_column_count) > 2:  # Allow small differences
                logger.warning(f"Column count mismatch: Data has {data_columns} columns, semantic layer has {semantic_column_count}")
                # Don't fail for this, just warn
            
            logger.info(f"Data consistency validation passed for {data_source.name}")
            return True
            
        except Exception as e:
            logger.error(f"Data consistency validation failed: {e}")
            return False

    @staticmethod
    def validate_stage_transition(data_source, from_step: WorkflowStep, to_step: WorkflowStep, 
                                integration_service=None, transformation_results=None, 
                                semantic_result=None) -> Tuple[bool, str]:
        """Comprehensive validation for stage transitions"""
        try:
            if from_step == WorkflowStep.ETL_COMPLETED and to_step == WorkflowStep.SEMANTICS_COMPLETED:
                # Validate ETL → Semantic transition
                if transformation_results and not WorkflowManager.validate_etl_transformation_results(transformation_results):
                    return False, "ETL transformations failed validation. Please fix transformation errors before proceeding."
                
                if integration_service and not WorkflowManager.validate_data_consistency(data_source, integration_service):
                    return False, "Data consistency validation failed. Please ensure ETL data is properly stored."
                
                return True, "ETL to Semantic transition validated successfully"
            
            elif from_step == WorkflowStep.SEMANTICS_COMPLETED and to_step == WorkflowStep.QUERY_ENABLED:
                # Validate Semantic → Query transition
                if semantic_result and not WorkflowManager.validate_semantic_layer_creation(semantic_result):
                    return False, "Semantic layer creation failed validation. Please regenerate semantic metadata."
                
                if integration_service and not WorkflowManager.validate_data_consistency(data_source, integration_service):
                    return False, "Data consistency validation failed between semantic and data layers."
                
                return True, "Semantic to Query transition validated successfully"
            
            # For other transitions, use basic validation
            return WorkflowManager.can_transition_to(data_source.workflow_status or {}, to_step), \
                   "Basic transition validation passed" if WorkflowManager.can_transition_to(data_source.workflow_status or {}, to_step) else "Transition not allowed"
        
        except Exception as e:
            logger.error(f"Stage transition validation failed: {e}")
            return False, f"Validation error: {str(e)}" 