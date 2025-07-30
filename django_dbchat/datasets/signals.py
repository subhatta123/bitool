"""
Django signals for automatic cleanup when data sources are deleted
and for ETL schedule management.
"""

import logging
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver
from django.db import transaction

from .models import DataSource, ETLOperation, ScheduledETLJob
from core.models import QueryLog

logger = logging.getLogger(__name__)

# CRITICAL FIX: Global flag to prevent infinite recursion
_updating_etl_schedule = set()

@receiver(post_delete, sender=DataSource)
def cleanup_related_data_on_datasource_delete(sender, instance, **kwargs):
    """
    Automatically clean up related data when a DataSource is deleted
    """
    try:
        with transaction.atomic():
            cleanup_count = {
                'etl_operations': 0,
                'query_logs': 0,
                'dashboard_items': 0
            }
            
            # Clean up ETL operations that reference this data source
            etl_operations = ETLOperation.objects.filter(
                created_by=instance.created_by
            )
            
            # Filter ETL operations that reference this data source
            etl_to_delete = []
            for etl_op in etl_operations:
                if (str(instance.id) in str(etl_op.source_tables) or 
                    str(instance.id) in str(etl_op.parameters) or
                    instance.name in str(etl_op.source_tables) or
                    instance.name in str(etl_op.parameters)):
                    etl_to_delete.append(etl_op.id)
            
            if etl_to_delete:
                cleanup_count['etl_operations'] = ETLOperation.objects.filter(
                    id__in=etl_to_delete
                ).delete()[0]
            
            # Clean up query logs that reference this data source
            query_logs = QueryLog.objects.filter(user=instance.created_by)
            query_logs_to_delete = []
            
            for log in query_logs:
                # Check if query log references this data source
                if (str(instance.id) in str(log.query_metadata) or 
                    instance.name in str(log.query_metadata) or
                    str(instance.id) in str(log.additional_context)):
                    query_logs_to_delete.append(log.id)
            
            if query_logs_to_delete:
                cleanup_count['query_logs'] = QueryLog.objects.filter(
                    id__in=query_logs_to_delete
                ).delete()[0]
            
            # Clean up dashboard items that reference this data source
            try:
                from dashboards.models import DashboardItem
                dashboard_items = DashboardItem.objects.filter(
                    data_source=str(instance.id)
                )
                if dashboard_items.exists():
                    cleanup_count['dashboard_items'] = dashboard_items.delete()[0]
            except ImportError:
                # Dashboard module not available
                pass
            
            logger.info(f"Cleaned up related data for DataSource {instance.name}: {cleanup_count}")
            
    except Exception as e:
        logger.error(f"Error cleaning up related data for DataSource {instance.name}: {e}")


@receiver(post_save, sender=ScheduledETLJob)
def handle_scheduled_etl_job_save(sender, instance, created, **kwargs):
    """
    Handle ScheduledETLJob save events to manage Celery Beat schedules.
    FIXED: Prevent infinite recursion when updating schedule information.
    """
    
    # CRITICAL FIX: Prevent infinite recursion
    if instance.id in _updating_etl_schedule:
        logger.debug(f"Skipping signal for {instance.name} - already being processed")
        return
    
    try:
        # CRITICAL FIX: Run schedule creation asynchronously after transaction commits
        # This prevents database deadlocks when creating jobs and running them immediately
        def create_schedule_after_commit():
            # CRITICAL FIX: Add instance to processing set to prevent recursion
            _updating_etl_schedule.add(instance.id)
            
            try:
                from services.etl_schedule_manager import etl_schedule_manager
                
                if created:
                    # New job created
                    logger.info(f"Creating Celery Beat schedule for new ETL job: {instance.name}")
                    success, message = etl_schedule_manager.create_or_update_schedule(instance)
                    
                    if success:
                        logger.info(f"Successfully created schedule for ETL job {instance.name}: {message}")
                        # FIXED: Update next_run time without triggering signals
                        try:
                            from datasets.models import ScheduledETLJob
                            # Use update() to avoid triggering post_save signal
                            ScheduledETLJob.objects.filter(id=instance.id).update(
                                next_run=instance.calculate_next_run()
                            )
                        except Exception as update_error:
                            logger.warning(f"Could not update next_run for {instance.name}: {update_error}")
                    else:
                        logger.error(f"Failed to create schedule for ETL job {instance.name}: {message}")
                else:
                    # Existing job updated - only process if it's not a schedule-related update
                    logger.info(f"Updating Celery Beat schedule for ETL job: {instance.name}")
                    
                    # Check if job is active and should have a schedule
                    if instance.is_active and instance.status == 'active':
                        success, message = etl_schedule_manager.create_or_update_schedule(instance)
                        if success:
                            logger.info(f"Successfully updated schedule for ETL job {instance.name}: {message}")
                            # FIXED: Update next_run time without triggering signals
                            try:
                                from datasets.models import ScheduledETLJob
                                ScheduledETLJob.objects.filter(id=instance.id).update(
                                    next_run=instance.calculate_next_run()
                                )
                            except Exception as update_error:
                                logger.warning(f"Could not update next_run for {instance.name}: {update_error}")
                        else:
                            logger.error(f"Failed to update schedule for ETL job {instance.name}: {message}")
                    else:
                        # Job is inactive, disable the schedule
                        success, message = etl_schedule_manager.disable_schedule(instance)
                        if success:
                            logger.info(f"Disabled schedule for inactive ETL job {instance.name}: {message}")
                        else:
                            logger.error(f"Failed to disable schedule for ETL job {instance.name}: {message}")
                            
            except Exception as schedule_error:
                logger.error(f"Error in async schedule creation for {instance.name}: {schedule_error}", exc_info=True)
            finally:
                # CRITICAL FIX: Always remove from processing set
                _updating_etl_schedule.discard(instance.id)
        
        # CRITICAL FIX: Use on_commit to run after the current transaction completes
        # This prevents deadlocks when the job creation and immediate execution happen together
        transaction.on_commit(create_schedule_after_commit)
        
    except Exception as e:
        logger.error(f"Error in signal handler for ScheduledETLJob {instance.name}: {e}", exc_info=True)
        # Ensure we clean up the processing set even on error
        _updating_etl_schedule.discard(instance.id)


@receiver(post_delete, sender=ScheduledETLJob)
def handle_scheduled_etl_job_delete(sender, instance, **kwargs):
    """
    Handle ScheduledETLJob deletion events to clean up Celery Beat schedules.
    """
    try:
        from services.etl_schedule_manager import etl_schedule_manager
        
        logger.info(f"Deleting Celery Beat schedule for ETL job: {instance.name}")
        success, message = etl_schedule_manager.disable_schedule(instance)
        
        if success:
            logger.info(f"Successfully deleted schedule for ETL job {instance.name}: {message}")
        else:
            logger.error(f"Failed to delete schedule for ETL job {instance.name}: {message}")
            
    except Exception as e:
        logger.error(f"Error handling ScheduledETLJob deletion for {instance.name}: {e}", exc_info=True)


def cleanup_orphaned_data_for_user(user):
    """
    Utility function to clean up orphaned data for a specific user
    """
    try:
        with transaction.atomic():
            cleanup_summary = {
                'etl_operations': 0,
                'query_logs': 0,
                'dashboard_items': 0
            }
            
            # Get user's active data sources
            user_data_sources = DataSource.objects.filter(created_by=user)
            user_data_source_ids = list(user_data_sources.values_list('id', flat=True))
            user_data_source_names = list(user_data_sources.values_list('name', flat=True))
            
            # If user has no data sources, clean up all related data
            if not user_data_sources.exists():
                logger.info(f"User {user.username} has no data sources, cleaning up all related data")
                
                cleanup_summary['etl_operations'] = ETLOperation.objects.filter(
                    created_by=user
                ).delete()[0]
                
                cleanup_summary['query_logs'] = QueryLog.objects.filter(
                    user=user
                ).delete()[0]
                
                try:
                    from dashboards.models import DashboardItem
                    cleanup_summary['dashboard_items'] = DashboardItem.objects.filter(
                        dashboard__owner=user
                    ).delete()[0]
                except ImportError:
                    pass
                    
            else:
                # Clean up orphaned ETL operations
                etl_operations = ETLOperation.objects.filter(created_by=user)
                etl_to_delete = []
                
                for etl_op in etl_operations:
                    # Check if any referenced data sources still exist
                    has_valid_reference = False
                    for ds_id in user_data_source_ids:
                        if str(ds_id) in str(etl_op.source_tables) or str(ds_id) in str(etl_op.parameters):
                            has_valid_reference = True
                            break
                    
                    for ds_name in user_data_source_names:
                        if ds_name in str(etl_op.source_tables) or ds_name in str(etl_op.parameters):
                            has_valid_reference = True
                            break
                    
                    if not has_valid_reference:
                        etl_to_delete.append(etl_op.id)
                
                if etl_to_delete:
                    cleanup_summary['etl_operations'] = ETLOperation.objects.filter(
                        id__in=etl_to_delete
                    ).delete()[0]
            
            logger.info(f"Orphaned data cleanup for {user.username}: {cleanup_summary}")
            return cleanup_summary
            
    except Exception as e:
        logger.error(f"Error cleaning up orphaned data for user {user.username}: {e}")
        return {'error': str(e)} 