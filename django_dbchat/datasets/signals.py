"""
Django signals for automatic cleanup when data sources are deleted
"""

import logging
from django.db.models.signals import post_delete
from django.dispatch import receiver
from django.db import transaction

from .models import DataSource, ETLOperation
from core.models import QueryLog

logger = logging.getLogger(__name__)


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
            
            for query_log in query_logs:
                # Check if this data source is mentioned in query fields
                text_to_check = ' '.join(filter(None, [
                    query_log.natural_query or '',
                    query_log.generated_sql or '',
                    query_log.final_sql or ''
                ]))
                
                if (instance.name in text_to_check or 
                    str(instance.id) in text_to_check or
                    str(instance.id) in str(query_log.query_results)):
                    query_logs_to_delete.append(query_log.pk)
            
            if query_logs_to_delete:
                cleanup_count['query_logs'] = QueryLog.objects.filter(
                    id__in=query_logs_to_delete
                ).delete()[0]
            
            # Clean up dashboard items that reference this data source
            try:
                from dashboards.models import DashboardItem
                dashboard_items = DashboardItem.objects.filter(
                    dashboard__owner=instance.created_by
                )
                
                items_to_delete = []
                for item in dashboard_items:
                    if (instance.name in item.query or 
                        str(instance.id) in item.query or
                        instance.name == item.data_source or 
                        str(instance.id) == item.data_source):
                        items_to_delete.append(item.id)
                
                if items_to_delete:
                    cleanup_count['dashboard_items'] = DashboardItem.objects.filter(
                        id__in=items_to_delete
                    ).delete()[0]
                    
            except ImportError:
                logger.info("Dashboard models not available for cleanup")
            
            if any(cleanup_count.values()):
                logger.info(f"Auto-cleanup completed for DataSource {instance.name}: {cleanup_count}")
            
    except Exception as e:
        logger.error(f"Error in auto-cleanup for DataSource {instance.name}: {e}")


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