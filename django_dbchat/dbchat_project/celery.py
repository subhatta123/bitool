"""
Celery configuration for ConvaBI Django application.
"""
import os
import logging
from celery import Celery
from celery.schedules import crontab
from django.conf import settings

logger = logging.getLogger(__name__)

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')

app = Celery('dbchat')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone=settings.TIME_ZONE,
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    result_expires=3600,  # 1 hour
    
    # Task routing configuration
    task_routes={
        'services.email_service.send_dashboard_email_task': {'queue': 'emails'},
        'services.email_service.send_dashboard_share_notification': {'queue': 'emails'},
        'services.integration_service.execute_etl_operation': {'queue': 'data_processing'},
        'services.scheduled_etl_service.execute_scheduled_etl_job': {'queue': 'scheduled_etl'},
        'services.scheduled_etl_service.schedule_pending_etl_jobs': {'queue': 'scheduling'},
        'services.scheduled_etl_service.cleanup_old_etl_logs': {'queue': 'maintenance'},
        'services.scheduled_etl_service.update_etl_job_schedules': {'queue': 'scheduling'},
        'celery_app.send_dashboard_email_task': {'queue': 'emails'},
        'celery_app.run_etl_operation_task': {'queue': 'data_processing'},
        'celery_app.export_dashboard_task': {'queue': 'exports'},
        'celery_app.update_semantic_layer_task': {'queue': 'semantic'},
        'celery_app.refresh_data_sources_task': {'queue': 'data_refresh'},
        'celery_app.cleanup_old_query_logs': {'queue': 'maintenance'},
        'celery_app.generate_dashboard_thumbnails': {'queue': 'thumbnails'},
    },
    
    # Rate limiting
    task_annotations={
        'celery_app.send_dashboard_email_task': {'rate_limit': '20/m'},
        'celery_app.export_dashboard_task': {'rate_limit': '10/m'},
        'celery_app.run_etl_operation_task': {'rate_limit': '5/m'},
        'celery_app.update_semantic_layer_task': {'rate_limit': '3/m'},
        'celery_app.refresh_data_sources_task': {'rate_limit': '10/h'},
        'services.scheduled_etl_service.execute_scheduled_etl_job': {'rate_limit': '10/m'},
        'services.scheduled_etl_service.schedule_pending_etl_jobs': {'rate_limit': '1/m'},
        'services.scheduled_etl_service.cleanup_old_etl_logs': {'rate_limit': '1/h'},
        'services.scheduled_etl_service.update_etl_job_schedules': {'rate_limit': '1/h'},
    },
    
    # Periodic task schedule
    beat_schedule={
        'cleanup-old-query-logs': {
            'task': 'celery_app.cleanup_old_query_logs',
            'schedule': crontab(hour=2, minute=0),  # Daily at 2 AM
        },
        'refresh-data-sources': {
            'task': 'celery_app.refresh_data_sources_task',
            'schedule': crontab(minute=0),  # Every hour
        },
        'generate-dashboard-thumbnails': {
            'task': 'celery_app.generate_dashboard_thumbnails',
            'schedule': crontab(hour=3, minute=0),  # Daily at 3 AM
        },
        'update-semantic-metadata': {
            'task': 'celery_app.update_semantic_layer_task',
            'schedule': crontab(hour=1, minute=0, day_of_week=1),  # Weekly on Monday at 1 AM
        },
        # ETL Scheduling tasks
        'schedule-pending-etl-jobs': {
            'task': 'services.scheduled_etl_service.schedule_pending_etl_jobs',
            'schedule': crontab(minute='*/5'),  # Every 5 minutes
        },
        'cleanup-old-etl-logs': {
            'task': 'services.scheduled_etl_service.cleanup_old_etl_logs',
            'schedule': crontab(hour=4, minute=0),  # Daily at 4 AM
        },
        'update-etl-job-schedules': {
            'task': 'services.scheduled_etl_service.update_etl_job_schedules',
            'schedule': crontab(hour=0, minute=30),  # Daily at 12:30 AM
        },
    },
)


@app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery setup."""
    print(f'Request: {self.request!r}')
    return 'Celery is working!'


@app.task(bind=True, max_retries=3)
def send_dashboard_email_task(self, dashboard_id, recipient_email, email_config=None):
    """
    Celery task for sending dashboard emails with attachments
    """
    try:
        from services.email_service import EmailService
        from dashboards.models import Dashboard
        
        # Get dashboard
        dashboard = Dashboard.objects.get(id=dashboard_id)
        email_service = EmailService()
        
        # Generate dashboard content
        dashboard_items = list(dashboard.dashboard_items.all().values())
        dashboard_html = email_service.generate_dashboard_html(dashboard_items, dashboard.name)
        
        # Prepare attachments
        attachments = []
        
        # HTML attachment
        attachments.append({
            'content': dashboard_html,
            'filename': dashboard.name.replace(' ', '_'),
            'type': 'html'
        })
        
        # Image attachment
        image_bytes = email_service.generate_dashboard_image(dashboard_html, dashboard.name)
        if image_bytes:
            attachments.append({
                'content': image_bytes,
                'filename': dashboard.name.replace(' ', '_'),
                'type': 'png'
            })
        
        # Send email
        subject = f"Dashboard Report: {dashboard.name}"
        body = f"<p>Your dashboard <strong>{dashboard.name}</strong> is attached.</p>"
        
        success = email_service.send_dashboard_email(
            recipient_email=recipient_email,
            subject=subject,
            body=body,
            attachments=attachments,
            schedule_info={'is_scheduled_job': True}
        )
        
        logger.info(f"Dashboard email sent successfully to {recipient_email}: {success}")
        return {'success': success, 'dashboard_id': dashboard_id, 'recipient': recipient_email}
        
    except Exception as exc:
        logger.error(f"Failed to send dashboard email: {exc}")
        if self.request.retries < self.max_retries:
            raise self.retry(countdown=60 * (self.request.retries + 1), exc=exc)
        raise


@app.task(bind=True, max_retries=2)
def run_etl_operation_task(self, etl_operation_id):
    """
    Celery task for executing ETL operations
    """
    try:
        from services.integration_service import DataIntegrationService
        from datasets.models import ETLOperation
        
        # Get ETL operation
        etl_operation = ETLOperation.objects.get(id=etl_operation_id)
        integration_service = DataIntegrationService()
        
        # Update status to running
        etl_operation.status = 'running'
        etl_operation.save()
        
        # Execute the operation (this would call the actual execution logic)
        success = True  # Placeholder - actual implementation would execute the ETL
        
        if success:
            etl_operation.status = 'completed'
            etl_operation.save()
            logger.info(f"ETL operation {etl_operation_id} completed successfully")
        else:
            etl_operation.status = 'failed'
            etl_operation.save()
            logger.error(f"ETL operation {etl_operation_id} failed")
        
        return {'success': success, 'operation_id': etl_operation_id}
        
    except Exception as exc:
        logger.error(f"Failed to execute ETL operation {etl_operation_id}: {exc}")
        try:
            etl_operation = ETLOperation.objects.get(id=etl_operation_id)
            etl_operation.status = 'failed'
            etl_operation.error_message = str(exc)
            etl_operation.save()
        except:
            pass
        
        if self.request.retries < self.max_retries:
            raise self.retry(countdown=60 * (self.request.retries + 1), exc=exc)
        raise


@app.task(bind=True, max_retries=2)
def export_dashboard_task(self, dashboard_id, export_format='html', options=None):
    """
    Celery task for exporting dashboards to various formats
    """
    try:
        from services.email_service import EmailService
        from dashboards.models import Dashboard
        
        dashboard = Dashboard.objects.get(id=dashboard_id)
        email_service = EmailService()
        
        # Get dashboard items
        dashboard_items = list(dashboard.dashboard_items.all().values())
        
        if export_format == 'html':
            content = email_service.generate_dashboard_html(dashboard_items, dashboard.name)
        elif export_format == 'png':
            dashboard_html = email_service.generate_dashboard_html(dashboard_items, dashboard.name)
            content = email_service.generate_dashboard_image(dashboard_html, dashboard.name)
        else:
            raise ValueError(f"Unsupported export format: {export_format}")
        
        logger.info(f"Dashboard {dashboard_id} exported successfully as {export_format}")
        return {
            'success': True,
            'dashboard_id': dashboard_id,
            'format': export_format,
            'content_size': len(content) if content else 0
        }
        
    except Exception as exc:
        logger.error(f"Failed to export dashboard {dashboard_id}: {exc}")
        if self.request.retries < self.max_retries:
            raise self.retry(countdown=60 * (self.request.retries + 1), exc=exc)
        raise


@app.task(bind=True)
def update_semantic_layer_task(self, force_update=False):
    """
    Celery task for updating semantic layer metadata
    """
    try:
        from services.semantic_service import SemanticService
        from services.integration_service import DataIntegrationService
        
        semantic_service = SemanticService()
        integration_service = DataIntegrationService()
        
        # Auto-generate or update semantic metadata
        success = semantic_service.auto_generate_metadata_from_data_integration(integration_service)
        
        logger.info(f"Semantic layer update completed: {success}")
        return {'success': success, 'force_update': force_update}
        
    except Exception as exc:
        logger.error(f"Failed to update semantic layer: {exc}")
        raise


@app.task(bind=True)
def refresh_data_sources_task(self):
    """
    Celery task for refreshing data source connections and metadata
    """
    try:
        from services.data_service import DataService
        from datasets.models import DataSource
        
        data_service = DataService()
        refreshed_sources = []
        failed_sources = []
        
        # Get all active data sources
        data_sources = DataSource.objects.filter(status='active')
        
        for source in data_sources:
            try:
                # Test connection
                success, message = data_service.test_connection(source.connection_params)
                
                if success:
                    # Update schema info if needed
                    schema_info = data_service.get_schema_info(source.connection_params)
                    if schema_info:
                        source.schema_info = schema_info
                        source.save()
                    refreshed_sources.append(source.id)
                else:
                    failed_sources.append({'id': source.id, 'error': message})
                    
            except Exception as e:
                failed_sources.append({'id': source.id, 'error': str(e)})
        
        logger.info(f"Data sources refresh completed: {len(refreshed_sources)} successful, {len(failed_sources)} failed")
        return {
            'refreshed_count': len(refreshed_sources),
            'failed_count': len(failed_sources),
            'refreshed_sources': refreshed_sources,
            'failed_sources': failed_sources
        }
        
    except Exception as exc:
        logger.error(f"Failed to refresh data sources: {exc}")
        raise


@app.task(bind=True)
def cleanup_old_query_logs(self, days_to_keep=30):
    """
    Celery task for cleaning up old query logs
    """
    try:
        from django.utils import timezone
        from datetime import timedelta
        from core.models import QueryLog
        
        cutoff_date = timezone.now() - timedelta(days=days_to_keep)
        
        # Delete old query logs
        deleted_count, _ = QueryLog.objects.filter(created_at__lt=cutoff_date).delete()
        
        logger.info(f"Cleaned up {deleted_count} old query logs")
        return {'deleted_count': deleted_count, 'cutoff_date': cutoff_date.isoformat()}
        
    except Exception as exc:
        logger.error(f"Failed to cleanup old query logs: {exc}")
        raise


@app.task(bind=True)
def generate_dashboard_thumbnails(self):
    """
    Celery task for generating dashboard thumbnails
    """
    try:
        from services.email_service import EmailService
        from dashboards.models import Dashboard
        
        email_service = EmailService()
        generated_count = 0
        
        # Get dashboards that need thumbnail generation
        dashboards = Dashboard.objects.filter(thumbnail__isnull=True)[:10]  # Process 10 at a time
        
        for dashboard in dashboards:
            try:
                # Generate dashboard HTML
                dashboard_items = list(dashboard.dashboard_items.all().values())
                dashboard_html = email_service.generate_dashboard_html(dashboard_items, dashboard.name)
                
                # Generate thumbnail image
                thumbnail_bytes = email_service.generate_dashboard_image(dashboard_html, dashboard.name)
                
                if thumbnail_bytes:
                    # Save thumbnail (would need to implement file storage)
                    generated_count += 1
                    logger.info(f"Generated thumbnail for dashboard {dashboard.id}")
                    
            except Exception as e:
                logger.error(f"Failed to generate thumbnail for dashboard {dashboard.id}: {e}")
        
        logger.info(f"Generated {generated_count} dashboard thumbnails")
        return {'generated_count': generated_count}
        
    except Exception as exc:
        logger.error(f"Failed to generate dashboard thumbnails: {exc}")
        raise


# Error handling
@app.task(bind=True)
def handle_task_failure(self, task_id, error, traceback):
    """Handle task failures and log them."""
    try:
        from core.models import QueryLog  # Using QueryLog for general logging
        
        # Log the failure
        logger.error(f'Task {task_id} failed: {error}')
        
        # Could also store in database if needed
        # QueryLog.objects.create(
        #     query=f"TASK_FAILURE:{task_id}",
        #     status='FAILURE',
        #     error_message=str(error),
        #     rows_returned=0
        # )
        
    except Exception as log_error:
        logger.error(f"Failed to log task failure: {log_error}")


# Health check task
@app.task(bind=True)
def health_check_task(self):
    """Health check task for monitoring Celery workers"""
    try:
        from django.db import connection
        from django.core.cache import cache
        
        # Test database connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        
        # Test cache connection
        cache.set('celery_health_check', 'ok', 60)
        cache_value = cache.get('celery_health_check')
        
        logger.info("Celery health check passed")
        return {
            'status': 'healthy',
            'database': 'ok',
            'cache': 'ok' if cache_value == 'ok' else 'failed',
            'timestamp': timezone.now().isoformat()
        }
        
    except Exception as exc:
        logger.error(f"Celery health check failed: {exc}")
        return {
            'status': 'unhealthy',
            'error': str(exc),
            'timestamp': timezone.now().isoformat()
        } 