"""
ETL Schedule Manager for dynamic Celery Beat task scheduling.
"""
import logging
import pytz
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from django.utils import timezone
from django.conf import settings

try:
    from django_celery_beat.models import PeriodicTask, CrontabSchedule, IntervalSchedule
    from django_celery_beat import schedulers
    CELERY_BEAT_AVAILABLE = True
except ImportError:
    CELERY_BEAT_AVAILABLE = False
    PeriodicTask = None
    CrontabSchedule = None
    IntervalSchedule = None

from datasets.models import ScheduledETLJob

logger = logging.getLogger(__name__)


class ETLScheduleManager:
    """Manager for dynamic ETL job scheduling with Celery Beat."""
    
    def __init__(self):
        if not CELERY_BEAT_AVAILABLE:
            logger.warning("django-celery-beat not available. ETL scheduling will be limited.")
    
    def create_or_update_schedule(self, etl_job: ScheduledETLJob) -> Tuple[bool, str]:
        """
        Create or update a Celery Beat schedule for an ETL job.
        
        Args:
            etl_job: The ScheduledETLJob instance
            
        Returns:
            Tuple of (success, message)
        """
        if not CELERY_BEAT_AVAILABLE:
            return False, "django-celery-beat is not available"
        
        try:
            # Generate task name
            task_name = f"scheduled_etl_{etl_job.id}"
            
            # Create schedule based on job configuration
            schedule_obj = self._create_schedule_object(etl_job)
            if not schedule_obj:
                return False, f"Failed to create schedule object for {etl_job.schedule_type}"
            
            # Create defaults dict with schedule object
            defaults_dict = {
                'task': 'services.scheduled_etl_service.execute_scheduled_etl_job',
                'enabled': etl_job.is_active and etl_job.status == 'active',
                'description': f"Scheduled ETL job: {etl_job.name}",
                'kwargs': f'{{"job_id": "{etl_job.id}", "triggered_by": "schedule"}}',
            }
            
            # Add the schedule object to defaults
            if isinstance(schedule_obj, CrontabSchedule):
                defaults_dict['crontab'] = schedule_obj
            elif isinstance(schedule_obj, IntervalSchedule):
                defaults_dict['interval'] = schedule_obj
            
            # Create or update PeriodicTask
            periodic_task, created = PeriodicTask.objects.get_or_create(
                name=task_name,
                defaults=defaults_dict
            )
            
            # Update the periodic task if it already existed
            if not created:
                periodic_task.enabled = etl_job.is_active and etl_job.status == 'active'
                periodic_task.description = f"Scheduled ETL job: {etl_job.name}"
                periodic_task.kwargs = f'{{"job_id": "{etl_job.id}", "triggered_by": "schedule"}}'
                
                # Update the schedule
                if isinstance(schedule_obj, CrontabSchedule):
                    periodic_task.crontab = schedule_obj
                    periodic_task.interval = None
                elif isinstance(schedule_obj, IntervalSchedule):
                    periodic_task.interval = schedule_obj
                    periodic_task.crontab = None
                
                periodic_task.save()
            
            # Update ETL job with Celery task information
            # CRITICAL FIX: Use update() instead of save() to prevent triggering post_save signal
            ScheduledETLJob.objects.filter(id=etl_job.id).update(
                celery_task_name=task_name,
                celery_schedule_id=str(periodic_task.id)
            )
            
            action = "Created" if created else "Updated"
            logger.info(f"{action} Celery Beat schedule for ETL job: {etl_job.name}")
            
            return True, f"{action} schedule successfully"
            
        except Exception as e:
            error_msg = f"Error creating/updating schedule for {etl_job.name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def _create_schedule_object(self, etl_job: ScheduledETLJob):
        """Create the appropriate schedule object (Crontab or Interval)."""
        
        try:
            if etl_job.schedule_type in ['15min', '30min']:
                # Use IntervalSchedule for frequent intervals
                if etl_job.schedule_type == '15min':
                    minutes = 15
                else:
                    minutes = 30
                
                schedule, created = IntervalSchedule.objects.get_or_create(
                    every=minutes,
                    period=IntervalSchedule.MINUTES,
                )
                return schedule
            
            else:
                # Use CrontabSchedule for time-based schedules
                cron_params = self._get_cron_params(etl_job)
                
                schedule, created = CrontabSchedule.objects.get_or_create(
                    **cron_params,
                    timezone=etl_job.timezone
                )
                return schedule
                
        except Exception as e:
            logger.error(f"Error creating schedule object: {e}")
            return None
    
    def _get_cron_params(self, etl_job: ScheduledETLJob) -> Dict[str, Any]:
        """Get crontab parameters for the ETL job."""
        
        if etl_job.schedule_type == 'hourly':
            return {
                'minute': etl_job.minute,
                'hour': '*',
                'day_of_week': '*',
                'day_of_month': '*',
                'month_of_year': '*',
            }
        elif etl_job.schedule_type == 'daily':
            return {
                'minute': etl_job.minute,
                'hour': etl_job.hour,
                'day_of_week': '*',
                'day_of_month': '*',
                'month_of_year': '*',
            }
        elif etl_job.schedule_type == 'weekly':
            return {
                'minute': etl_job.minute,
                'hour': etl_job.hour,
                'day_of_week': etl_job.day_of_week or 1,
                'day_of_month': '*',
                'month_of_year': '*',
            }
        elif etl_job.schedule_type == 'monthly':
            return {
                'minute': etl_job.minute,
                'hour': etl_job.hour,
                'day_of_week': '*',
                'day_of_month': etl_job.day_of_month or 1,
                'month_of_year': '*',
            }
        else:
            # Default to daily at 2 AM
            return {
                'minute': 0,
                'hour': 2,
                'day_of_week': '*',
                'day_of_month': '*',
                'month_of_year': '*',
            }
    
    def delete_schedule(self, etl_job: ScheduledETLJob) -> Tuple[bool, str]:
        """
        Delete the Celery Beat schedule for an ETL job.
        
        Args:
            etl_job: The ScheduledETLJob instance
            
        Returns:
            Tuple of (success, message)
        """
        if not CELERY_BEAT_AVAILABLE:
            return False, "django-celery-beat is not available"
        
        try:
            if etl_job.celery_task_name:
                # Delete the PeriodicTask
                deleted_count, _ = PeriodicTask.objects.filter(
                    name=etl_job.celery_task_name
                ).delete()
                
                if deleted_count > 0:
                    logger.info(f"Deleted Celery Beat schedule for ETL job: {etl_job.name}")
                    
                    # Clear ETL job references
                    etl_job.celery_task_name = ''
                    etl_job.celery_schedule_id = ''
                    etl_job.save()
                    
                    return True, "Schedule deleted successfully"
                else:
                    return True, "No schedule found to delete"
            else:
                return True, "No schedule was configured"
                
        except Exception as e:
            error_msg = f"Error deleting schedule for {etl_job.name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def enable_schedule(self, etl_job: ScheduledETLJob) -> Tuple[bool, str]:
        """Enable the Celery Beat schedule for an ETL job."""
        if not CELERY_BEAT_AVAILABLE:
            return False, "django-celery-beat is not available"
        
        try:
            if etl_job.celery_task_name:
                updated_count = PeriodicTask.objects.filter(
                    name=etl_job.celery_task_name
                ).update(enabled=True)
                
                if updated_count > 0:
                    logger.info(f"Enabled Celery Beat schedule for ETL job: {etl_job.name}")
                    return True, "Schedule enabled successfully"
                else:
                    # Schedule doesn't exist, create it
                    return self.create_or_update_schedule(etl_job)
            else:
                # No schedule exists, create it
                return self.create_or_update_schedule(etl_job)
                
        except Exception as e:
            error_msg = f"Error enabling schedule for {etl_job.name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def disable_schedule(self, etl_job: ScheduledETLJob) -> Tuple[bool, str]:
        """Disable the Celery Beat schedule for an ETL job."""
        if not CELERY_BEAT_AVAILABLE:
            return False, "django-celery-beat is not available"
        
        try:
            if etl_job.celery_task_name:
                updated_count = PeriodicTask.objects.filter(
                    name=etl_job.celery_task_name
                ).update(enabled=False)
                
                if updated_count > 0:
                    logger.info(f"Disabled Celery Beat schedule for ETL job: {etl_job.name}")
                    return True, "Schedule disabled successfully"
                else:
                    return True, "No schedule found to disable"
            else:
                return True, "No schedule was configured"
                
        except Exception as e:
            error_msg = f"Error disabling schedule for {etl_job.name}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    def get_schedule_status(self, etl_job: ScheduledETLJob) -> Dict[str, Any]:
        """Get the status of a scheduled ETL job."""
        if not CELERY_BEAT_AVAILABLE:
            return {
                'scheduled': False,
                'enabled': False,
                'error': 'django-celery-beat is not available'
            }
        
        try:
            if etl_job.celery_task_name:
                try:
                    periodic_task = PeriodicTask.objects.get(name=etl_job.celery_task_name)
                    
                    return {
                        'scheduled': True,
                        'enabled': periodic_task.enabled,
                        'task_name': periodic_task.name,
                        'last_run_at': periodic_task.last_run_at.isoformat() if periodic_task.last_run_at else None,
                        'total_run_count': periodic_task.total_run_count,
                        'schedule_type': 'interval' if periodic_task.interval else 'crontab',
                        'schedule_details': self._get_schedule_details(periodic_task)
                    }
                except PeriodicTask.DoesNotExist:
                    return {
                        'scheduled': False,
                        'enabled': False,
                        'error': 'Periodic task not found in database'
                    }
            else:
                return {
                    'scheduled': False,
                    'enabled': False,
                    'message': 'No schedule configured'
                }
                
        except Exception as e:
            return {
                'scheduled': False,
                'enabled': False,
                'error': str(e)
            }
    
    def _get_schedule_details(self, periodic_task) -> Dict[str, Any]:
        """Get detailed schedule information."""
        if periodic_task.interval:
            return {
                'type': 'interval',
                'every': periodic_task.interval.every,
                'period': periodic_task.interval.period
            }
        elif periodic_task.crontab:
            crontab = periodic_task.crontab
            return {
                'type': 'crontab',
                'minute': crontab.minute,
                'hour': crontab.hour,
                'day_of_week': crontab.day_of_week,
                'day_of_month': crontab.day_of_month,
                'month_of_year': crontab.month_of_year,
                'timezone': str(crontab.timezone) if crontab.timezone else 'UTC'
            }
        else:
            return {'type': 'unknown'}
    
    def list_all_schedules(self) -> Dict[str, Any]:
        """List all ETL job schedules."""
        if not CELERY_BEAT_AVAILABLE:
            return {
                'schedules': [],
                'error': 'django-celery-beat is not available'
            }
        
        try:
            # Get all ETL-related periodic tasks
            etl_tasks = PeriodicTask.objects.filter(
                task='services.scheduled_etl_service.execute_scheduled_etl_job'
            )
            
            schedules = []
            for task in etl_tasks:
                # Try to get the corresponding ETL job
                try:
                    # Extract job_id from kwargs
                    import json
                    kwargs = json.loads(task.kwargs)
                    job_id = kwargs.get('job_id')
                    
                    if job_id:
                        try:
                            etl_job = ScheduledETLJob.objects.get(id=job_id)
                            schedule_info = {
                                'job_id': str(etl_job.id),
                                'job_name': etl_job.name,
                                'task_name': task.name,
                                'enabled': task.enabled,
                                'last_run_at': task.last_run_at.isoformat() if task.last_run_at else None,
                                'total_run_count': task.total_run_count,
                                'schedule_details': self._get_schedule_details(task)
                            }
                            schedules.append(schedule_info)
                        except ScheduledETLJob.DoesNotExist:
                            # ETL job was deleted but task still exists
                            schedule_info = {
                                'job_id': job_id,
                                'job_name': 'DELETED',
                                'task_name': task.name,
                                'enabled': task.enabled,
                                'last_run_at': task.last_run_at.isoformat() if task.last_run_at else None,
                                'total_run_count': task.total_run_count,
                                'schedule_details': self._get_schedule_details(task),
                                'orphaned': True
                            }
                            schedules.append(schedule_info)
                except:
                    # Could not parse kwargs
                    continue
            
            return {
                'schedules': schedules,
                'total_count': len(schedules)
            }
            
        except Exception as e:
            return {
                'schedules': [],
                'error': str(e)
            }
    
    def cleanup_orphaned_schedules(self) -> Tuple[int, str]:
        """Clean up periodic tasks for deleted ETL jobs."""
        if not CELERY_BEAT_AVAILABLE:
            return 0, "django-celery-beat is not available"
        
        try:
            # Get all ETL-related periodic tasks
            etl_tasks = PeriodicTask.objects.filter(
                task='services.scheduled_etl_service.execute_scheduled_etl_job'
            )
            
            orphaned_count = 0
            
            for task in etl_tasks:
                try:
                    # Extract job_id from kwargs
                    import json
                    kwargs = json.loads(task.kwargs)
                    job_id = kwargs.get('job_id')
                    
                    if job_id:
                        # Check if ETL job still exists
                        if not ScheduledETLJob.objects.filter(id=job_id).exists():
                            # ETL job was deleted, remove the periodic task
                            task.delete()
                            orphaned_count += 1
                            logger.info(f"Deleted orphaned periodic task: {task.name}")
                except:
                    # Could not parse kwargs or other error, skip
                    continue
            
            return orphaned_count, f"Cleaned up {orphaned_count} orphaned schedules"
            
        except Exception as e:
            error_msg = f"Error cleaning up orphaned schedules: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return 0, error_msg


# Singleton instance
etl_schedule_manager = ETLScheduleManager() 