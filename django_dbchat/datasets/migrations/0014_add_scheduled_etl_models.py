# Generated migration for ETL scheduling models

from django.conf import settings
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0013_datasourceshare_datasource_shared_with_users'),
    ]

    operations = [
        migrations.CreateModel(
            name='ScheduledETLJob',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('name', models.CharField(db_index=True, help_text='Descriptive name for the scheduled job', max_length=255)),
                ('description', models.TextField(blank=True, help_text='Optional description of what this job does')),
                ('schedule_type', models.CharField(choices=[('15min', 'Every 15 minutes'), ('30min', 'Every 30 minutes'), ('hourly', 'Every hour'), ('daily', 'Daily'), ('weekly', 'Weekly'), ('monthly', 'Monthly')], default='daily', help_text='How frequently to run this job', max_length=20)),
                ('timezone', models.CharField(choices=[('UTC', 'UTC'), ('US/Eastern', 'Eastern Time (US)'), ('US/Central', 'Central Time (US)'), ('US/Mountain', 'Mountain Time (US)'), ('US/Pacific', 'Pacific Time (US)'), ('Europe/London', 'London'), ('Europe/Paris', 'Paris'), ('Europe/Berlin', 'Berlin'), ('Europe/Amsterdam', 'Amsterdam'), ('Asia/Tokyo', 'Tokyo'), ('Asia/Shanghai', 'Shanghai'), ('Asia/Kolkata', 'India'), ('Asia/Dubai', 'Dubai'), ('Australia/Sydney', 'Sydney'), ('Australia/Melbourne', 'Melbourne'), ('America/New_York', 'New York'), ('America/Los_Angeles', 'Los Angeles'), ('America/Chicago', 'Chicago'), ('America/Denver', 'Denver'), ('America/Toronto', 'Toronto'), ('America/Sao_Paulo', 'São Paulo')], default='UTC', help_text='Timezone for scheduling execution', max_length=50)),
                ('hour', models.IntegerField(default=2, help_text='Hour to run (0-23), used for daily/weekly schedules', validators=[MinValueValidator(0), MaxValueValidator(23)])),
                ('minute', models.IntegerField(default=0, help_text='Minute to run (0-59), used for daily/weekly schedules', validators=[MinValueValidator(0), MaxValueValidator(59)])),
                ('day_of_week', models.IntegerField(blank=True, help_text='Day of week for weekly schedules (0=Monday, 6=Sunday)', null=True, validators=[MinValueValidator(0), MaxValueValidator(6)])),
                ('day_of_month', models.IntegerField(blank=True, help_text='Day of month for monthly schedules (1-28)', null=True, validators=[MinValueValidator(1), MaxValueValidator(28)])),
                ('is_active', models.BooleanField(default=True, help_text='Whether this job is enabled')),
                ('status', models.CharField(choices=[('active', 'Active'), ('inactive', 'Inactive'), ('error', 'Error'), ('paused', 'Paused')], db_index=True, default='active', max_length=20)),
                ('last_run', models.DateTimeField(blank=True, help_text='Last execution time', null=True)),
                ('next_run', models.DateTimeField(blank=True, help_text='Next scheduled execution time', null=True)),
                ('last_run_status', models.CharField(blank=True, help_text='Status of last run', max_length=20)),
                ('consecutive_failures', models.IntegerField(default=0, help_text='Number of consecutive failures')),
                ('max_retries', models.IntegerField(default=3, help_text='Maximum retry attempts on failure')),
                ('retry_delay_minutes', models.IntegerField(default=5, help_text='Minutes to wait before retry')),
                ('failure_threshold', models.IntegerField(default=5, help_text='Number of failures before marking as error')),
                ('etl_config', models.JSONField(blank=True, default=dict, help_text='ETL configuration options (incremental vs full refresh, etc.)')),
                ('notify_on_success', models.BooleanField(default=False, help_text='Send notification on successful completion')),
                ('notify_on_failure', models.BooleanField(default=True, help_text='Send notification on failure')),
                ('notification_emails', models.JSONField(blank=True, default=list, help_text='List of email addresses to notify')),
                ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('celery_task_name', models.CharField(blank=True, help_text='Associated Celery task name', max_length=255)),
                ('celery_schedule_id', models.CharField(blank=True, help_text='Celery Beat schedule ID', max_length=255)),
                ('created_by', models.ForeignKey(db_index=True, on_delete=django.db.models.deletion.CASCADE, related_name='created_etl_jobs', to=settings.AUTH_USER_MODEL)),
                ('updated_by', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='updated_etl_jobs', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'Scheduled ETL Job',
                'verbose_name_plural': 'Scheduled ETL Jobs',
                'db_table': 'scheduled_etl_jobs',
                'ordering': ['-created_at'],
            },
        ),
        migrations.CreateModel(
            name='ETLJobRunLog',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('status', models.CharField(choices=[('started', 'Started'), ('running', 'Running'), ('success', 'Success'), ('failed', 'Failed'), ('cancelled', 'Cancelled'), ('timeout', 'Timeout')], db_index=True, default='started', max_length=20)),
                ('started_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('completed_at', models.DateTimeField(blank=True, null=True)),
                ('execution_time_seconds', models.FloatField(blank=True, help_text='Total execution time in seconds', null=True)),
                ('data_sources_processed', models.JSONField(default=list, help_text='List of data source IDs that were processed')),
                ('data_sources_failed', models.JSONField(default=list, help_text='List of data source IDs that failed processing')),
                ('data_sources_skipped', models.JSONField(default=list, help_text='List of data source IDs that were skipped')),
                ('total_records_processed', models.BigIntegerField(default=0, help_text='Total number of records processed')),
                ('total_records_added', models.BigIntegerField(default=0, help_text='Number of new records added')),
                ('total_records_updated', models.BigIntegerField(default=0, help_text='Number of records updated')),
                ('total_records_deleted', models.BigIntegerField(default=0, help_text='Number of records deleted')),
                ('error_message', models.TextField(blank=True, help_text='Error message if job failed')),
                ('error_details', models.JSONField(blank=True, default=dict, help_text='Detailed error information')),
                ('retry_count', models.IntegerField(default=0, help_text='Number of retry attempts for this run')),
                ('peak_memory_usage_mb', models.FloatField(blank=True, help_text='Peak memory usage in MB', null=True)),
                ('cpu_time_seconds', models.FloatField(blank=True, help_text='CPU time used in seconds', null=True)),
                ('execution_log', models.TextField(blank=True, help_text='Detailed execution log')),
                ('triggered_by', models.CharField(default='schedule', help_text='What triggered this run (schedule, manual, api, etc.)', max_length=50)),
                ('celery_task_id', models.CharField(blank=True, help_text='Celery task ID', max_length=255)),
                ('worker_hostname', models.CharField(blank=True, help_text='Worker that executed the job', max_length=255)),
                ('scheduled_job', models.ForeignKey(db_index=True, on_delete=django.db.models.deletion.CASCADE, related_name='run_logs', to='datasets.scheduledetljob')),
            ],
            options={
                'verbose_name': 'ETL Job Run Log',
                'verbose_name_plural': 'ETL Job Run Logs',
                'db_table': 'etl_job_run_logs',
                'ordering': ['-started_at'],
            },
        ),
        migrations.AddField(
            model_name='scheduledetljob',
            name='data_sources',
            field=models.ManyToManyField(help_text='One or more data sources to refresh during this job', related_name='scheduled_jobs', to='datasets.datasource'),
        ),
        migrations.AddIndex(
            model_name='scheduledetljob',
            index=models.Index(fields=['is_active', 'status'], name='scheduled_et_is_acti_c8b95a_idx'),
        ),
        migrations.AddIndex(
            model_name='scheduledetljob',
            index=models.Index(fields=['schedule_type', 'next_run'], name='scheduled_et_schedul_bf6234_idx'),
        ),
        migrations.AddIndex(
            model_name='scheduledetljob',
            index=models.Index(fields=['created_by', 'is_active'], name='scheduled_et_created_e2a89b_idx'),
        ),
        migrations.AddIndex(
            model_name='scheduledetljob',
            index=models.Index(fields=['next_run', 'is_active'], name='scheduled_et_next_ru_7f7c8a_idx'),
        ),
        migrations.AddIndex(
            model_name='scheduledetljob',
            index=models.Index(fields=['celery_schedule_id'], name='scheduled_et_celery__9c5f22_idx'),
        ),
        migrations.AddIndex(
            model_name='etljobrunlog',
            index=models.Index(fields=['scheduled_job', 'started_at'], name='etl_job_run_schedul_2a6d84_idx'),
        ),
        migrations.AddIndex(
            model_name='etljobrunlog',
            index=models.Index(fields=['status', 'started_at'], name='etl_job_run_status_5e8f9c_idx'),
        ),
        migrations.AddIndex(
            model_name='etljobrunlog',
            index=models.Index(fields=['started_at', 'status'], name='etl_job_run_started_d4b7fe_idx'),
        ),
        migrations.AddIndex(
            model_name='etljobrunlog',
            index=models.Index(fields=['celery_task_id'], name='etl_job_run_celery__94c3a2_idx'),
        ),
    ] 