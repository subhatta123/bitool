# ETL Scheduling System Documentation

## Overview

The ConvaBI ETL Scheduling System provides automatic data refresh capabilities with timezone-aware scheduling, multi-source support, and comprehensive error handling. This system allows users to schedule ETL jobs that can refresh one or multiple data sources at regular intervals.

## Features

### 1. **Timezone-Aware Scheduling**
- Support for 20+ global timezones
- User timezone preferences stored in user model
- Accurate execution time calculation across different timezones
- Timezone conversion for scheduling and logging

### 2. **Flexible Schedule Types**
- **15 minutes**: Every 15 minutes
- **30 minutes**: Every 30 minutes  
- **Hourly**: Every hour at specified minute
- **Daily**: Every day at specified time
- **Weekly**: Every week on specified day and time
- **Monthly**: Every month on specified day and time

### 3. **Multi-Source Support**
- Single ETL job can process multiple data sources
- Support for CSV, PostgreSQL, MySQL, Oracle, SQL Server, and API sources
- Failure isolation - if one source fails, others continue processing
- Individual source processing tracking and reporting

### 4. **Advanced Error Handling**
- Configurable retry attempts (default: 3)
- Exponential backoff retry strategy
- Failure threshold configuration
- Automatic job disabling after consecutive failures
- Detailed error logging and notification

### 5. **Comprehensive Monitoring**
- Execution logs with detailed metrics
- Performance tracking (execution time, memory usage)
- Success/failure statistics
- Real-time job status monitoring

### 6. **Notification System**
- Email notifications for success/failure
- Configurable notification preferences
- Multiple email recipient support
- Rich HTML email templates

## Architecture

### Models

#### `ScheduledETLJob`
Main model for ETL job configuration:
```python
- name: Job display name
- description: Optional job description
- data_sources: ManyToMany relationship to DataSource
- schedule_type: Frequency (15min, hourly, daily, etc.)
- timezone: Execution timezone
- hour/minute: Time specification for daily/weekly jobs
- day_of_week: Day for weekly jobs (0=Monday)
- day_of_month: Day for monthly jobs (1-28)
- is_active: Enable/disable flag
- status: Job status (active, inactive, error, paused)
- etl_config: JSON configuration for ETL options
- notification settings: Email notification preferences
```

#### `ETLJobRunLog`
Execution tracking model:
```python
- scheduled_job: FK to ScheduledETLJob
- status: Execution status (started, running, success, failed)
- execution_time_seconds: Total execution time
- data_sources_processed/failed/skipped: Processing results
- total_records_processed/added/updated: Data metrics
- error_message: Failure details
- resource_usage: Memory and CPU tracking
- celery_task_id: Background task tracking
```

### Services

#### `ScheduledETLService`
Core ETL execution service:
- Multi-source data processing
- ETL mode support (full vs incremental)
- Resource monitoring
- Error handling and retry logic

#### `ETLScheduleManager`
Dynamic Celery Beat schedule management:
- Create/update/delete periodic tasks
- Timezone-aware cron expression generation
- Schedule status monitoring
- Orphaned schedule cleanup

## Installation and Setup

### 1. Install Dependencies

Add to `requirements.txt`:
```
django-celery-beat==2.5.0
pytz==2023.3
```

### 2. Update Django Settings

Add to `INSTALLED_APPS`:
```python
INSTALLED_APPS = [
    # ... other apps
    'django_celery_beat',
    # ... rest of apps
]
```

### 3. Run Migrations

```bash
python manage.py migrate
```

### 4. Start Celery Services

```bash
# Start Celery Worker
celery -A dbchat_project worker --loglevel=info

# Start Celery Beat Scheduler
celery -A dbchat_project beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler
```

## Usage

### Creating Scheduled ETL Jobs

#### Via API (Recommended)

```python
POST /datasets/scheduled-jobs/create/

{
    "name": "Daily Sales Data Refresh",
    "description": "Refresh sales data from multiple sources",
    "data_source_ids": ["uuid1", "uuid2", "uuid3"],
    "schedule_type": "daily",
    "timezone": "America/New_York", 
    "hour": 6,
    "minute": 0,
    "etl_config": {
        "mode": "incremental",
        "batch_size": 1000
    },
    "notify_on_success": false,
    "notify_on_failure": true,
    "notification_emails": ["admin@company.com"]
}
```

#### Via Django Admin

1. Navigate to Django Admin → Scheduled ETL Jobs
2. Click "Add Scheduled ETL Job"
3. Fill in job configuration
4. Select data sources
5. Configure schedule and timezone
6. Save

### Managing Jobs

#### Enable/Disable Jobs
```python
POST /datasets/scheduled-jobs/{job_id}/enable/
POST /datasets/scheduled-jobs/{job_id}/disable/
```

#### Run Job Immediately
```python
POST /datasets/scheduled-jobs/{job_id}/run/
```

#### View Job Status
```python
GET /datasets/scheduled-jobs/{job_id}/
```

#### View Execution Logs
```python
GET /datasets/scheduled-jobs/{job_id}/logs/?page=1&page_size=20&status=failed
```

### Configuration Options

#### ETL Configuration
```json
{
    "mode": "full|incremental",
    "batch_size": 1000,
    "timeout_minutes": 60,
    "parallel_processing": true,
    "custom_sql": "SELECT * FROM table WHERE updated_at > :last_sync"
}
```

#### Timezone Options
- UTC (default)
- US/Eastern, US/Central, US/Mountain, US/Pacific
- Europe/London, Europe/Paris, Europe/Berlin
- Asia/Tokyo, Asia/Shanghai, Asia/Kolkata
- Australia/Sydney, Australia/Melbourne
- America/New_York, America/Los_Angeles, etc.

## Monitoring and Troubleshooting

### Job Status Monitoring

Jobs have the following statuses:
- **active**: Running normally
- **inactive**: Manually disabled
- **error**: Consecutive failures exceeded threshold
- **paused**: Temporarily stopped

### Execution Status

Run logs track:
- **started**: Job initiated
- **running**: Currently executing
- **success**: Completed successfully
- **failed**: Execution failed
- **cancelled**: Manually cancelled
- **timeout**: Exceeded time limit

### Common Issues

#### 1. Job Not Running
- Check if job is active: `is_active=True`
- Verify Celery workers are running
- Check Celery Beat scheduler is running
- Review timezone configuration

#### 2. Data Source Failures
- Check data source connection status
- Verify ETL workflow completion
- Review data source permissions
- Check network connectivity

#### 3. Schedule Not Updating
- Verify Celery Beat database scheduler
- Check for orphaned periodic tasks
- Review timezone calculations
- Restart Celery Beat if needed

### Performance Optimization

#### 1. Batch Processing
```json
{
    "etl_config": {
        "batch_size": 5000,
        "parallel_processing": true
    }
}
```

#### 2. Incremental Updates
```json
{
    "etl_config": {
        "mode": "incremental",
        "timestamp_column": "updated_at",
        "lookback_minutes": 60
    }
}
```

#### 3. Resource Limits
```python
# In Celery configuration
task_time_limit = 30 * 60  # 30 minutes
task_soft_time_limit = 25 * 60  # 25 minutes
worker_max_memory_per_child = 200000  # 200MB
```

## API Reference

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/datasets/scheduled-jobs/` | List all jobs |
| POST | `/datasets/scheduled-jobs/create/` | Create new job |
| GET | `/datasets/scheduled-jobs/{id}/` | Get job details |
| POST | `/datasets/scheduled-jobs/{id}/run/` | Run job now |
| POST | `/datasets/scheduled-jobs/{id}/enable/` | Enable job |
| POST | `/datasets/scheduled-jobs/{id}/disable/` | Disable job |
| DELETE | `/datasets/scheduled-jobs/{id}/delete/` | Delete job |
| GET | `/datasets/scheduled-jobs/{id}/logs/` | Get execution logs |

### Response Formats

#### Job List Response
```json
{
    "success": true,
    "jobs": [
        {
            "id": "uuid",
            "name": "Job Name",
            "schedule_display": "Daily at 06:00",
            "timezone": "America/New_York",
            "is_active": true,
            "status": "active",
            "last_run": "2025-01-16T06:00:00Z",
            "next_run": "2025-01-17T06:00:00Z",
            "success_rate": 95.5,
            "data_sources_count": 3
        }
    ]
}
```

#### Execution Log Response
```json
{
    "success": true,
    "logs": [
        {
            "id": "uuid",
            "status": "success",
            "started_at": "2025-01-16T06:00:00Z",
            "completed_at": "2025-01-16T06:05:00Z",
            "duration": "5m 0s",
            "total_records_processed": 10000,
            "data_sources_processed": ["uuid1", "uuid2"],
            "data_sources_failed": [],
            "triggered_by": "schedule"
        }
    ],
    "pagination": {
        "current_page": 1,
        "total_pages": 5,
        "total_logs": 100
    }
}
```

## Security Considerations

1. **Access Control**: Only job creators can manage their jobs
2. **Data Source Permissions**: Users can only schedule jobs for their own data sources
3. **Email Validation**: Notification emails are validated
4. **Resource Limits**: Job execution is time and memory limited
5. **Audit Trail**: All job actions are logged

## Best Practices

1. **Schedule Naming**: Use descriptive names indicating data sources and frequency
2. **Timezone Selection**: Choose timezone closest to data source location
3. **Notification Setup**: Always enable failure notifications
4. **Resource Planning**: Stagger job schedules to avoid resource conflicts
5. **Incremental Processing**: Use incremental mode for large datasets
6. **Monitoring**: Regularly review execution logs and success rates

## Troubleshooting Commands

```bash
# Check Celery worker status
celery -A dbchat_project inspect active

# Check scheduled tasks
celery -A dbchat_project inspect scheduled

# Restart Celery services
pkill -f celery
celery -A dbchat_project worker --detach
celery -A dbchat_project beat --detach

# View Celery logs
tail -f celery.log
```

## Support

For issues or questions:
1. Check execution logs in Django Admin
2. Review Celery worker logs
3. Verify data source connectivity
4. Check timezone configuration
5. Contact system administrator if issues persist 