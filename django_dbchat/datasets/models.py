"""
Models for data sources, semantic layer, and data integration.
"""
from django.db import models
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.core.validators import MinValueValidator, MaxValueValidator
import uuid
import json

User = get_user_model()


class DataSource(models.Model):
    """Data source connection and metadata with enhanced workflow tracking."""
    
    SOURCE_TYPES = [
        ('csv', 'CSV File'),
        ('postgresql', 'PostgreSQL'),
        ('mysql', 'MySQL'),
        ('oracle', 'Oracle'),
        ('sqlserver', 'SQL Server'),
        ('sqlite', 'SQLite'),
        ('api', 'REST API'),
        ('excel', 'Excel File'),
        ('json', 'JSON File'),
    ]
    
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('error', 'Error'),
        ('syncing', 'Syncing'),
        ('pending', 'Pending'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=200, help_text='Display name for the data source', db_index=True)
    source_type = models.CharField(max_length=50, choices=SOURCE_TYPES, db_index=True)
    connection_info = models.JSONField(default=dict, help_text='Connection parameters (encrypted in production)')
    schema_info = models.JSONField(default=dict, help_text='Schema and column information')
    sample_data = models.JSONField(default=dict, blank=True, help_text='Sample data for preview')
    table_name = models.CharField(max_length=200, blank=True, help_text='Sanitized table name used in DuckDB', db_index=True)
    status = models.CharField(
        max_length=50, 
        choices=STATUS_CHOICES,
        default='pending',
        db_index=True
    )
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, db_index=True)
    shared_with_users = models.ManyToManyField(
        User,
        through='DataSourceShare',
        through_fields=('data_source', 'user'),
        related_name='shared_data_sources',
        blank=True
    )
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_synced = models.DateTimeField(null=True, blank=True)
    sync_frequency = models.CharField(max_length=50, default='manual', help_text='Sync frequency')
    
    # Workflow tracking for mandatory ETL → Semantics → Query → Dashboard flow
    workflow_status = models.JSONField(
        default=dict,
        help_text="Tracks completion of mandatory workflow steps: etl_completed, semantics_completed, query_enabled, dashboard_enabled"
    )
    
    # Soft delete mechanism
    deleted_at = models.DateTimeField(null=True, blank=True, help_text='Soft delete timestamp')
    is_deleted = models.BooleanField(default=False, db_index=True)
    
    # Data lineage tracking
    source_lineage = models.JSONField(default=dict, blank=True, help_text='Source data lineage information')
    transformation_history = models.JSONField(default=list, blank=True, help_text='History of transformations applied')
    
    # Enhanced metadata
    data_quality_score = models.FloatField(null=True, blank=True, help_text='Data quality score (0-1)')
    estimated_row_count = models.BigIntegerField(null=True, blank=True, help_text='Estimated row count')
    file_size_bytes = models.BigIntegerField(null=True, blank=True, help_text='File size in bytes for file sources')
    
    class Meta:
        db_table = 'data_sources'
        verbose_name = 'Data Source'
        verbose_name_plural = 'Data Sources'
        ordering = ['-created_at']
        unique_together = [('created_by', 'name')]
        indexes = [
            models.Index(fields=['created_by', 'status']),
            models.Index(fields=['source_type', 'status']),
            models.Index(fields=['created_at', 'status']),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.source_type})"
    
    def soft_delete(self):
        """Implement soft delete"""
        self.deleted_at = timezone.now()
        self.is_deleted = True
        self.status = 'inactive'
        self.save()
    
    def restore(self):
        """Restore from soft delete"""
        self.deleted_at = None
        self.is_deleted = False
        self.status = 'active'
        self.save()


class DataSourceShare(models.Model):
    """Data source sharing permissions."""
    
    PERMISSION_CHOICES = [
        ('view', 'View Only'),
        ('query', 'Query Only'), 
        ('edit', 'Can Edit'),
    ]
    
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    permission = models.CharField(max_length=20, choices=PERMISSION_CHOICES, default='query')
    shared_at = models.DateTimeField(default=timezone.now)
    shared_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='data_source_shares_given')
    
    class Meta:
        db_table = 'data_source_shares'
        unique_together = ['data_source', 'user']
    
    def __str__(self):
        return f"{self.data_source.name} -> {self.user.username} ({self.permission})"


class SemanticTable(models.Model):
    """Semantic layer table metadata with enhanced business context."""
    
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE, related_name='semantic_tables')
    name = models.CharField(max_length=200, help_text='Physical table name', db_index=True)
    display_name = models.CharField(max_length=200, help_text='Human-readable name')
    description = models.TextField(blank=True, help_text='Table description')
    business_purpose = models.TextField(blank=True, help_text='Business context and purpose')
    tags = models.JSONField(blank=True, default=list, help_text='Tags for categorization')
    is_fact_table = models.BooleanField(default=False, help_text='Whether this is a fact table')
    is_dimension_table = models.BooleanField(default=False, help_text='Whether this is a dimension table')
    primary_key = models.CharField(max_length=200, blank=True, help_text='Primary key column')
    row_count_estimate = models.BigIntegerField(null=True, blank=True, help_text='Estimated row count')
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Enhanced metadata
    data_classification = models.CharField(max_length=50, blank=True, help_text='Data classification level')
    business_owner = models.CharField(max_length=200, blank=True, help_text='Business owner or steward')
    refresh_frequency = models.CharField(max_length=50, default='daily', help_text='Data refresh frequency')
    
    class Meta:
        db_table = 'semantic_tables'
        verbose_name = 'Semantic Table'
        verbose_name_plural = 'Semantic Tables'
        unique_together = ['data_source', 'name']
        indexes = [
            models.Index(fields=['data_source', 'name']),
            models.Index(fields=['is_fact_table', 'is_dimension_table']),
        ]
    
    def __str__(self):
        return f"{self.display_name} ({self.name})"


class SemanticColumn(models.Model):
    """Semantic layer column metadata with enhanced business logic."""
    
    DATA_TYPES = [
        ('string', 'String'),
        ('integer', 'Integer'),
        ('float', 'Float'),
        ('boolean', 'Boolean'),
        ('date', 'Date'),
        ('datetime', 'DateTime'),
        ('time', 'Time'),
        ('json', 'JSON'),
        ('binary', 'Binary'),
    ]
    
    AGGREGATION_TYPES = [
        ('sum', 'Sum'),
        ('avg', 'Average'),
        ('count', 'Count'),
        ('min', 'Minimum'),
        ('max', 'Maximum'),
        ('count_distinct', 'Count Distinct'),
    ]
    
    semantic_table = models.ForeignKey(SemanticTable, on_delete=models.CASCADE, related_name='columns', db_column='semantic_table_id')
    name = models.CharField(max_length=200, help_text='Physical column name', db_index=True)
    display_name = models.CharField(max_length=200, help_text='Human-readable name')
    description = models.TextField(blank=True, help_text='Column description')
    data_type = models.CharField(max_length=50, choices=DATA_TYPES)
    semantic_type = models.CharField(max_length=50, blank=True, help_text='Semantic type (dimension, measure, etc.)')
    is_nullable = models.BooleanField(default=True)
    is_primary_key = models.BooleanField(default=False)
    is_foreign_key = models.BooleanField(default=False)
    is_measure = models.BooleanField(default=False, help_text='Whether this column is a measure')
    is_dimension = models.BooleanField(default=False, help_text='Whether this column is a dimension')
    aggregation_default = models.CharField(max_length=50, choices=AGGREGATION_TYPES, blank=True, null=True)
    business_rules = models.JSONField(default=list, blank=True, help_text='Business rules and constraints')
    format_string = models.CharField(max_length=100, blank=True, help_text='Display format')
    sample_values = models.JSONField(default=list, blank=True, help_text='Sample values')
    common_filters = models.JSONField(default=list, blank=True, help_text='Common filter conditions')
    unique_value_count = models.IntegerField(null=True, blank=True, help_text='Estimated unique values')
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Enhanced business context
    business_glossary_term = models.CharField(max_length=200, blank=True, help_text='Related business glossary term')
    data_quality_rules = models.JSONField(default=list, blank=True, help_text='Data quality validation rules')
    
    # ETL integration fields
    is_editable = models.BooleanField(default=True, help_text='Whether this column can be edited by users')
    etl_enriched = models.BooleanField(default=False, help_text='Whether this column has been enriched through ETL operations')
    
    class Meta:
        db_table = 'semantic_columns'
        verbose_name = 'Semantic Column'
        verbose_name_plural = 'Semantic Columns'
        unique_together = ['semantic_table', 'name']
        indexes = [
            models.Index(fields=['semantic_table', 'name']),
            models.Index(fields=['is_measure', 'is_dimension']),
            models.Index(fields=['data_type', 'semantic_type']),
        ]
    
    def __str__(self):
        return f"{self.semantic_table.display_name}.{self.display_name}"


# SemanticRelationship model removed - relationships functionality disabled
# class SemanticRelationship(models.Model):
#     """Relationships between semantic tables with enhanced confidence tracking."""
#     pass


class SemanticMetric(models.Model):
    """Business metrics defined in the semantic layer with proper field names."""
    
    METRIC_TYPES = [
        ('simple', 'Simple Metric'),
        ('calculated', 'Calculated Metric'),
        ('ratio', 'Ratio Metric'),
        ('growth', 'Growth Metric'),
    ]
    
    name = models.CharField(max_length=200, help_text='Metric name', db_index=True)
    display_name = models.CharField(max_length=200, help_text='Human-readable name')
    description = models.TextField(blank=True, help_text='Metric description')
    metric_type = models.CharField(max_length=50, choices=METRIC_TYPES, db_index=True)
    calculation = models.TextField(help_text='Calculation formula or SQL expression')
    base_table = models.ForeignKey(SemanticTable, on_delete=models.CASCADE, related_name='metrics', null=True, blank=True)
    dependent_columns = models.ManyToManyField(SemanticColumn, blank=True, help_text='Columns used in calculation')
    format_string = models.CharField(max_length=100, blank=True, help_text='Display format')
    unit = models.CharField(max_length=50, blank=True, help_text='Unit of measurement')
    is_active = models.BooleanField(default=True, db_index=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, db_index=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Enhanced metric metadata
    business_owner = models.CharField(max_length=200, blank=True, help_text='Business owner of the metric')
    validation_rules = models.JSONField(default=list, blank=True, help_text='Validation rules for the metric')
    tags = models.JSONField(default=list, blank=True, help_text='Tags for categorizing the metric')
    
    class Meta:
        db_table = 'semantic_metrics'
        verbose_name = 'Semantic Metric'
        verbose_name_plural = 'Semantic Metrics'
        indexes = [
            models.Index(fields=['created_by', 'is_active']),
            models.Index(fields=['metric_type', 'is_active']),
        ]
    
    def __str__(self):
        return self.display_name


class ETLOperation(models.Model):
    """ETL operations for data integration with enhanced result tracking."""
    
    OPERATION_TYPES = [
        ('join', 'Join'),
        ('union', 'Union'),
        ('aggregate', 'Aggregate'),
        ('filter', 'Filter'),
        ('transform', 'Transform'),
        ('pivot', 'Pivot'),
        ('unpivot', 'Unpivot'),
    ]
    
    STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('paused', 'Paused'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=200, help_text='Operation name', db_index=True)
    operation_type = models.CharField(max_length=50, choices=OPERATION_TYPES, db_index=True)
    source_tables = models.JSONField(default=list, help_text='Source table identifiers')
    parameters = models.JSONField(default=dict, help_text='Operation parameters')
    sql_query = models.TextField(blank=True, help_text='Generated SQL query')
    output_table_name = models.CharField(max_length=200, help_text='Output table name')
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='draft', db_index=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, db_index=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_run = models.DateTimeField(null=True, blank=True)
    execution_time = models.FloatField(null=True, blank=True, help_text='Execution time in seconds')
    row_count = models.BigIntegerField(null=True, blank=True, help_text='Output row count')
    error_message = models.TextField(blank=True)
    
    # Fixed: Add result_summary field (was result_info in service)
    result_summary = models.JSONField(null=True, blank=True, help_text='ETL operation results summary')
    
    # Enhanced ETL tracking
    retry_count = models.IntegerField(default=0, help_text='Number of retry attempts')
    parent_operation = models.ForeignKey('self', null=True, blank=True, on_delete=models.SET_NULL, help_text='Parent operation if this is a retry')
    data_lineage = models.JSONField(null=True, blank=True, help_text='Data lineage information')
    
    class Meta:
        db_table = 'etl_operations'
        verbose_name = 'ETL Operation'
        verbose_name_plural = 'ETL Operations'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['created_by', 'status']),
            models.Index(fields=['operation_type', 'status']),
            models.Index(fields=['created_at', 'status']),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.operation_type})"


class DataIntegrationJob(models.Model):
    """Data integration job tracking with enhanced monitoring."""
    
    JOB_TYPES = [
        ('sync', 'Data Sync'),
        ('etl', 'ETL Pipeline'),
        ('schema_analysis', 'Schema Analysis'),
        ('relationship_detection', 'Relationship Detection'),
        ('semantic_generation', 'Semantic Metadata Generation'),
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=200, help_text='Job name', db_index=True)
    job_type = models.CharField(max_length=50, choices=JOB_TYPES, db_index=True)
    data_sources = models.ManyToManyField(DataSource, blank=True, help_text='Related data sources')
    etl_operations = models.ManyToManyField(ETLOperation, blank=True, help_text='Related ETL operations')
    parameters = models.JSONField(default=dict, help_text='Job parameters')
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='pending', db_index=True)
    progress = models.IntegerField(default=0, help_text='Progress percentage (0-100)')
    started_by = models.ForeignKey(User, on_delete=models.CASCADE, db_index=True)
    started_at = models.DateTimeField(default=timezone.now, db_index=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    execution_time = models.FloatField(null=True, blank=True, help_text='Execution time in seconds')
    result_summary = models.JSONField(default=dict, blank=True, help_text='Job results summary')
    error_message = models.TextField(blank=True)
    logs = models.TextField(blank=True, help_text='Job execution logs')
    
    # Enhanced job tracking
    priority = models.IntegerField(default=0, help_text='Job priority (higher number = higher priority)')
    retry_count = models.IntegerField(default=0, help_text='Number of retry attempts')
    max_retries = models.IntegerField(default=3, help_text='Maximum retry attempts')
    next_retry_at = models.DateTimeField(null=True, blank=True, help_text='Next retry time')
    
    class Meta:
        db_table = 'data_integration_jobs'
        verbose_name = 'Data Integration Job'
        verbose_name_plural = 'Data Integration Jobs'
        ordering = ['-started_at']
        indexes = [
            models.Index(fields=['started_by', 'status']),
            models.Index(fields=['job_type', 'status']),
            models.Index(fields=['priority', 'started_at']),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.job_type})"


class ScheduledETLJob(models.Model):
    """Scheduled ETL job for automatic data refresh with timezone support."""
    
    SCHEDULE_TYPE_CHOICES = [
        ('15min', 'Every 15 minutes'),
        ('30min', 'Every 30 minutes'),
        ('hourly', 'Every hour'),
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
    ]
    
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('error', 'Error'),
        ('paused', 'Paused'),
    ]
    
    TIMEZONE_CHOICES = [
        ('UTC', 'UTC'),
        ('US/Eastern', 'Eastern Time (US)'),
        ('US/Central', 'Central Time (US)'),
        ('US/Mountain', 'Mountain Time (US)'),
        ('US/Pacific', 'Pacific Time (US)'),
        ('Europe/London', 'London'),
        ('Europe/Paris', 'Paris'),
        ('Europe/Berlin', 'Berlin'),
        ('Europe/Amsterdam', 'Amsterdam'),
        ('Asia/Tokyo', 'Tokyo'),
        ('Asia/Shanghai', 'Shanghai'),
        ('Asia/Kolkata', 'India'),
        ('Asia/Dubai', 'Dubai'),
        ('Australia/Sydney', 'Sydney'),
        ('Australia/Melbourne', 'Melbourne'),
        ('America/New_York', 'New York'),
        ('America/Los_Angeles', 'Los Angeles'),
        ('America/Chicago', 'Chicago'),
        ('America/Denver', 'Denver'),
        ('America/Toronto', 'Toronto'),
        ('America/Sao_Paulo', 'São Paulo'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, help_text='Descriptive name for the scheduled job', db_index=True)
    description = models.TextField(blank=True, help_text='Optional description of what this job does')
    
    # Data sources to refresh
    data_sources = models.ManyToManyField(
        DataSource, 
        help_text='One or more data sources to refresh during this job',
        related_name='scheduled_jobs'
    )
    
    # Scheduling configuration
    schedule_type = models.CharField(
        max_length=20, 
        choices=SCHEDULE_TYPE_CHOICES, 
        default='daily',
        help_text='How frequently to run this job'
    )
    timezone = models.CharField(
        max_length=50, 
        choices=TIMEZONE_CHOICES, 
        default='UTC',
        help_text='Timezone for scheduling execution'
    )
    
    # Time-specific settings for daily/weekly schedules
    hour = models.IntegerField(
        default=2, 
        help_text='Hour to run (0-23), used for daily/weekly schedules',
        validators=[MinValueValidator(0), MaxValueValidator(23)]
    )
    minute = models.IntegerField(
        default=0, 
        help_text='Minute to run (0-59), used for daily/weekly schedules',
        validators=[MinValueValidator(0), MaxValueValidator(59)]
    )
    day_of_week = models.IntegerField(
        null=True, 
        blank=True,
        help_text='Day of week for weekly schedules (0=Monday, 6=Sunday)',
        validators=[MinValueValidator(0), MaxValueValidator(6)]
    )
    day_of_month = models.IntegerField(
        null=True, 
        blank=True,
        help_text='Day of month for monthly schedules (1-28)',
        validators=[MinValueValidator(1), MaxValueValidator(28)]
    )
    
    # Job management
    is_active = models.BooleanField(default=True, help_text='Whether this job is enabled')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active', db_index=True)
    
    # Execution tracking
    last_run = models.DateTimeField(null=True, blank=True, help_text='Last execution time')
    next_run = models.DateTimeField(null=True, blank=True, help_text='Next scheduled execution time')
    last_run_status = models.CharField(max_length=20, blank=True, help_text='Status of last run')
    consecutive_failures = models.IntegerField(default=0, help_text='Number of consecutive failures')
    
    # Error handling
    max_retries = models.IntegerField(default=3, help_text='Maximum retry attempts on failure')
    retry_delay_minutes = models.IntegerField(default=5, help_text='Minutes to wait before retry')
    failure_threshold = models.IntegerField(default=5, help_text='Number of failures before marking as error')
    
    # ETL configuration
    etl_config = models.JSONField(
        default=dict, 
        blank=True,
        help_text='ETL configuration options (incremental vs full refresh, etc.)'
    )
    
    # Notifications
    notify_on_success = models.BooleanField(default=False, help_text='Send notification on successful completion')
    notify_on_failure = models.BooleanField(default=True, help_text='Send notification on failure')
    notification_emails = models.JSONField(
        default=list, 
        blank=True,
        help_text='List of email addresses to notify'
    )
    
    # Metadata
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='created_etl_jobs', db_index=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='updated_etl_jobs')
    
    # Celery Beat integration
    celery_task_name = models.CharField(max_length=255, blank=True, help_text='Associated Celery task name')
    celery_schedule_id = models.CharField(max_length=255, blank=True, help_text='Celery Beat schedule ID')
    
    class Meta:
        db_table = 'scheduled_etl_jobs'
        verbose_name = 'Scheduled ETL Job'
        verbose_name_plural = 'Scheduled ETL Jobs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['is_active', 'status']),
            models.Index(fields=['schedule_type', 'next_run']),
            models.Index(fields=['created_by', 'is_active']),
            models.Index(fields=['next_run', 'is_active']),
            models.Index(fields=['celery_schedule_id']),
        ]
    
    def get_schedule_display(self):
        """Get human-readable schedule display."""
        schedule_map = {
            '15min': 'Every 15 minutes',
            '30min': 'Every 30 minutes', 
            'hourly': 'Every hour',
            'daily': 'Daily',
            'weekly': 'Weekly',
            'monthly': 'Monthly'
        }
        return schedule_map.get(self.schedule_type, self.schedule_type)
    
    def __str__(self):
        return f"{self.name} ({self.get_schedule_display()})"
    
    def get_cron_expression(self):
        """Generate cron expression based on schedule configuration."""
        if self.schedule_type == '15min':
            return {'minute': '*/15'}
        elif self.schedule_type == '30min':
            return {'minute': '*/30'}
        elif self.schedule_type == 'hourly':
            return {'minute': self.minute}
        elif self.schedule_type == 'daily':
            return {'hour': self.hour, 'minute': self.minute}
        elif self.schedule_type == 'weekly':
            return {
                'hour': self.hour, 
                'minute': self.minute, 
                'day_of_week': self.day_of_week or 1
            }
        elif self.schedule_type == 'monthly':
            return {
                'hour': self.hour, 
                'minute': self.minute, 
                'day_of_month': self.day_of_month or 1
            }
        return {}
    
    def calculate_next_run(self):
        """Calculate next run time in the specified timezone."""
        import pytz
        from datetime import datetime, timedelta
        from celery.schedules import crontab
        from django.utils import timezone as django_timezone
        
        try:
            tz = pytz.timezone(self.timezone)
            now = django_timezone.now().astimezone(tz)
            
            if self.schedule_type == '15min':
                # Next 15-minute interval
                minutes_to_add = 15 - (now.minute % 15)
                next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_add)
            elif self.schedule_type == '30min':
                # Next 30-minute interval
                minutes_to_add = 30 - (now.minute % 30)
                next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_add)
            elif self.schedule_type == 'hourly':
                # Next hour at specified minute
                if now.minute <= self.minute:
                    next_run = now.replace(minute=self.minute, second=0, microsecond=0)
                else:
                    next_run = (now + timedelta(hours=1)).replace(minute=self.minute, second=0, microsecond=0)
            elif self.schedule_type == 'daily':
                # Next day at specified time
                next_run = now.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(days=1)
            elif self.schedule_type == 'weekly':
                # Next week on specified day and time
                days_ahead = (self.day_of_week or 1) - now.weekday()
                if days_ahead <= 0:  # Target day already happened this week
                    days_ahead += 7
                next_run = now.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0) + timedelta(days=days_ahead)
            elif self.schedule_type == 'monthly':
                # Next month on specified day and time
                next_run = now.replace(day=self.day_of_month or 1, hour=self.hour, minute=self.minute, second=0, microsecond=0)
                if next_run <= now:
                    # Move to next month
                    if next_run.month == 12:
                        next_run = next_run.replace(year=next_run.year + 1, month=1)
                    else:
                        next_run = next_run.replace(month=next_run.month + 1)
            
            # Convert back to UTC for storage
            return next_run.astimezone(pytz.UTC)
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error calculating next run for job {self.id}: {e}")
            # Fallback: return current time + 1 hour
            return django_timezone.now() + timedelta(hours=1)
    
    def update_next_run(self):
        """Update the next_run field."""
        self.next_run = self.calculate_next_run()
        self.save(update_fields=['next_run'])
    
    def can_run_now(self):
        """Check if this job can run now."""
        if not self.is_active or self.status != 'active':
            return False
        
        from django.utils import timezone as django_timezone
        if self.next_run and django_timezone.now() < self.next_run:
            return False
        
        return True
    
    def mark_failure(self, error_message=''):
        """Mark job as failed and update failure tracking."""
        self.consecutive_failures += 1
        self.last_run_status = 'failed'
        
        if self.consecutive_failures >= self.failure_threshold:
            self.status = 'error'
        
        # Update next run for retry
        from django.utils import timezone as django_timezone
        from datetime import timedelta
        if self.consecutive_failures <= self.max_retries:
            retry_time = django_timezone.now() + timedelta(minutes=self.retry_delay_minutes)
            self.next_run = retry_time
        else:
            self.update_next_run()
        
        self.save(update_fields=['consecutive_failures', 'last_run_status', 'status', 'next_run'])
    
    def mark_success(self):
        """Mark job as successful and reset failure tracking."""
        from django.utils import timezone as django_timezone
        self.consecutive_failures = 0
        self.last_run_status = 'success'
        self.last_run = django_timezone.now()
        if self.status == 'error':
            self.status = 'active'
        
        # Calculate next regular run
        self.update_next_run()
        
        self.save(update_fields=['consecutive_failures', 'last_run_status', 'last_run', 'status', 'next_run'])


class ETLJobRunLog(models.Model):
    """Execution log for scheduled ETL jobs."""
    
    STATUS_CHOICES = [
        ('started', 'Started'),
        ('running', 'Running'),
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
        ('timeout', 'Timeout'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    scheduled_job = models.ForeignKey(
        ScheduledETLJob, 
        on_delete=models.CASCADE, 
        related_name='run_logs',
        db_index=True
    )
    
    # Execution details
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='started', db_index=True)
    started_at = models.DateTimeField(auto_now_add=True, db_index=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    execution_time_seconds = models.FloatField(null=True, blank=True, help_text='Total execution time in seconds')
    
    # Data source processing details
    data_sources_processed = models.JSONField(
        default=list, 
        help_text='List of data source IDs that were processed'
    )
    data_sources_failed = models.JSONField(
        default=list, 
        help_text='List of data source IDs that failed processing'
    )
    data_sources_skipped = models.JSONField(
        default=list, 
        help_text='List of data source IDs that were skipped'
    )
    
    # Results summary
    total_records_processed = models.BigIntegerField(default=0, help_text='Total number of records processed')
    total_records_added = models.BigIntegerField(default=0, help_text='Number of new records added')
    total_records_updated = models.BigIntegerField(default=0, help_text='Number of records updated')
    total_records_deleted = models.BigIntegerField(default=0, help_text='Number of records deleted')
    
    # Error handling
    error_message = models.TextField(blank=True, help_text='Error message if job failed')
    error_details = models.JSONField(default=dict, blank=True, help_text='Detailed error information')
    retry_count = models.IntegerField(default=0, help_text='Number of retry attempts for this run')
    
    # Resource usage
    peak_memory_usage_mb = models.FloatField(null=True, blank=True, help_text='Peak memory usage in MB')
    cpu_time_seconds = models.FloatField(null=True, blank=True, help_text='CPU time used in seconds')
    
    # Detailed logs
    execution_log = models.TextField(blank=True, help_text='Detailed execution log')
    
    # Metadata
    triggered_by = models.CharField(
        max_length=50, 
        default='schedule', 
        help_text='What triggered this run (schedule, manual, api, etc.)'
    )
    celery_task_id = models.CharField(max_length=255, blank=True, help_text='Celery task ID')
    worker_hostname = models.CharField(max_length=255, blank=True, help_text='Worker that executed the job')
    
    class Meta:
        db_table = 'etl_job_run_logs'
        verbose_name = 'ETL Job Run Log'
        verbose_name_plural = 'ETL Job Run Logs'
        ordering = ['-started_at']
        indexes = [
            models.Index(fields=['scheduled_job', 'started_at']),
            models.Index(fields=['status', 'started_at']),
            models.Index(fields=['started_at', 'status']),
            models.Index(fields=['celery_task_id']),
        ]
    
    def __str__(self):
        return f"{self.scheduled_job.name} - {self.started_at.strftime('%Y-%m-%d %H:%M:%S')} ({self.status})"
    
    def duration(self):
        """Get execution duration as a timedelta object."""
        if self.completed_at and self.started_at:
            return self.completed_at - self.started_at
        return None
    
    def duration_formatted(self):
        """Get formatted duration string."""
        duration = self.duration()
        if duration:
            total_seconds = int(duration.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            if hours:
                return f"{hours}h {minutes}m {seconds}s"
            elif minutes:
                return f"{minutes}m {seconds}s"
            else:
                return f"{seconds}s"
        return "N/A"
    
    def success_rate(self):
        """Calculate success rate for data sources in this run."""
        total_sources = len(self.data_sources_processed)
        failed_sources = len(self.data_sources_failed)
        if total_sources > 0:
            return ((total_sources - failed_sources) / total_sources) * 100
        return 0.0
    
    def mark_completed(self, status='success', error_message=''):
        """Mark the run as completed."""
        from django.utils import timezone as django_timezone
        self.completed_at = django_timezone.now()
        self.status = status
        if error_message:
            self.error_message = error_message
        
        # Calculate execution time
        if self.started_at:
            duration = self.completed_at - self.started_at
            self.execution_time_seconds = duration.total_seconds()
        
        self.save(update_fields=['completed_at', 'status', 'error_message', 'execution_time_seconds']) 