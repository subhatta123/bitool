"""
Models for data sources, semantic layer, and data integration.
"""
from django.db import models
from django.utils import timezone
from django.contrib.auth import get_user_model
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
    scheduled_time = models.DateTimeField(null=True, blank=True, help_text='Scheduled execution time')
    retry_count = models.IntegerField(default=0, help_text='Number of retry attempts')
    max_retries = models.IntegerField(default=3, help_text='Maximum retry attempts')
    
    class Meta:
        db_table = 'data_integration_jobs'
        verbose_name = 'Data Integration Job'
        verbose_name_plural = 'Data Integration Jobs'
        ordering = ['-started_at']
        indexes = [
            models.Index(fields=['started_by', 'status']),
            models.Index(fields=['job_type', 'status']),
            models.Index(fields=['started_at', 'status']),
            models.Index(fields=['priority', 'scheduled_time']),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.job_type}) - {self.status}" 