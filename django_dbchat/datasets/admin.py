"""
Django admin configuration for datasets app.
"""
from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
from django.db import models
from django.forms import Textarea
import json

from .models import (
    DataSource, DataSourceShare, SemanticTable, SemanticColumn, 
    ETLOperation, DataIntegrationJob, ScheduledETLJob, ETLJobRunLog
)


@admin.register(DataSource)
class DataSourceAdmin(admin.ModelAdmin):
    """Admin interface for DataSource model."""
    
    list_display = ('name', 'source_type', 'status', 'created_by', 'created_at', 'last_synced')
    list_filter = ('source_type', 'status', 'created_at', 'last_synced')
    search_fields = ('name', 'created_by__username', 'created_by__email')
    readonly_fields = ('id', 'created_at', 'updated_at', 'last_synced')
    ordering = ('-created_at',)
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('id', 'name', 'source_type', 'status', 'created_by')
        }),
        ('Schema & Data', {
            'fields': ('table_name', 'estimated_row_count', 'file_size_bytes', 'data_quality_score'),
            'classes': ('collapse',)
        }),
        ('Connection Information', {
            'fields': ('connection_info', 'schema_info', 'sample_data'),
            'classes': ('collapse',)
        }),
        ('Workflow Status', {
            'fields': ('workflow_status', 'sync_frequency'),
            'classes': ('collapse',)
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at', 'last_synced'),
            'classes': ('collapse',)
        })
    )
    
    formfield_overrides = {
        models.JSONField: {'widget': Textarea(attrs={'rows': 4, 'cols': 80})},
    }


@admin.register(DataSourceShare)
class DataSourceShareAdmin(admin.ModelAdmin):
    """Admin interface for DataSourceShare model."""
    
    list_display = ('data_source', 'user', 'permission', 'shared_at', 'shared_by')
    list_filter = ('permission', 'shared_at')
    search_fields = ('data_source__name', 'user__username', 'shared_by__username')
    readonly_fields = ('shared_at',)


@admin.register(SemanticTable)
class SemanticTableAdmin(admin.ModelAdmin):
    """Admin interface for SemanticTable model."""
    
    list_display = ('display_name', 'name', 'data_source', 'is_fact_table', 'is_dimension_table', 'created_at')
    list_filter = ('is_fact_table', 'is_dimension_table', 'data_classification', 'created_at')
    search_fields = ('name', 'display_name', 'description', 'business_owner')
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('data_source', 'name', 'display_name', 'description')
        }),
        ('Table Classification', {
            'fields': ('is_fact_table', 'is_dimension_table', 'primary_key')
        }),
        ('Business Context', {
            'fields': ('business_purpose', 'business_owner', 'data_classification'),
            'classes': ('collapse',)
        }),
        ('Metadata', {
            'fields': ('row_count_estimate', 'refresh_frequency', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )


@admin.register(SemanticColumn)
class SemanticColumnAdmin(admin.ModelAdmin):
    """Admin interface for SemanticColumn model."""
    
    list_display = ('display_name', 'name', 'semantic_table', 'data_type', 'is_measure', 'is_dimension')
    list_filter = ('data_type', 'is_measure', 'is_dimension', 'is_primary_key', 'is_foreign_key')
    search_fields = ('name', 'display_name', 'description', 'semantic_table__name')
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('semantic_table', 'name', 'display_name', 'description')
        }),
        ('Data Type & Properties', {
            'fields': ('data_type', 'semantic_type', 'is_nullable', 'aggregation_default')
        }),
        ('Classification', {
            'fields': ('is_measure', 'is_dimension', 'is_primary_key', 'is_foreign_key')
        }),
        ('Business Context', {
            'fields': ('business_glossary_term', 'format_string'),
            'classes': ('collapse',)
        }),
        ('Sample Data & Rules', {
            'fields': ('sample_values', 'business_rules', 'data_quality_rules'),
            'classes': ('collapse',)
        })
    )


@admin.register(ETLOperation)
class ETLOperationAdmin(admin.ModelAdmin):
    """Admin interface for ETLOperation model."""
    
    list_display = ('name', 'operation_type', 'status', 'created_by', 'created_at', 'last_run')
    list_filter = ('operation_type', 'status', 'created_at', 'last_run')
    search_fields = ('name', 'created_by__username')
    readonly_fields = ('id', 'created_at', 'updated_at', 'last_run', 'execution_time', 'row_count')
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('id', 'name', 'operation_type', 'status', 'created_by')
        }),
        ('Configuration', {
            'fields': ('source_tables', 'parameters', 'output_table_name')
        }),
        ('SQL Query', {
            'fields': ('sql_query',),
            'classes': ('collapse',)
        }),
        ('Execution Results', {
            'fields': ('last_run', 'execution_time', 'row_count', 'result_summary', 'error_message'),
            'classes': ('collapse',)
        }),
        ('Retry & Lineage', {
            'fields': ('retry_count', 'parent_operation', 'data_lineage'),
            'classes': ('collapse',)
        })
    )


class ETLJobRunLogInline(admin.TabularInline):
    """Inline admin for ETL job run logs."""
    
    model = ETLJobRunLog
    extra = 0
    readonly_fields = ('status', 'started_at', 'completed_at', 'execution_time_seconds', 'total_records_processed')
    fields = ('status', 'started_at', 'completed_at', 'execution_time_seconds', 'total_records_processed', 'triggered_by')
    
    def has_add_permission(self, request, obj=None):
        return False


@admin.register(ScheduledETLJob)
class ScheduledETLJobAdmin(admin.ModelAdmin):
    """Admin interface for ScheduledETLJob model."""
    
    list_display = ('name', 'schedule_display', 'timezone', 'status', 'is_active', 'last_run_status', 'created_by')
    list_filter = ('schedule_type', 'timezone', 'status', 'is_active', 'last_run_status', 'created_at')
    search_fields = ('name', 'description', 'created_by__username')
    readonly_fields = (
        'id', 'created_at', 'updated_at', 'last_run', 'next_run', 
        'consecutive_failures', 'celery_task_name', 'celery_schedule_id'
    )
    filter_horizontal = ('data_sources',)
    inlines = [ETLJobRunLogInline]
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('id', 'name', 'description', 'created_by', 'updated_by')
        }),
        ('Data Sources', {
            'fields': ('data_sources',)
        }),
        ('Schedule Configuration', {
            'fields': ('schedule_type', 'timezone', 'hour', 'minute', 'day_of_week', 'day_of_month')
        }),
        ('Job Management', {
            'fields': ('is_active', 'status')
        }),
        ('Execution Tracking', {
            'fields': ('last_run', 'next_run', 'last_run_status', 'consecutive_failures'),
            'classes': ('collapse',)
        }),
        ('Error Handling', {
            'fields': ('max_retries', 'retry_delay_minutes', 'failure_threshold'),
            'classes': ('collapse',)
        }),
        ('ETL Configuration', {
            'fields': ('etl_config',),
            'classes': ('collapse',)
        }),
        ('Notifications', {
            'fields': ('notify_on_success', 'notify_on_failure', 'notification_emails'),
            'classes': ('collapse',)
        }),
        ('System Information', {
            'fields': ('celery_task_name', 'celery_schedule_id', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    actions = ['enable_jobs', 'disable_jobs', 'run_now', 'update_schedules']
    
    def schedule_display(self, obj):
        """Display schedule in a readable format."""
        schedule = obj.get_schedule_type_display()
        if obj.schedule_type in ['daily', 'weekly']:
            schedule += f" at {obj.hour:02d}:{obj.minute:02d}"
        if obj.schedule_type == 'weekly' and obj.day_of_week is not None:
            days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            schedule += f" on {days[obj.day_of_week]}"
        elif obj.schedule_type == 'monthly' and obj.day_of_month is not None:
            schedule += f" on day {obj.day_of_month}"
        return schedule
    schedule_display.short_description = 'Schedule'
    
    def enable_jobs(self, request, queryset):
        """Enable selected jobs."""
        updated = queryset.update(is_active=True, status='active')
        self.message_user(request, f'Enabled {updated} jobs.')
    enable_jobs.short_description = 'Enable selected jobs'
    
    def disable_jobs(self, request, queryset):
        """Disable selected jobs."""
        updated = queryset.update(is_active=False, status='inactive')
        self.message_user(request, f'Disabled {updated} jobs.')
    disable_jobs.short_description = 'Disable selected jobs'
    
    def run_now(self, request, queryset):
        """Trigger immediate execution of selected jobs."""
        from services.scheduled_etl_service import execute_scheduled_etl_job
        
        triggered_count = 0
        for job in queryset:
            if job.is_active and job.status == 'active':
                execute_scheduled_etl_job.delay(str(job.id), 'manual_admin')
                triggered_count += 1
        
        self.message_user(request, f'Triggered {triggered_count} jobs for immediate execution.')
    run_now.short_description = 'Run selected jobs now'
    
    def update_schedules(self, request, queryset):
        """Update next_run times for selected jobs."""
        updated_count = 0
        for job in queryset:
            if job.is_active:
                job.update_next_run()
                updated_count += 1
        
        self.message_user(request, f'Updated schedules for {updated_count} jobs.')
    update_schedules.short_description = 'Update job schedules'


@admin.register(ETLJobRunLog)
class ETLJobRunLogAdmin(admin.ModelAdmin):
    """Admin interface for ETLJobRunLog model."""
    
    list_display = (
        'scheduled_job', 'status', 'started_at', 'duration_display', 
        'total_records_processed', 'triggered_by'
    )
    list_filter = ('status', 'triggered_by', 'started_at')
    search_fields = ('scheduled_job__name', 'celery_task_id', 'worker_hostname')
    readonly_fields = (
        'id', 'started_at', 'completed_at', 'execution_time_seconds',
        'celery_task_id', 'worker_hostname'
    )
    date_hierarchy = 'started_at'
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('id', 'scheduled_job', 'status', 'triggered_by')
        }),
        ('Execution Timing', {
            'fields': ('started_at', 'completed_at', 'execution_time_seconds')
        }),
        ('Data Processing Results', {
            'fields': (
                'data_sources_processed', 'data_sources_failed', 'data_sources_skipped',
                'total_records_processed', 'total_records_added', 'total_records_updated', 'total_records_deleted'
            ),
            'classes': ('collapse',)
        }),
        ('Error Information', {
            'fields': ('error_message', 'error_details', 'retry_count'),
            'classes': ('collapse',)
        }),
        ('Resource Usage', {
            'fields': ('peak_memory_usage_mb', 'cpu_time_seconds'),
            'classes': ('collapse',)
        }),
        ('System Information', {
            'fields': ('celery_task_id', 'worker_hostname'),
            'classes': ('collapse',)
        }),
        ('Detailed Log', {
            'fields': ('execution_log',),
            'classes': ('collapse',)
        })
    )
    
    def duration_display(self, obj):
        """Display execution duration."""
        return obj.duration_formatted()
    duration_display.short_description = 'Duration'
    
    def has_add_permission(self, request):
        """Prevent manual creation of run logs."""
        return False


@admin.register(DataIntegrationJob)
class DataIntegrationJobAdmin(admin.ModelAdmin):
    """Admin interface for DataIntegrationJob model."""
    
    list_display = ('name', 'job_type', 'status', 'progress', 'started_by', 'started_at')
    list_filter = ('job_type', 'status', 'priority', 'started_at')
    search_fields = ('name', 'started_by__username')
    readonly_fields = ('id', 'started_at', 'completed_at', 'execution_time')
    filter_horizontal = ('data_sources', 'etl_operations')
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('id', 'name', 'job_type', 'status', 'progress', 'started_by')
        }),
        ('Related Objects', {
            'fields': ('data_sources', 'etl_operations')
        }),
        ('Configuration', {
            'fields': ('parameters', 'priority')
        }),
        ('Execution Details', {
            'fields': ('started_at', 'completed_at', 'execution_time'),
            'classes': ('collapse',)
        }),
        ('Results & Errors', {
            'fields': ('result_summary', 'error_message', 'logs'),
            'classes': ('collapse',)
        }),
        ('Retry Information', {
            'fields': ('retry_count', 'max_retries', 'next_retry_at'),
            'classes': ('collapse',)
        })
    )


# Customize admin site header and title
admin.site.site_header = "ConvaBI Data Management"
admin.site.site_title = "ConvaBI Admin"
admin.site.index_title = "Data Sources & ETL Management" 