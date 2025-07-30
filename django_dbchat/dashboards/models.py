"""
Models for dashboard management and sharing.
"""
from django.db import models
from django.utils import timezone
from django.contrib.auth import get_user_model
import uuid
import json

User = get_user_model()


class Dashboard(models.Model):
    """Dashboard model for storing user dashboards."""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=200, help_text='Dashboard name')
    description = models.TextField(blank=True, help_text='Dashboard description')
    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name='owned_dashboards')
    is_public = models.BooleanField(default=False, help_text='Whether dashboard is publicly accessible')
    shared_with_users = models.ManyToManyField(
        User,
        through='DashboardShare',
        through_fields=('dashboard', 'user'),
        related_name='shared_dashboards',
        blank=True
    )
    layout_config = models.JSONField(default=dict, help_text='Dashboard layout configuration')
    filters = models.JSONField(default=list, help_text='Dashboard-level filters')
    refresh_interval = models.IntegerField(default=0, help_text='Auto-refresh interval in seconds')
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    last_accessed = models.DateTimeField(null=True, blank=True)
    access_count = models.IntegerField(default=0, help_text='Number of times accessed')
    
    class Meta:
        db_table = 'dashboards'
        verbose_name = 'Dashboard'
        verbose_name_plural = 'Dashboards'
        ordering = ['-updated_at']
    
    def __str__(self):
        return self.name


class DashboardShare(models.Model):
    """Dashboard sharing permissions."""
    
    PERMISSION_CHOICES = [
        ('view', 'View Only'),
        ('edit', 'Can Edit'),
        ('admin', 'Admin'),
    ]
    
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    permission = models.CharField(max_length=20, choices=PERMISSION_CHOICES, default='view')
    shared_at = models.DateTimeField(default=timezone.now)
    shared_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='dashboard_shares_given')
    
    class Meta:
        db_table = 'dashboard_shares'
        unique_together = ['dashboard', 'user']
    
    def __str__(self):
        return f"{self.dashboard.name} -> {self.user.username} ({self.permission})"


class DashboardItem(models.Model):
    """Individual dashboard items (charts, KPIs, tables)."""
    
    ITEM_TYPES = [
        ('chart', 'Chart'),
        ('kpi', 'KPI'),
        ('table', 'Table'),
        ('text', 'Text'),
        ('filter', 'Filter'),
        ('image', 'Image'),
    ]
    
    CHART_TYPES = [
        ('bar', 'Bar Chart'),
        ('line', 'Line Chart'),
        ('pie', 'Pie Chart'),
        ('scatter', 'Scatter Plot'),
        ('histogram', 'Histogram'),
        ('heatmap', 'Heatmap'),
        ('treemap', 'Treemap'),
        ('gauge', 'Gauge'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE, related_name='items')
    title = models.CharField(max_length=200, help_text='Item title')
    item_type = models.CharField(max_length=50, choices=ITEM_TYPES)
    chart_type = models.CharField(max_length=50, choices=CHART_TYPES, blank=True)
    query = models.TextField(blank=True, help_text='SQL query for data')
    chart_config = models.JSONField(default=dict, help_text='Chart configuration')
    result_data = models.JSONField(default=list, blank=True, help_text='Cached query result data')
    position_x = models.IntegerField(default=0, help_text='X position in grid')
    position_y = models.IntegerField(default=0, help_text='Y position in grid')
    width = models.IntegerField(default=4, help_text='Width in grid units')
    height = models.IntegerField(default=3, help_text='Height in grid units')
    data_source = models.CharField(max_length=200, blank=True, help_text='Data source identifier')
    refresh_interval = models.IntegerField(default=0, help_text='Refresh interval in seconds')
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'dashboard_items'
        verbose_name = 'Dashboard Item'
        verbose_name_plural = 'Dashboard Items'
        ordering = ['position_y', 'position_x']
    
    def __str__(self):
        return f"{self.dashboard.name} - {self.title}"


class DashboardFilter(models.Model):
    """AI-generated filters for dashboards."""
    
    FILTER_TYPES = [
        ('date_range', 'Date Range'),
        ('select', 'Select'),
        ('multi_select', 'Multi Select'),
        ('text', 'Text Input'),
        ('numeric_range', 'Numeric Range'),
        ('boolean', 'Boolean'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE, related_name='dashboard_filters')
    name = models.CharField(max_length=200, help_text='Filter name')
    display_name = models.CharField(max_length=200, help_text='Display name')
    filter_type = models.CharField(max_length=50, choices=FILTER_TYPES)
    column_reference = models.CharField(max_length=200, help_text='Column being filtered')
    default_value = models.JSONField(default=dict, blank=True, help_text='Default filter value')
    options = models.JSONField(default=list, blank=True, help_text='Available filter options')
    is_required = models.BooleanField(default=False, help_text='Whether filter is required')
    is_global = models.BooleanField(default=True, help_text='Whether filter applies to all items')
    target_items = models.ManyToManyField(DashboardItem, blank=True, help_text='Specific items this filter applies to')
    created_at = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'dashboard_filters'
        verbose_name = 'Dashboard Filter'
        verbose_name_plural = 'Dashboard Filters'
    
    def __str__(self):
        return f"{self.dashboard.name} - {self.display_name}"


class DashboardVersion(models.Model):
    """Dashboard version history."""
    
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE, related_name='versions')
    version_number = models.IntegerField(help_text='Version number')
    version_name = models.CharField(max_length=200, blank=True, help_text='Version name or description')
    dashboard_data = models.JSONField(help_text='Complete dashboard configuration')
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(default=timezone.now)
    is_published = models.BooleanField(default=False, help_text='Whether this version is published')
    change_summary = models.TextField(blank=True, help_text='Summary of changes')
    
    class Meta:
        db_table = 'dashboard_versions'
        verbose_name = 'Dashboard Version'
        verbose_name_plural = 'Dashboard Versions'
        unique_together = ['dashboard', 'version_number']
        ordering = ['-version_number']
    
    def __str__(self):
        return f"{self.dashboard.name} v{self.version_number}"


class DashboardExport(models.Model):
    """Dashboard export jobs and files."""
    
    EXPORT_FORMATS = [
        ('pdf', 'PDF'),
        ('png', 'PNG Image'),
        ('html', 'HTML'),
        ('excel', 'Excel'),
        ('csv', 'CSV Data'),
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE, related_name='exports')
    export_format = models.CharField(max_length=20, choices=EXPORT_FORMATS)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    file_path = models.CharField(max_length=500, blank=True, help_text='Path to exported file')
    file_size = models.BigIntegerField(null=True, blank=True, help_text='File size in bytes')
    export_settings = models.JSONField(default=dict, help_text='Export configuration')
    requested_by = models.ForeignKey(User, on_delete=models.CASCADE)
    requested_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True)
    download_count = models.IntegerField(default=0, help_text='Number of times downloaded')
    expires_at = models.DateTimeField(null=True, blank=True, help_text='When export expires')
    
    class Meta:
        db_table = 'dashboard_exports'
        verbose_name = 'Dashboard Export'
        verbose_name_plural = 'Dashboard Exports'
        ordering = ['-requested_at']
    
    def __str__(self):
        return f"{self.dashboard.name} - {self.export_format} ({self.status})" 