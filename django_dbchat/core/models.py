"""
Core models for query functionality and LLM integration.
"""
from django.db import models
from django.utils import timezone
from django.contrib.auth import get_user_model
import json

User = get_user_model()


class LLMConfig(models.Model):
    """LLM configuration model (singleton pattern)."""
    
    PROVIDER_CHOICES = [
        ('openai', 'OpenAI'),
        ('azure', 'Azure OpenAI'),
        ('local', 'Local LLM'),
        ('anthropic', 'Anthropic'),
        ('google', 'Google AI'),
    ]
    
    provider = models.CharField(
        max_length=50, 
        choices=PROVIDER_CHOICES, 
        default='openai',
        help_text='LLM provider to use'
    )
    api_key = models.TextField(
        blank=True, 
        help_text='API key for the LLM provider'
    )
    base_url = models.URLField(
        blank=True, 
        help_text='Base URL for API calls (for custom endpoints)'
    )
    model_name = models.CharField(
        max_length=100, 
        default='gpt-3.5-turbo',
        help_text='Name of the model to use'
    )
    temperature = models.FloatField(
        default=0.1,
        help_text='Temperature setting for LLM responses (0.0 to 1.0)'
    )
    max_tokens = models.IntegerField(
        default=1000,
        help_text='Maximum tokens for LLM responses'
    )
    system_prompt = models.TextField(
        default='You are a helpful assistant that converts natural language to SQL queries.',
        help_text='System prompt to use for the LLM'
    )
    additional_settings = models.JSONField(
        default=dict, 
        blank=True,
        help_text='Additional provider-specific settings'
    )
    is_active = models.BooleanField(
        default=True,
        help_text='Whether this configuration is active'
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        User, 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        help_text='User who last updated this configuration'
    )
    
    class Meta:
        db_table = 'app_llm_config'
        verbose_name = 'LLM Configuration'
        verbose_name_plural = 'LLM Configurations'
    
    def __str__(self):
        return f"{self.provider} - {self.model_name}"
    
    @classmethod
    def get_active_config(cls):
        """Get the active LLM configuration."""
        return cls.objects.filter(is_active=True).first()
    
    def save(self, *args, **kwargs):
        # Ensure only one active configuration
        if self.is_active:
            cls = self.__class__
            cls.objects.filter(is_active=True).update(is_active=False)
        super().save(*args, **kwargs)


class EmailConfig(models.Model):
    """Email server configuration model (singleton pattern)."""
    
    ENCRYPTION_CHOICES = [
        ('none', 'None'),
        ('tls', 'TLS'),
        ('ssl', 'SSL'),
    ]
    
    # SMTP Configuration
    smtp_host = models.CharField(
        max_length=255,
        help_text='SMTP server hostname (e.g., smtp.gmail.com)'
    )
    smtp_port = models.IntegerField(
        default=587,
        help_text='SMTP server port (587 for TLS, 465 for SSL, 25 for no encryption)'
    )
    smtp_username = models.CharField(
        max_length=255,
        help_text='SMTP username (usually your email address)'
    )
    smtp_password = models.TextField(
        help_text='SMTP password or app-specific password'
    )
    
    # Email Settings
    sender_email = models.EmailField(
        help_text='Default sender email address'
    )
    sender_name = models.CharField(
        max_length=255,
        default='ConvaBI System',
        help_text='Default sender name'
    )
    
    # Security Settings
    encryption = models.CharField(
        max_length=10,
        choices=ENCRYPTION_CHOICES,
        default='tls',
        help_text='Email encryption method'
    )
    use_tls = models.BooleanField(
        default=True,
        help_text='Use TLS encryption'
    )
    use_ssl = models.BooleanField(
        default=False,
        help_text='Use SSL encryption'
    )
    
    # Additional Settings
    timeout = models.IntegerField(
        default=30,
        help_text='Connection timeout in seconds'
    )
    fail_silently = models.BooleanField(
        default=False,
        help_text='Fail silently on email errors'
    )
    
    # Status and Metadata
    is_active = models.BooleanField(
        default=True,
        help_text='Whether this email configuration is active'
    )
    is_verified = models.BooleanField(
        default=False,
        help_text='Whether the email configuration has been tested successfully'
    )
    last_tested = models.DateTimeField(
        null=True,
        blank=True,
        help_text='When the email configuration was last tested'
    )
    test_status = models.TextField(
        blank=True,
        help_text='Result of the last test'
    )
    
    # Tracking
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text='User who last updated this configuration'
    )
    
    class Meta:
        db_table = 'app_email_config'
        verbose_name = 'Email Configuration'
        verbose_name_plural = 'Email Configurations'
    
    def __str__(self):
        return f"Email Config - {self.smtp_host}:{self.smtp_port}"
    
    @classmethod
    def get_active_config(cls):
        """Get the active email configuration."""
        return cls.objects.filter(is_active=True).first()
    
    def save(self, *args, **kwargs):
        # Ensure only one active configuration
        if self.is_active:
            cls = self.__class__
            cls.objects.filter(is_active=True).update(is_active=False)
        
        # Update encryption boolean fields based on encryption choice
        if self.encryption == 'tls':
            self.use_tls = True
            self.use_ssl = False
        elif self.encryption == 'ssl':
            self.use_tls = False
            self.use_ssl = True
        else:
            self.use_tls = False
            self.use_ssl = False
        
        super().save(*args, **kwargs)
    
    def get_email_settings_dict(self):
        """Get email settings as dictionary for email service."""
        return {
            'smtp_host': self.smtp_host,
            'smtp_port': self.smtp_port,
            'smtp_user': self.smtp_username,
            'smtp_password': self.smtp_password,
            'sender_email': self.sender_email,
            'sender_name': self.sender_name,
            'use_tls': self.use_tls,
            'use_ssl': self.use_ssl,
            'timeout': self.timeout,
            'fail_silently': self.fail_silently
        }
    
    def test_connection(self):
        """Test the email configuration."""
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        try:
            # Create SMTP connection
            server = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.timeout)
            
            if self.use_tls:
                server.starttls()
            
            # Login
            server.login(self.smtp_username, self.smtp_password)
            
            # Close connection
            server.quit()
            
            # Update test status
            self.is_verified = True
            self.last_tested = timezone.now()
            self.test_status = "Connection successful"
            self.save()
            
            return True, "Email configuration test successful"
            
        except Exception as e:
            # Update test status
            self.is_verified = False
            self.last_tested = timezone.now()
            self.test_status = f"Connection failed: {str(e)}"
            self.save()
            
            return False, str(e)


class QueryLog(models.Model):
    """Log of user queries and responses."""
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('clarification_needed', 'Clarification Needed'),
        ('error', 'Error'),
        ('cancelled', 'Cancelled'),
    ]
    
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE,
        help_text='User who made the query'
    )
    session_id = models.CharField(
        max_length=255, 
        blank=True,
        help_text='Session identifier for grouping related queries'
    )
    natural_query = models.TextField(
        help_text='Original natural language query from user'
    )
    generated_sql = models.TextField(
        blank=True,
        help_text='SQL query generated by LLM'
    )
    clarification_question = models.TextField(
        blank=True,
        help_text='Clarification question asked by LLM'
    )
    user_response = models.TextField(
        blank=True,
        help_text='User response to clarification question'
    )
    final_sql = models.TextField(
        blank=True,
        help_text='Final SQL query after clarification'
    )
    query_results = models.JSONField(
        default=dict, 
        blank=True,
        help_text='Query execution results'
    )
    execution_time = models.FloatField(
        null=True, 
        blank=True,
        help_text='Query execution time in seconds'
    )
    status = models.CharField(
        max_length=50, 
        choices=STATUS_CHOICES, 
        default='pending'
    )
    error_message = models.TextField(
        blank=True,
        help_text='Error message if query failed'
    )
    llm_provider = models.CharField(
        max_length=50, 
        blank=True,
        help_text='LLM provider used for this query'
    )
    llm_model = models.CharField(
        max_length=100, 
        blank=True,
        help_text='LLM model used for this query'
    )
    tokens_used = models.IntegerField(
        null=True, 
        blank=True,
        help_text='Number of tokens used by LLM'
    )
    created_at = models.DateTimeField(default=timezone.now)
    completed_at = models.DateTimeField(
        null=True, 
        blank=True,
        help_text='When the query was completed'
    )
    
    class Meta:
        db_table = 'app_query_logs'
        verbose_name = 'Query Log'
        verbose_name_plural = 'Query Logs'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.user.username} - {self.natural_query[:50]}..."
    
    def mark_completed(self):
        """Mark query as completed."""
        self.status = 'completed'
        self.completed_at = timezone.now()
        self.save()
    
    def mark_error(self, error_message):
        """Mark query as error with message."""
        self.status = 'error'
        self.error_message = error_message
        self.completed_at = timezone.now()
        self.save()


class AppLog(models.Model):
    """Application activity logs."""
    
    LEVEL_CHOICES = [
        ('DEBUG', 'Debug'),
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('CRITICAL', 'Critical'),
    ]
    
    CATEGORY_CHOICES = [
        ('auth', 'Authentication'),
        ('query', 'Query Processing'),
        ('llm', 'LLM Integration'),
        ('database', 'Database Operations'),
        ('dashboard', 'Dashboard'),
        ('data_integration', 'Data Integration'),
        ('email', 'Email'),
        ('system', 'System'),
        ('api', 'API'),
    ]
    
    level = models.CharField(
        max_length=20, 
        choices=LEVEL_CHOICES,
        default='INFO'
    )
    category = models.CharField(
        max_length=50, 
        choices=CATEGORY_CHOICES,
        default='system'
    )
    message = models.TextField(
        help_text='Log message'
    )
    details = models.JSONField(
        default=dict, 
        blank=True,
        help_text='Additional details as JSON'
    )
    user = models.ForeignKey(
        User, 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        help_text='User associated with this log entry'
    )
    ip_address = models.GenericIPAddressField(
        null=True, 
        blank=True,
        help_text='IP address of the user'
    )
    user_agent = models.TextField(
        blank=True,
        help_text='User agent string'
    )
    created_at = models.DateTimeField(default=timezone.now)
    
    class Meta:
        db_table = 'app_logs'
        verbose_name = 'Application Log'
        verbose_name_plural = 'Application Logs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['level', 'category']),
            models.Index(fields=['created_at']),
            models.Index(fields=['user']),
        ]
    
    def __str__(self):
        return f"{self.level} - {self.category} - {self.message[:50]}..."
    
    @classmethod
    def log(cls, level, category, message, details=None, user=None, request=None):
        """Convenience method for creating log entries."""
        log_entry = cls(
            level=level,
            category=category,
            message=message,
            details=details or {},
            user=user
        )
        
        if request:
            log_entry.ip_address = cls.get_client_ip(request)
            log_entry.user_agent = request.META.get('HTTP_USER_AGENT', '')
            if not user and hasattr(request, 'user') and request.user.is_authenticated:
                log_entry.user = request.user
        
        log_entry.save()
        return log_entry
    
    @staticmethod
    def get_client_ip(request):
        """Get client IP address from request."""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip


class DataSourceConnection(models.Model):
    """Data source connection information."""
    
    CONNECTION_TYPES = [
        ('postgresql', 'PostgreSQL'),
        ('mysql', 'MySQL'),
        ('sqlite', 'SQLite'),
        ('oracle', 'Oracle'),
        ('sqlserver', 'SQL Server'),
        ('csv', 'CSV File'),
        ('api', 'API'),
    ]
    
    name = models.CharField(
        max_length=200,
        help_text='Display name for the connection'
    )
    connection_type = models.CharField(
        max_length=50,
        choices=CONNECTION_TYPES
    )
    host = models.CharField(
        max_length=255, 
        blank=True,
        help_text='Database host'
    )
    port = models.IntegerField(
        null=True, 
        blank=True,
        help_text='Database port'
    )
    database_name = models.CharField(
        max_length=255, 
        blank=True,
        help_text='Database name'
    )
    username = models.CharField(
        max_length=255, 
        blank=True,
        help_text='Database username'
    )
    password = models.TextField(
        blank=True,
        help_text='Database password (encrypted)'
    )
    connection_string = models.TextField(
        blank=True,
        help_text='Full connection string (if not using individual fields)'
    )
    schema_info = models.JSONField(
        default=dict, 
        blank=True,
        help_text='Database schema information'
    )
    is_active = models.BooleanField(
        default=True,
        help_text='Whether this connection is active'
    )
    created_by = models.ForeignKey(
        User, 
        on_delete=models.CASCADE,
        help_text='User who created this connection'
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    last_tested = models.DateTimeField(
        null=True, 
        blank=True,
        help_text='When the connection was last tested'
    )
    test_status = models.CharField(
        max_length=50, 
        default='unknown',
        help_text='Status of last connection test'
    )
    
    class Meta:
        db_table = 'data_source_connections'
        verbose_name = 'Data Source Connection'
        verbose_name_plural = 'Data Source Connections'
        unique_together = ['name', 'created_by']
    
    def __str__(self):
        return f"{self.name} ({self.connection_type})" 