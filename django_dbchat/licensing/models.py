from django.db import models
from django.contrib.auth.models import User
from django.conf import settings
from django.utils import timezone
from django.core.exceptions import ValidationError
import hashlib
import hmac
import re


class License(models.Model):
    """License model to store license information"""
    
    LICENSE_TYPES = [
        ('creator', 'Creator'),
        ('viewer', 'Viewer'),
    ]
    
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('expired', 'Expired'),
        ('revoked', 'Revoked'),
    ]
    
    license_code = models.CharField(max_length=16, unique=True, primary_key=True)
    license_type = models.CharField(max_length=20, choices=LICENSE_TYPES)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    
    # License validity
    issued_at = models.DateTimeField(auto_now_add=True)
    valid_from = models.DateTimeField(default=timezone.now)
    valid_until = models.DateTimeField(null=True, blank=True)  # None means no expiration
    
    # Metadata
    description = models.TextField(blank=True)
    max_users = models.IntegerField(default=1)  # Max users for this license
    features = models.JSONField(default=dict, blank=True)  # Additional features
    
    # Tracking
    created_by = models.CharField(max_length=100, default='system')
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-issued_at']
        verbose_name = 'License'
        verbose_name_plural = 'Licenses'
    
    def __str__(self):
        return f"{self.license_code} ({self.get_license_type_display()})"
    
    def clean(self):
        """Validate license code format"""
        if not re.match(r'^[A-Z0-9]{16}$', self.license_code):
            raise ValidationError('License code must be 16 alphanumeric characters')
    
    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)
    
    def is_valid(self):
        """Check if license is currently valid"""
        now = timezone.now()
        
        # Check status
        if self.status != 'active':
            return False
        
        # Check validity period
        if self.valid_from > now:
            return False
        
        if self.valid_until and self.valid_until < now:
            return False
        
        return True
    
    def get_assigned_users_count(self):
        """Get number of users assigned to this license"""
        return self.user_licenses.filter(is_active=True).count()
    
    def can_assign_user(self):
        """Check if license can be assigned to more users"""
        return self.get_assigned_users_count() < self.max_users
    
    def get_permissions(self):
        """Get permissions for this license type"""
        base_permissions = {
            'can_query': True,
            'can_view_dashboards': True,
            'can_create_dashboards': True,
        }
        
        if self.license_type == 'creator':
            creator_permissions = {
                'can_upload_data': True,
                'can_manage_data_sources': True,
                'can_perform_etl': True,
                'can_manage_semantic_layer': True,
                'can_export_dashboards': True,
                'can_share_dashboards': True,
                'can_view_query_history': True,
                'can_manage_account': True,
                # Restricted permissions
                'can_change_llm_model': False,
                'can_change_email_config': False,
                'can_view_user_profile': False,
            }
            base_permissions.update(creator_permissions)
        
        elif self.license_type == 'viewer':
            viewer_permissions = {
                'can_upload_data': False,
                'can_manage_data_sources': False,
                'can_perform_etl': False,
                'can_manage_semantic_layer': False,
                'can_export_dashboards': False,
                'can_share_dashboards': False,
                'can_view_query_history': False,
                'can_manage_account': False,
                'can_change_llm_model': False,
                'can_change_email_config': False,
                'can_view_user_profile': False,
            }
            base_permissions.update(viewer_permissions)
        
        return base_permissions


class UserLicense(models.Model):
    """Association between users and licenses"""
    
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='user_licenses')
    license = models.ForeignKey(License, on_delete=models.CASCADE, related_name='user_licenses')
    
    # Assignment metadata
    assigned_at = models.DateTimeField(auto_now_add=True)
    assigned_by = models.CharField(max_length=100, default='admin')
    is_active = models.BooleanField(default=True)
    
    # Deactivation info
    deactivated_at = models.DateTimeField(null=True, blank=True)
    deactivated_by = models.CharField(max_length=100, null=True, blank=True)
    deactivation_reason = models.TextField(blank=True)
    
    class Meta:
        unique_together = ['user', 'license']
        ordering = ['-assigned_at']
        verbose_name = 'User License'
        verbose_name_plural = 'User Licenses'
    
    def __str__(self):
        return f"{self.user.username} - {self.license.license_code}"
    
    def is_valid(self):
        """Check if user license is valid"""
        return self.is_active and self.license.is_valid()
    
    def deactivate(self, reason="", deactivated_by="admin"):
        """Deactivate this user license"""
        self.is_active = False
        self.deactivated_at = timezone.now()
        self.deactivated_by = deactivated_by
        self.deactivation_reason = reason
        self.save()
    
    def activate(self):
        """Reactivate this user license"""
        self.is_active = True
        self.deactivated_at = None
        self.deactivated_by = None
        self.deactivation_reason = ""
        self.save()


class LicenseValidationLog(models.Model):
    """Log license validation attempts"""
    
    license_code = models.CharField(max_length=16)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, null=True, blank=True)
    validation_result = models.BooleanField()
    error_message = models.TextField(blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-timestamp']
        verbose_name = 'License Validation Log'
        verbose_name_plural = 'License Validation Logs'
    
    def __str__(self):
        status = "Success" if self.validation_result else "Failed"
        return f"{self.license_code} - {status} - {self.timestamp}"


# Utility functions for license management
def validate_license_code(license_code):
    """Validate license code format and checksum"""
    if not license_code or len(license_code) != 16:
        return False, "License code must be 16 characters"
    
    if not re.match(r'^[A-Z0-9]{16}$', license_code):
        return False, "License code must contain only uppercase letters and numbers"
    
    # Basic checksum validation (simple implementation)
    # In a real implementation, this would use a proper checksum algorithm
    checksum = sum(ord(c) for c in license_code) % 256
    if checksum == 0:
        return False, "Invalid license code checksum"
    
    return True, "Valid format"


def get_user_license_info(user):
    """Get comprehensive license info for a user"""
    try:
        user_license = UserLicense.objects.filter(
            user=user, 
            is_active=True
        ).select_related('license').first()
        
        if not user_license:
            return {
                'has_license': False,
                'license_type': None,
                'permissions': {},
                'status': 'no_license'
            }
        
        license_obj = user_license.license
        
        return {
            'has_license': True,
            'license_type': license_obj.license_type,
            'license_code': license_obj.license_code,
            'permissions': license_obj.get_permissions(),
            'status': 'active' if user_license.is_valid() else 'invalid',
            'valid_until': license_obj.valid_until,
            'assigned_at': user_license.assigned_at,
        }
    
    except Exception as e:
        return {
            'has_license': False,
            'license_type': None,
            'permissions': {},
            'status': 'error',
            'error': str(e)
        }
