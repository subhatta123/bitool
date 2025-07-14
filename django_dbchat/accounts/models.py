"""
User models for the ConvaBI application.
"""
from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone
import json


class CustomUser(AbstractUser):
    """Custom user model with additional fields for roles and preferences."""
    
    ROLE_CHOICES = [
        ('admin', 'Administrator'),
        ('user', 'User'),
        ('viewer', 'Viewer'),
    ]
    
    email = models.EmailField(unique=True)
    roles = models.JSONField(default=list, blank=True, help_text="List of user roles")
    preferences = models.JSONField(default=dict, blank=True, help_text="User preferences and settings")
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    last_login_ip = models.GenericIPAddressField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    
    class Meta:
        db_table = 'auth_user'
        verbose_name = 'User'
        verbose_name_plural = 'Users'
    
    def __str__(self):
        return self.username
    
    def has_role(self, role):
        """Check if user has a specific role."""
        if isinstance(self.roles, list):
            return role in self.roles
        return False
    
    def add_role(self, role):
        """Add a role to the user."""
        if isinstance(self.roles, list):
            if role not in self.roles:
                self.roles.append(role)
                self.save()
        else:
            self.roles = [role]
            self.save()
    
    def remove_role(self, role):
        """Remove a role from the user."""
        if isinstance(self.roles, list) and role in self.roles:
            self.roles.remove(role)
            self.save()
    
    def is_admin(self):
        """Check if user has admin role."""
        return self.has_role('admin') or self.is_superuser
    
    def can_manage_users(self):
        """Check if user can manage other users."""
        return self.is_admin()
    
    def can_configure_llm(self):
        """Check if user can configure LLM settings."""
        return self.is_admin()
    
    def get_preference(self, key, default=None):
        """Get a user preference value."""
        if isinstance(self.preferences, dict):
            return self.preferences.get(key, default)
        return default
    
    def set_preference(self, key, value):
        """Set a user preference value."""
        if not isinstance(self.preferences, dict):
            self.preferences = {}
        self.preferences[key] = value
        self.save()
    
    def get_display_name(self):
        """Get display name for the user."""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        elif self.first_name:
            return self.first_name
        else:
            return self.username 