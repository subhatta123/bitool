"""
License validation and management services
"""

import hashlib
import hmac
import secrets
import re
from datetime import datetime, timedelta
from typing import Tuple, Dict, Optional, Any
from django.utils import timezone
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db import transaction
import logging

from .models import License, UserLicense, LicenseValidationLog, get_user_license_info

logger = logging.getLogger(__name__)


class LicenseValidationService:
    """Service for validating and managing licenses"""
    
    def __init__(self):
        # Secret key for license validation (should be stored securely)
        # In production, this should be loaded from environment variables
        self.validation_secret = "convabi-license-validation-secret-key-2024"
    
    def validate_license_code(self, license_code: str, user: User = None, 
                            ip_address: str = None, user_agent: str = None) -> Tuple[bool, str, Dict]:
        """
        Validate a license code
        
        Returns:
            (is_valid, message, license_info)
        """
        try:
            # Basic format validation
            if not self._validate_license_format(license_code):
                self._log_validation_attempt(license_code, user, False, 
                                           "Invalid license code format", ip_address, user_agent)
                return False, "Invalid license code format", {}
            
            # Check if license exists in database
            try:
                license_obj = License.objects.get(license_code=license_code)
            except License.DoesNotExist:
                self._log_validation_attempt(license_code, user, False, 
                                           "License not found", ip_address, user_agent)
                return False, "License not found", {}
            
            # Check if license is valid
            if not license_obj.is_valid():
                reason = self._get_license_invalid_reason(license_obj)
                self._log_validation_attempt(license_code, user, False, 
                                           f"License invalid: {reason}", ip_address, user_agent)
                return False, f"License invalid: {reason}", {}
            
            # Check if license can be assigned to more users
            if user and not license_obj.can_assign_user():
                # Check if user is already assigned to this license
                existing_assignment = UserLicense.objects.filter(
                    user=user, license=license_obj, is_active=True
                ).first()
                
                if not existing_assignment:
                    self._log_validation_attempt(license_code, user, False, 
                                               "License has reached maximum users", ip_address, user_agent)
                    return False, "License has reached maximum users", {}
            
            # If we get here, license is valid
            license_info = {
                'license_type': license_obj.license_type,
                'status': license_obj.status,
                'valid_until': license_obj.valid_until,
                'max_users': license_obj.max_users,
                'assigned_users': license_obj.get_assigned_users_count(),
                'permissions': license_obj.get_permissions(),
                'features': license_obj.features,
                'description': license_obj.description,
            }
            
            self._log_validation_attempt(license_code, user, True, 
                                       "License validation successful", ip_address, user_agent)
            
            return True, "License validation successful", license_info
            
        except Exception as e:
            logger.error(f"Error validating license {license_code}: {e}")
            self._log_validation_attempt(license_code, user, False, 
                                       f"System error: {str(e)}", ip_address, user_agent)
            return False, "System error during validation", {}
    
    def assign_license_to_user(self, license_code: str, user: User, 
                             assigned_by: str = "admin") -> Tuple[bool, str]:
        """
        Assign a license to a user
        
        Returns:
            (success, message)
        """
        try:
            with transaction.atomic():
                # Validate license first
                is_valid, message, license_info = self.validate_license_code(license_code, user)
                
                if not is_valid:
                    return False, message
                
                # Get the license object
                license_obj = License.objects.get(license_code=license_code)
                
                # Check if user already has an active license
                existing_license = UserLicense.objects.filter(
                    user=user, is_active=True
                ).first()
                
                if existing_license:
                    # Deactivate existing license
                    existing_license.deactivate(
                        reason=f"Replaced with new license {license_code}",
                        deactivated_by=assigned_by
                    )
                    logger.info(f"Deactivated existing license {existing_license.license.license_code} for user {user.username}")
                
                # Create new license assignment
                user_license, created = UserLicense.objects.get_or_create(
                    user=user,
                    license=license_obj,
                    defaults={
                        'assigned_by': assigned_by,
                        'is_active': True,
                    }
                )
                
                if not created:
                    # Reactivate existing assignment
                    user_license.activate()
                    logger.info(f"Reactivated license {license_code} for user {user.username}")
                    return True, f"License {license_code} reactivated for user {user.username}"
                else:
                    logger.info(f"Assigned license {license_code} to user {user.username}")
                    return True, f"License {license_code} assigned to user {user.username}"
                
        except Exception as e:
            logger.error(f"Error assigning license {license_code} to user {user.username}: {e}")
            return False, f"System error: {str(e)}"
    
    def revoke_user_license(self, user: User, reason: str = "", 
                          revoked_by: str = "admin") -> Tuple[bool, str]:
        """
        Revoke a user's license
        
        Returns:
            (success, message)
        """
        try:
            user_license = UserLicense.objects.filter(
                user=user, is_active=True
            ).first()
            
            if not user_license:
                return False, f"User {user.username} has no active license"
            
            user_license.deactivate(reason=reason, deactivated_by=revoked_by)
            logger.info(f"Revoked license {user_license.license.license_code} from user {user.username}")
            
            return True, f"License revoked from user {user.username}"
            
        except Exception as e:
            logger.error(f"Error revoking license for user {user.username}: {e}")
            return False, f"System error: {str(e)}"
    
    def get_license_usage_stats(self, license_code: str) -> Dict[str, Any]:
        """Get usage statistics for a license"""
        try:
            license_obj = License.objects.get(license_code=license_code)
            
            user_licenses = UserLicense.objects.filter(license=license_obj)
            active_users = user_licenses.filter(is_active=True)
            
            stats = {
                'license_code': license_code,
                'license_type': license_obj.license_type,
                'status': license_obj.status,
                'is_valid': license_obj.is_valid(),
                'max_users': license_obj.max_users,
                'total_assignments': user_licenses.count(),
                'active_assignments': active_users.count(),
                'available_slots': license_obj.max_users - active_users.count(),
                'issued_at': license_obj.issued_at,
                'valid_until': license_obj.valid_until,
                'active_users': [
                    {
                        'username': ul.user.username,
                        'email': ul.user.email,
                        'assigned_at': ul.assigned_at,
                        'assigned_by': ul.assigned_by,
                    }
                    for ul in active_users
                ],
                'recent_validations': self._get_recent_validations(license_code),
            }
            
            return stats
            
        except License.DoesNotExist:
            return {'error': 'License not found'}
        except Exception as e:
            logger.error(f"Error getting license stats for {license_code}: {e}")
            return {'error': str(e)}
    
    def _validate_license_format(self, license_code: str) -> bool:
        """Validate license code format"""
        if not license_code or len(license_code) != 16:
            return False
        
        if not re.match(r'^[A-Z0-9]{16}$', license_code):
            return False
        
        return True
    
    def _get_license_invalid_reason(self, license_obj: License) -> str:
        """Get reason why license is invalid"""
        now = timezone.now()
        
        if license_obj.status != 'active':
            return f"License status is {license_obj.status}"
        
        if license_obj.valid_from > now:
            return f"License not valid until {license_obj.valid_from}"
        
        if license_obj.valid_until and license_obj.valid_until < now:
            return f"License expired on {license_obj.valid_until}"
        
        return "Unknown reason"
    
    def _log_validation_attempt(self, license_code: str, user: User, 
                              result: bool, message: str, 
                              ip_address: str = None, user_agent: str = None):
        """Log license validation attempt"""
        try:
            LicenseValidationLog.objects.create(
                license_code=license_code,
                user=user,
                validation_result=result,
                error_message=message if not result else '',
                ip_address=ip_address,
                user_agent=user_agent[:1000] if user_agent else ''  # Truncate long user agents
            )
        except Exception as e:
            logger.error(f"Error logging validation attempt: {e}")
    
    def _get_recent_validations(self, license_code: str, limit: int = 10) -> list:
        """Get recent validation attempts for a license"""
        try:
            validations = LicenseValidationLog.objects.filter(
                license_code=license_code
            ).order_by('-timestamp')[:limit]
            
            return [
                {
                    'timestamp': v.timestamp,
                    'user': v.user.username if v.user else 'Anonymous',
                    'result': v.validation_result,
                    'message': v.error_message,
                    'ip_address': v.ip_address,
                }
                for v in validations
            ]
        except Exception as e:
            logger.error(f"Error getting recent validations: {e}")
            return []


class LicenseGenerationService:
    """Service for generating license codes (external tool)"""
    
    def __init__(self):
        # This should be the same secret used in validation
        self.generation_secret = "convabi-license-validation-secret-key-2024"
    
    def generate_license_code(self, license_type: str = 'creator', 
                            seed: str = None) -> str:
        """
        Generate a 16-character license code
        
        This method is provided for reference but should be implemented
        as a separate external tool for security reasons.
        """
        if seed is None:
            seed = secrets.token_hex(8)
        
        # Create a hash based on the seed and secret
        hash_input = f"{seed}{license_type}{self.generation_secret}"
        hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()
        
        # Take first 16 characters and convert to uppercase alphanumeric
        license_code = ""
        for i, char in enumerate(hash_digest):
            if len(license_code) >= 16:
                break
            if char.isalnum():
                license_code += char.upper()
        
        # Pad with random characters if needed
        while len(license_code) < 16:
            license_code += secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
        
        return license_code[:16]
    
    def bulk_generate_licenses(self, license_type: str, count: int, 
                             max_users: int = 1, valid_days: int = None) -> list:
        """
        Generate multiple license codes
        
        This is for reference only - should be implemented as external tool
        """
        licenses = []
        
        for i in range(count):
            license_code = self.generate_license_code(license_type, f"bulk_{i}")
            
            license_data = {
                'license_code': license_code,
                'license_type': license_type,
                'max_users': max_users,
                'valid_until': timezone.now() + timedelta(days=valid_days) if valid_days else None,
                'description': f'Bulk generated {license_type} license #{i+1}',
            }
            
            licenses.append(license_data)
        
        return licenses


def check_user_permission(user: User, permission: str) -> bool:
    """
    Check if user has a specific permission based on their license
    
    Args:
        user: Django User instance
        permission: Permission string (e.g., 'can_upload_data')
    
    Returns:
        bool: True if user has permission, False otherwise
    """
    try:
        license_info = get_user_license_info(user)
        
        if not license_info['has_license']:
            return False
        
        if license_info['status'] != 'active':
            return False
        
        permissions = license_info.get('permissions', {})
        return permissions.get(permission, False)
        
    except Exception as e:
        logger.error(f"Error checking permission {permission} for user {user.username}: {e}")
        return False


def get_user_permissions(user: User) -> Dict[str, bool]:
    """
    Get all permissions for a user based on their license
    
    Args:
        user: Django User instance
    
    Returns:
        Dict[str, bool]: Dictionary of permissions
    """
    try:
        license_info = get_user_license_info(user)
        
        if not license_info['has_license'] or license_info['status'] != 'active':
            return {}
        
        return license_info.get('permissions', {})
        
    except Exception as e:
        logger.error(f"Error getting permissions for user {user.username}: {e}")
        return {} 