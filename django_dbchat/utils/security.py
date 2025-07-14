"""
Security utilities for ConvaBI Application
Provides encryption, input validation, and security helpers
"""

import re
import hashlib
import hmac
import base64
import logging
from typing import Dict, Any, List, Optional, Tuple
from functools import wraps
from django.conf import settings
from django.core.cache import cache
from django.http import JsonResponse
from django.utils import timezone
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)

class SecurityManager:
    """Central security manager for encryption and validation"""
    
    def __init__(self):
        self.encryption_key = self._get_or_create_encryption_key()
        self.cipher_suite = Fernet(self.encryption_key)
    
    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for sensitive data"""
        try:
            # Try to get key from settings
            secret_key = getattr(settings, 'ENCRYPTION_SECRET_KEY', settings.SECRET_KEY)
            
            # Derive encryption key from secret
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'dbchat_salt',  # In production, use a random salt
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(secret_key.encode()))
            return key
            
        except Exception as e:
            logger.error(f"Failed to create encryption key: {e}")
            # Fallback to a default key (not recommended for production)
            return Fernet.generate_key()
    
    def encrypt_sensitive_data(self, data: str) -> str:
        """Encrypt sensitive data like passwords and API keys"""
        try:
            if not data:
                return ""
            
            encrypted = self.cipher_suite.encrypt(data.encode())
            return base64.urlsafe_b64encode(encrypted).decode()
            
        except Exception as e:
            logger.error(f"Failed to encrypt data: {e}")
            raise ValueError("Encryption failed")
    
    def decrypt_sensitive_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data"""
        try:
            if not encrypted_data:
                return ""
            
            # Decode and decrypt
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode())
            decrypted = self.cipher_suite.decrypt(encrypted_bytes)
            return decrypted.decode()
            
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            raise ValueError("Decryption failed")
    
    def encrypt_connection_info(self, connection_info: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive fields in connection info"""
        try:
            encrypted_info = connection_info.copy()
            sensitive_fields = ['password', 'api_key', 'secret']
            
            for field in sensitive_fields:
                if field in encrypted_info and encrypted_info[field]:
                    encrypted_info[field] = self.encrypt_sensitive_data(str(encrypted_info[field]))
                    encrypted_info[f'{field}_encrypted'] = True
            
            return encrypted_info
            
        except Exception as e:
            logger.error(f"Failed to encrypt connection info: {e}")
            return connection_info
    
    def decrypt_connection_info(self, connection_info: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt sensitive fields in connection info"""
        try:
            decrypted_info = connection_info.copy()
            sensitive_fields = ['password', 'api_key', 'secret']
            
            for field in sensitive_fields:
                if f'{field}_encrypted' in decrypted_info and decrypted_info.get(f'{field}_encrypted'):
                    if field in decrypted_info:
                        decrypted_info[field] = self.decrypt_sensitive_data(decrypted_info[field])
                        # Remove encryption flag for cleaner dict
                        del decrypted_info[f'{field}_encrypted']
            
            return decrypted_info
            
        except Exception as e:
            logger.error(f"Failed to decrypt connection info: {e}")
            return connection_info

# Initialize global security manager
security_manager = SecurityManager()

# SQL Injection Prevention
class SQLValidator:
    """SQL query validation and injection prevention"""
    
    # Dangerous SQL keywords that should be blocked
    DANGEROUS_KEYWORDS = [
        'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 
        'TRUNCATE', 'EXEC', 'EXECUTE', 'MERGE', 'CALL', 'GRANT', 
        'REVOKE', 'COMMIT', 'ROLLBACK', 'SAVEPOINT'
    ]
    
    # Allowed SQL functions for queries
    ALLOWED_FUNCTIONS = [
        'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING',
        'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
        'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'DISTINCT', 'AS',
        'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS NULL', 'IS NOT NULL'
    ]
    
    @staticmethod
    def validate_sql_query(query: str) -> Tuple[bool, str]:
        """Validate SQL query for safety"""
        try:
            if not query or not query.strip():
                return False, "Query cannot be empty"
            
            query_upper = query.upper().strip()
            
            # Must start with SELECT
            if not query_upper.startswith('SELECT'):
                return False, "Only SELECT statements are allowed"
            
            # Check for dangerous keywords
            for keyword in SQLValidator.DANGEROUS_KEYWORDS:
                if keyword in query_upper:
                    return False, f"Dangerous keyword '{keyword}' not allowed"
            
            # Check for multiple statements (simple check)
            if ';' in query[:-1]:  # Allow semicolon at the end
                return False, "Multiple statements not allowed"
            
            # Check for comment injection
            if '--' in query or '/*' in query or '*/' in query:
                return False, "SQL comments not allowed"
            
            # Basic parentheses balance check
            if query.count('(') != query.count(')'):
                return False, "Mismatched parentheses in query"
            
            return True, "Query is valid"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    @staticmethod
    def sanitize_identifier(identifier: str) -> str:
        """Sanitize SQL identifiers (table/column names)"""
        if not identifier:
            return ""
        
        # Remove any non-alphanumeric characters except underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '', identifier)
        
        # Ensure it starts with a letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"
        
        return sanitized[:64]  # Limit length
    
    @staticmethod
    def build_parameterized_query(base_query: str, parameters: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Build parameterized query to prevent injection"""
        try:
            safe_params = {}
            
            for key, value in parameters.items():
                # Sanitize parameter keys
                safe_key = SQLValidator.sanitize_identifier(key)
                if safe_key != key:
                    logger.warning(f"Parameter key sanitized: {key} -> {safe_key}")
                
                # Validate parameter values
                if isinstance(value, str):
                    # Escape single quotes
                    safe_params[safe_key] = value.replace("'", "''")
                else:
                    safe_params[safe_key] = value
            
            return base_query, safe_params
            
        except Exception as e:
            logger.error(f"Failed to build parameterized query: {e}")
            return base_query, parameters

# Input Validation
class InputValidator:
    """Input validation and sanitization utilities"""
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email address format"""
        if not email:
            return False
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(email_pattern, email) is not None
    
    @staticmethod
    def validate_password(password: str) -> Tuple[bool, str]:
        """Validate password strength"""
        if not password:
            return False, "Password cannot be empty"
        
        if len(password) < 8:
            return False, "Password must be at least 8 characters long"
        
        if len(password) > 128:
            return False, "Password too long"
        
        # Check for at least one uppercase, lowercase, digit, and special character
        if not re.search(r'[A-Z]', password):
            return False, "Password must contain at least one uppercase letter"
        
        if not re.search(r'[a-z]', password):
            return False, "Password must contain at least one lowercase letter"
        
        if not re.search(r'\d', password):
            return False, "Password must contain at least one digit"
        
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False, "Password must contain at least one special character"
        
        return True, "Password is valid"
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Sanitize filename for safe storage"""
        if not filename:
            return "untitled"
        
        # Remove path components
        filename = filename.split('/')[-1].split('\\')[-1]
        
        # Remove dangerous characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '', filename)
        
        # Limit length
        if len(sanitized) > 255:
            name, ext = sanitized.rsplit('.', 1) if '.' in sanitized else (sanitized, '')
            sanitized = name[:250] + ('.' + ext if ext else '')
        
        return sanitized or "untitled"
    
    @staticmethod
    def validate_file_size(file_size: int, max_size_mb: int = 100) -> Tuple[bool, str]:
        """Validate file size"""
        max_size_bytes = max_size_mb * 1024 * 1024
        
        if file_size > max_size_bytes:
            return False, f"File size exceeds maximum allowed size of {max_size_mb}MB"
        
        return True, "File size is valid"
    
    @staticmethod
    def validate_file_type(filename: str, allowed_extensions: List[str]) -> Tuple[bool, str]:
        """Validate file type by extension"""
        if not filename:
            return False, "Filename cannot be empty"
        
        extension = filename.lower().split('.')[-1] if '.' in filename else ''
        
        if extension not in [ext.lower() for ext in allowed_extensions]:
            return False, f"File type '{extension}' not allowed. Allowed types: {', '.join(allowed_extensions)}"
        
        return True, "File type is valid"

# Rate Limiting
class RateLimiter:
    """Rate limiting utilities"""
    
    @staticmethod
    def check_rate_limit(identifier: str, max_requests: int = 60, window_minutes: int = 1) -> Tuple[bool, int]:
        """Check if request is within rate limit"""
        try:
            cache_key = f"rate_limit_{identifier}"
            current_time = timezone.now()
            window_start = current_time - timezone.timedelta(minutes=window_minutes)
            
            # Get existing requests
            requests = cache.get(cache_key, [])
            
            # Filter requests within the time window
            recent_requests = [req_time for req_time in requests if req_time > window_start]
            
            # Check if limit exceeded
            if len(recent_requests) >= max_requests:
                return False, len(recent_requests)
            
            # Add current request
            recent_requests.append(current_time)
            
            # Store updated requests
            cache.set(cache_key, recent_requests, timeout=window_minutes * 60)
            
            return True, len(recent_requests)
            
        except Exception as e:
            logger.error(f"Rate limit check failed: {e}")
            return True, 0  # Allow on error

# Security Decorators
def require_rate_limit(max_requests: int = 60, window_minutes: int = 1):
    """Decorator to enforce rate limiting on views"""
    def decorator(view_func):
        @wraps(view_func)
        def wrapper(request, *args, **kwargs):
            # Use user ID or IP address as identifier
            identifier = str(request.user.id) if request.user.is_authenticated else request.META.get('REMOTE_ADDR', 'unknown')
            
            allowed, count = RateLimiter.check_rate_limit(identifier, max_requests, window_minutes)
            
            if not allowed:
                return JsonResponse({
                    'error': 'Rate limit exceeded',
                    'retry_after': window_minutes * 60
                }, status=429)
            
            # Add rate limit headers
            response = view_func(request, *args, **kwargs)
            if hasattr(response, '__setitem__'):
                response['X-RateLimit-Limit'] = str(max_requests)
                response['X-RateLimit-Remaining'] = str(max_requests - count)
            
            return response
        return wrapper
    return decorator

def validate_sql_input(view_func):
    """Decorator to validate SQL input in request data"""
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if request.method == 'POST':
            try:
                import json
                data = json.loads(request.body)
                
                # Check for SQL query in request
                if 'query' in data:
                    is_valid, error = SQLValidator.validate_sql_query(data['query'])
                    if not is_valid:
                        return JsonResponse({
                            'error': 'Invalid SQL query',
                            'details': error
                        }, status=400)
                
            except Exception as e:
                logger.warning(f"SQL validation failed: {e}")
        
        return view_func(request, *args, **kwargs)
    return wrapper

# Audit Logging
class AuditLogger:
    """Security audit logging"""
    
    @staticmethod
    def log_security_event(event_type: str, user_id: Optional[int], details: Dict[str, Any]):
        """Log security-related events"""
        try:
            log_entry = {
                'timestamp': timezone.now().isoformat(),
                'event_type': event_type,
                'user_id': user_id,
                'details': details
            }
            
            logger.info(f"SECURITY_EVENT: {json.dumps(log_entry)}")
            
            # Could also store in database or send to external logging service
            
        except Exception as e:
            logger.error(f"Failed to log security event: {e}")
    
    @staticmethod
    def log_failed_login(username: str, ip_address: str):
        """Log failed login attempt"""
        AuditLogger.log_security_event(
            'FAILED_LOGIN',
            None,
            {'username': username, 'ip_address': ip_address}
        )
    
    @staticmethod
    def log_sql_injection_attempt(user_id: Optional[int], query: str, ip_address: str):
        """Log potential SQL injection attempt"""
        AuditLogger.log_security_event(
            'SQL_INJECTION_ATTEMPT',
            user_id,
            {'query': query[:100], 'ip_address': ip_address}
        )
    
    @staticmethod
    def log_rate_limit_exceeded(identifier: str):
        """Log rate limit exceeded"""
        AuditLogger.log_security_event(
            'RATE_LIMIT_EXCEEDED',
            None,
            {'identifier': identifier}
        )

# CSRF Protection helpers
def generate_csrf_token() -> str:
    """Generate CSRF token"""
    return hmac.new(
        settings.SECRET_KEY.encode(),
        str(timezone.now().timestamp()).encode(),
        hashlib.sha256
    ).hexdigest()

def validate_csrf_token(token: str) -> bool:
    """Validate CSRF token"""
    try:
        # Simple validation (could be enhanced with timestamp checking)
        return len(token) == 64 and all(c in '0123456789abcdef' for c in token)
    except:
        return False 