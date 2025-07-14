# ConvaBI License System

This document describes the ConvaBI license system that controls user access to different features of the application.

## Overview

The ConvaBI license system provides two types of licenses with different permission levels:

### Creator License
- **Full Access Features:**
  - Query & Analysis
  - Dashboard Creation & Viewing
  - Data Source Management
  - ETL Operations
  - Semantic Layer Management
  - Dashboard Export
  - Dashboard Sharing
  - Query History
  - Account Management

- **Restricted Features:**
  - LLM Model Configuration
  - Email Configuration
  - User Profile Management

### Viewer License
- **Available Features:**
  - Query & Analysis
  - Dashboard Creation & Viewing

- **Restricted Features:**
  - Data Source Management
  - ETL Operations
  - Semantic Layer Management
  - Dashboard Export
  - Dashboard Sharing
  - Query History
  - Account Management
  - LLM Model Configuration
  - Email Configuration
  - User Profile Management

## Admin Access

### Admin Login
- **Username:** `admin`
- **Password:** `admin123`
- **Access URL:** `/licensing/admin/login/`

The admin user has full access to all features and can manage licenses for all users.

## License Generation

### External License Generator Tool

Use the `license_generator.py` tool to generate license codes:

```bash
# Generate 10 creator licenses
python license_generator.py --type creator --count 10

# Generate 5 viewer licenses valid for 1 year
python license_generator.py --type viewer --count 5 --days 365

# Generate licenses and save to file
python license_generator.py --type creator --count 10 --output licenses.json

# Generate batch with custom settings
python license_generator.py --type creator --count 10 --max-users 5 --days 180 --output batch_licenses.json

# Validate a license code format
python license_generator.py --validate ABC123DEF456GHI7
```

### License Code Format
- **Length:** 16 characters
- **Characters:** Uppercase letters (A-Z) and numbers (0-9)
- **Example:** `ABC123DEF456GHI7`

## License Management

### Admin Interface

Access the license management interface at `/licensing/admin/` after logging in as admin.

#### Dashboard Features
- View license statistics
- Monitor recent license activities
- Track user assignments
- Check expiring licenses

#### License Management
- Create new licenses
- View all licenses with filtering
- View detailed license information
- Track license usage

#### User Management
- Assign licenses to users
- View user license history
- Revoke user licenses
- Monitor license validation logs

### API Endpoints

#### License Validation
```http
POST /licensing/api/validate/
Content-Type: application/json

{
  "license_code": "ABC123DEF456GHI7"
}
```

#### License Status
```http
GET /licensing/api/status/
```

## Database Schema

### License Model
- `license_code`: Primary key, 16-character unique code
- `license_type`: 'creator' or 'viewer'
- `status`: 'active', 'inactive', 'expired', 'revoked'
- `max_users`: Maximum users per license
- `valid_from`: License validity start date
- `valid_until`: License expiration date (optional)
- `description`: License description
- `features`: JSON field for additional features

### UserLicense Model
- `user`: Foreign key to User model
- `license`: Foreign key to License model
- `assigned_at`: Assignment timestamp
- `assigned_by`: Admin who assigned the license
- `is_active`: License assignment status

### LicenseValidationLog Model
- `license_code`: License code that was validated
- `user`: User who attempted validation
- `validation_result`: Success/failure result
- `ip_address`: Client IP address
- `timestamp`: Validation timestamp

## Implementation Details

### Permission Decorators

#### View Decorators
```python
from licensing.decorators import viewer_or_creator_required, creator_required, admin_required

@viewer_or_creator_required
def query_view(request):
    # Available to both viewer and creator licenses
    pass

@creator_required
def data_management_view(request):
    # Available only to creator licenses
    pass

@admin_required
def admin_view(request):
    # Available only to admin users
    pass
```

#### API Decorators
```python
from licensing.decorators import api_license_required

@api_license_required(permission='can_upload_data')
def api_upload_view(request):
    # API endpoint with specific permission check
    pass
```

### Permission Checking

#### Function-Based Permission Check
```python
from licensing.services import check_user_permission

if check_user_permission(request.user, 'can_upload_data'):
    # User has permission
    pass
```

#### Template Permission Check
```html
{% if request.license_info.permissions.can_upload_data %}
    <button>Upload Data</button>
{% endif %}
```

### License Middleware

The `LicenseMiddleware` automatically adds license information to all requests:

```python
# Access license info in views
license_info = request.license_info
has_license = license_info['has_license']
license_type = license_info['license_type']
permissions = license_info['permissions']
```

## Security Considerations

### License Generation
- License codes are generated using cryptographic functions
- The generation secret should be kept secure
- Use the external generator tool, not the Django service
- Generated codes are unique and cannot be regenerated

### License Validation
- All license operations are logged
- Failed validation attempts are tracked
- License codes are validated on each request
- Expired licenses are automatically detected

### Admin Access
- Admin login uses hardcoded credentials
- Admin users bypass license checks
- All admin actions are logged
- Admin interface is protected by authentication

## Deployment

### Database Migration
```bash
# Apply license migrations
python manage.py migrate licensing
```

### Settings Configuration
Ensure the following are configured in `settings.py`:

```python
INSTALLED_APPS = [
    # ... other apps
    'licensing',
]

MIDDLEWARE = [
    # ... other middleware
    'licensing.decorators.LicenseMiddleware',
]
```

### URL Configuration
Add to main `urls.py`:

```python
urlpatterns = [
    # ... other patterns
    path('licensing/', include('licensing.urls')),
]
```

## Troubleshooting

### Common Issues

#### User Cannot Access Features
1. Check if user has an active license
2. Verify license is not expired
3. Confirm license type has required permissions
4. Check license validation logs

#### License Validation Fails
1. Verify license code format (16 characters, uppercase alphanumeric)
2. Check if license exists in database
3. Confirm license status is 'active'
4. Check license expiration date

#### Admin Access Issues
1. Verify admin credentials (admin/admin123)
2. Check if admin user exists in database
3. Confirm admin user has superuser privileges

### Monitoring

#### License Usage
- Monitor license assignment counts
- Track license validation logs
- Check for expired licenses
- Review failed validation attempts

#### System Health
- Monitor database performance
- Check middleware response times
- Review error logs
- Track user access patterns

## Support

For technical support or questions about the license system:

1. Check the license validation logs in the admin interface
2. Review the database for license and user assignment data
3. Verify the license generation and validation processes
4. Contact the development team for advanced troubleshooting

---

**Note:** This license system is designed for per-user licensing with role-based access control. All license operations are logged and tracked for security and compliance purposes. 