#!/usr/bin/env python
"""
Script to check current license and user license status
"""

import os
import sys
import django

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from licensing.models import License, UserLicense
from accounts.models import CustomUser

def check_license_status():
    """Check current license and user license status"""
    
    print("=== LICENSE STATUS ===")
    for license in License.objects.all():
        current_users = license.userlicense_set.count()
        print(f"License: {license.code} ({license.license_type})")
        print(f"  Max Users: {license.max_users}")
        print(f"  Current Users: {current_users}")
        print(f"  Available: {license.max_users - current_users}")
        print()
    
    print("=== USER LICENSES ===")
    for ul in UserLicense.objects.all():
        print(f"User: {ul.user.username} - License: {ul.license.code} ({ul.license.license_type})")
    
    print("\n=== USERS WITHOUT LICENSES ===")
    licensed_users = UserLicense.objects.values_list('user_id', flat=True)
    unlicensed_users = CustomUser.objects.exclude(id__in=licensed_users)
    
    for user in unlicensed_users:
        print(f"User: {user.username} ({user.email}) - No license")

if __name__ == '__main__':
    check_license_status() 