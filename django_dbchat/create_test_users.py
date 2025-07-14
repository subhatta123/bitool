#!/usr/bin/env python
"""
Script to create test users with different license types for testing the license system
"""

import os
import sys
import django

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from accounts.models import CustomUser
from licensing.services import LicenseValidationService

def create_test_users():
    """Create test users with different license types"""
    
    validation_service = LicenseValidationService()
    
    # Test users to create
    test_users = [
        {
            'username': 'creator_user',
            'email': 'creator@test.com',
            'password': 'testpass123',
            'first_name': 'Creator',
            'last_name': 'User',
            'license_code': '6C5551C138AE46A3'  # Creator license
        },
        {
            'username': 'viewer_user',
            'email': 'viewer@test.com',
            'password': 'testpass123',
            'first_name': 'Viewer',
            'last_name': 'User',
            'license_code': '2C4EA092EB67A558'  # Viewer license
        }
    ]
    
    created_count = 0
    existing_count = 0
    
    for user_data in test_users:
        # Check if user already exists
        if CustomUser.objects.filter(username=user_data['username']).exists():
            print(f"ℹ️  User already exists: {user_data['username']}")
            existing_count += 1
            continue
        
        # Create user
        user = CustomUser.objects.create_user(
            username=user_data['username'],
            email=user_data['email'],
            password=user_data['password'],
            first_name=user_data['first_name'],
            last_name=user_data['last_name']
        )
        
        # Assign license
        success, message = validation_service.assign_license_to_user(
            user_data['license_code'], user, "test_script"
        )
        
        if success:
            print(f"✅ Created user: {user_data['username']} with {user_data['license_code']} license")
            created_count += 1
        else:
            print(f"❌ Failed to assign license to {user_data['username']}: {message}")
            # Delete the user if license assignment failed
            user.delete()
    
    print(f"\n📊 Summary:")
    print(f"   - Created: {created_count} new users")
    print(f"   - Existing: {existing_count} users")
    print(f"   - Total: {len(test_users)} users processed")
    
    print(f"\n🔑 Test Login Credentials:")
    print(f"   Creator User:")
    print(f"     Username: creator_user")
    print(f"     Password: testpass123")
    print(f"     License: Creator (full access)")
    print(f"\n   Viewer User:")
    print(f"     Username: viewer_user") 
    print(f"     Password: testpass123")
    print(f"     License: Viewer (limited access)")


if __name__ == '__main__':
    print("👥 Creating test users for license system testing...")
    print("=" * 60)
    
    try:
        create_test_users()
        print("\n✅ Test users created successfully!")
        print("\nYou can now test the license system by logging in with these users.")
        
    except Exception as e:
        print(f"\n❌ Error creating test users: {e}")
        sys.exit(1) 