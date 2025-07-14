#!/usr/bin/env python
"""
Script to add sample license codes to the ConvaBI database
Run this script to add the generated license codes for testing
"""

import os
import sys
import django

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from licensing.models import License
from django.utils import timezone

def add_sample_licenses():
    """Add sample license codes to the database"""
    
    # Sample license codes generated earlier
    sample_licenses = [
        {
            'license_code': '6C5551C138AE46A3',
            'license_type': 'creator',
            'description': 'Sample creator license #1'
        },
        {
            'license_code': '06FC2245689FADC8',
            'license_type': 'creator',
            'description': 'Sample creator license #2'
        },
        {
            'license_code': 'FF1DBB98524B2EC6',
            'license_type': 'creator',
            'description': 'Sample creator license #3'
        },
        {
            'license_code': '39E85429B0330590',
            'license_type': 'creator',
            'description': 'Sample creator license #4'
        },
        {
            'license_code': 'EDDE934CC3AF6A26',
            'license_type': 'creator',
            'description': 'Sample creator license #5'
        },
        # Viewer licenses
        {
            'license_code': '2C4EA092EB67A558',
            'license_type': 'viewer',
            'description': 'Sample viewer license #1'
        },
        {
            'license_code': '7AC53323B354FD61',
            'license_type': 'viewer',
            'description': 'Sample viewer license #2'
        },
        {
            'license_code': 'C2092B147C270EC5',
            'license_type': 'viewer',
            'description': 'Sample viewer license #3'
        }
    ]
    
    created_count = 0
    existing_count = 0
    
    for license_data in sample_licenses:
        license_obj, created = License.objects.get_or_create(
            license_code=license_data['license_code'],
            defaults={
                'license_type': license_data['license_type'],
                'description': license_data['description'],
                'max_users': 1,
                'status': 'active',
                'created_by': 'system_script',
            }
        )
        
        if created:
            print(f"✅ Created license: {license_data['license_code']} ({license_data['license_type']})")
            created_count += 1
        else:
            print(f"ℹ️  License already exists: {license_data['license_code']}")
            existing_count += 1
    
    print(f"\n📊 Summary:")
    print(f"   - Created: {created_count} new licenses")
    print(f"   - Existing: {existing_count} licenses")
    print(f"   - Total: {len(sample_licenses)} licenses processed")
    
    # Show all licenses in database
    all_licenses = License.objects.all().order_by('-issued_at')
    print(f"\n📋 All licenses in database:")
    for license_obj in all_licenses:
        status_emoji = "✅" if license_obj.status == 'active' else "❌"
        type_emoji = "👑" if license_obj.license_type == 'creator' else "👁️"
        print(f"   {status_emoji} {type_emoji} {license_obj.license_code} - {license_obj.license_type} ({license_obj.status})")


if __name__ == '__main__':
    print("🔑 Adding sample license codes to ConvaBI database...")
    print("=" * 60)
    
    try:
        add_sample_licenses()
        print("\n✅ Sample licenses added successfully!")
        print("\nCreator License Codes (full access):")
        print("   - 6C5551C138AE46A3")
        print("   - 06FC2245689FADC8") 
        print("   - FF1DBB98524B2EC6")
        print("   - 39E85429B0330590")
        print("   - EDDE934CC3AF6A26")
        print("\nViewer License Codes (limited access):")
        print("   - 2C4EA092EB67A558")
        print("   - 7AC53323B354FD61") 
        print("   - C2092B147C270EC5")
        
    except Exception as e:
        print(f"\n❌ Error adding sample licenses: {e}")
        sys.exit(1) 