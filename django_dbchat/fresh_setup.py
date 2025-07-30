import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from licensing.models import License, UserLicense
from django.contrib.auth import get_user_model
from django.utils import timezone
from datetime import timedelta

User = get_user_model()

print("🔧 Setting up fresh container...")

# 1. Run migrations first
print("📋 Running licensing migrations...")
from django.core.management import call_command
call_command('migrate', 'licensing', verbosity=0)

# 2. Create licenses
print("🔑 Creating licenses...")
creator_codes = ['6C5551C138AE46A3', '06FC2245689FADC8', 'FF1DBB98524B2EC6', '39E85429B0330590', 'EDDE934CC3AF6A26']
viewer_codes = ['2C4EA092EB67A558', '7AC53323B354FD61', 'C2092B147C270EC5']

for code in creator_codes:
    license_obj, created = License.objects.get_or_create(
        license_code=code,
        defaults={
            'license_type': 'creator',
            'status': 'active',
            'max_users': 5,
            'valid_from': timezone.now(),
            'valid_until': timezone.now() + timedelta(days=365),
            'description': 'Creator license with full access'
        }
    )
    print(f"  {'✅ Created' if created else '📋 Found'} creator license: {code}")

for code in viewer_codes:
    license_obj, created = License.objects.get_or_create(
        license_code=code,
        defaults={
            'license_type': 'viewer',
            'status': 'active',
            'max_users': 10,
            'valid_from': timezone.now(),
            'valid_until': timezone.now() + timedelta(days=365),
            'description': 'Viewer license with limited access'
        }
    )
    print(f"  {'✅ Created' if created else '📋 Found'} viewer license: {code}")

# 3. Create admin user if needed
print("👤 Setting up admin user...")
try:
    admin_user = User.objects.get(username='admin')
    print(f"  📋 Found existing admin user: {admin_user.username}")
except User.DoesNotExist:
    admin_user = User.objects.create_superuser(
        username='admin',
        email='admin@convabi.com',
        password='admin123'
    )
    print(f"  ✅ Created admin user: {admin_user.username}")

# 4. Assign license to admin
print("🎫 Assigning license to admin...")
creator_license = License.objects.filter(license_type='creator', status='active').first()
user_license, created = UserLicense.objects.get_or_create(
    user=admin_user,
    license=creator_license,
    defaults={
        'assigned_by': 'system',
        'is_active': True
    }
)
print(f"  {'✅ Assigned' if created else '📋 Already has'} creator license: {creator_license.license_code}")

print("\n✨ Setup completed successfully!")
print(f"📊 Total licenses: {License.objects.count()}")
print(f"🎯 Creator licenses: {License.objects.filter(license_type='creator').count()}")
print(f"👁️  Viewer licenses: {License.objects.filter(license_type='viewer').count()}")
print(f"👤 Admin has license: {UserLicense.objects.filter(user=admin_user, is_active=True).exists()}") 