import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from licensing.models import License, UserLicense
from django.contrib.auth import get_user_model
from django.utils import timezone
from datetime import timedelta

User = get_user_model()

# Create licenses
for code in ['6C5551C138AE46A3', '06FC2245689FADC8', 'FF1DBB98524B2EC6', '39E85429B0330590', 'EDDE934CC3AF6A26']:
    License.objects.get_or_create(license_code=code, defaults={'license_type': 'creator', 'status': 'active', 'max_users': 5})

for code in ['2C4EA092EB67A558', '7AC53323B354FD61', 'C2092B147C270EC5']:
    License.objects.get_or_create(license_code=code, defaults={'license_type': 'viewer', 'status': 'active', 'max_users': 10})

# Assign to admin
admin_user = User.objects.get(username='admin')
creator_license = License.objects.filter(license_type='creator').first()
UserLicense.objects.get_or_create(user=admin_user, license=creator_license, defaults={'assigned_by': 'system', 'is_active': True})

print("Setup complete!") 