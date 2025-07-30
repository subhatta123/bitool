import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from licensing.models import License, UserLicense
from django.contrib.auth import get_user_model

User = get_user_model()

print("🔍 Checking user jico...")

try:
    jico_user = User.objects.get(username='jico')
    print(f"✅ Found user: {jico_user.username}")
    
    # Check if already has license
    existing = UserLicense.objects.filter(user=jico_user, is_active=True).first()
    if existing:
        print(f"📋 User already has license: {existing.license.license_code} ({existing.license.license_type})")
    else:
        # Assign a creator license
        creator_license = License.objects.filter(license_type='creator', status='active').first()
        if creator_license:
            UserLicense.objects.create(
                user=jico_user,
                license=creator_license,
                assigned_by='system',
                is_active=True
            )
            print(f"✅ Assigned creator license {creator_license.license_code} to {jico_user.username}")
        else:
            print("❌ No creator licenses available")
            
except User.DoesNotExist:
    print("❌ User 'jico' not found")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n📊 Summary:")
print(f"Total licenses: {License.objects.count()}")
print(f"Active user licenses: {UserLicense.objects.filter(is_active=True).count()}") 