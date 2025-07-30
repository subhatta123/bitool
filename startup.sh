#!/bin/bash
set -e

echo "🚀 Starting ConvaBI Deployment Container..."

# Wait for database
echo "⏳ Waiting for database..."
while ! python manage.py check --database default; do
    echo "Database is unavailable - waiting 2 seconds..."
    sleep 2
done
echo "✅ Database is ready!"

# Run migrations
echo "🔄 Running database migrations..."
python manage.py migrate --noinput

# Collect static files
echo "📦 Collecting static files..."
python manage.py collectstatic --noinput --clear

# Create admin license and user if not exists
echo "🔑 Ensuring admin license and user exist..."
python -c "
import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from licensing.models import License
from licensing.services import LicenseGenerationService
from django.contrib.auth import get_user_model
User = get_user_model()

# Create creator license for admin
license_service = LicenseGenerationService()

# Check if we have any creator licenses
creator_licenses = License.objects.filter(license_type='creator', status='active')
if not creator_licenses.exists():
    # Generate a new creator license
    license_code = license_service.generate_license_code('creator', 'admin_default')
    
    license = License.objects.create(
        license_code=license_code,
        license_type='creator',
        status='active',
        max_users=1000,
        description='Default admin creator license for ConvaBI deployment',
        features={'admin': True, 'creator': True, 'full_access': True},
        created_by='system'
    )
    print(f'✅ Creator license created: {license_code}')
else:
    license = creator_licenses.first()
    print(f'✅ Creator license already exists: {license.license_code}')

# Create superuser if not exists
if not User.objects.filter(is_superuser=True).exists():
    admin_user = User.objects.create_superuser('admin', 'admin@convabi.com', 'admin123')
    print('✅ Admin user created: admin/admin123')
    
    # Assign the license to the admin user
    from licensing.models import UserLicense
    user_license, created = UserLicense.objects.get_or_create(
        user=admin_user,
        license=license,
        defaults={'is_active': True, 'assigned_by': 'system'}
    )
    if created:
        print('✅ License assigned to admin user')
    else:
        print('✅ License already assigned to admin user')
else:
    print('✅ Admin user already exists')
"

# Start the application
echo "🌟 Starting ConvaBI application..."
if [ "$1" = "celery" ]; then
    exec celery -A dbchat_project worker --loglevel=info --concurrency=2
elif [ "$1" = "celery-beat" ]; then
    exec celery -A dbchat_project beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler
else
    exec gunicorn dbchat_project.wsgi:application --bind 0.0.0.0:8000 --workers 3 --timeout 120
fi 