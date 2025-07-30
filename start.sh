#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to log messages with timestamp
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [STARTUP] $1"
}

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    log "Waiting for PostgreSQL to be ready..."
    
    until PGPASSWORD=$DATABASE_PASSWORD psql -h "$DATABASE_HOST" -U "$DATABASE_USER" -d "$DATABASE_NAME" -c '\q' 2>/dev/null; do
        log "PostgreSQL is unavailable - sleeping for 2 seconds"
        sleep 2
    done
    
    log "PostgreSQL is up and running!"
}

# Function to wait for Redis to be ready
wait_for_redis() {
    log "Waiting for Redis to be ready..."
    
    # Extract Redis details from REDIS_URL if provided
    if [[ -n "$REDIS_URL" ]]; then
        # Parse redis://[:password@]host:port[/db]
        REDIS_HOST=$(echo $REDIS_URL | sed -E 's/redis:\/\/(:[^@]*@)?([^:]+).*/\2/')
        REDIS_PORT=$(echo $REDIS_URL | sed -E 's/redis:\/\/[^:]*:?[^@]*@?[^:]+:([0-9]+).*/\1/')
        
        # Default to standard Redis port if not specified
        REDIS_PORT=${REDIS_PORT:-6379}
    else
        REDIS_HOST=${REDIS_HOST:-redis}
        REDIS_PORT=${REDIS_PORT:-6379}
    fi
    
    until timeout 1 bash -c "echo > /dev/tcp/$REDIS_HOST/$REDIS_PORT" 2>/dev/null; do
        log "Redis is unavailable - sleeping for 2 seconds"
        sleep 2
    done
    
    log "Redis is up and running!"
}

# Function to run Django migrations
run_migrations() {
    log "Running Django migrations..."
    
    # Try to run migrations with better error handling
    max_retries=3
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        log "Migration attempt $((retry_count + 1)) of $max_retries"
        
        # First try to make migrations
        if python manage.py makemigrations --noinput; then
            log "Makemigrations completed successfully"
        else
            log "Warning: Makemigrations had issues, continuing with migrate"
        fi
        
        # Then try to run migrations
        if python manage.py migrate --noinput; then
            log "Migrations completed successfully!"
            return 0
        else
            log "Migration attempt $((retry_count + 1)) failed"
            retry_count=$((retry_count + 1))
            
            if [ $retry_count -lt $max_retries ]; then
                log "Waiting 5 seconds before retry..."
                sleep 5
                
                # Try to reset database connection
                log "Attempting to flush any problematic connections..."
                python manage.py dbshell --command="SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction';" 2>/dev/null || true
            fi
        fi
    done
    
    log "ERROR: All migration attempts failed. Trying fallback approach..."
    
    # Fallback: Try to migrate specific apps one by one
    log "Attempting individual app migrations as fallback..."
    
    # Core Django apps first
    for app in contenttypes auth admin sessions authtoken; do
        log "Migrating $app..."
        python manage.py migrate $app --noinput || log "Warning: $app migration failed"
    done
    
    # Then custom apps
    for app in accounts core datasets dashboards licensing; do
        log "Migrating $app..."
        python manage.py migrate $app --noinput || log "Warning: $app migration failed"
    done
    
    log "Migration process completed with potential warnings"
}

# Function to collect static files
collect_static() {
    log "Collecting static files..."
    
    python manage.py collectstatic --noinput --clear
    
    log "Static files collected successfully!"
}

# Function to create superuser if it doesn't exist
create_superuser() {
    log "Creating superuser if it doesn't exist..."
    
    python manage.py shell << EOF
from django.contrib.auth import get_user_model
User = get_user_model()

# Check if any superuser exists
if not User.objects.filter(is_superuser=True).exists():
    # Create superuser with default credentials
    username = '${DJANGO_SUPERUSER_USERNAME:-admin}'
    email = '${DJANGO_SUPERUSER_EMAIL:-admin@convabi.com}'
    password = '${DJANGO_SUPERUSER_PASSWORD:-admin123}'
    
    User.objects.create_superuser(username=username, email=email, password=password)
    print(f"Superuser '{username}' created successfully!")
else:
    print("Superuser already exists, skipping creation.")
EOF
    
    log "Superuser setup completed!"
}

# Function to ensure required licenses are present
ensure_licenses() {
    log "Ensuring required license codes are present..."
    
    # Simple license check without complex logging to avoid display issues
    python -c "
import os, sys, django
sys.path.append('/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from licensing.models import License

# Required licenses
required_licenses = [
    ('6C5551C138AF46A3', 'creator'),
    ('06FC2245689FADC8', 'creator'),
    ('FF1DBB98524B2EC6', 'creator'),
    ('39E85429B0330590', 'creator'),
    ('EDDE934CC3AF6A26', 'creator'),
    ('2C4EA092EB67A558', 'viewer'),
    ('7AC53323B354FD61', 'viewer'),
    ('C2092B147C270EC5', 'viewer')
]

created = 0
existing = 0

for code, license_type in required_licenses:
    obj, created_flag = License.objects.get_or_create(
        license_code=code,
        defaults={
            'license_type': license_type,
            'description': f'{license_type.title()} License - {code}',
            'max_users': 1,
            'status': 'active',
            'created_by': 'deployment'
        }
    )
    if created_flag:
        created += 1
    else:
        existing += 1

print(f'License deployment: {created} created, {existing} existing, {len(required_licenses)} total')
"
    
    log "License deployment completed!"
}

# Function to fix user roles and license alignment
fix_user_roles() {
    log "Fixing user roles and license alignment..."
    
    # Simple role/license alignment without complex logging
    python -c "
import os, sys, django
sys.path.append('/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.contrib.auth import get_user_model
from licensing.models import UserLicense

User = get_user_model()

# Get all users with licenses and fix their roles
user_licenses = UserLicense.objects.filter(is_active=True).select_related('user', 'license')
updated_count = 0

for user_license in user_licenses:
    user = user_license.user
    license_type = user_license.license.license_type
    
    if not isinstance(user.roles, list):
        user.roles = []
    
    roles_updated = False
    
    if license_type == 'creator':
        if 'creator' not in user.roles:
            user.roles.append('creator')
            roles_updated = True
        if 'user' not in user.roles:
            user.roles.append('user')
            roles_updated = True
    elif license_type == 'viewer':
        if 'viewer' not in user.roles:
            user.roles.append('viewer')
            roles_updated = True
        if 'user' not in user.roles:
            user.roles.append('user')
            roles_updated = True
    
    if roles_updated:
        user.save()
        updated_count += 1

print(f'Role alignment: {updated_count} users updated')
"
    
    log "User roles and licenses aligned!"
}

# Function to initialize app data
initialize_data() {
    log "Initializing application data..."
    
    # Run any custom management commands for data initialization
    # python manage.py loaddata initial_data.json 2>/dev/null || log "No initial data fixture found"
    
    # Create necessary directories
    mkdir -p /app/data /app/logs /app/media /app/staticfiles
    chmod 755 /app/data /app/logs /app/media /app/staticfiles
    
    log "Application data initialization completed!"
}

# Function to check application health
health_check() {
    log "Performing application health check..."
    
    python manage.py check --deploy 2>/dev/null || log "Health check completed with warnings"
    
    log "Application health check completed!"
}

# Main startup function
main() {
    log "Starting ConvaBI Django Application..."
    log "Environment: ${DEBUG:-Production}"
    log "Database: ${DATABASE_HOST:-db}:${DATABASE_PORT:-5432}/${DATABASE_NAME:-dbchat}"
    
    # Check if we're using PostgreSQL (default) or SQLite
    if [[ "${USE_SQLITE:-False}" == "False" ]]; then
        wait_for_postgres
    else
        log "Using SQLite database - skipping PostgreSQL wait"
    fi
    
    # Check if Redis is configured
    if [[ -n "$REDIS_URL" ]] || [[ -n "$CELERY_BROKER_URL" ]]; then
        wait_for_redis
    else
        log "Redis not configured - skipping Redis wait"
    fi
    
    # Django setup
    run_migrations
    collect_static
    create_superuser
    ensure_licenses
    fix_user_roles
    initialize_data
    health_check
    
    log "Startup sequence completed successfully!"
    
    # Determine how to start the application
    if [[ "${DEBUG:-False}" == "True" ]]; then
        log "Starting Django development server..."
        exec python manage.py runserver 0.0.0.0:8000
    else
        log "Starting Gunicorn production server..."
        exec gunicorn dbchat_project.wsgi:application \
            --bind 0.0.0.0:8000 \
            --workers ${GUNICORN_WORKERS:-3} \
            --worker-class ${GUNICORN_WORKER_CLASS:-sync} \
            --worker-connections ${GUNICORN_WORKER_CONNECTIONS:-1000} \
            --max-requests ${GUNICORN_MAX_REQUESTS:-1000} \
            --max-requests-jitter ${GUNICORN_MAX_REQUESTS_JITTER:-100} \
            --timeout ${GUNICORN_TIMEOUT:-30} \
            --keep-alive ${GUNICORN_KEEPALIVE:-2} \
            --log-level ${LOG_LEVEL:-info} \
            --access-logfile - \
            --error-logfile - \
            --capture-output \
            --enable-stdio-inheritance
    fi
}

# Trap signals for graceful shutdown
trap 'log "Received shutdown signal, exiting gracefully..."; exit 0' SIGTERM SIGINT

# Run main function
main "$@" 