#!/bin/bash

# ConvaBI Docker Entrypoint Script
# Handles initialization and service startup

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

# Function to wait for a service
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    
    info "Waiting for $service_name at $host:$port..."
    
    # Use Python to test TCP connection instead of nc
    until python3 -c "import socket; socket.create_connection(('$host', $port), timeout=5)" 2>/dev/null; do
        warn "$service_name is not ready yet. Waiting..."
        sleep 2
    done
    
    log "$service_name is ready!"
}

# Function to check database connection
check_database() {
    info "Checking database connection..."
    
    cd /app/django_dbchat
    
    if python manage.py check --database default >/dev/null 2>&1; then
        log "Database connection successful"
        return 0
    else
        warn "Database connection failed"
        return 1
    fi
}

# Function to run database migrations
run_migrations() {
    info "Running database migrations..."
    
    cd /app/django_dbchat
    
    # Create migrations if needed
    if python manage.py makemigrations --dry-run >/dev/null 2>&1; then
        log "Creating migrations..."
        python manage.py makemigrations || warn "Migration creation failed"
    fi
    
    # Apply migrations
    log "Applying migrations..."
    python manage.py migrate || warn "Migration application failed"
    
    # Create superuser if needed
    if [ "$CREATE_SUPERUSER" = "true" ]; then
        info "Creating superuser..."
        python manage.py shell << EOF
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser('admin', 'admin@example.com', 'admin123')
    print("Superuser created: admin/admin123")
else:
    print("Superuser already exists")
EOF
    fi
}

# Function to collect static files
collect_static() {
    info "Collecting static files..."
    
    cd /app/django_dbchat
    python manage.py collectstatic --noinput --clear || warn "Static file collection failed"
}

# Function to setup directories
setup_directories() {
    info "Setting up directories and permissions..."
    
    # Create necessary directories
    mkdir -p /app/logs /app/media /app/staticfiles /app/data /app/backups
    
    # Set permissions
    chown -R django:django /app/logs /app/media /app/staticfiles /app/data /app/backups
    chmod 755 /app/logs /app/media /app/staticfiles /app/data /app/backups
    
    # Create log files
    touch /app/logs/django.log /app/logs/celery.log /app/logs/nginx.log
    chown django:django /app/logs/*.log
}

# Function to start services based on role
start_services() {
    local role=${1:-web}
    
    case "$role" in
        "web")
            info "Starting Django web server..."
            cd /app/django_dbchat
            exec python manage.py runserver 0.0.0.0:8000
            ;;
        "celery")
            info "Starting Celery worker..."
            cd /app/django_dbchat
            exec celery -A dbchat_project worker --loglevel=info --concurrency=4
            ;;
        "celery-beat")
            info "Starting Celery beat scheduler..."
            cd /app/django_dbchat
            exec celery -A dbchat_project beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler
            ;;
        "supervisor")
            info "Starting services with supervisor..."
            exec supervisord -c /etc/supervisor/conf.d/supervisor.conf
            ;;
        *)
            info "Starting default services..."
            cd /app/django_dbchat
            exec python manage.py runserver 0.0.0.0:8000
            ;;
    esac
}

# Main execution
main() {
    log "ConvaBI Docker Container Starting..."
    
    # Setup environment
    export DJANGO_SETTINGS_MODULE=dbchat_project.settings
    
    # Setup directories
    setup_directories
    
    # Wait for external services if configured
    if [ "$USE_REDIS" = "true" ] && [ "$REDIS_HOST" ]; then
        wait_for_service "${REDIS_HOST:-redis}" "${REDIS_PORT:-6379}" "Redis"
    fi
    
    if [ "$DATABASE_URL" ] && [[ "$DATABASE_URL" == *"postgres"* ]]; then
        wait_for_service "${POSTGRES_HOST:-postgres}" "${POSTGRES_PORT:-5432}" "PostgreSQL"
    fi
    
    # Check database and run migrations (only for web and first-time setup)
    if [ "${1:-web}" = "web" ] || [ "${1:-web}" = "supervisor" ]; then
        if check_database; then
            run_migrations
            collect_static
        else
            warn "Database check failed, continuing anyway..."
        fi
    fi
    
    # Start services
    if [ $# -eq 0 ]; then
        start_services "web"
    else
        start_services "$1"
    fi
}

# Health check endpoint
if [ "$1" = "health" ]; then
    curl -f http://localhost:8000/health/ || exit 1
    exit 0
fi

# Execute main function
main "$@" 