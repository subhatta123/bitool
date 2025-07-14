#!/bin/bash
set -e

# Database connectivity is handled by Docker Compose depends_on and healthcheck
echo "Starting Django application..."
sleep 5  # Brief wait to ensure database is ready

# Skip migrations for now to get Django running
echo "Skipping migrations temporarily to get web server started..."

# Skip static files collection for now
echo "Skipping static files collection..."

# Skip superuser creation for now
echo "Skipping superuser creation..."

# Execute the main command
echo "Starting Django web server..."
exec "$@" 