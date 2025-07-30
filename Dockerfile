# ConvaBI Business Intelligence Platform Docker Image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libpq-dev \
    postgresql-client \
    redis-tools \
    supervisor \
    nginx \
    cron \
    wget \
    unzip \
    libssl-dev \
    libffi-dev \
    pkg-config \
    default-libmysqlclient-dev \
    freetds-dev \
    unixodbc-dev \
    gnupg2 \
    apt-transport-https \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft SQL Server ODBC Driver
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements-docker.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy project files
COPY . /app/

# Install Node.js and Puppeteer for dashboard exports
COPY install_puppeteer.sh /app/
RUN chmod +x /app/install_puppeteer.sh && /app/install_puppeteer.sh

# Create necessary directories
RUN mkdir -p /app/logs \
    /app/media \
    /app/staticfiles \
    /app/data \
    /app/backups \
    /app/ssl

# Copy configuration files
COPY docker/supervisor.conf /etc/supervisor/conf.d/
COPY docker/nginx.conf /etc/nginx/sites-available/default
COPY docker/entrypoint.sh /app/
COPY docker/wait-for-it.sh /app/

# Make scripts executable
RUN chmod +x /app/entrypoint.sh /app/wait-for-it.sh

# Create Django user
RUN useradd --create-home --shell /bin/bash django
RUN chown -R django:django /app
RUN chown -R django:django /var/log/nginx

# Switch to Django directory
WORKDIR /app/django_dbchat

# Collect static files (with fallback)
RUN python manage.py collectstatic --noinput --clear || echo "Static files collection failed, continuing..."

# Create database migrations (with fallback)
RUN python manage.py makemigrations || echo "Migration creation failed, continuing..."
RUN python manage.py migrate || echo "Migration failed, continuing..."

# Set proper permissions
RUN chown -R django:django /app

# Expose ports
EXPOSE 8000 80 443

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health/ || exit 1

# Switch back to app directory
WORKDIR /app

# Entry point
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisor.conf"] 