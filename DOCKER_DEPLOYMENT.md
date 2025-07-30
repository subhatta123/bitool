# 🐳 ConvaBI Docker Deployment Guide

Complete deployment guide for ConvaBI Business Intelligence Platform using Docker containers on a single VM.

## 📋 Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Deployment Options](#deployment-options)
- [Monitoring & Maintenance](#monitoring--maintenance)
- [Troubleshooting](#troubleshooting)
- [Security Considerations](#security-considerations)
- [Backup & Recovery](#backup--recovery)

## 🎯 Overview

This deployment includes:
- **ConvaBI Django Application** - Main business intelligence platform
- **PostgreSQL Database** - Primary data storage
- **Redis** - Caching and Celery message broker
- **Celery Worker** - Background task processing
- **Celery Beat** - ETL job scheduling
- **Nginx** - Reverse proxy and static file serving

## 🛠 Prerequisites

### System Requirements

**Minimum:**
- 4 GB RAM
- 2 CPU cores
- 20 GB storage
- Docker 20.10+
- Docker Compose 2.0+

**Recommended:**
- 8 GB RAM
- 4 CPU cores
- 50 GB storage
- SSD storage for better performance

### Software Requirements

```bash
# Install Docker (Ubuntu/Debian)
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker --version
docker-compose --version
```

## 🚀 Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone https://github.com/subhatta123/conva.git
cd conva

# Make deployment script executable
chmod +x deploy.sh
```

### 2. Configure Environment

```bash
# Copy environment template
cp docker.env.template .env

# Edit configuration (IMPORTANT!)
nano .env
```

**Key settings to configure:**
```bash
# Security
SECRET_KEY=your-very-long-random-secret-key-here
ALLOWED_HOSTS=your-domain.com,localhost,127.0.0.1,0.0.0.0

# Database
POSTGRES_PASSWORD=your-secure-database-password

# Email (Optional)
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password

# LLM Integration (Optional)
OPENAI_API_KEY=your-openai-api-key
```

### 3. Deploy

```bash
# Full deployment
./deploy.sh deploy

# OR manual deployment
docker-compose up -d
```

### 4. Access Application

- **Main Application**: http://localhost:8000
- **Admin Panel**: http://localhost:8000/admin
- **Health Check**: http://localhost:8000/health
- **Default Admin**: admin / admin123

## ⚙️ Configuration

### Environment Variables

#### Core Django Settings
```bash
DEBUG=False                    # Set to True for development
SECRET_KEY=your-secret-key     # Generate with: python -c "import secrets; print(secrets.token_urlsafe(50))"
ALLOWED_HOSTS=localhost,your-domain.com
TIME_ZONE=UTC                  # Your timezone
LANGUAGE_CODE=en-us
```

#### Database Configuration
```bash
# PostgreSQL (Production)
DATABASE_URL=postgresql://convabiuser:convabipass@postgres:5432/convabi
POSTGRES_DB=convabi
POSTGRES_USER=convabiuser
POSTGRES_PASSWORD=convabipass

# SQLite (Development)
# DATABASE_URL=sqlite:///db.sqlite3
```

#### Redis & Celery
```bash
USE_REDIS=True
REDIS_URL=redis://redis:6379/0
CELERY_BROKER_URL=redis://redis:6379/1
CELERY_RESULT_BACKEND=redis://redis:6379/2
CELERY_TASK_ALWAYS_EAGER=False  # Set to True for development
```

#### LLM Integration
```bash
# OpenAI
OPENAI_API_KEY=sk-your-openai-key

# Ollama (Local LLM)
OLLAMA_BASE_URL=http://localhost:11434
```

#### Email Configuration
```bash
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password  # Use App Password for Gmail
EMAIL_USE_TLS=True
```

#### Security Settings
```bash
SECURE_SSL_REDIRECT=False      # Set to True for HTTPS
SESSION_COOKIE_SECURE=False    # Set to True for HTTPS
CSRF_COOKIE_SECURE=False       # Set to True for HTTPS
```

### Port Configuration
```bash
WEB_PORT=8000        # Django application
NGINX_PORT=80        # Nginx HTTP
NGINX_SSL_PORT=443   # Nginx HTTPS
POSTGRES_PORT=5432   # PostgreSQL
REDIS_PORT=6379      # Redis
```

## 🎛 Deployment Options

### Option 1: Full Deployment (All Services)
```bash
# Deploy everything
./deploy.sh deploy

# Services included:
# - PostgreSQL database
# - Redis cache
# - Django web application
# - Celery worker
# - Celery beat scheduler
# - Nginx reverse proxy
```

### Option 2: Development Mode
```bash
# Use SQLite and disable background tasks
./deploy.sh --env development deploy
```

### Option 3: Custom Configuration
```bash
# Skip migrations
./deploy.sh --no-migrations deploy

# Skip image building
./deploy.sh --no-build deploy

# Use custom compose file
./deploy.sh -f docker-compose.production.yml deploy
```

### Option 4: Manual Docker Commands
```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f web
```

## 📊 Monitoring & Maintenance

### Service Management

```bash
# Check deployment status
./deploy.sh status

# View logs
./deploy.sh logs [service_name]

# Restart services
./deploy.sh restart

# Stop services
./deploy.sh stop

# Health check
./deploy.sh health
```

### Individual Service Commands

```bash
# Check specific service logs
docker-compose logs -f web      # Django application
docker-compose logs -f celery   # Background tasks
docker-compose logs -f postgres # Database
docker-compose logs -f redis    # Cache

# Execute commands in containers
docker-compose exec web python manage.py shell
docker-compose exec postgres psql -U convabiuser -d convabi
docker-compose exec redis redis-cli
```

### Performance Monitoring

```bash
# Container resource usage
docker stats

# Disk usage
docker system df

# Service health
curl http://localhost:8000/health/
```

### Database Operations

```bash
# Create database backup
docker-compose exec postgres pg_dump -U convabiuser convabi > backup.sql

# Restore database
docker-compose exec -T postgres psql -U convabiuser convabi < backup.sql

# Run migrations
docker-compose exec web python manage.py migrate

# Create superuser
docker-compose exec web python manage.py createsuperuser
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Container Won't Start
```bash
# Check logs
docker-compose logs web

# Check if ports are in use
netstat -tulpn | grep :8000

# Restart specific service
docker-compose restart web
```

#### 2. Database Connection Issues
```bash
# Check PostgreSQL logs
docker-compose logs postgres

# Test database connection
docker-compose exec web python manage.py dbshell

# Reset database
docker-compose down -v
docker-compose up -d
```

#### 3. Permission Issues
```bash
# Fix ownership
sudo chown -R $USER:$USER .

# Fix permissions
chmod 755 logs data media backups
```

#### 4. Static Files Not Loading
```bash
# Collect static files
docker-compose exec web python manage.py collectstatic --noinput

# Check Nginx configuration
docker-compose exec nginx nginx -t
```

#### 5. Celery Tasks Not Running
```bash
# Check Celery worker
docker-compose logs celery

# Check Celery beat scheduler
docker-compose logs celery-beat

# Restart Celery services
docker-compose restart celery celery-beat
```

### Debugging Commands

```bash
# Enter container shell
docker-compose exec web bash

# Check Django configuration
docker-compose exec web python manage.py check

# View environment variables
docker-compose exec web env

# Test database migrations
docker-compose exec web python manage.py migrate --dry-run
```

### Log Locations

```bash
# Application logs
./logs/django.log
./logs/celery.log
./logs/nginx_access.log
./logs/nginx_error.log

# Container logs
docker-compose logs web
docker-compose logs postgres
docker-compose logs redis
```

## 🔒 Security Considerations

### 1. Change Default Passwords
```bash
# Database password
POSTGRES_PASSWORD=your-secure-password

# Admin user password
docker-compose exec web python manage.py changepassword admin
```

### 2. Enable HTTPS (Production)
```bash
# Update .env
SECURE_SSL_REDIRECT=True
SESSION_COOKIE_SECURE=True
CSRF_COOKIE_SECURE=True

# Configure SSL certificates in nginx.conf
# Place certificates in ./docker/ssl/
```

### 3. Network Security
```bash
# Restrict external access to database
# Remove PostgreSQL port mapping in docker-compose.yml
# ports:
#   - "5432:5432"  # Remove this line
```

### 4. Firewall Configuration
```bash
# Allow only necessary ports
sudo ufw allow 80    # HTTP
sudo ufw allow 443   # HTTPS
sudo ufw allow 22    # SSH
sudo ufw enable
```

## 💾 Backup & Recovery

### Automated Backups

```bash
# Create backup
./deploy.sh backup

# Backup includes:
# - Database dump
# - Data directories
# - Configuration files
```

### Manual Backup

```bash
# Database
docker-compose exec postgres pg_dump -U convabiuser convabi > database_backup.sql

# Application data
tar -czf data_backup.tar.gz data/ media/ logs/

# Configuration
cp .env config_backup.env
```

### Recovery

```bash
# Restore database
docker-compose exec -T postgres psql -U convabiuser convabi < database_backup.sql

# Restore data
tar -xzf data_backup.tar.gz

# Restart services
docker-compose restart
```

### Scheduled Backups

Create a cron job for automated backups:

```bash
# Edit crontab
crontab -e

# Add daily backup at 2 AM
0 2 * * * /path/to/conva/deploy.sh backup
```

## 🔄 Updates & Maintenance

### Updating ConvaBI

```bash
# Pull latest code
git pull origin main

# Rebuild and restart
./deploy.sh rebuild

# Or manually
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Database Migrations

```bash
# Run migrations
docker-compose exec web python manage.py migrate

# Create new migrations
docker-compose exec web python manage.py makemigrations
```

### Cleanup

```bash
# Remove unused Docker resources
./deploy.sh cleanup

# Or manually
docker system prune -f
docker volume prune -f
```

## 📞 Support

### Getting Help

1. **Check logs**: Always start with container logs
2. **Health check**: Verify all services are running
3. **Configuration**: Ensure .env file is properly configured
4. **Resources**: Check system resources (RAM, disk space)

### Debug Mode

```bash
# Enable debug mode
export DEBUG=True
docker-compose restart web

# View detailed error messages in browser
```

### Performance Optimization

```bash
# Increase worker processes
# Edit docker-compose.yml celery service:
command: celery -A dbchat_project worker --loglevel=info --concurrency=8

# Optimize database
docker-compose exec postgres psql -U convabiuser -d convabi -c "VACUUM ANALYZE;"
```

---

## 🎉 Next Steps

After successful deployment:

1. **Login**: Access http://localhost:8000 with admin/admin123
2. **Configure**: Set up LLM and email configurations
3. **Data Sources**: Add your first data source
4. **ETL Jobs**: Create scheduled ETL operations
5. **Dashboards**: Build your first dashboard

For detailed usage instructions, see the main README.md file.

---

**Happy Analytics! 📊✨** 