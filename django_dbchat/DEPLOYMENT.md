# 🚀 Django DBChat - Complete Docker Deployment Guide

This guide will help you deploy the complete Django DBChat application using Docker with all its dependencies, functionality, and UI components.

## 📋 Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Deployment Options](#deployment-options)
- [Configuration](#configuration)
- [Services Overview](#services-overview)
- [Management Commands](#management-commands)
- [Monitoring](#monitoring)
- [Backup & Restore](#backup--restore)
- [Security](#security)
- [Troubleshooting](#troubleshooting)
- [Advanced Usage](#advanced-usage)

## 🔍 Overview

The Django DBChat application is now completely containerized with the following features:

### ✨ Key Features
- **Full-stack Django application** with all dependencies
- **PostgreSQL database** with monitoring and analytics schemas
- **Redis caching** for sessions and background tasks
- **Celery workers** for background processing
- **Nginx reverse proxy** with SSL support
- **Comprehensive monitoring** with Prometheus and Grafana
- **Health checks** and automated backups
- **DuckDB integration** for analytics
- **LLM integration** (OpenAI, Anthropic, Google AI)
- **Email functionality** with SMTP support
- **File processing** (CSV, Excel, PDF generation)
- **Multi-stage Docker build** for optimization

### 🏗️ Architecture Components
- **Web Application**: Django app with Gunicorn
- **Database**: PostgreSQL 15 with custom schemas
- **Cache**: Redis 7 with persistence
- **Background Tasks**: Celery with beat scheduler
- **Monitoring**: Flower, Prometheus, Grafana
- **Proxy**: Nginx with SSL termination
- **Storage**: Persistent volumes for data, logs, media

## 🛠️ Prerequisites

### System Requirements
- **OS**: Linux (Ubuntu 20.04+), macOS, or Windows with WSL2
- **CPU**: Minimum 2 cores, Recommended 4+ cores
- **Memory**: Minimum 4GB RAM, Recommended 8GB+ RAM
- **Storage**: Minimum 20GB free space, Recommended 50GB+
- **Network**: Ports 80, 443, 8000, 5432, 6379, 5555, 3000, 9090

### Software Requirements
- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+
- **Git**: For cloning the repository
- **OpenSSL**: For SSL certificate generation

## 🚀 Quick Start

### 1. Clone and Navigate
```bash
git clone <repository-url>
cd django_dbchat
```

### 2. Quick Deployment
```bash
# Full deployment with all services
./build.sh full-deploy

# Or for development with monitoring
./build.sh dev-deploy

# Or for production with SSL and monitoring
./build.sh prod-deploy
```

### 3. Access the Application
- **Web Interface**: http://localhost:8000
- **Admin Panel**: http://localhost:8000/admin
- **Default Login**: admin / admin123

## 📦 Deployment Options

### Option 1: Full Deployment (Recommended)
```bash
./build.sh full-deploy
```
**Includes**: Web app, PostgreSQL, Redis, Celery, basic monitoring

### Option 2: Development Deployment
```bash
./build.sh dev-deploy
```
**Includes**: All services + Flower monitoring + Grafana dashboards

### Option 3: Production Deployment
```bash
./build.sh prod-deploy
```
**Includes**: All services + SSL + monitoring + backup + security features

### Option 4: Manual Step-by-Step
```bash
# 1. Install Docker (if needed)
./build.sh install-docker

# 2. Setup environment
./build.sh setup-env

# 3. Generate SSL certificates
./build.sh generate-ssl

# 4. Build Docker image
./build.sh build

# 5. Start services
./build.sh start production

# 6. Run migrations
./build.sh migrate

# 7. Collect static files
./build.sh collectstatic

# 8. Create superuser
./build.sh createsuperuser
```

## ⚙️ Configuration

### Environment Setup
1. **Copy environment template**:
   ```bash
   cp env.example .env
   ```

2. **Edit configuration**:
   ```bash
   nano .env
   ```

### Essential Configuration
```env
# Security (REQUIRED)
SECRET_KEY=your-very-secure-secret-key-here
DATABASE_PASSWORD=your-secure-database-password
REDIS_PASSWORD=your-secure-redis-password

# Database
USE_SQLITE=False  # Use PostgreSQL for production
DATABASE_NAME=dbchat
DATABASE_USER=postgres

# LLM Configuration
OPENAI_API_KEY=your-openai-api-key
LLM_PROVIDER=openai

# Email (Optional)
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-email-password

# Domain (Production)
ALLOWED_HOSTS=localhost,127.0.0.1,your-domain.com
CUSTOM_DOMAIN=your-domain.com
```

### Advanced Configuration
```env
# Performance
CELERY_WORKER_CONCURRENCY=4
DATABASE_CONNECTION_POOL_SIZE=10

# File Uploads
MAX_CSV_FILE_SIZE=524288000  # 500MB

# Monitoring
FLOWER_PASSWORD=secure-password
GRAFANA_PASSWORD=secure-password

# SSL/Security
FORCE_HTTPS=True  # Enable for production
```

## 🔧 Services Overview

### Core Services
| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| Web App | `dbchat_web` | 8000 | Django application |
| Database | `dbchat_postgres` | 5432 | PostgreSQL database |
| Cache | `dbchat_redis` | 6379 | Redis cache/sessions |
| Worker | `dbchat_celery` | - | Background tasks |
| Scheduler | `dbchat_celery_beat` | - | Periodic tasks |

### Optional Services (Profiles)
| Service | Container | Port | Profile | Purpose |
|---------|-----------|------|---------|---------|
| Nginx | `dbchat_nginx` | 80,443 | `production` | Reverse proxy |
| Flower | `dbchat_flower` | 5555 | `monitoring` | Celery monitoring |
| Prometheus | `dbchat_prometheus` | 9090 | `monitoring` | Metrics collection |
| Grafana | `dbchat_grafana` | 3000 | `monitoring` | Dashboards |
| Backup | `dbchat_backup` | - | `backup` | Automated backups |

## 📋 Management Commands

### Service Management
```bash
# Start services
./build.sh start [profile]

# Stop services
./build.sh stop

# Restart services
./build.sh restart

# View logs
./build.sh logs [service]

# Health check
./build.sh health
```

### Database Management
```bash
# Run migrations
./build.sh migrate

# Create superuser
./build.sh createsuperuser

# Backup database
./build.sh backup

# Restore database
./build.sh restore backups/backup_file.sql
```

### Docker Management
```bash
# Build image
./build.sh build

# Pull images
./build.sh pull

# Cleanup unused resources
./build.sh cleanup
```

## 📊 Monitoring

### Available Monitoring Services

#### 1. Application Health
```bash
# Check application health
curl http://localhost:8000/health/

# Run comprehensive health check
./build.sh health
```

#### 2. Flower (Celery Monitoring)
- **URL**: http://localhost:5555
- **Login**: admin / (see FLOWER_PASSWORD in .env)
- **Features**: Task monitoring, worker stats, task history

#### 3. Grafana (Dashboards)
- **URL**: http://localhost:3000
- **Login**: admin / (see GRAFANA_PASSWORD in .env)
- **Features**: System metrics, application performance, custom dashboards

#### 4. Prometheus (Metrics)
- **URL**: http://localhost:9090
- **Features**: Raw metrics, alerting rules, target status

### Monitoring Endpoints
- **Health Check**: `/health/`
- **Metrics**: `/metrics/`
- **Admin**: `/admin/`
- **API Status**: `/api/`

## 💾 Backup & Restore

### Automated Backups
```bash
# Enable backup service
docker-compose --profile backup up -d

# Manual backup
./build.sh backup

# List backups
ls -la backups/

# Restore from backup
./build.sh restore backups/db_backup_20231215_143022.sql
```

### Database Backup
```bash
# Create backup
docker-compose exec db pg_dump -U postgres dbchat > backup.sql

# Restore backup
docker-compose exec -T db psql -U postgres -d dbchat < backup.sql
```

### Volume Backup
```bash
# Backup persistent data
docker run --rm -v dbchat_data_volume:/data -v $(pwd)/backups:/backup alpine tar czf /backup/data_backup.tar.gz /data
```

## 🔐 Security

### SSL Configuration
```bash
# Generate SSL certificates
./build.sh generate-ssl

# Use custom certificates
cp your-cert.pem ssl/cert.pem
cp your-key.pem ssl/key.pem
```

### Security Best Practices
1. **Change default passwords** in `.env`
2. **Use strong SECRET_KEY**
3. **Enable HTTPS** in production
4. **Configure firewall**:
   ```bash
   sudo ufw allow 80/tcp
   sudo ufw allow 443/tcp
   sudo ufw allow 22/tcp
   sudo ufw enable
   ```
5. **Regular updates**:
   ```bash
   docker-compose pull
   docker-compose up -d
   ```

### Security Headers
The nginx configuration includes:
- HSTS (HTTP Strict Transport Security)
- X-Frame-Options
- X-Content-Type-Options
- X-XSS-Protection
- Content Security Policy

## 🔍 Troubleshooting

### Common Issues

#### 1. Services Won't Start
```bash
# Check logs
./build.sh logs

# Check system resources
docker system df
docker stats

# Clean up
./build.sh cleanup
```

#### 2. Database Connection Issues
```bash
# Check database status
docker-compose exec db pg_isready -U postgres

# Reset database
docker-compose down
docker volume rm dbchat_postgres_data
docker-compose up -d
```

#### 3. Permission Issues
```bash
# Fix permissions
sudo chown -R $USER:$USER ./data ./logs ./media
```

#### 4. Memory Issues
```bash
# Check memory usage
docker stats

# Reduce workers
# Edit .env: CELERY_WORKER_CONCURRENCY=1
```

#### 5. SSL Certificate Issues
```bash
# Regenerate certificates
rm -rf ssl/
./build.sh generate-ssl
```

### Debug Mode
```bash
# Enable debug logging
echo "DEBUG=True" >> .env
docker-compose restart web

# View detailed logs
./build.sh logs web
```

## 🚀 Advanced Usage

### Custom Profiles
```bash
# Start with multiple profiles
docker-compose --profile production --profile monitoring up -d

# Available profiles
# - production: Nginx + SSL
# - monitoring: Flower + Prometheus + Grafana
# - backup: Automated backup service
```

### Scaling Services
```bash
# Scale web workers
docker-compose up -d --scale web=3

# Scale Celery workers
docker-compose up -d --scale celery=2
```

### Custom Docker Build
```bash
# Build with custom tag
docker build -t dbchat:custom .

# Build with build args
docker build --build-arg PYTHON_VERSION=3.11 -t dbchat:custom .
```

### Environment-Specific Deployments
```bash
# Development
export COMPOSE_FILE=docker-compose.yml:docker-compose.dev.yml

# Production
export COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml

# Staging
export COMPOSE_FILE=docker-compose.yml:docker-compose.staging.yml
```

## 📊 Performance Optimization

### Database Optimization
```sql
-- Run in PostgreSQL
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
SELECT pg_reload_conf();
```

### Redis Optimization
```bash
# Edit Redis config
docker-compose exec redis redis-cli CONFIG SET maxmemory 512mb
```

### Application Optimization
```env
# In .env file
CELERY_WORKER_CONCURRENCY=4
DATABASE_CONNECTION_POOL_SIZE=10
```

## 🔄 Updates and Maintenance

### Regular Updates
```bash
# Update Docker images
docker-compose pull

# Rebuild application
./build.sh build

# Restart services
./build.sh restart

# Run migrations
./build.sh migrate
```

### Maintenance Tasks
```bash
# Clean up old data
./build.sh cleanup

# Check health
./build.sh health

# Update dependencies
# Edit requirements.txt and rebuild
```

## 📞 Support

### Service URLs
- **Web Application**: http://localhost:8000
- **Admin Panel**: http://localhost:8000/admin
- **API Documentation**: http://localhost:8000/api/docs
- **Health Check**: http://localhost:8000/health/

### Default Credentials
- **Django Admin**: admin / admin123
- **Flower**: admin / (see FLOWER_PASSWORD in .env)
- **Grafana**: admin / (see GRAFANA_PASSWORD in .env)

### Useful Commands
```bash
# Shell access
docker-compose exec web bash

# Database shell
docker-compose exec db psql -U postgres -d dbchat

# Redis shell
docker-compose exec redis redis-cli

# Django shell
docker-compose exec web python manage.py shell
```

---

## 🎉 Congratulations!

You now have a complete, production-ready Django DBChat application running in Docker with:

✅ **Full Django application** with all dependencies  
✅ **PostgreSQL database** with monitoring  
✅ **Redis caching** and session management  
✅ **Celery background tasks**  
✅ **Nginx reverse proxy** with SSL  
✅ **Comprehensive monitoring** stack  
✅ **Automated backups** and health checks  
✅ **Security best practices** implemented  
✅ **Scalable architecture** ready for production  

**Happy deploying!** 🚀

---

*For issues or questions, please refer to the logs and troubleshooting section above.* 