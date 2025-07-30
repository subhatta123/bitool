#!/bin/bash

# ConvaBI Docker Deployment Script
# Comprehensive deployment for single VM

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
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

# Configuration
PROJECT_NAME="ConvaBI"
COMPOSE_FILE="docker-compose.yml"
ENV_FILE=".env"
ENV_TEMPLATE="docker.env.template"
BACKUP_DIR="./backups/$(date +%Y%m%d_%H%M%S)"

# Default values
ENVIRONMENT="production"
PULL_IMAGES=true
BUILD_IMAGES=true
RUN_MIGRATIONS=true
CREATE_SUPERUSER=true

# Help function
show_help() {
    cat << EOF
ConvaBI Docker Deployment Script

Usage: $0 [OPTIONS] [COMMAND]

Commands:
    deploy          Deploy the complete application (default)
    start           Start existing containers
    stop            Stop running containers
    restart         Restart all containers
    rebuild         Rebuild and restart containers
    logs            Show container logs
    status          Show container status
    backup          Create backup of data and database
    restore         Restore from backup
    cleanup         Clean up unused Docker resources
    health          Check application health

Options:
    -e, --env ENV           Environment (production/development) [default: production]
    --no-pull              Don't pull latest images
    --no-build             Don't build images
    --no-migrations        Don't run database migrations
    --no-superuser         Don't create superuser
    -f, --file FILE        Docker compose file [default: docker-compose.yml]
    -h, --help             Show this help message

Examples:
    $0 deploy              # Full deployment
    $0 start               # Start containers
    $0 logs web            # Show web container logs
    $0 backup              # Create backup
    $0 --env development   # Deploy in development mode

EOF
    exit 0
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            --no-pull)
                PULL_IMAGES=false
                shift
                ;;
            --no-build)
                BUILD_IMAGES=false
                shift
                ;;
            --no-migrations)
                RUN_MIGRATIONS=false
                shift
                ;;
            --no-superuser)
                CREATE_SUPERUSER=false
                shift
                ;;
            -f|--file)
                COMPOSE_FILE="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                ;;
            deploy|start|stop|restart|rebuild|logs|status|backup|restore|cleanup|health)
                COMMAND="$1"
                shift
                break
                ;;
            *)
                error "Unknown option: $1"
                show_help
                ;;
        esac
    done
    
    # Default command
    COMMAND=${COMMAND:-deploy}
}

# Check prerequisites
check_prerequisites() {
    info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    log "Prerequisites check passed"
}

# Setup environment file
setup_environment() {
    info "Setting up environment configuration..."
    
    if [[ ! -f "$ENV_FILE" ]]; then
        if [[ -f "$ENV_TEMPLATE" ]]; then
            warn "Environment file not found. Creating from template..."
            cp "$ENV_TEMPLATE" "$ENV_FILE"
            
            # Generate random secret key
            SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(50))" 2>/dev/null || openssl rand -base64 50 | tr -d '\n')
            sed -i "s/your-secret-key-change-this-in-production-make-it-very-long-and-random/$SECRET_KEY/" "$ENV_FILE"
            
            warn "Please edit $ENV_FILE with your specific configuration before continuing."
            warn "Pay special attention to:"
            warn "  - SECRET_KEY (already generated)"
            warn "  - ALLOWED_HOSTS (add your domain)"
            warn "  - Database credentials"
            warn "  - Email configuration"
            warn "  - LLM API keys"
            
            read -p "Press Enter after you've configured $ENV_FILE..."
        else
            error "Environment template file not found: $ENV_TEMPLATE"
            exit 1
        fi
    else
        log "Environment file found: $ENV_FILE"
    fi
    
    # Set environment-specific overrides
    if [[ "$ENVIRONMENT" == "development" ]]; then
        export DEBUG=True
        export USE_REDIS=False
        export CELERY_TASK_ALWAYS_EAGER=True
    fi
}

# Create necessary directories
create_directories() {
    info "Creating necessary directories..."
    
    mkdir -p logs data media backups ssl docker
    chmod 755 logs data media backups ssl docker
    
    log "Directories created successfully"
}

# Pull Docker images
pull_images() {
    if [[ "$PULL_IMAGES" == true ]]; then
        info "Pulling latest Docker images..."
        docker-compose -f "$COMPOSE_FILE" pull
        log "Images pulled successfully"
    fi
}

# Build Docker images
build_images() {
    if [[ "$BUILD_IMAGES" == true ]]; then
        info "Building Docker images..."
        docker-compose -f "$COMPOSE_FILE" build --no-cache
        log "Images built successfully"
    fi
}

# Run database migrations
run_migrations() {
    if [[ "$RUN_MIGRATIONS" == true ]]; then
        info "Running database migrations..."
        docker-compose -f "$COMPOSE_FILE" run --rm web python manage.py migrate
        log "Migrations completed successfully"
    fi
}

# Create superuser
create_superuser() {
    if [[ "$CREATE_SUPERUSER" == true ]]; then
        info "Creating superuser..."
        docker-compose -f "$COMPOSE_FILE" run --rm -e CREATE_SUPERUSER=true web python manage.py shell -c "
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser('admin', 'admin@convabi.local', 'admin123')
    print('Superuser created: admin/admin123')
else:
    print('Superuser already exists')
"
        log "Superuser setup completed"
    fi
}

# Deploy function
deploy() {
    log "Starting $PROJECT_NAME deployment..."
    
    check_prerequisites
    setup_environment
    create_directories
    pull_images
    build_images
    
    info "Starting services..."
    docker-compose -f "$COMPOSE_FILE" up -d
    
    info "Waiting for services to be ready..."
    sleep 30
    
    run_migrations
    create_superuser
    
    info "Collecting static files..."
    docker-compose -f "$COMPOSE_FILE" exec web python manage.py collectstatic --noinput || warn "Static files collection failed"
    
    log "$PROJECT_NAME deployment completed successfully!"
    
    show_access_info
}

# Start containers
start() {
    info "Starting $PROJECT_NAME containers..."
    docker-compose -f "$COMPOSE_FILE" up -d
    log "Containers started successfully"
    show_access_info
}

# Stop containers
stop() {
    info "Stopping $PROJECT_NAME containers..."
    docker-compose -f "$COMPOSE_FILE" down
    log "Containers stopped successfully"
}

# Restart containers
restart() {
    info "Restarting $PROJECT_NAME containers..."
    docker-compose -f "$COMPOSE_FILE" restart
    log "Containers restarted successfully"
    show_access_info
}

# Rebuild and restart
rebuild() {
    info "Rebuilding and restarting $PROJECT_NAME..."
    docker-compose -f "$COMPOSE_FILE" down
    docker-compose -f "$COMPOSE_FILE" build --no-cache
    docker-compose -f "$COMPOSE_FILE" up -d
    log "Rebuild completed successfully"
    show_access_info
}

# Show logs
show_logs() {
    local service=${1:-}
    if [[ -n "$service" ]]; then
        docker-compose -f "$COMPOSE_FILE" logs -f "$service"
    else
        docker-compose -f "$COMPOSE_FILE" logs -f
    fi
}

# Show status
show_status() {
    info "$PROJECT_NAME container status:"
    docker-compose -f "$COMPOSE_FILE" ps
    
    info "Docker system status:"
    docker system df
}

# Create backup
create_backup() {
    info "Creating backup..."
    
    mkdir -p "$BACKUP_DIR"
    
    # Backup database
    if docker-compose -f "$COMPOSE_FILE" ps postgres | grep -q "Up"; then
        info "Backing up PostgreSQL database..."
        docker-compose -f "$COMPOSE_FILE" exec postgres pg_dump -U convabiuser convabi > "$BACKUP_DIR/database.sql"
    fi
    
    # Backup data directories
    info "Backing up data directories..."
    cp -r data "$BACKUP_DIR/" 2>/dev/null || true
    cp -r media "$BACKUP_DIR/" 2>/dev/null || true
    cp -r logs "$BACKUP_DIR/" 2>/dev/null || true
    
    # Backup configuration
    cp "$ENV_FILE" "$BACKUP_DIR/" 2>/dev/null || true
    cp "$COMPOSE_FILE" "$BACKUP_DIR/" 2>/dev/null || true
    
    log "Backup created: $BACKUP_DIR"
}

# Health check
health_check() {
    info "Performing health check..."
    
    # Check container status
    if ! docker-compose -f "$COMPOSE_FILE" ps | grep -q "Up"; then
        error "Some containers are not running"
        docker-compose -f "$COMPOSE_FILE" ps
        return 1
    fi
    
    # Check web service
    if curl -f http://localhost:8000/health/ &>/dev/null; then
        log "Web service is healthy"
    else
        error "Web service health check failed"
        return 1
    fi
    
    log "Health check passed"
}

# Cleanup unused resources
cleanup() {
    info "Cleaning up unused Docker resources..."
    
    docker system prune -f
    docker volume prune -f
    docker network prune -f
    
    log "Cleanup completed"
}

# Show access information
show_access_info() {
    log "=== $PROJECT_NAME Access Information ==="
    echo -e "${GREEN}Application URL: ${BLUE}http://localhost:8000${NC}"
    echo -e "${GREEN}Admin Panel: ${BLUE}http://localhost:8000/admin${NC}"
    echo -e "${GREEN}Default Admin: ${BLUE}admin / admin123${NC}"
    echo -e "${GREEN}API Documentation: ${BLUE}http://localhost:8000/api/docs/${NC}"
    echo -e "${GREEN}Health Check: ${BLUE}http://localhost:8000/health/${NC}"
    echo ""
    echo -e "${YELLOW}To view logs: ${NC}./deploy.sh logs [service_name]"
    echo -e "${YELLOW}To stop: ${NC}./deploy.sh stop"
    echo -e "${YELLOW}To check status: ${NC}./deploy.sh status"
}

# Main execution
main() {
    parse_args "$@"
    
    case "$COMMAND" in
        deploy)
            deploy
            ;;
        start)
            start
            ;;
        stop)
            stop
            ;;
        restart)
            restart
            ;;
        rebuild)
            rebuild
            ;;
        logs)
            show_logs "$1"
            ;;
        status)
            show_status
            ;;
        backup)
            create_backup
            ;;
        health)
            health_check
            ;;
        cleanup)
            cleanup
            ;;
        *)
            error "Unknown command: $COMMAND"
            show_help
            ;;
    esac
}

# Run main function
main "$@" 