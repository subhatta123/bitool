# ConvaBI Docker Operations Makefile
# Simplifies common Docker operations for development and deployment

.PHONY: help build deploy start stop restart logs status clean backup health test

# Default target
.DEFAULT_GOAL := help

# Variables
COMPOSE_FILE = docker-compose.yml
ENV_FILE = .env
PROJECT_NAME = convabi

# Colors for output
BLUE = \033[36m
GREEN = \033[32m
YELLOW = \033[33m
RED = \033[31m
NC = \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)ConvaBI Docker Operations$(NC)"
	@echo "$(BLUE)=========================$(NC)"
	@echo ""
	@echo "$(GREEN)Available commands:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(BLUE)%-15s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Examples:$(NC)"
	@echo "  make setup     # Initial setup and deployment"
	@echo "  make logs      # View all container logs"
	@echo "  make logs web  # View web container logs only"
	@echo "  make backup    # Create backup"
	@echo "  make clean     # Clean up Docker resources"

setup: check-env build deploy ## Initial setup and deployment
	@echo "$(GREEN)✅ ConvaBI setup completed successfully!$(NC)"
	@echo "$(BLUE)Access your application at: http://localhost:8000$(NC)"
	@echo "$(BLUE)Default admin credentials: admin / admin123$(NC)"

check-env: ## Check if environment file exists
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "$(YELLOW)⚠️  Environment file not found. Creating from template...$(NC)"; \
		cp docker.env.template $(ENV_FILE); \
		echo "$(RED)⚠️  IMPORTANT: Please edit $(ENV_FILE) with your configuration!$(NC)"; \
		echo "$(YELLOW)Press Enter after configuring $(ENV_FILE)...$(NC)"; \
		read; \
	fi

build: ## Build Docker images
	@echo "$(BLUE)🔨 Building Docker images...$(NC)"
	docker-compose -f $(COMPOSE_FILE) build

build-no-cache: ## Build Docker images without cache
	@echo "$(BLUE)🔨 Building Docker images (no cache)...$(NC)"
	docker-compose -f $(COMPOSE_FILE) build --no-cache

deploy: ## Deploy all services
	@echo "$(BLUE)🚀 Deploying ConvaBI...$(NC)"
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "$(BLUE)⏳ Waiting for services to start...$(NC)"
	@sleep 30
	@make migrate
	@make collect-static
	@make create-superuser
	@echo "$(GREEN)✅ Deployment completed!$(NC)"

start: ## Start all services
	@echo "$(BLUE)▶️  Starting services...$(NC)"
	docker-compose -f $(COMPOSE_FILE) up -d

stop: ## Stop all services
	@echo "$(BLUE)⏹️  Stopping services...$(NC)"
	docker-compose -f $(COMPOSE_FILE) down

restart: ## Restart all services
	@echo "$(BLUE)🔄 Restarting services...$(NC)"
	docker-compose -f $(COMPOSE_FILE) restart

restart-web: ## Restart web service only
	@echo "$(BLUE)🔄 Restarting web service...$(NC)"
	docker-compose -f $(COMPOSE_FILE) restart web

logs: ## View logs (specify service name as argument: make logs web)
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" ]; then \
		docker-compose -f $(COMPOSE_FILE) logs -f $(filter-out $@,$(MAKECMDGOALS)); \
	else \
		docker-compose -f $(COMPOSE_FILE) logs -f; \
	fi

status: ## Show service status
	@echo "$(BLUE)📊 Service Status:$(NC)"
	@docker-compose -f $(COMPOSE_FILE) ps
	@echo ""
	@echo "$(BLUE)💾 Disk Usage:$(NC)"
	@docker system df

health: ## Check application health
	@echo "$(BLUE)🏥 Health Check:$(NC)"
	@curl -f http://localhost:8000/health/ 2>/dev/null && echo "$(GREEN)✅ Application is healthy$(NC)" || echo "$(RED)❌ Application is unhealthy$(NC)"

migrate: ## Run database migrations
	@echo "$(BLUE)🗄️  Running database migrations...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec web python manage.py migrate

makemigrations: ## Create new migrations
	@echo "$(BLUE)🗄️  Creating migrations...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec web python manage.py makemigrations

collect-static: ## Collect static files
	@echo "$(BLUE)📦 Collecting static files...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec web python manage.py collectstatic --noinput

create-superuser: ## Create Django superuser (if not exists)
	@echo "$(BLUE)👤 Creating superuser...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) exec web python manage.py shell -c "from django.contrib.auth import get_user_model; User = get_user_model(); User.objects.filter(username='admin').exists() or User.objects.create_superuser('admin', 'admin@convabi.local', 'admin123')" 2>/dev/null || echo "$(YELLOW)Superuser may already exist$(NC)"

shell: ## Open Django shell
	@echo "$(BLUE)🐚 Opening Django shell...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec web python manage.py shell

dbshell: ## Open database shell
	@echo "$(BLUE)🗄️  Opening database shell...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec postgres psql -U convabiuser -d convabi

redis-cli: ## Open Redis CLI
	@echo "$(BLUE)🔴 Opening Redis CLI...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec redis redis-cli

backup: ## Create backup
	@echo "$(BLUE)💾 Creating backup...$(NC)"
	@BACKUP_DIR=./backups/$(shell date +%Y%m%d_%H%M%S) && \
	mkdir -p $$BACKUP_DIR && \
	docker-compose -f $(COMPOSE_FILE) exec postgres pg_dump -U convabiuser convabi > $$BACKUP_DIR/database.sql && \
	cp -r data $$BACKUP_DIR/ 2>/dev/null || true && \
	cp -r media $$BACKUP_DIR/ 2>/dev/null || true && \
	cp -r logs $$BACKUP_DIR/ 2>/dev/null || true && \
	cp $(ENV_FILE) $$BACKUP_DIR/ 2>/dev/null || true && \
	cp $(COMPOSE_FILE) $$BACKUP_DIR/ 2>/dev/null || true && \
	echo "$(GREEN)✅ Backup created: $$BACKUP_DIR$(NC)"

restore-db: ## Restore database from backup file (usage: make restore-db BACKUP_FILE=path/to/backup.sql)
	@if [ -z "$(BACKUP_FILE)" ]; then \
		echo "$(RED)❌ Please specify BACKUP_FILE: make restore-db BACKUP_FILE=backup.sql$(NC)"; \
		exit 1; \
	fi
	@echo "$(BLUE)🔄 Restoring database from $(BACKUP_FILE)...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec -T postgres psql -U convabiuser convabi < $(BACKUP_FILE)
	@echo "$(GREEN)✅ Database restored$(NC)"

clean: ## Clean up Docker resources
	@echo "$(BLUE)🧹 Cleaning up Docker resources...$(NC)"
	docker system prune -f
	docker volume prune -f
	docker network prune -f
	@echo "$(GREEN)✅ Cleanup completed$(NC)"

clean-all: ## Clean up all Docker resources including images
	@echo "$(BLUE)🧹 Cleaning up all Docker resources...$(NC)"
	docker-compose -f $(COMPOSE_FILE) down -v
	docker system prune -af
	docker volume prune -f
	docker network prune -f
	@echo "$(GREEN)✅ Complete cleanup finished$(NC)"

test: ## Run application tests
	@echo "$(BLUE)🧪 Running tests...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec web python manage.py test

lint: ## Run code linting
	@echo "$(BLUE)🔍 Running linting...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec web flake8 . || echo "$(YELLOW)Flake8 not installed or configured$(NC)"

dev: ## Start in development mode
	@echo "$(BLUE)🔧 Starting in development mode...$(NC)"
	@export DEBUG=True && \
	export USE_REDIS=False && \
	export CELERY_TASK_ALWAYS_EAGER=True && \
	docker-compose -f $(COMPOSE_FILE) up -d

prod: ## Switch to production mode
	@echo "$(BLUE)🚀 Switching to production mode...$(NC)"
	@export DEBUG=False && \
	export USE_REDIS=True && \
	export CELERY_TASK_ALWAYS_EAGER=False && \
	docker-compose -f $(COMPOSE_FILE) up -d

update: ## Update application (pull, rebuild, restart)
	@echo "$(BLUE)🔄 Updating ConvaBI...$(NC)"
	git pull origin main
	@make build-no-cache
	@make stop
	@make start
	@make migrate
	@make collect-static
	@echo "$(GREEN)✅ Update completed$(NC)"

monitor: ## Monitor container resources
	@echo "$(BLUE)📊 Monitoring container resources...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop monitoring$(NC)"
	docker stats

env-check: ## Check environment configuration
	@echo "$(BLUE)🔍 Environment Configuration Check:$(NC)"
	@echo "$(BLUE)Docker version:$(NC) $(shell docker --version)"
	@echo "$(BLUE)Docker Compose version:$(NC) $(shell docker-compose --version)"
	@echo "$(BLUE)Environment file:$(NC) $(if $(wildcard $(ENV_FILE)),✅ Found,❌ Missing)"
	@echo "$(BLUE)Compose file:$(NC) $(if $(wildcard $(COMPOSE_FILE)),✅ Found,❌ Missing)"

quick-start: ## Quick start for development
	@echo "$(BLUE)⚡ Quick start for development...$(NC)"
	@make check-env
	@make build
	@make dev
	@echo "$(GREEN)✅ Development environment ready!$(NC)"
	@echo "$(BLUE)Access: http://localhost:8000$(NC)"

# Security commands
security-scan: ## Basic security scan
	@echo "$(BLUE)🔒 Running basic security scan...$(NC)"
	@echo "$(BLUE)Checking for default passwords...$(NC)"
	@grep -n "admin123\|password\|secret" $(ENV_FILE) || echo "$(GREEN)No obvious default passwords found$(NC)"

# Performance optimization
optimize: ## Run performance optimizations
	@echo "$(BLUE)⚡ Running performance optimizations...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec postgres psql -U convabiuser -d convabi -c "VACUUM ANALYZE;" 2>/dev/null || echo "$(YELLOW)Database optimization skipped$(NC)"
	@make clean

# Development helpers
reset-db: ## Reset database (WARNING: destroys all data)
	@echo "$(RED)⚠️  WARNING: This will destroy all database data!$(NC)"
	@echo "$(YELLOW)Type 'yes' to continue:$(NC)"
	@read confirm && [ "$$confirm" = "yes" ] || (echo "$(BLUE)Cancelled$(NC)"; exit 1)
	docker-compose -f $(COMPOSE_FILE) down -v
	docker-compose -f $(COMPOSE_FILE) up -d postgres redis
	@sleep 10
	docker-compose -f $(COMPOSE_FILE) up -d web
	@sleep 20
	@make migrate
	@make create-superuser
	@echo "$(GREEN)✅ Database reset completed$(NC)"

install-deps: ## Install additional dependencies
	@echo "$(BLUE)📦 Installing additional dependencies...$(NC)"
	docker-compose -f $(COMPOSE_FILE) exec web pip install -r requirements.txt

# Catch-all target for service names in logs command
%:
	@:

# Help for specific commands
help-deploy: ## Detailed help for deployment
	@echo "$(BLUE)Deployment Help:$(NC)"
	@echo "  1. $(YELLOW)make check-env$(NC) - Ensure .env file exists and is configured"
	@echo "  2. $(YELLOW)make build$(NC) - Build Docker images"
	@echo "  3. $(YELLOW)make deploy$(NC) - Deploy all services"
	@echo "  4. $(YELLOW)make health$(NC) - Check if deployment succeeded"

help-dev: ## Detailed help for development
	@echo "$(BLUE)Development Help:$(NC)"
	@echo "  1. $(YELLOW)make quick-start$(NC) - Quick development setup"
	@echo "  2. $(YELLOW)make logs web$(NC) - View web service logs"
	@echo "  3. $(YELLOW)make shell$(NC) - Open Django shell"
	@echo "  4. $(YELLOW)make test$(NC) - Run tests" 