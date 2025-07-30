@echo off
echo ==================================================
echo  ConvaBI Docker Deployment for Windows
echo ==================================================

echo.
echo [1/6] Stopping any existing containers...
docker-compose down

echo.
echo [2/6] Setting up environment for SQLite...
echo # ConvaBI Simple Configuration > .env.simple
echo COMPOSE_PROJECT_NAME=convabi >> .env.simple
echo DEBUG=True >> .env.simple
echo SECRET_KEY=convabi-docker-secret-key-for-testing-only >> .env.simple
echo ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0 >> .env.simple
echo DATABASE_URL=sqlite:///db.sqlite3 >> .env.simple
echo USE_REDIS=True >> .env.simple
echo REDIS_URL=redis://redis:6379/0 >> .env.simple
echo CELERY_BROKER_URL=redis://redis:6379/1 >> .env.simple
echo CELERY_RESULT_BACKEND=redis://redis:6379/2 >> .env.simple
echo CELERY_TASK_ALWAYS_EAGER=False >> .env.simple
echo TIME_ZONE=UTC >> .env.simple

echo.
echo [3/6] Starting Redis (cache and message broker)...
docker-compose up -d redis

echo.
echo [4/6] Waiting for Redis to be ready...
timeout /t 10 /nobreak

echo.
echo [5/6] Building and starting web application...
set DATABASE_URL=sqlite:///db.sqlite3
docker-compose up -d web

echo.
echo [6/6] Waiting for application to initialize...
timeout /t 30 /nobreak

echo.
echo ==================================================
echo  Checking Application Status
echo ==================================================
docker-compose ps
echo.

echo Testing application...
timeout /t 5 /nobreak
curl http://localhost:8000/health/ 2>nul || echo Health check failed - app may still be starting

echo.
echo ==================================================
echo  ConvaBI Deployment Complete!
echo ==================================================
echo.
echo ^> Application URL: http://localhost:8000
echo ^> Admin Panel: http://localhost:8000/admin  
echo ^> Default Admin: admin / admin123
echo.
echo ^> To view logs: docker-compose logs web
echo ^> To stop: docker-compose down
echo.
echo If the application isn't responding, check logs:
echo    docker-compose logs web
echo.
pause 