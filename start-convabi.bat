@echo off
echo ==================================================
echo  🚀 Starting ConvaBI Business Intelligence Platform
echo ==================================================

echo.
echo [1/4] Starting database and cache services...
docker-compose up -d postgres redis

echo.
echo [2/4] Waiting for services to be ready...
timeout /t 15 /nobreak

echo.
echo [3/4] Starting ConvaBI application...
docker-compose up -d web

echo.
echo [4/4] Starting Django server directly...
timeout /t 10 /nobreak
docker-compose exec -d web bash -c "cd /app/django_dbchat && python manage.py runserver 0.0.0.0:8000"

echo.
echo ==================================================
echo  🎉 ConvaBI is now running!
echo ==================================================
echo.
echo 📊 Application URL: http://localhost:8000
echo 🔧 Admin Panel: http://localhost:8000/admin
echo 📊 PostgreSQL: localhost:5432  
echo 💾 Redis: localhost:6379
echo.
echo ⚡ To view logs: docker-compose logs web
echo 🛑 To stop: docker-compose down
echo.
echo Give the application 30 seconds to fully start...
timeout /t 30 /nobreak
echo Testing application...
powershell -Command "try { $r = Invoke-WebRequest -Uri 'http://localhost:8000/' -UseBasicParsing -TimeoutSec 5; Write-Host '✅ ConvaBI is responding! Status:' $r.StatusCode } catch { Write-Host '⏳ ConvaBI is still starting up. Please try http://localhost:8000 in your browser.' }"
echo.
pause 