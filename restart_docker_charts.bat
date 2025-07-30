@echo off
echo 🚀 Restarting Docker containers for chart fixes...
echo =====================================================

echo 📋 Stopping containers...
docker-compose down

echo 🔧 Rebuilding web container (this may take a few minutes)...
docker-compose build web

echo 🎯 Starting all containers...
docker-compose up -d

echo ⏳ Waiting for services to be ready...
timeout /t 10 /nobreak >nul

echo 🧪 Testing web service...
curl -s -o nul -w "%%{http_code}" http://localhost:8000 > temp_status.txt
set /p STATUS=<temp_status.txt
del temp_status.txt

if "%STATUS%"=="200" (
    echo ✅ Web service is running successfully!
    echo 🌐 Access your dashboard at: http://localhost:8000/dashboards/
) else (
    echo ❌ Web service may not be ready yet. Status: %STATUS%
    echo 📝 Run: docker-compose logs web
)

echo.
echo 🎉 Docker restart completed!
echo 📋 Manual verification steps:
echo    1. Open: http://localhost:8000/dashboards/
echo    2. Check that charts render (with sample data)
echo    3. Verify no JavaScript errors in browser console

pause
