@echo off
echo.
echo 🚀 Starting ConvaBI Development Server
echo ========================================

REM Check if we're in the right directory
if not exist "manage.py" (
    echo ❌ Error: manage.py not found. Please run this from django_dbchat directory
    pause
    exit /b 1
)

REM Check if .env file exists
if not exist ".env" (
    echo 📝 Creating .env file for development...
    (
        echo # Development Configuration
        echo DEBUG=True
        echo USE_REDIS=False
        echo SECRET_KEY=development-key-changeme
        echo.
        echo # Database
        echo USE_SQLITE=True
        echo.
        echo # LLM Configuration ^(add your keys if needed^)
        echo # OPENAI_API_KEY=your_key_here
        echo # OLLAMA_URL=http://localhost:11434
        echo.
        echo # Email ^(optional^)
        echo # EMAIL_HOST_USER=your_email@gmail.com
        echo # EMAIL_HOST_PASSWORD=your_app_password
    ) > .env
    echo ✅ Created .env file with development settings
)

REM Check for required directories
echo 📁 Creating required directories...
if not exist "data" mkdir data
if not exist "logs" mkdir logs
if not exist "media" mkdir media
if not exist "staticfiles" mkdir staticfiles

REM Check for SQLite database
if not exist "db.sqlite3" (
    echo 🔧 Running initial migrations...
    python manage.py migrate
    
    echo 👤 Creating superuser...
    echo from django.contrib.auth import get_user_model; User = get_user_model(); User.objects.create_superuser('admin', 'admin@example.com', 'admin123') if not User.objects.filter(username='admin').exists() else None | python manage.py shell
    echo ✅ Created admin user: username=admin, password=admin123
)

REM Start Django development server
echo.
echo 🌐 Starting Django Development Server...
echo    - Web Interface: http://localhost:8000
echo    - Admin Panel: http://localhost:8000/admin (admin/admin123)
echo.
echo 💡 ETL Scheduling is configured to work WITHOUT Redis in development mode
echo    - Tasks will execute synchronously (immediate execution)
echo    - Perfect for testing and development
echo.
echo Press Ctrl+C to stop the server
echo.

REM Set environment for development
set USE_REDIS=False
set DEBUG=True

REM Start Django development server
python manage.py runserver 0.0.0.0:8000 