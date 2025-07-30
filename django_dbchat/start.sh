#!/bin/bash

# ConvaBI Startup Script
# This script starts Django without Redis dependency for development

echo "🚀 Starting ConvaBI Development Server"
echo "========================================"

# Check if Python virtual environment is activated
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✅ Virtual environment detected: $VIRTUAL_ENV"
else
    echo "⚠️  No virtual environment detected. Consider using 'python -m venv venv' and 'source venv/bin/activate'"
fi

# Check if we're in the right directory
if [ ! -f "manage.py" ]; then
    echo "❌ Error: manage.py not found. Please run this from django_dbchat directory"
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file for development..."
    cat > .env << EOF
# Development Configuration
DEBUG=True
USE_REDIS=False
SECRET_KEY=development-key-$(openssl rand -hex 16)

# Database
USE_SQLITE=True

# LLM Configuration (add your keys if needed)
# OPENAI_API_KEY=your_key_here
# OLLAMA_URL=http://localhost:11434

# Email (optional)
# EMAIL_HOST_USER=your_email@gmail.com
# EMAIL_HOST_PASSWORD=your_app_password
EOF
    echo "✅ Created .env file with development settings"
fi

# Check for required directories
echo "📁 Creating required directories..."
mkdir -p data logs media staticfiles
chmod 755 data logs media staticfiles

# Check for SQLite database
if [ ! -f "db.sqlite3" ]; then
    echo "🔧 Running initial migrations..."
    python manage.py migrate
    
    echo "👤 Creating superuser..."
    echo "from django.contrib.auth import get_user_model; User = get_user_model(); User.objects.create_superuser('admin', 'admin@example.com', 'admin123') if not User.objects.filter(username='admin').exists() else None" | python manage.py shell
    echo "✅ Created admin user: username=admin, password=admin123"
fi

# Start Django development server
echo ""
echo "🌐 Starting Django Development Server..."
echo "   - Web Interface: http://localhost:8000"
echo "   - Admin Panel: http://localhost:8000/admin (admin/admin123)"
echo ""
echo "💡 ETL Scheduling is configured to work WITHOUT Redis in development mode"
echo "   - Tasks will execute synchronously (immediate execution)"
echo "   - Perfect for testing and development"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Set environment for development
export USE_REDIS=False
export DEBUG=True

# Start Django development server
python manage.py runserver 0.0.0.0:8000 