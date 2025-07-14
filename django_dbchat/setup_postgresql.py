#!/usr/bin/env python
"""
PostgreSQL Setup Script for ConvaBI
This script helps set up PostgreSQL as the database backend for ConvaBI.
"""

import os
import sys
import subprocess
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def check_postgresql_installed():
    """Check if PostgreSQL is installed and accessible"""
    try:
        result = subprocess.run(['psql', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✓ PostgreSQL found: {result.stdout.strip()}")
            return True
        else:
            print("✗ PostgreSQL not found in PATH")
            return False
    except FileNotFoundError:
        print("✗ PostgreSQL not installed or not in PATH")
        return False

def create_database(db_name, db_user, db_password, db_host='localhost', db_port='5432'):
    """Create PostgreSQL database and user"""
    try:
        # Connect to PostgreSQL as superuser (usually postgres)
        print(f"Creating database '{db_name}' and user '{db_user}'...")
        
        # You may need to run this as postgres user or provide superuser credentials
        admin_conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user='postgres',  # Default superuser
            password=input("Enter PostgreSQL superuser (postgres) password: ")
        )
        admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = admin_conn.cursor()
        
        # Create user if not exists
        cursor.execute(f"""
            DO $$ 
            BEGIN
                IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{db_user}') THEN
                    CREATE USER {db_user} WITH PASSWORD '{db_password}';
                    ALTER USER {db_user} CREATEDB;
                END IF;
            END
            $$;
        """)
        
        # Create database if not exists
        cursor.execute(f"""
            SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'
        """)
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {db_name} OWNER {db_user}")
            print(f"✓ Database '{db_name}' created successfully")
        else:
            print(f"✓ Database '{db_name}' already exists")
        
        # Grant privileges
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user}")
        
        cursor.close()
        admin_conn.close()
        
        return True
        
    except psycopg2.Error as e:
        print(f"✗ Error creating database: {e}")
        return False
    except KeyboardInterrupt:
        print("\n✗ Setup cancelled by user")
        return False

def test_connection(db_name, db_user, db_password, db_host='localhost', db_port='5432'):
    """Test connection to the created database"""
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        conn.close()
        print(f"✓ Successfully connected to database '{db_name}'")
        return True
    except psycopg2.Error as e:
        print(f"✗ Connection test failed: {e}")
        return False

def create_env_file(db_name, db_user, db_password, db_host='localhost', db_port='5432'):
    """Create or update .env file with PostgreSQL configuration"""
    env_content = f"""# PostgreSQL Configuration for ConvaBI
USE_SQLITE=False
DATABASE_NAME={db_name}
DATABASE_USER={db_user}
DATABASE_PASSWORD={db_password}
DATABASE_HOST={db_host}
DATABASE_PORT={db_port}

# Optional: Enable Redis for production
USE_REDIS=False
REDIS_URL=redis://localhost:6379

# Security
SECRET_KEY=django-insecure-change-me-in-production-{os.urandom(10).hex()}
DEBUG=True

# Email Configuration (optional)
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_USE_TLS=True
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password

# LLM Configuration
OPENAI_API_KEY=your-openai-api-key-here
LLM_PROVIDER=openai
OLLAMA_URL=http://localhost:11434
"""
    
    try:
        with open('.env', 'w') as f:
            f.write(env_content)
        print("✓ Created .env file with PostgreSQL configuration")
        return True
    except Exception as e:
        print(f"✗ Error creating .env file: {e}")
        return False

def run_django_migrations():
    """Run Django migrations"""
    try:
        print("Running Django migrations...")
        result = subprocess.run([sys.executable, 'manage.py', 'migrate'], check=True)
        print("✓ Django migrations completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Django migrations failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🐘 PostgreSQL Setup for ConvaBI")
    print("=" * 40)
    
    # Check if PostgreSQL is installed
    if not check_postgresql_installed():
        print("\nPlease install PostgreSQL first:")
        print("- Windows: Download from https://www.postgresql.org/download/windows/")
        print("- macOS: brew install postgresql")
        print("- Ubuntu: sudo apt-get install postgresql postgresql-contrib")
        return False
    
    # Get database configuration
    print("\nEnter PostgreSQL configuration:")
    db_name = input("Database name [dbchat]: ").strip() or 'dbchat'
    db_user = input("Database user [dbchat_user]: ").strip() or 'dbchat_user'
    db_password = input("Database password: ").strip()
    
    if not db_password:
        print("✗ Password is required")
        return False
    
    db_host = input("Database host [localhost]: ").strip() or 'localhost'
    db_port = input("Database port [5432]: ").strip() or '5432'
    
    # Create database and user
    if not create_database(db_name, db_user, db_password, db_host, db_port):
        return False
    
    # Test connection
    if not test_connection(db_name, db_user, db_password, db_host, db_port):
        return False
    
    # Create .env file
    if not create_env_file(db_name, db_user, db_password, db_host, db_port):
        return False
    
    # Run migrations
    if not run_django_migrations():
        return False
    
    print("\n🎉 PostgreSQL setup completed successfully!")
    print("\nNext steps:")
    print("1. Update your .env file with actual API keys and email settings")
    print("2. Run: python manage.py createsuperuser")
    print("3. Run: python manage.py runserver")
    print("\nThe application will now use PostgreSQL instead of SQLite!")
    
    return True

if __name__ == "__main__":
    main() 