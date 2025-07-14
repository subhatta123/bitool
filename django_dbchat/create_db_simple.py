#!/usr/bin/env python
"""
Create PostgreSQL database and user for ConvaBI - Simple Version
Usage: python create_db_simple.py your_postgres_password
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

def create_database(postgres_password):
    """Create PostgreSQL database and user for ConvaBI"""
    
    print("🐘 Creating PostgreSQL Database for ConvaBI")
    print("=" * 45)
    
    try:
        # Connect to PostgreSQL as superuser
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host='localhost',
            port='5432',
            user='postgres',
            password=postgres_password,
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Database and user details
        db_name = 'dbchat'
        db_user = 'dbchat_user'
        db_password = 'dbchat_secure_password_2024'
        
        print(f"Creating database '{db_name}' and user '{db_user}'...")
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()
        
        if exists:
            print(f"✓ Database '{db_name}' already exists")
        else:
            cursor.execute(f'CREATE DATABASE "{db_name}"')
            print(f"✓ Database '{db_name}' created successfully")
        
        # Check if user exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s", (db_user,))
        user_exists = cursor.fetchone()
        
        if user_exists:
            print(f"✓ User '{db_user}' already exists")
            cursor.execute(f'ALTER USER "{db_user}" WITH PASSWORD %s', (db_password,))
            print(f"✓ Updated password for user '{db_user}'")
        else:
            cursor.execute(f'CREATE USER "{db_user}" WITH PASSWORD %s', (db_password,))
            print(f"✓ User '{db_user}' created successfully")
        
        # Grant privileges
        cursor.execute(f'ALTER USER "{db_user}" CREATEDB')
        cursor.execute(f'GRANT ALL PRIVILEGES ON DATABASE "{db_name}" TO "{db_user}"')
        print(f"✓ Granted all privileges on '{db_name}' to '{db_user}'")
        
        cursor.close()
        conn.close()
        
        # Test connection
        print("\nTesting connection with new user...")
        test_conn = psycopg2.connect(
            host='localhost',
            port='5432',
            database=db_name,
            user=db_user,
            password=db_password
        )
        test_conn.close()
        print("✓ Connection test successful!")
        
        print(f"\n🎉 Database setup completed!")
        print(f"Database: {db_name}")
        print(f"User: {db_user}")
        print(f"Password: {db_password}")
        
        return True
        
    except psycopg2.Error as e:
        print(f"✗ PostgreSQL error: {e}")
        print("\nIf you get 'database does not exist' error, that's normal.")
        print("The important thing is that we can connect to PostgreSQL.")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python create_db_simple.py <postgres_password>")
        print("Example: python create_db_simple.py mypassword")
        sys.exit(1)
    
    postgres_password = sys.argv[1]
    create_database(postgres_password) 