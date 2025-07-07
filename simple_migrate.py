#!/usr/bin/env python3
"""
Simple Database Migration Script for Semantic Layer
A standalone script to create semantic layer tables
"""

import psycopg2
import os
import sys

def get_connection_from_env():
    """Get database connection from environment variables or user input"""
    host = os.environ.get('POSTGRES_HOST') or input("PostgreSQL Host (default: localhost): ") or "localhost"
    port = os.environ.get('POSTGRES_PORT') or input("PostgreSQL Port (default: 5432): ") or "5432"
    dbname = os.environ.get('POSTGRES_DBNAME') or input("Database Name: ")
    user = os.environ.get('POSTGRES_USER') or input("Username: ")
    password = os.environ.get('POSTGRES_PASSWORD') or input("Password: ")
    
    if not dbname or not user or not password:
        print("❌ Database name, username, and password are required")
        return None
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=int(port),
            dbname=dbname,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def create_semantic_tables(conn):
    """Create semantic layer tables"""
    cursor = conn.cursor()
    
    print("📊 Creating semantic_tables table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS semantic_tables (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) UNIQUE NOT NULL,
            display_name VARCHAR(255) NOT NULL,
            description TEXT,
            business_purpose TEXT,
            common_queries JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    print("📝 Creating semantic_columns table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS semantic_columns (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            column_name VARCHAR(255) NOT NULL,
            display_name VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
            semantic_type VARCHAR(50) NOT NULL,
            data_type VARCHAR(100),
            sample_values JSONB,
            common_filters JSONB,
            business_rules JSONB,
            aggregation_default VARCHAR(50),
            is_nullable BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(table_name, column_name)
        )
    """)
    
    print("🔗 Creating semantic_relationships table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS semantic_relationships (
            id SERIAL PRIMARY KEY,
            from_table VARCHAR(255) NOT NULL,
            from_column VARCHAR(255) NOT NULL,
            to_table VARCHAR(255) NOT NULL,
            to_column VARCHAR(255) NOT NULL,
            relationship_type VARCHAR(50) NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    print("📊 Creating semantic_metrics table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS semantic_metrics (
            id SERIAL PRIMARY KEY,
            metric_name VARCHAR(255) UNIQUE NOT NULL,
            display_name VARCHAR(255) NOT NULL,
            description TEXT,
            formula TEXT NOT NULL,
            category VARCHAR(100),
            tables_involved JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    print("📚 Creating semantic_glossary table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS semantic_glossary (
            id SERIAL PRIMARY KEY,
            term VARCHAR(255) UNIQUE NOT NULL,
            definition TEXT NOT NULL,
            category VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    print("⚡ Creating indexes...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_columns_table ON semantic_columns(table_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_relationships_from ON semantic_relationships(from_table, from_column)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_relationships_to ON semantic_relationships(to_table, to_column)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_metrics_category ON semantic_metrics(category)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_glossary_category ON semantic_glossary(category)")
    
    conn.commit()
    print("✅ All semantic layer tables created successfully!")

def main():
    """Main migration function"""
    print("🚀 ConvaBI Semantic Layer Simple Migration")
    print("=" * 50)
    
    # Get database connection
    conn = get_connection_from_env()
    if not conn:
        sys.exit(1)
    
    try:
        create_semantic_tables(conn)
        print("=" * 50)
        print("🎉 Migration completed successfully!")
        print("")
        print("Next steps:")
        print("1. Start your ConvaBI application")
        print("2. Navigate to the 'Semantic Layer' page")
        print("3. Click 'Auto-Generate Semantic Metadata'")
        print("4. Configure and enhance as needed")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main() 