#!/usr/bin/env python3
"""
Database Migration Script for Semantic Layer
Run this script to create the semantic layer tables in your PostgreSQL database
"""

import os
import sys

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import database
    from semantic_layer import SemanticLayer
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please ensure you're running this script from the ConvaBI application directory")
    sys.exit(1)

def create_semantic_layer_tables():
    """Create semantic layer tables in the database"""
    print("🧠 Creating Semantic Layer Database Tables...")
    
    try:
        # Get database connection
        conn = database.get_db_connection()
        if not conn:
            print("❌ Failed to connect to database. Please check your database configuration.")
            return False
        
        cursor = conn.cursor()
        
        # Create semantic layer tables
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
        
        # Create indexes for better performance
        print("⚡ Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_columns_table ON semantic_columns(table_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_relationships_from ON semantic_relationships(from_table, from_column)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_relationships_to ON semantic_relationships(to_table, to_column)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_metrics_category ON semantic_metrics(category)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_semantic_glossary_category ON semantic_glossary(category)")
        
        # Commit changes
        conn.commit()
        
        print("✅ Semantic layer database tables created successfully!")
        
        # Test the setup by loading the semantic layer
        print("🧪 Testing semantic layer functionality...")
        semantic_layer = SemanticLayer.load_from_database(conn)
        print(f"✅ Semantic layer test successful - loaded {len(semantic_layer.tables)} tables")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating semantic layer tables: {e}")
        try:
            if 'conn' in locals() and conn is not None:
                conn.rollback()  # type: ignore
                conn.close()  # type: ignore
        except Exception:
            pass  # Connection might already be closed or None
        return False

def main():
    """Main migration function"""
    print("🚀 ConvaBI Semantic Layer Database Migration")
    print("=" * 50)
    
    # Check if database is configured
    try:
        conn = database.get_db_connection()
        if not conn:
            print("❌ Database connection failed. Please configure your database first.")
            sys.exit(1)
        conn.close()
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        sys.exit(1)
    
    # Create semantic layer tables
    if create_semantic_layer_tables():
        print("=" * 50)
        print("🎉 Migration completed successfully!")
        print("")
        print("Next steps:")
        print("1. Start your ConvaBI application")
        print("2. Navigate to the 'Semantic Layer' page")
        print("3. Click 'Auto-Generate Semantic Metadata' to analyze your data")
        print("4. Configure and enhance the metadata as needed")
        print("5. Test improved query generation in the 'Ask Questions' page")
        print("")
        print("The semantic layer will now enhance your AI query generation!")
    else:
        print("❌ Migration failed. Please check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    main() 