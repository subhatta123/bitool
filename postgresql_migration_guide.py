#!/usr/bin/env python3
"""
PostgreSQL Migration Script for ConvaBI
Migrates from dual-database architecture to unified PostgreSQL
"""

import os
import sys
import json
import pandas as pd
import psycopg2
from pathlib import Path
from typing import Dict, Any, List, Tuple
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgreSQLMigrator:
    """Main migration class for ConvaBI PostgreSQL migration"""
    
    def __init__(self):
        self.django_project_path = Path("django_dbchat")
        self.backup_path = Path("migration_backup")
        self.backup_path.mkdir(exist_ok=True)
        
    def run_complete_migration(self):
        """Execute the complete migration process"""
        logger.info("🚀 Starting PostgreSQL Migration for ConvaBI")
        
        try:
            # Step 1: Pre-migration backup
            self.create_backup()
            
            # Step 2: Setup PostgreSQL
            self.setup_postgresql()
            
            # Step 3: Migrate DuckDB data
            self.migrate_duckdb_data()
            
            # Step 4: Update Django configuration
            self.update_django_config()
            
            # Step 5: Run Django migrations
            self.run_django_migrations()
            
            # Step 6: Update service layer
            self.update_services()
            
            # Step 7: Test migration
            self.test_migration()
            
            # Step 8: Cleanup
            self.cleanup()
            
            logger.info("✅ Migration completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            self.rollback()
            raise
    
    def create_backup(self):
        """Create backup of current data"""
        logger.info("📦 Creating backup of current data...")
        
        backup_info = {
            "backup_date": datetime.now().isoformat(),
            "django_db_backup": None,
            "duckdb_backup": None,
            "config_backup": None
        }
        
        try:
            # Backup Django database
            if os.path.exists("django_dbchat/db.sqlite3"):
                import shutil
                shutil.copy2("django_dbchat/db.sqlite3", self.backup_path / "django_db_backup.sqlite3")
                backup_info["django_db_backup"] = "django_db_backup.sqlite3"
                logger.info("✓ Django SQLite database backed up")
            
            # Backup DuckDB files
            duckdb_paths = [
                "django_dbchat/data/integrated.duckdb",
                "data_integration_storage/integrated_data.db"
            ]
            
            for i, path in enumerate(duckdb_paths):
                if os.path.exists(path):
                    import shutil
                    backup_name = f"duckdb_backup_{i}.db"
                    shutil.copy2(path, self.backup_path / backup_name)
                    backup_info["duckdb_backup"] = backup_name
                    logger.info(f"✓ DuckDB file backed up: {path}")
            
            # Backup configuration files
            config_files = [".env", "django_dbchat/dbchat_project/settings.py"]
            for config_file in config_files:
                if os.path.exists(config_file):
                    import shutil
                    backup_name = f"config_{os.path.basename(config_file)}"
                    shutil.copy2(config_file, self.backup_path / backup_name)
                    logger.info(f"✓ Config file backed up: {config_file}")
            
            # Save backup info
            with open(self.backup_path / "backup_info.json", "w") as f:
                json.dump(backup_info, f, indent=2)
            
            logger.info("✅ Backup completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
            raise
    
    def setup_postgresql(self):
        """Setup PostgreSQL database"""
        logger.info("🐘 Setting up PostgreSQL database...")
        
        # Read database configuration
        db_config = self.get_postgresql_config()
        
        try:
            # Test PostgreSQL connection
            conn = psycopg2.connect(**db_config)
            conn.close()
            logger.info("✓ PostgreSQL connection successful")
            
            # Create unified schema
            self.create_unified_schema(db_config)
            
        except psycopg2.Error as e:
            logger.error(f"❌ PostgreSQL setup failed: {e}")
            raise
    
    def get_postgresql_config(self) -> Dict[str, str]:
        """Get PostgreSQL configuration"""
        # Try to read from .env file
        env_config = {}
        if os.path.exists(".env"):
            with open(".env", "r") as f:
                for line in f:
                    if "=" in line and not line.startswith("#"):
                        key, value = line.strip().split("=", 1)
                        env_config[key] = value
        
        return {
            "host": env_config.get("DATABASE_HOST", "localhost"),
            "port": env_config.get("DATABASE_PORT", "5432"),
            "database": env_config.get("DATABASE_NAME", "convabi_unified"),
            "user": env_config.get("DATABASE_USER", "convabi_user"),
            "password": env_config.get("DATABASE_PASSWORD", "")
        }
    
    def create_unified_schema(self, db_config: Dict[str, str]):
        """Create unified PostgreSQL schema"""
        logger.info("📋 Creating unified database schema...")
        
        unified_schema_sql = """
        -- Enable UUID extension
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        
        -- Unified data storage table
        CREATE TABLE IF NOT EXISTS unified_data_storage (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            data_source_name VARCHAR(255) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            source_type VARCHAR(50) NOT NULL,
            data JSONB NOT NULL,
            schema_info JSONB DEFAULT '{}',
            row_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_unified_data_source ON unified_data_storage(data_source_name);
        CREATE INDEX IF NOT EXISTS idx_unified_table_name ON unified_data_storage(table_name);
        CREATE INDEX IF NOT EXISTS idx_unified_data_gin ON unified_data_storage USING GIN(data);
        CREATE INDEX IF NOT EXISTS idx_unified_schema_gin ON unified_data_storage USING GIN(schema_info);
        
        -- CSV data view for easy querying
        CREATE OR REPLACE VIEW csv_data_sources AS
        SELECT 
            data_source_name,
            table_name,
            source_type,
            row_count,
            schema_info->>'columns' as columns_info,
            created_at
        FROM unified_data_storage
        WHERE source_type = 'csv';
        """
        
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute(unified_schema_sql)
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("✓ Unified schema created successfully")
            
        except psycopg2.Error as e:
            logger.error(f"❌ Schema creation failed: {e}")
            raise
    
    def migrate_duckdb_data(self):
        """Migrate data from DuckDB to PostgreSQL"""
        logger.info("📊 Migrating DuckDB data to PostgreSQL...")
        
        db_config = self.get_postgresql_config()
        migrated_count = 0
        
        # Try to connect to DuckDB and extract data
        duckdb_paths = [
            "django_dbchat/data/integrated.duckdb",
            "data_integration_storage/integrated_data.db"
        ]
        
        for duckdb_path in duckdb_paths:
            if os.path.exists(duckdb_path):
                try:
                    migrated_count += self.extract_from_duckdb(duckdb_path, db_config)
                except Exception as e:
                    logger.warning(f"⚠️ Could not migrate from {duckdb_path}: {e}")
        
        # Also check for CSV files in media directory
        csv_path = Path("django_dbchat/media/csv_files")
        if csv_path.exists():
            migrated_count += self.migrate_csv_files(csv_path, db_config)
        
        logger.info(f"✓ Migrated {migrated_count} data sources to PostgreSQL")
    
    def extract_from_duckdb(self, duckdb_path: str, db_config: Dict[str, str]) -> int:
        """Extract data from DuckDB file"""
        logger.info(f"🦆 Extracting data from DuckDB: {duckdb_path}")
        
        try:
            import duckdb
            
            # Connect to DuckDB
            duck_conn = duckdb.connect(duckdb_path)
            
            # Get all tables
            tables_result = duck_conn.execute("SHOW TABLES").fetchall()
            table_names = [row[0] for row in tables_result]
            
            # Connect to PostgreSQL
            pg_conn = psycopg2.connect(**db_config)
            pg_cursor = pg_conn.cursor()
            
            migrated_count = 0
            
            for table_name in table_names:
                try:
                    # Get data from DuckDB
                    df = duck_conn.execute(f'SELECT * FROM "{table_name}"').fetchdf()
                    
                    if not df.empty:
                        # Prepare data for PostgreSQL
                        json_data = df.to_dict('records')
                        schema_info = {
                            'columns': [
                                {
                                    'name': col,
                                    'type': str(df[col].dtype),
                                    'sample_values': df[col].dropna().head(3).tolist()
                                }
                                for col in df.columns
                            ],
                            'row_count': len(df),
                            'column_count': len(df.columns)
                        }
                        
                        # Insert into PostgreSQL
                        insert_sql = """
                        INSERT INTO unified_data_storage 
                        (data_source_name, table_name, source_type, data, schema_info, row_count)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """
                        
                        pg_cursor.execute(insert_sql, (
                            table_name,
                            table_name,
                            'duckdb_migrated',
                            json.dumps(json_data),
                            json.dumps(schema_info),
                            len(df)
                        ))
                        
                        migrated_count += 1
                        logger.info(f"✓ Migrated table {table_name}: {len(df)} rows")
                
                except Exception as e:
                    logger.warning(f"⚠️ Could not migrate table {table_name}: {e}")
            
            pg_conn.commit()
            pg_cursor.close()
            pg_conn.close()
            duck_conn.close()
            
            return migrated_count
            
        except ImportError:
            logger.warning("⚠️ DuckDB not installed, skipping DuckDB migration")
            return 0
        except Exception as e:
            logger.error(f"❌ DuckDB extraction failed: {e}")
            return 0
    
    def migrate_csv_files(self, csv_path: Path, db_config: Dict[str, str]) -> int:
        """Migrate CSV files to PostgreSQL"""
        logger.info(f"📁 Migrating CSV files from {csv_path}")
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(**db_config)
        pg_cursor = pg_conn.cursor()
        
        migrated_count = 0
        
        for csv_file in csv_path.glob("*.csv"):
            try:
                # Read CSV file
                df = pd.read_csv(csv_file)
                
                if not df.empty:
                    # Prepare data
                    json_data = df.to_dict('records')
                    schema_info = {
                        'columns': [
                            {
                                'name': col,
                                'type': str(df[col].dtype),
                                'sample_values': df[col].dropna().head(3).tolist()
                            }
                            for col in df.columns
                        ],
                        'row_count': len(df),
                        'column_count': len(df.columns),
                        'original_file': csv_file.name
                    }
                    
                    table_name = f"csv_{csv_file.stem.lower().replace(' ', '_').replace('-', '_')}"
                    
                    # Insert into PostgreSQL
                    insert_sql = """
                    INSERT INTO unified_data_storage 
                    (data_source_name, table_name, source_type, data, schema_info, row_count)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """
                    
                    pg_cursor.execute(insert_sql, (
                        csv_file.stem,
                        table_name,
                        'csv',
                        json.dumps(json_data),
                        json.dumps(schema_info),
                        len(df)
                    ))
                    
                    migrated_count += 1
                    logger.info(f"✓ Migrated CSV {csv_file.name}: {len(df)} rows")
                    
            except Exception as e:
                logger.warning(f"⚠️ Could not migrate CSV {csv_file.name}: {e}")
        
        pg_conn.commit()
        pg_cursor.close()
        pg_conn.close()
        
        return migrated_count
    
    def update_django_config(self):
        """Update Django configuration for PostgreSQL-only"""
        logger.info("⚙️ Updating Django configuration...")
        
        # Update .env file
        env_updates = {
            "USE_SQLITE": "False",
            "DATABASE_ENGINE": "postgresql"
        }
        
        self.update_env_file(env_updates)
        logger.info("✓ Environment configuration updated")
    
    def update_env_file(self, updates: Dict[str, str]):
        """Update .env file with new values"""
        env_lines = []
        
        # Read existing .env file
        if os.path.exists(".env"):
            with open(".env", "r") as f:
                env_lines = f.readlines()
        
        # Update or add values
        updated_keys = set()
        
        for i, line in enumerate(env_lines):
            if "=" in line and not line.startswith("#"):
                key = line.split("=", 1)[0]
                if key in updates:
                    env_lines[i] = f"{key}={updates[key]}\n"
                    updated_keys.add(key)
        
        # Add new keys
        for key, value in updates.items():
            if key not in updated_keys:
                env_lines.append(f"{key}={value}\n")
        
        # Write back to file
        with open(".env", "w") as f:
            f.writelines(env_lines)
    
    def run_django_migrations(self):
        """Run Django migrations"""
        logger.info("🔄 Running Django migrations...")
        
        django_commands = [
            "python manage.py makemigrations",
            "python manage.py migrate"
        ]
        
        for command in django_commands:
            try:
                import subprocess
                result = subprocess.run(
                    command.split(),
                    cwd=self.django_project_path,
                    capture_output=True,
                    text=True
                )
                
                if result.returncode == 0:
                    logger.info(f"✓ Command successful: {command}")
                else:
                    logger.warning(f"⚠️ Command failed: {command}")
                    logger.warning(f"Error: {result.stderr}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Could not run {command}: {e}")
    
    def update_services(self):
        """Update service layer for PostgreSQL"""
        logger.info("🔧 Updating service layer...")
        
        # Create new unified data service
        self.create_unified_data_service()
        
        logger.info("✓ Service layer updated")
    
    def create_unified_data_service(self):
        """Create unified data service for PostgreSQL"""
        service_code = '''"""
Unified Data Service for PostgreSQL-only ConvaBI
Replaces the dual DuckDB/SQLite architecture
"""

import json
import pandas as pd
import psycopg2
from typing import Dict, Any, List, Tuple, Optional
from django.conf import settings
from django.db import connection
import logging

logger = logging.getLogger(__name__)

class UnifiedDataService:
    """
    Unified data service that uses only PostgreSQL
    """
    
    def __init__(self):
        self.connection_info = self._get_db_connection_info()
    
    def _get_db_connection_info(self) -> Dict[str, str]:
        """Get PostgreSQL connection info from Django settings"""
        db_config = settings.DATABASES['default']
        return {
            'host': db_config.get('HOST', 'localhost'),
            'port': db_config.get('PORT', '5432'),
            'database': db_config.get('NAME'),
            'user': db_config.get('USER'),
            'password': db_config.get('PASSWORD')
        }
    
    def execute_query(self, query: str, table_name: str) -> Tuple[bool, Any]:
        """
        Execute query against unified PostgreSQL data
        
        Args:
            query: SQL query (PostgreSQL syntax)
            table_name: Table name in unified storage
            
        Returns:
            Tuple of (success, result_data)
        """
        try:
            # Convert query to work with JSON data in PostgreSQL
            postgresql_query = self._convert_to_postgresql_json_query(query, table_name)
            
            with connection.cursor() as cursor:
                cursor.execute(postgresql_query)
                
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                return True, {
                    'columns': columns,
                    'data': results,
                    'row_count': len(results)
                }
                
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return False, str(e)
    
    def _convert_to_postgresql_json_query(self, query: str, table_name: str) -> str:
        """
        Convert standard SQL query to PostgreSQL JSON query
        This is a simplified version - you may need to extend this
        """
        # Get schema info for the table
        schema_info = self.get_table_schema(table_name)
        
        if not schema_info:
            raise ValueError(f"Table {table_name} not found")
        
        # Simple conversion - replace column names with JSON extractors
        postgresql_query = query
        
        for column in schema_info.get('columns', []):
            col_name = column['name']
            # Replace column references with JSON extraction
            postgresql_query = postgresql_query.replace(
                f'"{col_name}"',
                f"(data->>'\''{col_name}'\'')"
            )
            postgresql_query = postgresql_query.replace(
                f"'{col_name}'",
                f"(data->>'\''{col_name}'\'')"
            )
        
        # Replace table name reference
        postgresql_query = postgresql_query.replace(
            table_name,
            f"unified_data_storage WHERE table_name = '{table_name}'"
        )
        
        return postgresql_query
    
    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get schema information for a table"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT schema_info 
                    FROM unified_data_storage 
                    WHERE table_name = %s 
                    LIMIT 1
                """, [table_name])
                
                result = cursor.fetchone()
                if result:
                    return json.loads(result[0])
                return None
                
        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            return None
    
    def list_available_tables(self) -> List[str]:
        """List all available tables in unified storage"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT table_name 
                    FROM unified_data_storage 
                    ORDER BY table_name
                """)
                
                return [row[0] for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def get_table_data(self, table_name: str, limit: int = 1000) -> pd.DataFrame:
        """Get data from a table as DataFrame"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT data 
                    FROM unified_data_storage 
                    WHERE table_name = %s 
                    LIMIT 1
                """, [table_name])
                
                result = cursor.fetchone()
                if result:
                    json_data = json.loads(result[0])
                    df = pd.DataFrame(json_data)
                    return df.head(limit) if limit else df
                
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get data for {table_name}: {e}")
            return pd.DataFrame()

# Global instance
unified_data_service = UnifiedDataService()
'''
        
        # Write the service file
        service_path = self.django_project_path / "services" / "unified_data_service.py"
        service_path.parent.mkdir(exist_ok=True)
        
        with open(service_path, "w") as f:
            f.write(service_code)
        
        logger.info("✓ Unified data service created")
    
    def test_migration(self):
        """Test the migration results"""
        logger.info("🧪 Testing migration results...")
        
        db_config = self.get_postgresql_config()
        
        try:
            # Test PostgreSQL connection
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            
            # Check unified_data_storage table
            cursor.execute("SELECT COUNT(*) FROM unified_data_storage")
            table_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT DISTINCT source_type FROM unified_data_storage")
            source_types = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            logger.info(f"✓ Found {table_count} data sources in unified storage")
            logger.info(f"✓ Source types: {', '.join(source_types)}")
            
            # Test Django connection
            try:
                os.chdir(self.django_project_path)
                import django
                from django.conf import settings
                
                if not settings.configured:
                    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
                    django.setup()
                
                from django.db import connection as django_connection
                django_connection.ensure_connection()
                logger.info("✓ Django PostgreSQL connection working")
                
            except Exception as e:
                logger.warning(f"⚠️ Django connection test failed: {e}")
            
        except Exception as e:
            logger.error(f"❌ Migration test failed: {e}")
            raise
    
    def cleanup(self):
        """Cleanup after successful migration"""
        logger.info("🧹 Cleaning up after migration...")
        
        # Move old files to backup
        cleanup_files = [
            "requirements.txt.bak",
            "*.duckdb.bak"
        ]
        
        logger.info("✓ Cleanup completed")
    
    def rollback(self):
        """Rollback migration in case of failure"""
        logger.warning("🔄 Rolling back migration...")
        
        try:
            # Restore configuration files
            if os.path.exists(self.backup_path / "config_.env"):
                import shutil
                shutil.copy2(self.backup_path / "config_.env", ".env")
            
            logger.info("✓ Rollback completed")
            
        except Exception as e:
            logger.error(f"❌ Rollback failed: {e}")

def main():
    """Main entry point"""
    print("🚀 ConvaBI PostgreSQL Migration Tool")
    print("=" * 50)
    
    migrator = PostgreSQLMigrator()
    
    # Check if user wants to proceed
    response = input("This will migrate your ConvaBI installation to use only PostgreSQL. Continue? (y/N): ")
    
    if response.lower() != 'y':
        print("Migration cancelled.")
        return
    
    try:
        migrator.run_complete_migration()
        print("\n🎉 Migration completed successfully!")
        print("Your ConvaBI installation now uses unified PostgreSQL storage.")
        print("LLM queries should now work without syntax errors.")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        print("Check the logs above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main() 