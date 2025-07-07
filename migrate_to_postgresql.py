#!/usr/bin/env python3
"""
PostgreSQL Migration Script for ConvaBI
Migrates from dual-database architecture to unified PostgreSQL
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def backup_current_data():
    """Create backup of current system"""
    logger.info(" Creating backup of current data...")
    
    backup_dir = Path("migration_backup_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
    backup_dir.mkdir(exist_ok=True)
    
    # Backup files to copy
    files_to_backup = [
        "django_dbchat/db.sqlite3",
        "django_dbchat/data/integrated.duckdb", 
        "data_integration_storage/integrated_data.db",
        ".env",
        "django_dbchat/dbchat_project/settings.py"
    ]
    
    import shutil
    backed_up = []
    
    for file_path in files_to_backup:
        if os.path.exists(file_path):
            try:
                backup_name = f"{backup_dir}/{os.path.basename(file_path)}.backup"
                shutil.copy2(file_path, backup_name)
                backed_up.append(file_path)
                logger.info(f" Backed up: {file_path}")
            except Exception as e:
                logger.warning(f" Could not backup {file_path}: {e}")
    
    # Save backup info
    backup_info = {
        "backup_date": datetime.now().isoformat(),
        "backed_up_files": backed_up,
        "backup_directory": str(backup_dir)
    }
    
    with open(backup_dir / "backup_info.json", "w") as f:
        json.dump(backup_info, f, indent=2)
    
    logger.info(f" Backup completed in: {backup_dir}")
    return backup_dir

def setup_postgresql():
    """Setup PostgreSQL database"""
    logger.info(" Setting up PostgreSQL database...")
    
    # Check if PostgreSQL setup script exists
    setup_script = Path("django_dbchat/setup_postgresql.py")
    
    if setup_script.exists():
        logger.info("Running PostgreSQL setup script...")
        import subprocess
        try:
            result = subprocess.run([sys.executable, str(setup_script)], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(" PostgreSQL setup completed")
            else:
                logger.error(f" PostgreSQL setup failed: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f" Failed to run PostgreSQL setup: {e}")
            return False
    else:
        logger.warning(" PostgreSQL setup script not found. Please configure PostgreSQL manually.")
    
    return True

def migrate_duckdb_data():
    """Migrate data from DuckDB to PostgreSQL"""
    logger.info(" Migrating DuckDB data to PostgreSQL...")
    
    duckdb_files = [
        "django_dbchat/data/integrated.duckdb",
        "data_integration_storage/integrated_data.db"
    ]
    
    migrated_tables = []
    
    for duckdb_file in duckdb_files:
        if os.path.exists(duckdb_file):
            try:
                tables = extract_duckdb_tables(duckdb_file)
                migrated_tables.extend(tables)
                logger.info(f" Migrated {len(tables)} tables from {duckdb_file}")
            except Exception as e:
                logger.warning(f" Could not migrate {duckdb_file}: {e}")
    
    logger.info(f" Total migrated tables: {len(migrated_tables)}")
    return migrated_tables

def extract_duckdb_tables(duckdb_file):
    """Extract tables from DuckDB file"""
    try:
        import duckdb
        
        conn = duckdb.connect(duckdb_file)
        
        # Get table list
        tables_result = conn.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables_result]
        
        extracted_tables = []
        
        for table_name in table_names:
            try:
                # Extract data
                df = conn.execute(f'SELECT * FROM "{table_name}"').fetchdf()
                
                if not df.empty:
                    # Save as JSON for PostgreSQL import
                    json_file = f"migration_data_{table_name}.json"
                    
                    table_data = {
                        "table_name": table_name,
                        "data": df.to_dict('records'),
                        "schema": {
                            "columns": [{"name": col, "type": str(df[col].dtype)} 
                                      for col in df.columns],
                            "row_count": len(df)
                        }
                    }
                    
                    with open(json_file, 'w') as f:
                        json.dump(table_data, f, indent=2)
                    
                    extracted_tables.append(table_name)
                    logger.info(f" Extracted {table_name}: {len(df)} rows")
                    
            except Exception as e:
                logger.warning(f" Could not extract table {table_name}: {e}")
        
        conn.close()
        return extracted_tables
        
    except ImportError:
        logger.warning(" DuckDB not installed, skipping DuckDB migration")
        return []
    except Exception as e:
        logger.error(f" DuckDB extraction failed: {e}")
        return []

def migrate_csv_files():
    """Migrate CSV files to PostgreSQL format"""
    logger.info(" Migrating CSV files...")
    
    csv_dirs = [
        "django_dbchat/media/csv_files",
        "media/csv_files"
    ]
    
    migrated_files = []
    
    for csv_dir in csv_dirs:
        csv_path = Path(csv_dir)
        if csv_path.exists():
            for csv_file in csv_path.glob("*.csv"):
                try:
                    df = pd.read_csv(csv_file)
                    
                    if not df.empty:
                        # Prepare for PostgreSQL
                        table_name = f"csv_{csv_file.stem.lower().replace(' ', '_').replace('-', '_')}"
                        
                        csv_data = {
                            "table_name": table_name,
                            "source_file": csv_file.name,
                            "data": df.to_dict('records'),
                            "schema": {
                                "columns": [{"name": col, "type": str(df[col].dtype)} 
                                          for col in df.columns],
                                "row_count": len(df)
                            }
                        }
                        
                        json_file = f"migration_csv_{table_name}.json"
                        with open(json_file, 'w') as f:
                            json.dump(csv_data, f, indent=2)
                        
                        migrated_files.append(csv_file.name)
                        logger.info(f" Prepared CSV {csv_file.name}: {len(df)} rows")
                        
                except Exception as e:
                    logger.warning(f" Could not process CSV {csv_file.name}: {e}")
    
    logger.info(f" Prepared {len(migrated_files)} CSV files for migration")
    return migrated_files

def create_postgresql_import_script():
    """Create script to import data into PostgreSQL"""
    logger.info(" Creating PostgreSQL import script...")
    
    import_script = '''
-- PostgreSQL Data Import Script
-- Generated by ConvaBI Migration Tool

-- Create unified data storage table
CREATE TABLE IF NOT EXISTS unified_data_storage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

-- Create view for CSV data
CREATE OR REPLACE VIEW csv_data_view AS
SELECT 
    data_source_name,
    table_name,
    row_count,
    schema_info,
    created_at
FROM unified_data_storage
WHERE source_type = 'csv';

COMMIT;
'''
    
    with open("postgresql_import_schema.sql", "w") as f:
        f.write(import_script)
    
    logger.info(" PostgreSQL import script created")

def update_django_settings():
    """Update Django settings for PostgreSQL-only"""
    logger.info(" Updating Django settings...")
    
    # Update .env file
    env_updates = {
        "USE_SQLITE": "False",
        "DATABASE_ENGINE": "postgresql",
    }
    
    # Read current .env
    env_content = []
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            env_content = f.readlines()
    
    # Update or add settings
    updated_keys = set()
    for i, line in enumerate(env_content):
        if "=" in line and not line.startswith("#"):
            key = line.split("=", 1)[0]
            if key in env_updates:
                env_content[i] = f"{key}={env_updates[key]}\n"
                updated_keys.add(key)
    
    # Add new keys
    for key, value in env_updates.items():
        if key not in updated_keys:
            env_content.append(f"{key}={value}\n")
    
    # Write back
    with open(".env", "w") as f:
        f.writelines(env_content)
    
    logger.info(" Django settings updated")

def create_migration_summary():
    """Create migration summary report"""
    logger.info(" Creating migration summary...")
    
    # Count migration files
    migration_files = list(Path(".").glob("migration_*.json"))
    
    summary = {
        "migration_date": datetime.now().isoformat(),
        "migrated_files": len(migration_files),
        "migration_files": [f.name for f in migration_files],
        "next_steps": [
            "1. Review postgresql_import_schema.sql",
            "2. Import data into PostgreSQL using Django management commands",
            "3. Run Django migrations: python manage.py migrate", 
            "4. Test LLM queries",
            "5. Remove old DuckDB files after verification"
        ]
    }
    
    with open("migration_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    logger.info(" Migration summary created")
    return summary

def main():
    """Main migration function"""
    print(" ConvaBI PostgreSQL Migration Tool")
    print("=" * 50)
    
    try:
        # Step 1: Backup
        backup_dir = backup_current_data()
        
        # Step 2: Setup PostgreSQL
        if not setup_postgresql():
            raise Exception("PostgreSQL setup failed")
        
        # Step 3: Migrate DuckDB data
        duckdb_tables = migrate_duckdb_data()
        
        # Step 4: Migrate CSV files
        csv_files = migrate_csv_files()
        
        # Step 5: Create import scripts
        create_postgresql_import_script()
        
        # Step 6: Update Django settings
        update_django_settings()
        
        # Step 7: Create summary
        summary = create_migration_summary()
        
        print("\n Migration preparation completed successfully!")
        print(f" Backup created in: {backup_dir}")
        print(f" Prepared {len(duckdb_tables)} DuckDB tables")
        print(f" Prepared {len(csv_files)} CSV files")
        print("\n Next Steps:")
        for step in summary["next_steps"]:
            print(f"   {step}")
        
        print("\n  Important Notes:")
        print("   - Review all migration files before importing")
        print("   - Test thoroughly before removing backup files")
        print("   - LLM queries should work after PostgreSQL migration")
        
    except Exception as e:
        print(f"\n Migration failed: {e}")
        print("Check migration logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
