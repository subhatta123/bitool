"""
Django Management Command for PostgreSQL Migration
Usage: python manage.py migrate_to_postgresql
"""

import os
import json
import pandas as pd
from django.core.management.base import BaseCommand
from django.db import transaction, connection
from django.conf import settings
from datasets.models import DataSource
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Migrate from DuckDB/SQLite to unified PostgreSQL storage'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be migrated without actually doing it',
        )
        parser.add_argument(
            '--backup',
            action='store_true',
            help='Create backup before migration',
        )
    
    def handle(self, *args, **options):
        self.stdout.write('🚀 Starting PostgreSQL Migration for ConvaBI')
        self.stdout.write('=' * 50)
        
        if options['backup']:
            self.create_backup()
        
        if options['dry_run']:
            self.stdout.write('🔍 DRY RUN MODE - No changes will be made')
            self.analyze_migration()
        else:
            self.execute_migration()
    
    def create_backup(self):
        """Create backup before migration"""
        self.stdout.write('📦 Creating backup...')
        
        from datetime import datetime
        backup_dir = f"migration_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(backup_dir, exist_ok=True)
        
        # Export current data
        from django.core.management import call_command
        backup_file = f"{backup_dir}/django_data_backup.json"
        
        with open(backup_file, 'w') as f:
            call_command('dumpdata', stdout=f)
        
        self.stdout.write(f'✓ Backup created in {backup_dir}')
    
    def analyze_migration(self):
        """Analyze what would be migrated"""
        self.stdout.write('🔍 Analyzing migration requirements...')
        
        # Check DuckDB files
        duckdb_files = self.find_duckdb_files()
        csv_files = self.find_csv_files()
        
        self.stdout.write(f'📊 Found {len(duckdb_files)} DuckDB files')
        self.stdout.write(f'📁 Found {len(csv_files)} CSV files')
        
        # Estimate data volume
        total_rows = 0
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                total_rows += len(df)
                self.stdout.write(f'  • {csv_file}: {len(df)} rows')
            except Exception as e:
                self.stdout.write(f'  ⚠️ {csv_file}: Error reading - {e}')
        
        self.stdout.write(f'📈 Total estimated rows to migrate: {total_rows:,}')
    
    def execute_migration(self):
        """Execute the actual migration"""
        self.stdout.write('🔄 Executing migration...')
        
        try:
            with transaction.atomic():
                # Step 1: Create unified storage schema
                self.create_unified_schema()
                
                # Step 2: Migrate DuckDB data
                duckdb_count = self.migrate_duckdb_data()
                
                # Step 3: Migrate CSV files
                csv_count = self.migrate_csv_files()
                
                # Step 4: Update data source references
                self.update_data_sources()
                
                self.stdout.write('✅ Migration completed successfully!')
                self.stdout.write(f'📊 Migrated {duckdb_count} DuckDB tables')
                self.stdout.write(f'📁 Migrated {csv_count} CSV files')
                
        except Exception as e:
            self.stdout.write(f'❌ Migration failed: {e}')
            raise
    
    def create_unified_schema(self):
        """Create unified data storage schema"""
        self.stdout.write('📋 Creating unified storage schema...')
        
        with connection.cursor() as cursor:
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'unified_data_storage'
                );
            """)
            
            result = cursor.fetchone()
            if not result or not result[0]:
                cursor.execute("""
                    CREATE TABLE unified_data_storage (
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
                    
                    CREATE INDEX idx_unified_data_source ON unified_data_storage(data_source_name);
                    CREATE INDEX idx_unified_table_name ON unified_data_storage(table_name);
                    CREATE INDEX idx_unified_data_gin ON unified_data_storage USING GIN(data);
                """)
                
                self.stdout.write('✓ Unified storage schema created')
            else:
                self.stdout.write('✓ Unified storage schema already exists')
    
    def find_duckdb_files(self):
        """Find DuckDB files in the project"""
        duckdb_paths = [
            'data/integrated.duckdb',
            'django_dbchat/data/integrated.duckdb',
            'data_integration_storage/integrated_data.db'
        ]
        
        return [path for path in duckdb_paths if os.path.exists(path)]
    
    def find_csv_files(self):
        """Find CSV files in media directories"""
        csv_files = []
        
        csv_dirs = [
            'django_dbchat/media/csv_files',
            'media/csv_files'
        ]
        
        for csv_dir in csv_dirs:
            if os.path.exists(csv_dir):
                import glob
                csv_files.extend(glob.glob(f"{csv_dir}/*.csv"))
        
        return csv_files
    
    def migrate_duckdb_data(self):
        """Migrate data from DuckDB files"""
        self.stdout.write('📊 Migrating DuckDB data...')
        
        duckdb_files = self.find_duckdb_files()
        migrated_count = 0
        
        for duckdb_file in duckdb_files:
            try:
                count = self.extract_from_duckdb(duckdb_file)
                migrated_count += count
                self.stdout.write(f'✓ Migrated {count} tables from {duckdb_file}')
            except Exception as e:
                self.stdout.write(f'⚠️ Could not migrate {duckdb_file}: {e}')
        
        return migrated_count
    
    def extract_from_duckdb(self, duckdb_file):
        """Extract data from a DuckDB file"""
        try:
            import duckdb
            
            conn = duckdb.connect(duckdb_file)
            tables = conn.execute("SHOW TABLES").fetchall()
            
            migrated_count = 0
            
            for table_row in tables:
                table_name = table_row[0]
                
                try:
                    # Get data
                    df = conn.execute(f'SELECT * FROM "{table_name}"').fetchdf()
                    
                    if not df.empty:
                        self.store_in_postgresql(table_name, df, 'duckdb_migrated')
                        migrated_count += 1
                        
                except Exception as e:
                    self.stdout.write(f'⚠️ Error migrating table {table_name}: {e}')
            
            conn.close()
            return migrated_count
            
        except ImportError:
            self.stdout.write('⚠️ DuckDB not installed, skipping DuckDB migration')
            return 0
    
    def migrate_csv_files(self):
        """Migrate CSV files to PostgreSQL"""
        self.stdout.write('📁 Migrating CSV files...')
        
        csv_files = self.find_csv_files()
        migrated_count = 0
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                
                if not df.empty:
                    # Create table name from file
                    import os
                    base_name = os.path.splitext(os.path.basename(csv_file))[0]
                    table_name = f"csv_{base_name.lower().replace(' ', '_').replace('-', '_')}"
                    
                    self.store_in_postgresql(table_name, df, 'csv', source_file=csv_file)
                    migrated_count += 1
                    self.stdout.write(f'✓ Migrated {os.path.basename(csv_file)}: {len(df)} rows')
                    
            except Exception as e:
                self.stdout.write(f'⚠️ Error migrating {csv_file}: {e}')
        
        return migrated_count
    
    def store_in_postgresql(self, table_name, df, source_type, source_file=None):
        """Store DataFrame in PostgreSQL unified storage"""
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
            'column_count': len(df.columns)
        }
        
        if source_file:
            schema_info['source_file'] = source_file
        
        # Insert into PostgreSQL
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO unified_data_storage 
                (data_source_name, table_name, source_type, data, schema_info, row_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (table_name) DO UPDATE SET
                    data = EXCLUDED.data,
                    schema_info = EXCLUDED.schema_info,
                    row_count = EXCLUDED.row_count,
                    updated_at = CURRENT_TIMESTAMP
            """, [
                table_name,
                table_name,
                source_type,
                json.dumps(json_data),
                json.dumps(schema_info),
                len(df)
            ])
    
    def update_data_sources(self):
        """Update Django DataSource models to reference unified storage"""
        self.stdout.write('🔄 Updating data source references...')
        
        # This would update existing DataSource models
        # to point to the new unified storage
        updated_count = 0
        
        for data_source in DataSource.objects.all():
            if hasattr(data_source, 'table_name') and data_source.table_name:
                # Update status to indicate migration to unified storage
                data_source.status = 'migrated_to_unified'
                data_source.save()
                updated_count += 1
        
        self.stdout.write(f'✓ Updated {updated_count} data source references') 