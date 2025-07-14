# PostgreSQL-compatible migration for ETL operation fields

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def add_column_if_not_exists_postgresql(apps, schema_editor):
    """
    Add columns only if they don't already exist - PostgreSQL compatible version
    """
    with schema_editor.connection.cursor() as cursor:
        # Clean up any malformed data first (PostgreSQL compatible)
        try:
            cursor.execute("DELETE FROM etl_operations WHERE data_lineage IS NOT NULL AND data_lineage !~ '^\\{.*\\}$';")
            cursor.execute("DELETE FROM etl_operations WHERE result_summary IS NOT NULL AND result_summary !~ '^\\{.*\\}$';")
            cursor.execute("DELETE FROM data_integration_jobs WHERE result_summary IS NOT NULL AND result_summary !~ '^\\{.*\\}$';")
        except:
            # If tables don't exist yet, ignore
            pass
        
        # Check and add columns for etl_operations table (PostgreSQL compatible)
        try:
            # Check if retry_count exists (PostgreSQL way)
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='etl_operations' AND column_name='retry_count';
            """)
            
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE etl_operations ADD COLUMN retry_count INTEGER DEFAULT 0;")
            
            # Check parent_operation_id
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='etl_operations' AND column_name='parent_operation_id';
            """)
            
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE etl_operations ADD COLUMN parent_operation_id VARCHAR(32);")
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS datasets_etloperation_parent_operation_id_idx 
                    ON etl_operations(parent_operation_id);
                """)
            
            # Check data_lineage
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='etl_operations' AND column_name='data_lineage';
            """)
            
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE etl_operations ADD COLUMN data_lineage TEXT;")
            
            # Check result_summary
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='etl_operations' AND column_name='result_summary';
            """)
            
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE etl_operations ADD COLUMN result_summary TEXT;")
                
        except Exception as e:
            print(f"Error handling etl_operations: {e}")
        
        # Check and add columns for data_integration_jobs table
        try:
            # Check priority
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='data_integration_jobs' AND column_name='priority';
            """)
            
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE data_integration_jobs ADD COLUMN priority INTEGER DEFAULT 0;")
            
            # Check scheduled_time
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='data_integration_jobs' AND column_name='scheduled_time';
            """)
            
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE data_integration_jobs ADD COLUMN scheduled_time TIMESTAMP;")
            
            # Check retry_count
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='data_integration_jobs' AND column_name='retry_count';
            """)
            
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE data_integration_jobs ADD COLUMN retry_count INTEGER DEFAULT 0;")
            
            # Check max_retries
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='data_integration_jobs' AND column_name='max_retries';
            """)
            
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE data_integration_jobs ADD COLUMN max_retries INTEGER DEFAULT 3;")
                
        except Exception as e:
            print(f"Error handling data_integration_jobs: {e}")


def reverse_add_columns_postgresql(apps, schema_editor):
    """Reverse the column additions - PostgreSQL compatible"""
    with schema_editor.connection.cursor() as cursor:
        try:
            # Remove columns from etl_operations (PostgreSQL syntax)
            cursor.execute("ALTER TABLE etl_operations DROP COLUMN IF EXISTS retry_count;")
            cursor.execute("ALTER TABLE etl_operations DROP COLUMN IF EXISTS parent_operation_id;")
            cursor.execute("ALTER TABLE etl_operations DROP COLUMN IF EXISTS data_lineage;")
            cursor.execute("ALTER TABLE etl_operations DROP COLUMN IF EXISTS result_summary;")
            
            # Remove columns from data_integration_jobs
            cursor.execute("ALTER TABLE data_integration_jobs DROP COLUMN IF EXISTS priority;")
            cursor.execute("ALTER TABLE data_integration_jobs DROP COLUMN IF EXISTS scheduled_time;")
            cursor.execute("ALTER TABLE data_integration_jobs DROP COLUMN IF EXISTS retry_count;")
            cursor.execute("ALTER TABLE data_integration_jobs DROP COLUMN IF EXISTS max_retries;")
        except Exception as e:
            print(f"Error in reverse migration: {e}")


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('datasets', '0003_add_pandas_type_field'),
    ]

    operations = [
        migrations.RunPython(
            code=add_column_if_not_exists_postgresql,
            reverse_code=reverse_add_columns_postgresql,
            atomic=False,
        ),
    ] 