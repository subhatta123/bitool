# Generated manually to fix missing tables and constraints
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0015_remove_dataintegrationjob_data_integr_started_6d4ad5_idx_and_more'),
    ]

    operations = [
        # 1. Create missing scheduled_etl_jobs_data_sources table
        migrations.RunSQL(
            sql="""
                CREATE TABLE IF NOT EXISTS scheduled_etl_jobs_data_sources (
                    id SERIAL PRIMARY KEY,
                    scheduledmjob_id bigint NOT NULL,
                    datasource_id uuid NOT NULL,
                    FOREIGN KEY (datasource_id) REFERENCES data_sources(id) ON DELETE CASCADE
                );
            """,
            reverse_sql="DROP TABLE IF EXISTS scheduled_etl_jobs_data_sources;"
        ),

        # 2. Create missing unified_data_storage table
        migrations.RunSQL(
            sql="""
                CREATE TABLE IF NOT EXISTS unified_data_storage (
                    id SERIAL PRIMARY KEY,
                    data_source_name VARCHAR(200),
                    table_name VARCHAR(200),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """,
            reverse_sql="DROP TABLE IF EXISTS unified_data_storage;"
        ),

        # 3. Remove problematic table_id column if it still exists
        migrations.RunSQL(
            sql="""
                DO $$ 
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'semantic_columns' AND column_name = 'table_id'
                    ) THEN
                        ALTER TABLE semantic_columns DROP COLUMN table_id;
                    END IF;
                END $$;
            """,
            reverse_sql="-- Cannot reverse table_id column removal"
        ),

        # 4. Make aggregation_default nullable if it isn't already
        migrations.RunSQL(
            sql="""
                DO $$
                BEGIN
                    ALTER TABLE semantic_columns ALTER COLUMN aggregation_default DROP NOT NULL;
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Column might already be nullable, ignore error
                        NULL;
                END $$;
            """,
            reverse_sql="ALTER TABLE semantic_columns ALTER COLUMN aggregation_default SET NOT NULL;"
        ),

        # 5. Make tags nullable if it isn't already
        migrations.RunSQL(
            sql="""
                DO $$
                BEGIN
                    ALTER TABLE semantic_columns ALTER COLUMN tags DROP NOT NULL;
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Column might already be nullable, ignore error
                        NULL;
                END $$;
            """,
            reverse_sql="ALTER TABLE semantic_columns ALTER COLUMN tags SET NOT NULL;"
        ),

        # 6. Ensure semantic_table_id column exists and is properly configured
        migrations.RunSQL(
            sql="""
                DO $$
                BEGIN
                    -- Add semantic_table_id column if it doesn't exist
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'semantic_columns' AND column_name = 'semantic_table_id'
                    ) THEN
                        ALTER TABLE semantic_columns 
                        ADD COLUMN semantic_table_id bigint 
                        REFERENCES semantic_tables(id) ON DELETE CASCADE;
                    END IF;
                    
                    -- Make sure it's nullable to avoid constraint violations during creation
                    ALTER TABLE semantic_columns ALTER COLUMN semantic_table_id DROP NOT NULL;
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Handle any errors gracefully
                        NULL;
                END $$;
            """,
            reverse_sql="-- Cannot reverse semantic_table_id column addition"
        ),

        # 7. Create indexes for performance
        migrations.RunSQL(
            sql="""
                CREATE INDEX IF NOT EXISTS scheduled_etl_jobs_data_sources_datasource_id_idx 
                ON scheduled_etl_jobs_data_sources(datasource_id);
                
                CREATE INDEX IF NOT EXISTS unified_data_storage_data_source_name_idx 
                ON unified_data_storage(data_source_name);
                
                CREATE INDEX IF NOT EXISTS unified_data_storage_table_name_idx 
                ON unified_data_storage(table_name);
            """,
            reverse_sql="""
                DROP INDEX IF EXISTS scheduled_etl_jobs_data_sources_datasource_id_idx;
                DROP INDEX IF EXISTS unified_data_storage_data_source_name_idx;
                DROP INDEX IF EXISTS unified_data_storage_table_name_idx;
            """
        ),
    ] 