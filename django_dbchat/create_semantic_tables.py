#!/usr/bin/env python
"""
Script to create semantic tables manually with the correct schema.
"""

import os
import sys
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.db import connection


def create_semantic_tables():
    """Create semantic tables with the correct schema"""
    print("=== CREATING SEMANTIC TABLES ===")
    
    with connection.cursor() as cursor:
        
        # Drop existing tables if they exist (to start fresh)
        tables_to_drop = ['semantic_columns', 'semantic_tables', 'semantic_relationships', 'semantic_metrics']
        for table in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"Dropped table: {table}")
            except Exception as e:
                print(f"Could not drop {table}: {e}")
        
        # 1. Create semantic_tables
        cursor.execute("""
            CREATE TABLE semantic_tables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source_id VARCHAR(32) NOT NULL,
                name VARCHAR(200) NOT NULL,
                display_name VARCHAR(200) NOT NULL,
                description TEXT NOT NULL,
                business_purpose TEXT NOT NULL,
                is_fact_table BOOLEAN NOT NULL DEFAULT 0,
                is_dimension_table BOOLEAN NOT NULL DEFAULT 0,
                primary_key VARCHAR(200) NOT NULL DEFAULT '',
                row_count_estimate BIGINT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                data_classification VARCHAR(50) NOT NULL DEFAULT '',
                business_owner VARCHAR(200) NOT NULL DEFAULT '',
                refresh_frequency VARCHAR(50) NOT NULL DEFAULT 'daily',
                FOREIGN KEY (data_source_id) REFERENCES data_sources (id),
                UNIQUE (data_source_id, name)
            )
        """)
        print("✓ Created semantic_tables")
        
        # 2. Create semantic_columns with semantic_table_id
        cursor.execute("""
            CREATE TABLE semantic_columns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                semantic_table_id INTEGER NOT NULL,
                name VARCHAR(200) NOT NULL,
                display_name VARCHAR(200) NOT NULL,
                description TEXT NOT NULL,
                data_type VARCHAR(50) NOT NULL,
                semantic_type VARCHAR(50) NOT NULL,
                is_nullable BOOLEAN NOT NULL DEFAULT 1,
                is_primary_key BOOLEAN NOT NULL DEFAULT 0,
                is_foreign_key BOOLEAN NOT NULL DEFAULT 0,
                is_measure BOOLEAN NOT NULL DEFAULT 0,
                is_dimension BOOLEAN NOT NULL DEFAULT 0,
                aggregation_default VARCHAR(50),
                business_rules TEXT NOT NULL,
                format_string VARCHAR(100) NOT NULL,
                sample_values TEXT NOT NULL,
                common_filters TEXT NOT NULL,
                unique_value_count INTEGER,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                business_glossary_term VARCHAR(200) NOT NULL,
                data_quality_rules TEXT NOT NULL,
                FOREIGN KEY (semantic_table_id) REFERENCES semantic_tables (id) ON DELETE CASCADE,
                UNIQUE (semantic_table_id, name)
            )
        """)
        print("✓ Created semantic_columns")
        
        # 3. Create semantic_relationships
        cursor.execute("""
            CREATE TABLE semantic_relationships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_table VARCHAR(200) NOT NULL,
                to_table VARCHAR(200) NOT NULL,
                from_column VARCHAR(200) NOT NULL,
                to_column VARCHAR(200) NOT NULL,
                relationship_type VARCHAR(50) NOT NULL,
                confidence_score REAL NOT NULL DEFAULT 0.0,
                is_verified BOOLEAN NOT NULL DEFAULT 0,
                description TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                verified_at DATETIME,
                join_type_suggestion VARCHAR(20) NOT NULL DEFAULT 'INNER',
                cardinality_estimate VARCHAR(50) NOT NULL,
                verified_by_id INTEGER,
                FOREIGN KEY (verified_by_id) REFERENCES auth_user (id) ON DELETE SET NULL,
                UNIQUE (from_table, to_table, from_column, to_column)
            )
        """)
        print("✓ Created semantic_relationships")
        
        # 4. Create semantic_metrics
        cursor.execute("""
            CREATE TABLE semantic_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(200) NOT NULL,
                display_name VARCHAR(200) NOT NULL,
                description TEXT NOT NULL,
                metric_type VARCHAR(50) NOT NULL,
                calculation TEXT NOT NULL,
                format_string VARCHAR(100) NOT NULL,
                unit VARCHAR(50) NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                business_owner VARCHAR(200) NOT NULL,
                validation_rules TEXT NOT NULL,
                base_table_id INTEGER,
                created_by_id INTEGER NOT NULL,
                FOREIGN KEY (base_table_id) REFERENCES semantic_tables (id) ON DELETE CASCADE,
                FOREIGN KEY (created_by_id) REFERENCES auth_user (id) ON DELETE CASCADE
            )
        """)
        print("✓ Created semantic_metrics")
        
        # 5. Create dependent_columns many-to-many table
        cursor.execute("""
            CREATE TABLE semantic_metrics_dependent_columns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                semanticmetric_id INTEGER NOT NULL,
                semanticcolumn_id INTEGER NOT NULL,
                FOREIGN KEY (semanticmetric_id) REFERENCES semantic_metrics (id) ON DELETE CASCADE,
                FOREIGN KEY (semanticcolumn_id) REFERENCES semantic_columns (id) ON DELETE CASCADE,
                UNIQUE (semanticmetric_id, semanticcolumn_id)
            )
        """)
        print("✓ Created semantic_metrics_dependent_columns")
        
        # 6. Create indexes
        indexes = [
            "CREATE INDEX semantic_ta_data_so_55d62b_idx ON semantic_tables (data_source_id, name)",
            "CREATE INDEX semantic_ta_is_fact_2148e6_idx ON semantic_tables (is_fact_table, is_dimension_table)",
            "CREATE INDEX semantic_co_table_i_3ef6c8_idx ON semantic_columns (semantic_table_id, name)",
            "CREATE INDEX semantic_co_is_meas_b10884_idx ON semantic_columns (is_measure, is_dimension)",
            "CREATE INDEX semantic_co_data_ty_43ed41_idx ON semantic_columns (data_type, semantic_type)",
            "CREATE INDEX semantic_re_from_ta_077719_idx ON semantic_relationships (from_table, to_table)",
            "CREATE INDEX semantic_re_confide_8262b2_idx ON semantic_relationships (confidence_score, is_verified)",
            "CREATE INDEX semantic_me_created_2349c9_idx ON semantic_metrics (created_by_id, is_active)",
            "CREATE INDEX semantic_me_metric__119c83_idx ON semantic_metrics (metric_type, is_active)",
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
                print(f"✓ Created index")
            except Exception as e:
                print(f"Index creation warning: {e}")


def main():
    """Main function"""
    print("Creating semantic tables with correct schema...")
    create_semantic_tables()
    print("\n=== SUCCESS ===")
    print("✓ All semantic tables created successfully!")
    print("✓ Foreign key relationships established")
    print("✓ Indexes created")
    print("\nYou can now use the semantic layer generation feature.")


if __name__ == '__main__':
    main() 