#!/usr/bin/env python3
"""
CSV Data Migration Script for PostgreSQL
"""

import os
import sys
import django
import json
import pandas as pd
import numpy as np
from pathlib import Path

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.db import connection

def safe_json_serialize(obj):
    """
    Safely serialize pandas data to JSON, handling NaN, inf, and other edge cases
    """
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        # Convert pandas objects to Python objects first
        obj = obj.to_dict('records') if isinstance(obj, pd.DataFrame) else obj.tolist()
    
    def convert_value(value):
        """Convert individual values to JSON-safe format"""
        if pd.isna(value) or value is None:
            return None
        elif isinstance(value, (np.integer, int)):
            return int(value)
        elif isinstance(value, (np.floating, float)):
            if np.isnan(value) or np.isinf(value):
                return None
            return float(value)
        elif isinstance(value, (np.bool_, bool)):
            return bool(value)
        elif isinstance(value, (np.str_, str)):
            return str(value)
        elif isinstance(value, (list, tuple)):
            return [convert_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: convert_value(v) for k, v in value.items()}
        else:
            # For any other type, convert to string
            return str(value)
    
    if isinstance(obj, list):
        return [convert_value(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: convert_value(v) for k, v in obj.items()}
    else:
        return convert_value(obj)

def migrate_csv_files():
    """Migrate CSV files to PostgreSQL unified storage"""
    print('📁 Looking for CSV files to migrate...')
    
    # Look for CSV files in common locations
    csv_dirs = [
        'media/csv_files',
        '../media/csv_files'
    ]
    
    migrated_count = 0
    
    for csv_dir in csv_dirs:
        if os.path.exists(csv_dir):
            print(f'📂 Found CSV directory: {csv_dir}')
            for csv_file in Path(csv_dir).glob('*.csv'):
                try:
                    print(f'📊 Processing {csv_file.name}...')
                    
                    # Read CSV
                    df = pd.read_csv(csv_file)
                    
                    if not df.empty:
                        # Clean data thoroughly (handle NaN, inf, and other edge cases)
                        df_cleaned = df.copy()
                        
                        # Replace NaN, inf, -inf with None/null values
                        df_cleaned = df_cleaned.replace([np.nan, np.inf, -np.inf], None)
                        
                        # Ensure all object columns are strings (handle mixed types)
                        for col in df_cleaned.columns:
                            if df_cleaned[col].dtype == 'object':
                                df_cleaned[col] = df_cleaned[col].astype(str)
                                # Replace 'None' strings back to actual None
                                df_cleaned[col] = df_cleaned[col].replace('None', None)
                        
                        # Use safe JSON serialization
                        json_data = safe_json_serialize(df_cleaned)
                        
                        # Create schema info with safe serialization
                        schema_info = {
                            'columns': [
                                {
                                    'name': col,
                                    'type': str(df_cleaned[col].dtype),
                                    'sample_values': safe_json_serialize(
                                        df_cleaned[col].dropna().head(3).tolist()
                                    )
                                }
                                for col in df_cleaned.columns
                            ],
                            'row_count': len(df_cleaned),
                            'column_count': len(df_cleaned.columns),
                            'source_file': csv_file.name
                        }
                        
                        # Create table name
                        table_name = f'csv_{csv_file.stem.lower().replace(" ", "_").replace("-", "_")}'
                        
                        # Insert into PostgreSQL
                        with connection.cursor() as cursor:
                            # Check if entry exists first
                            cursor.execute('SELECT id FROM unified_data_storage WHERE table_name = %s', [table_name])
                            exists = cursor.fetchone()
                            
                            if exists:
                                # Update existing entry
                                cursor.execute('''
                                    UPDATE unified_data_storage 
                                    SET data = %s, schema_info = %s, row_count = %s, updated_at = CURRENT_TIMESTAMP
                                    WHERE table_name = %s
                                ''', [
                                    json.dumps(json_data),
                                    json.dumps(schema_info),
                                    len(df_cleaned),
                                    table_name
                                ])
                            else:
                                # Insert new entry
                                cursor.execute('''
                                    INSERT INTO unified_data_storage 
                                    (data_source_name, table_name, source_type, data, schema_info, row_count)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                ''', [
                                    csv_file.stem,
                                    table_name,
                                    'csv',
                                    json.dumps(json_data),
                                    json.dumps(schema_info),
                                    len(df_cleaned)
                                ])
                        
                        migrated_count += 1
                        print(f'✅ Migrated {csv_file.name}: {len(df_cleaned)} rows as table {table_name}')
                    
                except Exception as e:
                    print(f'⚠️ Error migrating {csv_file.name}: {e}')
    
    print(f'\n🎉 Migration completed! Migrated {migrated_count} CSV files to PostgreSQL')
    
    # Verify the migration
    with connection.cursor() as cursor:
        cursor.execute('SELECT data_source_name, table_name, row_count FROM unified_data_storage')
        results = cursor.fetchall()
        
        if results:
            print('\n📋 Migrated data sources:')
            for source_name, table_name, row_count in results:
                print(f'  • {source_name}: {row_count:,} rows in table {table_name}')
        else:
            print('\n📝 No CSV files found to migrate. You can upload CSV files later.')

if __name__ == '__main__':
    migrate_csv_files() 