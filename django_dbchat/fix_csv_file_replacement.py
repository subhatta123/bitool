#!/usr/bin/env python3
"""
Fix CSV File Replacement

This script helps replace the system's uploaded CSV file with your updated version
while preserving all the ETL and schema configurations.
"""

import os
import sys
import django
import shutil

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
django.setup()

from datasets.models import DataSource
import pandas as pd
from django.conf import settings

def replace_csv_file():
    """Replace the system's CSV file with user's updated version"""
    
    print("🔄 CSV File Replacement Tool")
    print("=" * 40)
    
    # Step 1: Find the data source
    data_source = DataSource.objects.filter(
        source_type='csv',
        status='active'
    ).first()
    
    if not data_source:
        print("❌ No CSV data source found!")
        return
    
    print(f"📁 Current data source: {data_source.name}")
    system_file_path = data_source.connection_info.get('file_path')
    full_system_path = os.path.join(settings.BASE_DIR, system_file_path)
    
    print(f"🗂️  System file: {full_system_path}")
    print(f"📊 Current columns: {len(data_source.schema_info.get('columns', []))}")
    
    # Step 2: Get user's updated file path
    print(f"\n📝 Please provide the path to your updated CSV file:")
    print(f"   Example: C:\\Users\\YourName\\Downloads\\Sample - Superstore2.csv")
    
    try:
        user_file_path = input("📁 Enter path to your updated CSV file: ").strip().strip('"\'')
        
        if not os.path.exists(user_file_path):
            print(f"❌ File not found: {user_file_path}")
            return
        
        # Verify it's a CSV file
        if not user_file_path.lower().endswith('.csv'):
            print(f"❌ File must be a CSV file!")
            return
        
        # Step 3: Analyze the new file
        print(f"\n🔍 Analyzing your updated file...")
        
        try:
            new_df = pd.read_csv(user_file_path)
            print(f"✅ Successfully read CSV file")
            print(f"📊 New file has: {len(new_df.columns)} columns, {len(new_df)} rows")
            print(f"📋 Column names: {list(new_df.columns)}")
            
            # Compare with current
            current_columns = len(data_source.schema_info.get('columns', []))
            new_columns = len(new_df.columns)
            
            print(f"\n📈 Comparison:")
            print(f"   Current: {current_columns} columns")
            print(f"   New:     {new_columns} columns")
            print(f"   Change:  {new_columns - current_columns:+d} columns")
            
        except Exception as e:
            print(f"❌ Error reading CSV file: {e}")
            return
        
        # Step 4: Confirm replacement
        print(f"\n⚠️  This will:")
        print(f"   • Replace the system's CSV file")
        print(f"   • Update the schema to reflect {new_columns} columns")
        print(f"   • Trigger schema refresh in the UI")
        
        confirm = input(f"\n❓ Continue with replacement? (y/n): ").strip().lower()
        
        if confirm != 'y':
            print("❌ Operation cancelled")
            return
        
        # Step 5: Perform replacement
        print(f"\n🔄 Replacing CSV file...")
        
        # Backup original file
        backup_path = full_system_path + '.backup'
        shutil.copy2(full_system_path, backup_path)
        print(f"💾 Created backup: {backup_path}")
        
        # Replace with new file
        shutil.copy2(user_file_path, full_system_path)
        print(f"✅ File replaced successfully!")
        
        # Step 6: Update schema
        print(f"🔄 Updating schema...")
        
        # Generate new schema
        schema_info = {
            'columns': [],
            'total_columns': len(new_df.columns),
            'total_rows': len(new_df),
            'file_path': system_file_path,
            'last_updated': pd.Timestamp.now().isoformat(),
            'replacement_timestamp': pd.Timestamp.now().isoformat()
        }
        
        for col in new_df.columns:
            col_data = new_df[col]
            
            # Determine data type
            if pd.api.types.is_numeric_dtype(col_data):
                if col_data.dtype in ['int64', 'int32', 'int16', 'int8']:
                    data_type = 'integer'
                else:
                    data_type = 'float'
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                data_type = 'datetime'
            elif pd.api.types.is_bool_dtype(col_data):
                data_type = 'boolean'
            else:
                data_type = 'string'
            
            schema_info['columns'].append({
                'name': str(col),
                'type': data_type,
                'pandas_type': str(col_data.dtype),
                'nullable': col_data.isnull().any(),
                'sample_values': col_data.dropna().head(3).astype(str).tolist()
            })
        
        # Update data source
        data_source.schema_info = schema_info
        data_source.updated_at = pd.Timestamp.now()
        
        # Update connection info
        connection_info = data_source.connection_info.copy()
        connection_info['row_count'] = len(new_df)
        connection_info['column_count'] = len(new_df.columns)
        connection_info['file_replaced'] = True
        connection_info['replacement_timestamp'] = pd.Timestamp.now().isoformat()
        data_source.connection_info = connection_info
        
        data_source.save()
        
        print(f"✅ Schema updated successfully!")
        print(f"📊 New schema: {len(schema_info['columns'])} columns")
        
        # Step 7: Success message
        print(f"\n🎉 CSV File Replacement Complete!")
        print(f"✅ File replaced: {user_file_path} → {full_system_path}")
        print(f"✅ Schema updated: {current_columns} → {new_columns} columns") 
        print(f"✅ Data source refreshed")
        
        print(f"\n🚀 Next Steps:")
        print(f"   1. Go to the Data Integration page")
        print(f"   2. Click the refresh button (🔄) next to data source selector")
        print(f"   3. Select your data source - you should see {new_columns} columns")
        print(f"   4. Run ETL to refresh with the new data")
        
    except KeyboardInterrupt:
        print(f"\n❌ Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Error during replacement: {e}")

if __name__ == "__main__":
    replace_csv_file() 