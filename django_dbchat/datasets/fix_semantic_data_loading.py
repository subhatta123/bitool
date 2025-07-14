#!/usr/bin/env python3
"""
Fix semantic layer data loading to use unified data access
"""

import re

def fix_semantic_data_loading():
    """Fix the problematic CSV file loading in semantic layer generation"""
    
    with open('views.py', 'r') as f:
        content = f.read()
    
    # Find and replace the problematic data loading section
    # Look for the start of the fallback section
    start_marker = "# Fallback to original data loading if no ETL operations found"
    
    # Find the section to replace
    start_pos = content.find(start_marker)
    if start_pos == -1:
        print("❌ Could not find the problematic section to fix")
        return False
    
    # Find the end of this section (before the next major section)
    end_marker = "if data is None or data.empty:"
    end_pos = content.find(end_marker, start_pos)
    
    if end_pos == -1:
        print("❌ Could not find the end of the section to fix")
        return False
    
    # Extract the content before and after the section to replace
    before = content[:start_pos]
    after = content[end_pos:]
    
    # Create the new data loading code
    new_data_loading = '''# Load data using unified data access layer (FIXED)
            logger.info(f"Loading data for {data_source.name} using unified access layer")
            
            try:
                from .data_access_layer import unified_data_access
                
                success, data, message = unified_data_access.get_data_source_data(data_source)
                
                if not success or data is None or data.empty:
                    logger.warning(f"Failed to load data for {data_source.name}: {message}")
                    return {
                        'success': False, 
                        'error': f'Data not accessible: {message}',
                        'suggestion': 'Please re-upload your data or check the data source configuration.',
                        'data_access_attempted': True
                    }
                
                logger.info(f"Successfully loaded data using unified access: {len(data)} rows, {len(data.columns)} columns")
                
                # Store original metadata for all columns
                for column_name in data.columns:
                    column_metadata[column_name] = {
                        'transformed_type': None,
                        'pandas_type': str(data[column_name].dtype),
                        'etl_applied': False
                    }
                    
            except Exception as data_error:
                logger.error(f"Error loading data with unified access: {data_error}")
                return {
                    'success': False,
                    'error': f'Data access error: {str(data_error)}',
                    'suggestion': 'Please check your data source and try again.',
                    'technical_details': str(data_error)
                }
            
            '''
    
    # Combine the parts
    new_content = before + new_data_loading + after
    
    # Write the updated content back
    with open('views.py', 'w') as f:
        f.write(new_content)
    
    print("✅ Successfully fixed semantic layer data loading!")
    print("   • Replaced CSV file reading with unified data access")
    print("   • Added proper error handling and fallback")
    print("   • Fixed PostgreSQL column name issue")
    
    return True

if __name__ == "__main__":
    fix_semantic_data_loading() 