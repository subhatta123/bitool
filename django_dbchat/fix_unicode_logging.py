#!/usr/bin/env python3
"""
Fix Unicode logging issues by replacing emoji characters with ASCII equivalents
"""

import os
import re
from pathlib import Path

def fix_unicode_in_file(file_path):
    """Fix Unicode characters in a single file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace common emoji characters with ASCII equivalents
        replacements = {
            '✅': '[SUCCESS]',
            '❌': '[ERROR]',
            '⚠️': '[WARNING]',
            '🔧': '[FIX]',
            '🎉': '[COMPLETE]',
            '📋': '[INFO]',
            '🚀': '[START]',
        }
        
        original_content = content
        for emoji, ascii_equiv in replacements.items():
            content = content.replace(emoji, ascii_equiv)
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Fixed Unicode issues in: {file_path}")
            return True
        
        return False
        
    except Exception as e:
        print(f"Error fixing {file_path}: {e}")
        return False

def fix_unicode_in_services():
    """Fix Unicode issues in service files"""
    services_dir = Path("services")
    fixed_count = 0
    
    if services_dir.exists():
        for py_file in services_dir.glob("*.py"):
            if fix_unicode_in_file(py_file):
                fixed_count += 1
    
    return fixed_count

def main():
    """Fix Unicode logging issues"""
    print("Fixing Unicode logging issues...")
    
    # Fix specific service files
    files_to_fix = [
        "services/robust_table_validation_service.py",
        "services/improved_etl_join_service.py"
    ]
    
    fixed_count = 0
    for file_path in files_to_fix:
        if os.path.exists(file_path):
            if fix_unicode_in_file(file_path):
                fixed_count += 1
        else:
            print(f"File not found: {file_path}")
    
    print(f"Fixed Unicode issues in {fixed_count} files")
    print("Unicode logging fix complete!")

if __name__ == "__main__":
    main() 