#!/usr/bin/env python3
"""
Script to replace all instances of 'ConvaBI' with 'ConvaBI' in the codebase.
This script will:
1. Find all text files (excluding binary files and build directories)
2. Replace 'ConvaBI' with 'ConvaBI' case-sensitively
3. Report on the changes made

Usage: python replace_dbchat_with_convabi.py
"""

import os
import re
import mimetypes
from pathlib import Path

def is_text_file(file_path):
    """Check if a file is a text file by checking its MIME type"""
    try:
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type and mime_type.startswith('text/'):
            return True
        
        # Additional checks for files without clear MIME types
        text_extensions = {'.py', '.html', '.css', '.js', '.md', '.txt', '.yml', '.yaml', 
                          '.json', '.sql', '.sh', '.bat', '.xml', '.log', '.ini', '.cfg',
                          '.conf', '.env', '.toml', '.spec', '.iss'}
        
        if file_path.suffix.lower() in text_extensions:
            return True
        
        # Check if file has no extension but might be text (like manage.py, etc.)
        if not file_path.suffix:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    # Try to read first 1024 bytes as text
                    sample = f.read(1024)
                    # If it contains mostly printable characters, assume it's text
                    printable_ratio = sum(1 for c in sample if c.isprintable() or c.isspace()) / len(sample) if sample else 0
                    return printable_ratio > 0.7
            except:
                pass
        
        return False
    except:
        return False

def should_skip_path(path):
    """Check if we should skip this path"""
    skip_patterns = [
        'build/', 'dist/', '__pycache__/', '.git/', 'node_modules/',
        '.venv/', 'venv/', '.pytest_cache/', '.mypy_cache/',
        'staticfiles/', 'logs/', 'temp_images/', 'media/',
        'installer/', 'data_integration_storage/'
    ]
    
    path_str = str(path).replace('\\', '/')
    return any(pattern in path_str for pattern in skip_patterns)

def replace_dbchat_in_file(file_path):
    """Replace 'ConvaBI' with 'ConvaBI' in a single file"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Check if file contains 'ConvaBI'
        if 'ConvaBI' not in content:
            return 0, []
        
        # Find all instances with context
        matches = []
        for match in re.finditer(r'ConvaBI', content):
            start_idx = max(0, match.start() - 20)
            end_idx = min(len(content), match.end() + 20)
            context = content[start_idx:end_idx].replace('\n', '\\n')
            matches.append(f"  Line context: ...{context}...")
        
        # Replace all instances
        new_content = content.replace('ConvaBI', 'ConvaBI')
        
        # Write back to file
        with open(file_path, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(new_content)
        
        return len(matches), matches
    
    except Exception as e:
        print(f"  ERROR processing {file_path}: {e}")
        return 0, []

def main():
    """Main function to process all files"""
    print("🔄 ConvaBI Brand Replacement Script")
    print("=" * 50)
    print("Replacing all instances of 'ConvaBI' with 'ConvaBI'...")
    print()
    
    total_files_processed = 0
    total_files_changed = 0
    total_replacements = 0
    
    # Get current directory
    current_dir = Path('.')
    
    # Process all files
    for root, dirs, files in os.walk(current_dir):
        root_path = Path(root)
        
        # Skip certain directories
        if should_skip_path(root_path):
            continue
        
        for file in files:
            file_path = root_path / file
            
            # Skip if not a text file
            if not is_text_file(file_path):
                continue
            
            total_files_processed += 1
            
            # Process the file
            replacements, matches = replace_dbchat_in_file(file_path)
            
            if replacements > 0:
                total_files_changed += 1
                total_replacements += replacements
                
                print(f"📝 {file_path}")
                print(f"  Replaced {replacements} instance(s)")
                for match in matches[:3]:  # Show first 3 matches
                    print(match)
                if len(matches) > 3:
                    print(f"  ... and {len(matches) - 3} more")
                print()
    
    print("=" * 50)
    print("🎉 Replacement Summary:")
    print(f"  Files processed: {total_files_processed}")
    print(f"  Files changed: {total_files_changed}")
    print(f"  Total replacements: {total_replacements}")
    print()
    print("✅ All 'ConvaBI' instances have been replaced with 'ConvaBI'!")
    
    if total_files_changed > 0:
        print()
        print("📋 Next steps:")
        print("1. Review the changes to ensure they look correct")
        print("2. Test the application to make sure everything still works")
        print("3. Update any remaining configuration files or documentation")
        print("4. Consider updating project/directory names if needed")

if __name__ == "__main__":
    main() 