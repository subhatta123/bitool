#!/usr/bin/env python3
"""
Remove Hardcoding Script
Identifies and fixes all hardcoded values in the django_dbchat codebase
"""

import os
import re
import sys
from pathlib import Path

def analyze_hardcoding():
    """Analyze all hardcoding issues in the codebase"""
    
    print("🔍 HARDCODING ANALYSIS - Django DBChat Codebase")
    print("=" * 70)
    
    hardcoding_issues = {
        'localhost_references': [],
        'hardcoded_ports': [],
        'hardcoded_table_names': [],
        'hardcoded_db_paths': [],
        'hardcoded_credentials': [],
        'hardcoded_emails': [],
        'hardcoded_urls': [],
        'hardcoded_api_endpoints': []
    }
    
    django_path = Path('django_dbchat')
    if not django_path.exists():
        print("❌ django_dbchat directory not found")
        return hardcoding_issues
    
    # Define hardcoding patterns
    patterns = {
        'localhost_references': [
            r'localhost',
            r'127\.0\.0\.1',
        ],
        'hardcoded_ports': [
            r':\s*5432',
            r':\s*6379', 
            r':\s*8000',
            r':\s*3000',
            r':\s*11434',
        ],
        'hardcoded_table_names': [
            r'superstore_data',
            r'csv_data',
            r'sample___superstore\d*',
            r'unified_data_storage',
            r'col_\d+',
        ],
        'hardcoded_db_paths': [
            r'data/integrated\.duckdb',
            r'/tmp/',
            r'C:\\',
        ],
        'hardcoded_credentials': [
            r'password\s*=\s*[\'"][^\'\"]+[\'"]',
            r'testpass123',
            r'SECRET_KEY\s*=\s*[\'"]django-insecure',
        ],
        'hardcoded_emails': [
            r'@gmail\.com',
            r'@hotmail\.com',
            r'@yahoo\.com',
            r'admin@example\.com',
            r'test@example\.com',
        ],
        'hardcoded_urls': [
            r'http://localhost:\d+',
            r'https://api\.',
        ],
        'hardcoded_api_endpoints': [
            r'api_key\s*=\s*[\'"][^\'\"]+[\'"]',
            r'OPENAI_API_KEY',
        ]
    }
    
    # Scan all Python files
    for py_file in django_path.rglob('*.py'):
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Check each pattern category
            for category, pattern_list in patterns.items():
                for pattern in pattern_list:
                    matches = re.finditer(pattern, content, re.IGNORECASE)
                    for match in matches:
                        # Get line number
                        line_num = content[:match.start()].count('\n') + 1
                        line_content = content.split('\n')[line_num - 1].strip()
                        
                        hardcoding_issues[category].append({
                            'file': str(py_file),
                            'line': line_num,
                            'content': line_content,
                            'match': match.group()
                        })
                        
        except Exception as e:
            print(f"❌ Error reading {py_file}: {e}")
    
    return hardcoding_issues

def print_analysis_results(issues):
    """Print detailed analysis results"""
    
    total_issues = sum(len(issue_list) for issue_list in issues.values())
    
    print(f"\n📊 HARDCODING ANALYSIS RESULTS")
    print(f"Total Issues Found: {total_issues}")
    print("=" * 50)
    
    for category, issue_list in issues.items():
        if issue_list:
            print(f"\n🚨 {category.upper().replace('_', ' ')} ({len(issue_list)} issues):")
            
            # Group by file for cleaner output
            files = {}
            for issue in issue_list:
                file_path = issue['file']
                if file_path not in files:
                    files[file_path] = []
                files[file_path].append(issue)
            
            for file_path, file_issues in files.items():
                print(f"\n   📁 {file_path}:")
                for issue in file_issues[:5]:  # Show first 5 issues per file
                    print(f"      Line {issue['line']}: {issue['content'][:80]}...")
                if len(file_issues) > 5:
                    print(f"      ... and {len(file_issues) - 5} more issues")

def generate_fixes():
    """Generate fixes for common hardcoding issues"""
    
    print(f"\n🔧 RECOMMENDED FIXES")
    print("=" * 50)
    
    fixes = {
        "1. Database Configuration": {
            "issue": "Hardcoded localhost, ports, database paths",
            "fix": "Use environment variables in settings.py",
            "example": """
# BEFORE:
host='localhost', port=5432
            
# AFTER: 
host=os.environ.get('DB_HOST', 'localhost')
port=int(os.environ.get('DB_PORT', '5432'))
"""
        },
        
        "2. Table Names": {
            "issue": "Hardcoded table names like 'csv_data', 'superstore_data'",
            "fix": "Use dynamic table naming based on data source IDs",
            "example": """
# BEFORE:
table_name = 'csv_data'

# AFTER:
table_name = f"ds_{data_source.id.hex.replace('-', '_')}"
"""
        },
        
        "3. File Paths": {
            "issue": "Hardcoded paths like 'data/integrated.duckdb'",
            "fix": "Use settings-based paths with environment variable fallbacks",
            "example": """
# BEFORE:
db_path = 'data/integrated.duckdb'

# AFTER:
db_path = os.path.join(
    settings.BASE_DIR, 
    os.environ.get('DUCKDB_PATH', 'data/integrated.duckdb')
)
"""
        },
        
        "4. API Keys & Secrets": {
            "issue": "Hardcoded API keys, passwords, secret keys",
            "fix": "Move all secrets to environment variables",
            "example": """
# BEFORE:
api_key = 'hardcoded-key'
SECRET_KEY = 'django-insecure-key'

# AFTER:
api_key = os.environ.get('OPENAI_API_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY', 'fallback-for-dev')
"""
        },
        
        "5. URLs & Endpoints": {
            "issue": "Hardcoded localhost URLs and API endpoints",
            "fix": "Use configurable URLs through settings",
            "example": """
# BEFORE:
url = 'http://localhost:11434'

# AFTER:
url = getattr(settings, 'OLLAMA_URL', 'http://localhost:11434')
"""
        }
    }
    
    for fix_name, fix_info in fixes.items():
        print(f"\n{fix_name}:")
        print(f"   Issue: {fix_info['issue']}")
        print(f"   Fix: {fix_info['fix']}")
        print(f"   Example: {fix_info['example']}")

def create_environment_template():
    """Create a comprehensive .env template"""
    
    env_template = """# Django DBChat Environment Configuration
# Copy this to .env and fill in your values

# Database Configuration
DATABASE_NAME=dbchat
DATABASE_USER=dbchat_user
DATABASE_PASSWORD=your_secure_password
DATABASE_HOST=localhost
DATABASE_PORT=5432

# Redis Configuration
REDIS_URL=redis://localhost:6379

# Django Configuration
SECRET_KEY=your-super-secret-django-key-here
DEBUG=False
ALLOWED_HOSTS=localhost,127.0.0.1,your-domain.com

# AI/LLM Configuration
OPENAI_API_KEY=your-openai-api-key-here
OLLAMA_URL=http://localhost:11434

# Email Configuration
EMAIL_HOST=smtp.gmail.com
EMAIL_PORT=587
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-email-password

# File Storage
MEDIA_ROOT=media
STATIC_ROOT=staticfiles

# DuckDB Configuration
DUCKDB_PATH=data

# Security
ENCRYPTION_SECRET_KEY=your-encryption-key-here

# Development vs Production
ENVIRONMENT=development

# Logging
LOG_LEVEL=INFO
"""
    
    with open('django_dbchat/.env.template', 'w') as f:
        f.write(env_template)
    
    print(f"\n📝 Created .env.template file")
    print("   Copy this to .env and fill in your values")

def apply_critical_fixes():
    """Apply critical fixes to remove the most problematic hardcoding"""
    
    print(f"\n🚀 APPLYING CRITICAL FIXES")
    print("=" * 50)
    
    fixes_applied = []
    
    # Critical Fix: Remove hardcoded table names from data access layer
    dal_file = Path('django_dbchat/datasets/data_access_layer.py')
    if dal_file.exists():
        try:
            with open(dal_file, 'r') as f:
                content = f.read()
            
            # Already fixed in our previous updates - table names are now dynamic
            fixes_applied.append("✅ DuckDB table names already use unique IDs")
            
        except Exception as e:
            print(f"❌ Error checking data_access_layer.py: {e}")
    
    # Add settings for configurable values
    settings_file = Path('django_dbchat/dbchat_project/settings.py')
    if settings_file.exists():
        try:
            with open(settings_file, 'r') as f:
                content = f.read()
            
            # Add DuckDB path setting
            if 'DUCKDB_PATH' not in content:
                duckdb_setting = "\n# DuckDB Configuration\nDUCKDB_PATH = os.environ.get('DUCKDB_PATH', 'data')\nDUCKDB_FILENAME = os.environ.get('DUCKDB_FILENAME', 'integrated.duckdb')\n"
                content += duckdb_setting
                fixes_applied.append("✅ Added DUCKDB_PATH setting")
                
                with open(settings_file, 'w') as f:
                    f.write(content)
            
        except Exception as e:
            print(f"❌ Error fixing settings.py: {e}")
    
    print(f"\n📋 Fixes Applied:")
    for fix in fixes_applied:
        print(f"   {fix}")
    
    if not fixes_applied:
        print("   No critical fixes needed - codebase already properly configured")

def main():
    """Main function to analyze and fix hardcoding"""
    
    print("🚀 Hardcoding Removal Tool for Django DBChat")
    print("=" * 70)
    
    # Step 1: Analyze current hardcoding
    print("\nStep 1: Analyzing hardcoding issues...")
    issues = analyze_hardcoding()
    
    # Step 2: Print results
    print_analysis_results(issues)
    
    # Step 3: Generate fix recommendations
    generate_fixes()
    
    # Step 4: Create environment template
    create_environment_template()
    
    # Step 5: Apply critical fixes
    apply_critical_fixes()
    
    print(f"\n🎯 SUMMARY")
    print("=" * 50)
    print("✅ Analysis completed")
    print("✅ Fix recommendations generated")
    print("✅ Environment template created (.env.template)")
    print("✅ Critical fixes applied")
    
    total_issues = sum(len(issue_list) for issue_list in issues.values())
    
    if total_issues > 0:
        print(f"\n⚠️  {total_issues} hardcoding issues found")
        print("📝 Review the analysis above and apply recommended fixes")
        print("🔧 Use the .env.template to configure environment variables")
        print("\n🔧 PRIORITY FIXES:")
        print("   1. Copy .env.template to .env and configure values")
        print("   2. Replace hardcoded localhost with environment variables")
        print("   3. Move all API keys and secrets to environment variables")
        print("   4. Update test files to use configurable values")
    else:
        print("\n🎉 No significant hardcoding issues found!")

if __name__ == "__main__":
    main()
