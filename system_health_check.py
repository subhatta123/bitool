#!/usr/bin/env python
"""
Quick System Health Check
Identifies current system issues without making changes
"""

import os
import sys
import json
from datetime import datetime

def check_files_and_logs():
    """Check critical files and recent log errors"""
    
    print("🔍 QUICK SYSTEM HEALTH CHECK")
    print("=" * 50)
    
    issues = []
    
    # 1. Check database
    db_path = 'data_integration_storage/integrated_data.db'
    if os.path.exists(db_path):
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        print(f"✅ Database exists: {size_mb:.1f} MB")
    else:
        print("❌ Database missing!")
        issues.append("Integrated database not found")
    
    # 2. Check Django project
    django_path = 'django_dbchat/manage.py'
    if os.path.exists(django_path):
        print("✅ Django project found")
    else:
        print("❌ Django project missing!")
        issues.append("Django project not found")
    
    # 3. Check recent logs
    log_path = 'django_dbchat/logs/django.log'
    if os.path.exists(log_path):
        try:
            with open(log_path, 'r') as f:
                lines = f.readlines()
                recent_errors = []
                recent_warnings = []
                
                # Get last 100 lines
                for line in lines[-100:]:
                    if 'ERROR' in line:
                        recent_errors.append(line.strip())
                    elif 'WARNING' in line:
                        recent_warnings.append(line.strip())
                
                print(f"📊 Recent errors: {len(recent_errors)}")
                print(f"📊 Recent warnings: {len(recent_warnings)}")
                
                if recent_errors:
                    print("\n🔴 RECENT ERRORS:")
                    for error in recent_errors[-5:]:  # Show last 5
                        print(f"   {error[:100]}...")
                
                if recent_warnings:
                    print("\n🟡 RECENT WARNINGS:")
                    for warning in recent_warnings[-3:]:  # Show last 3
                        print(f"   {warning[:100]}...")
                        
        except Exception as e:
            print(f"❌ Error reading logs: {e}")
            issues.append("Cannot read log files")
    else:
        print("⚠️ Log file not found")
    
    # 4. Check configuration files
    config_files = [
        'django_dbchat/dbchat_project/settings.py',
        'requirements.txt',
        'django_dbchat/requirements.txt'
    ]
    
    print(f"\n📁 Configuration Files:")
    for config_file in config_files:
        if os.path.exists(config_file):
            print(f"   ✅ {config_file}")
        else:
            print(f"   ❌ {config_file}")
    
    # 5. Check for error patterns in current directory
    error_patterns = [
        "You cannot access body after reading from request",
        "UNIQUE constraint failed",
        "ETL validation failed",
        "High null rate"
    ]
    
    print(f"\n🔍 Checking for known error patterns...")
    for pattern in error_patterns:
        if os.path.exists(log_path):
            try:
                with open(log_path, 'r') as f:
                    content = f.read()
                    if pattern in content:
                        print(f"   🔴 Found: {pattern}")
                        issues.append(f"Known issue: {pattern}")
                    else:
                        print(f"   ✅ Not found: {pattern}")
            except:
                pass
    
    # Summary
    print(f"\n📋 HEALTH CHECK SUMMARY:")
    if issues:
        print(f"❌ {len(issues)} issues found:")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
    else:
        print("✅ No major issues detected!")
    
    print(f"\n💡 QUICK FIXES:")
    print("   1. Clear duplicate data sources")
    print("   2. Check date format settings (use DD-MM-YYYY)")
    print("   3. Restart Django server")
    print("   4. Check network/database connections")
    
    return issues

if __name__ == "__main__":
    check_files_and_logs() 