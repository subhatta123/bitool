#!/usr/bin/env python3
"""
Script to check and fix LLM configuration issues
"""
import os
import sys
import django
from pathlib import Path

# Add the Django project directory to the Python path
sys.path.append(str(Path(__file__).parent))

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from django.conf import settings
from core.models import LLMConfig
from services.llm_service import LLMService
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_llm_configuration():
    """Check LLM configuration and fix common issues"""
    print("🔍 Checking LLM Configuration...")
    
    issues_found = []
    fixes_applied = []
    
    # Check if LLMConfig exists in database
    try:
        active_config = LLMConfig.get_active_config()
        if not active_config:
            print("❌ No active LLM configuration found")
            issues_found.append("No active LLM configuration")
            
            # Create default OpenAI configuration
            openai_key = getattr(settings, 'OPENAI_API_KEY', None)
            if openai_key:
                print("✅ Creating default OpenAI configuration from settings...")
                LLMConfig.objects.create(
                    provider='openai',
                    api_key=openai_key,
                    model_name='gpt-3.5-turbo',
                    temperature=0.1,
                    max_tokens=1000,
                    system_prompt='You are an expert SQL query generator. Convert natural language questions into accurate SQL queries based on the provided database schema.',
                    is_active=True
                )
                fixes_applied.append("Created default OpenAI configuration")
            else:
                print("❌ No OpenAI API key found in settings")
                issues_found.append("No OpenAI API key in settings")
        else:
            print(f"✅ Active LLM configuration found: {active_config.provider} - {active_config.model_name}")
            
            # Check if API key is present
            if not active_config.api_key and active_config.provider in ['openai', 'azure', 'anthropic']:
                print(f"❌ No API key found for {active_config.provider}")
                issues_found.append(f"No API key for {active_config.provider}")
                
                # Try to get from settings
                if active_config.provider == 'openai':
                    openai_key = getattr(settings, 'OPENAI_API_KEY', None)
                    if openai_key:
                        active_config.api_key = openai_key
                        active_config.save()
                        print("✅ Updated API key from settings")
                        fixes_applied.append("Updated OpenAI API key from settings")
            
            # Test the configuration
            print("🧪 Testing LLM connection...")
            llm_service = LLMService()
            
            if active_config.provider == 'openai':
                success, message = llm_service.test_openai_connection(
                    active_config.api_key, 
                    active_config.model_name
                )
                if success:
                    print(f"✅ OpenAI connection test: {message}")
                else:
                    print(f"❌ OpenAI connection test failed: {message}")
                    issues_found.append(f"OpenAI connection failed: {message}")
            
            elif active_config.provider == 'local':
                success, message = llm_service.test_ollama_connection(
                    active_config.base_url or 'http://localhost:11434',
                    active_config.model_name
                )
                if success:
                    print(f"✅ Ollama connection test: {message}")
                else:
                    print(f"❌ Ollama connection test failed: {message}")
                    issues_found.append(f"Ollama connection failed: {message}")
    
    except Exception as e:
        print(f"❌ Error checking LLM configuration: {e}")
        issues_found.append(f"Configuration check error: {e}")
    
    # Check Django settings
    print("\n📋 Checking Django Settings...")
    
    # Check OPENAI_API_KEY
    openai_key = getattr(settings, 'OPENAI_API_KEY', None)
    if openai_key:
        if openai_key.startswith('sk-'):
            print("✅ OPENAI_API_KEY is present and appears valid")
        else:
            print("⚠️ OPENAI_API_KEY is present but doesn't start with 'sk-'")
            issues_found.append("OPENAI_API_KEY format looks incorrect")
    else:
        print("❌ OPENAI_API_KEY not found in settings")
        issues_found.append("OPENAI_API_KEY not in settings")
    
    # Check other LLM settings
    llm_provider = getattr(settings, 'LLM_PROVIDER', 'openai')
    print(f"📊 LLM_PROVIDER: {llm_provider}")
    
    openai_model = getattr(settings, 'OPENAI_MODEL', 'gpt-3.5-turbo')
    print(f"📊 OPENAI_MODEL: {openai_model}")
    
    ollama_url = getattr(settings, 'OLLAMA_URL', 'http://localhost:11434')
    print(f"📊 OLLAMA_URL: {ollama_url}")
    
    # Summary
    print(f"\n📊 Summary:")
    print(f"   Issues found: {len(issues_found)}")
    print(f"   Fixes applied: {len(fixes_applied)}")
    
    if issues_found:
        print("\n❌ Issues found:")
        for issue in issues_found:
            print(f"   - {issue}")
    
    if fixes_applied:
        print("\n✅ Fixes applied:")
        for fix in fixes_applied:
            print(f"   - {fix}")
    
    return len(issues_found) == 0

def fix_common_query_issues():
    """Fix common query processing issues"""
    print("\n🔧 Checking Query Processing...")
    
    # Check if query processing views are working
    try:
        from core.views import QueryView
        from django.test import RequestFactory
        from django.contrib.auth import get_user_model
        
        User = get_user_model()
        
        # Check if we have at least one user
        if not User.objects.exists():
            print("❌ No users found - creating admin user...")
            User.objects.create_superuser(
                'admin', 
                'admin@example.com', 
                'admin123'
            )
            print("✅ Created admin user (username: admin, password: admin123)")
        
        print("✅ Query processing views are importable")
        
    except Exception as e:
        print(f"❌ Error with query processing: {e}")
        return False
    
    return True

def check_database_connections():
    """Check database connections and integrated data"""
    print("\n🗄️ Checking Database Connections...")
    
    try:
        from datasets.models import DataSource
        from services.integration_service import IntegrationService
        
        data_sources = DataSource.objects.all()
        print(f"📊 Found {data_sources.count()} data sources")
        
        integration_service = IntegrationService()
        
        # Check integrated database
        try:
            tables = integration_service.list_all_tables()
            print(f"✅ Integrated database accessible with {len(tables)} tables")
            
            if tables:
                print("   Tables:")
                for table in tables[:5]:  # Show first 5
                    print(f"   - {table}")
                if len(tables) > 5:
                    print(f"   ... and {len(tables) - 5} more")
            
        except Exception as e:
            print(f"❌ Error accessing integrated database: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error checking databases: {e}")
        return False
    
    return True

def main():
    """Main function to run all checks and fixes"""
    print("🚀 ConvaBI LLM Configuration Check & Fix Tool")
    print("=" * 50)
    
    # Run all checks
    llm_ok = check_llm_configuration()
    query_ok = fix_common_query_issues() 
    db_ok = check_database_connections()
    
    print("\n" + "=" * 50)
    print("📋 Final Status:")
    print(f"   LLM Configuration: {'✅' if llm_ok else '❌'}")
    print(f"   Query Processing: {'✅' if query_ok else '❌'}")
    print(f"   Database Access: {'✅' if db_ok else '❌'}")
    
    if llm_ok and query_ok and db_ok:
        print("\n🎉 All systems are operational!")
        print("\n💡 If you're still experiencing token errors:")
        print("   1. Restart your Django server")
        print("   2. Check the browser console for JavaScript errors")
        print("   3. Verify your API key has sufficient credits")
        print("   4. Try a simple test query: 'Show all data'")
    else:
        print("\n⚠️ Some issues need manual attention.")
        print("\n💡 Next steps:")
        if not llm_ok:
            print("   1. Set OPENAI_API_KEY in your settings or environment")
            print("   2. Configure LLM settings in Admin panel")
        if not query_ok:
            print("   3. Check Django app configuration")
        if not db_ok:
            print("   4. Verify data integration setup")

if __name__ == "__main__":
    main() 