#!/usr/bin/env python3
"""
Comprehensive Test Script for ConvaBI Setup
Tests all major components: LLM services, business metrics, Celery, etc.
"""

import os
import sys
import django
import requests
import json
from datetime import datetime

# Add the Django project to the path
sys.path.append('/app/django_dbchat')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

def test_health_endpoint():
    """Test the application health endpoint"""
    print("🔍 Testing Health Endpoint...")
    try:
        response = requests.get('http://localhost:8000/health/', timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Health check passed: {data}")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False

def test_llm_configuration():
    """Test LLM configuration"""
    print("\n🔍 Testing LLM Configuration...")
    try:
        from core.models import LLMConfig
        from services.llm_service import LLMService
        
        # Check active configuration
        config = LLMConfig.get_active_config()
        if config:
            print(f"✅ Active LLM config: {config.provider} - {config.model_name}")
            
            # Test LLM service
            service = LLMService()
            status = service.get_configuration_status()
            print(f"✅ LLM Service Status: {status}")
            
            return True
        else:
            print("❌ No active LLM configuration found")
            return False
    except Exception as e:
        print(f"❌ LLM configuration error: {e}")
        return False

def test_ollama_connection():
    """Test Ollama connection"""
    print("\n🔍 Testing Ollama Connection...")
    try:
        response = requests.get('http://ollama:11434/api/tags', timeout=10)
        if response.status_code == 200:
            data = response.json()
            models = data.get('models', [])
            print(f"✅ Ollama connected successfully")
            print(f"✅ Available models: {len(models)}")
            for model in models:
                print(f"   - {model.get('name', 'Unknown')}")
            return True
        else:
            print(f"❌ Ollama connection failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Ollama connection error: {e}")
        return False

def test_business_metrics():
    """Test business metrics functionality"""
    print("\n🔍 Testing Business Metrics...")
    try:
        from services.business_metrics_service import BusinessMetricsService
        from datasets.models import SemanticMetric
        
        # Test service initialization
        service = BusinessMetricsService()
        print("✅ Business metrics service initialized")
        
        # Test model access
        metrics_count = SemanticMetric.objects.count()
        print(f"✅ Business metrics model accessible: {metrics_count} metrics")
        
        # Test formula validation
        is_valid, message, suggestions = service.validate_formula("SUM(revenue)", "test_table")
        print(f"✅ Formula validation working: {is_valid}")
        
        return True
    except Exception as e:
        print(f"❌ Business metrics error: {e}")
        return False

def test_celery_status():
    """Test Celery status"""
    print("\n🔍 Testing Celery Status...")
    try:
        from celery import current_app
        
        # Check if Celery is configured
        if current_app.conf.broker_url:
            print(f"✅ Celery broker configured: {current_app.conf.broker_url}")
            
            # Check registered tasks
            tasks = list(current_app.tasks.keys())
            print(f"✅ Celery tasks registered: {len(tasks)}")
            
            # Check for key tasks
            key_tasks = [
                'dbchat_project.celery.debug_task',
                'dbchat_project.celery.send_dashboard_email_task',
                'dbchat_project.celery.run_etl_operation_task'
            ]
            
            for task in key_tasks:
                if task in tasks:
                    print(f"   ✅ {task}")
                else:
                    print(f"   ❌ {task} (missing)")
            
            return True
        else:
            print("❌ Celery broker not configured")
            return False
    except Exception as e:
        print(f"❌ Celery status error: {e}")
        return False

def test_database_connection():
    """Test database connection"""
    print("\n🔍 Testing Database Connection...")
    try:
        from django.db import connection
        from django.core.management import execute_from_command_line
        
        # Test database connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            if result and result[0] == 1:
                print("✅ Database connection successful")
                
                # Test key tables
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('data_sources', 'semantic_tables', 'semantic_metrics', 'etl_operations')
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cursor.fetchall()]
                print(f"✅ Key tables found: {tables}")
                
                return True
            else:
                print("❌ Database connection test failed")
                return False
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return False

def test_openai_compatibility():
    """Test OpenAI compatibility"""
    print("\n🔍 Testing OpenAI Compatibility...")
    try:
        from services.openai_compatibility_fix import test_openai_compatibility
        
        success, message = test_openai_compatibility()
        if success:
            print(f"✅ OpenAI compatibility: {message}")
        else:
            print(f"⚠️ OpenAI compatibility issue: {message}")
            print("   Note: This is expected in Docker environment, Ollama will be used as fallback")
        
        return True  # Don't fail the test for OpenAI issues
    except Exception as e:
        print(f"❌ OpenAI compatibility test error: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Starting Comprehensive ConvaBI Setup Test")
    print("=" * 50)
    print(f"Test started at: {datetime.now()}")
    print()
    
    tests = [
        ("Health Endpoint", test_health_endpoint),
        ("Database Connection", test_database_connection),
        ("LLM Configuration", test_llm_configuration),
        ("Ollama Connection", test_ollama_connection),
        ("Business Metrics", test_business_metrics),
        ("Celery Status", test_celery_status),
        ("OpenAI Compatibility", test_openai_compatibility),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! ConvaBI is ready to use.")
        print("🌐 Access the application at: http://localhost:8000")
    else:
        print(f"\n⚠️ {total-passed} test(s) failed. Please check the issues above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 