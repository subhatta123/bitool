#!/usr/bin/env python3
"""
Debug LLM configuration and schema issues
"""

import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from services.llm_service import LLMService
from core.models import LLMConfig
from datasets.models import DataSource
import json

def check_llm_configuration():
    """Check the current LLM configuration"""
    
    print("🔍 Checking LLM Configuration")
    print("=" * 50)
    
    try:
        # Check database config
        llm_config = LLMConfig.get_active_config()
        if llm_config:
            print(f"✅ Active LLM Config found:")
            print(f"   Provider: {llm_config.provider}")
            print(f"   Model: {llm_config.model_name}")
            print(f"   Base URL: {llm_config.base_url}")
            print(f"   API Key: {'Present' if llm_config.api_key else 'Missing'}")
            print(f"   Is Active: {llm_config.is_active}")
        else:
            print("❌ No active LLM configuration found")
        
        # Check LLM service
        llm_service = LLMService()
        print(f"\n🤖 LLM Service Status:")
        print(f"   Preferred provider: {llm_service.preferred_provider}")
        print(f"   OpenAI configured: {bool(llm_service.openai_api_key)}")
        print(f"   Ollama URL: {llm_service.ollama_url}")
        
        return llm_service
        
    except Exception as e:
        print(f"❌ Error checking LLM config: {e}")
        return None

def check_data_source_schema():
    """Check the data source schema that's being sent to LLM"""
    
    print(f"\n📊 Checking Data Source Schema")
    print("=" * 50)
    
    try:
        # Get the test data source
        ds = DataSource.objects.filter(name='test').first()
        if not ds:
            print("❌ No 'test' data source found")
            return None
            
        print(f"✅ Data Source: {ds.name}")
        print(f"   ID: {ds.id}")
        print(f"   Table name: {ds.table_name}")
        print(f"   Status: {ds.status}")
        
        # Check schema info
        if ds.schema_info:
            print(f"\n📋 Schema Info:")
            print(f"   Type: {type(ds.schema_info)}")
            if isinstance(ds.schema_info, dict):
                print(f"   Keys: {list(ds.schema_info.keys())}")
                
                if 'columns' in ds.schema_info:
                    columns = ds.schema_info['columns']
                    print(f"   Columns ({len(columns)}): {columns[:10]}...")  # Show first 10
                
                if 'row_count' in ds.schema_info:
                    print(f"   Row count: {ds.schema_info['row_count']}")
            else:
                print(f"   Content: {str(ds.schema_info)[:200]}...")
        else:
            print("❌ No schema info found")
        
        return ds
        
    except Exception as e:
        print(f"❌ Error checking schema: {e}")
        return None

def test_llm_directly():
    """Test the LLM service directly with a simple query"""
    
    print(f"\n🧪 Testing LLM Service Directly")
    print("=" * 50)
    
    try:
        llm_service = LLMService()
        
        # Create a simple schema for testing
        test_schema = {
            "tables": {
                "csv_data": {
                    "columns": [
                        {"name": "Customer_Name", "type": "VARCHAR"},
                        {"name": "Sales", "type": "DECIMAL"},
                        {"name": "Region", "type": "VARCHAR"},
                        {"name": "Product_Name", "type": "VARCHAR"}
                    ]
                }
            }
        }
        
        # Test simple query
        query = "total sales in south region"
        print(f"📝 Testing query: '{query}'")
        
        try:
            success, result = llm_service.generate_sql(query, test_schema)
            print(f"🔍 Result:")
            print(f"   Success: {success}")
            print(f"   SQL: {result}")
            
            if not success:
                print(f"❌ LLM generation failed: {result}")
            elif not result or result.strip() == ";":
                print(f"❌ LLM returned empty/invalid SQL")
            else:
                print(f"✅ LLM generated valid SQL")
                
        except Exception as e:
            print(f"❌ LLM service error: {e}")
        
        # Check configuration status
        config_status = llm_service.get_configuration_status()
        print(f"\n⚙️ Configuration Status: {config_status}")
        
    except Exception as e:
        print(f"❌ Error testing LLM: {e}")

def check_ollama_connectivity():
    """Check if Ollama is running and accessible"""
    
    print(f"\n🔗 Checking Ollama Connectivity")
    print("=" * 50)
    
    try:
        import requests
        
        # Default Ollama URL
        ollama_url = "http://localhost:11434"
        
        # Test basic connectivity
        try:
            response = requests.get(f"{ollama_url}/api/tags", timeout=5)
            if response.status_code == 200:
                models = response.json().get('models', [])
                print(f"✅ Ollama is running at {ollama_url}")
                print(f"   Available models: {len(models)}")
                for model in models[:3]:  # Show first 3
                    print(f"     - {model.get('name', 'Unknown')}")
                return True
            else:
                print(f"❌ Ollama responded with status {response.status_code}")
                return False
        except requests.RequestException as e:
            print(f"❌ Cannot connect to Ollama: {e}")
            return False
            
    except ImportError:
        print("❌ requests library not available for testing")
        return False

if __name__ == "__main__":
    print("🚀 LLM Configuration Debug")
    print("=" * 60)
    
    # Step 1: Check LLM configuration
    llm_service = check_llm_configuration()
    
    # Step 2: Check data source schema
    data_source = check_data_source_schema()
    
    # Step 3: Test LLM directly
    if llm_service:
        test_llm_directly()
    
    # Step 4: Check Ollama connectivity
    ollama_running = check_ollama_connectivity()
    
    print(f"\n" + "=" * 60)
    print("🎯 SUMMARY:")
    print(f"   LLM Service: {'✅' if llm_service else '❌'}")
    print(f"   Data Source: {'✅' if data_source else '❌'}")
    print(f"   Ollama Running: {'✅' if ollama_running else '❌'}")
    
    if not ollama_running:
        print(f"\n💡 RECOMMENDATIONS:")
        print(f"   1. Check if Ollama is installed and running")
        print(f"   2. Try: ollama serve")
        print(f"   3. Or switch to OpenAI provider if you have API key")
        print(f"   4. Check Ollama models: ollama list")
    
    if llm_service and data_source and not ollama_running:
        print(f"\n🔧 QUICK FIX:")
        print(f"   The issue is likely that Ollama is not running")
        print(f"   Start Ollama service to fix the empty SQL generation") 