import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

print("🧪 Testing OpenAI Compatibility Fixes")

# Test the compatibility fix directly
try:
    from services.openai_compatibility_fix import create_safe_openai_client
    print("✅ OpenAI compatibility fix imported successfully")
    
    # Test with a dummy API key
    success, client, error_msg = create_safe_openai_client("test-api-key")
    
    if success and client:
        print("✅ SUCCESS: Safe OpenAI client created successfully!")
        print(f"   Client type: {type(client)}")
    else:
        print(f"❌ FAILED: Safe client creation failed: {error_msg}")
        
except Exception as e:
    print(f"❌ FAILED: Compatibility fix import failed: {e}")

# Test LLM service integration
print("\n🧪 Testing LLM Service Integration")
try:
    from services.llm_service import LLMService
    from services.dynamic_llm_service import DynamicLLMService
    
    print("✅ LLM services imported successfully")
    
    # Test LLM service client creation
    llm_service = LLMService()
    try:
        client = llm_service._create_openai_client("test-api-key")
        print("✅ LLM service OpenAI client creation successful!")
    except Exception as e:
        print(f"❌ LLM service client creation failed: {e}")
    
    # Test dynamic LLM service
    dynamic_service = DynamicLLMService()
    print("✅ Dynamic LLM service initialized successfully")
    
except Exception as e:
    print(f"❌ FAILED: LLM service integration failed: {e}")

# Test with actual database configuration
print("\n🧪 Testing with Database Configuration")
try:
    from core.models import LLMConfig
    config = LLMConfig.get_active_config()
    
    if config and config.provider == 'openai' and config.api_key:
        print(f"✅ Found active OpenAI config: {config.model_name}")
        
        # Test actual client creation with real API key
        try:
            success, client, error_msg = create_safe_openai_client(config.api_key)
            if success and client:
                print("✅ SUCCESS: Real OpenAI client created with database config!")
            else:
                print(f"❌ Real client creation failed: {error_msg}")
        except Exception as e:
            print(f"❌ Real client test failed: {e}")
    else:
        print("ℹ️  No OpenAI configuration found in database")
        
except Exception as e:
    print(f"❌ Database config test failed: {e}")

print("\n✨ OpenAI Compatibility Test Complete!") 