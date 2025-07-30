import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

print("🧪 Testing Ollama as OpenAI Fallback")

from services.llm_service import LLMService
from services.dynamic_llm_service import DynamicLLMService

# Test Ollama availability
service = LLMService()
print(f"📡 Ollama URL: {service.ollama_url}")
print(f"🔍 Ollama available: {service._check_ollama_available()}")

# Test Ollama query generation
try:
    dynamic_service = DynamicLLMService()
    
    # Set to local provider for testing
    dynamic_service.preferred_provider = 'local'
    dynamic_service.ollama_model = 'llama3.2'
    
    # Test a simple query
    test_prompt = "Generate SQL to find all customers: Table customers has columns: id, name, email"
    
    print(f"\n🧪 Testing Ollama SQL generation...")
    print(f"📝 Test prompt: {test_prompt}")
    
    success, result = dynamic_service._generate_sql_ollama(test_prompt)
    
    if success:
        print(f"✅ SUCCESS: Ollama SQL generation works!")
        print(f"📊 Generated SQL: {result}")
    else:
        print(f"❌ FAILED: Ollama error: {result}")
        
except Exception as e:
    print(f"❌ FAILED: Ollama test failed: {e}")

# Check database configuration options
print(f"\n🗃️ Checking database LLM configuration...")
try:
    from core.models import LLMConfig
    configs = LLMConfig.objects.all()
    
    for config in configs:
        print(f"  - Provider: {config.provider}, Model: {config.model_name}, Active: {config.is_active}")
        
    # Suggest creating a local config if OpenAI fails
    local_configs = LLMConfig.objects.filter(provider='local')
    if not local_configs.exists():
        print(f"\n💡 Suggestion: Create a local Ollama configuration as backup")
        
except Exception as e:
    print(f"❌ Database config check failed: {e}")

print(f"\n✨ Ollama Fallback Test Complete!") 