#!/usr/bin/env python3
"""
LLM Issues Fix Script
Fixes the identified LLM configuration problems
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

def fix_llm_configuration():
    """Fix LLM configuration issues"""
    print("🔧 Fixing LLM Configuration...")
    
    try:
        from core.models import LLMConfig
        from django.conf import settings
        
        # Option 1: Try to get OpenAI API key from environment
        openai_key = os.environ.get('OPENAI_API_KEY')
        if not openai_key:
            # Try to get from Django settings
            openai_key = getattr(settings, 'OPENAI_API_KEY', None)
        
        if openai_key and openai_key.strip():
            print("✅ Found OpenAI API key, configuring OpenAI...")
            
            # Delete existing configurations
            LLMConfig.objects.all().delete()
            
            # Create new OpenAI configuration
            config = LLMConfig.objects.create(
                provider='openai',
                api_key=openai_key.strip(),
                model_name='gpt-3.5-turbo',
                temperature=0.1,
                max_tokens=1000,
                system_prompt='''You are an expert SQL query generator. Your task is to convert natural language questions into accurate SQL queries.

Rules:
1. Generate ONLY valid SQL queries
2. Use exact table and column names from the schema
3. Include appropriate WHERE clauses, JOINs, and aggregations
4. If the question is ambiguous, start your response with "CLARIFICATION_NEEDED:"
5. Otherwise, respond with ONLY the SQL query, no explanations
6. Use proper SQL syntax for the specified database type''',
                is_active=True
            )
            
            print(f"✅ Created OpenAI configuration: {config}")
            
            # Test the configuration
            from services.llm_service import LLMService
            llm_service = LLMService()
            llm_service.update_configuration()  # Reload config
            
            success, message = llm_service.test_openai_connection(openai_key.strip())
            if success:
                print(f"✅ OpenAI connection test: {message}")
                return True
            else:
                print(f"❌ OpenAI test failed: {message}")
                print("💡 Please check:")
                print("   - API key is valid")
                print("   - API key has sufficient credits")
                print("   - Network connectivity to OpenAI")
                return False
        
        else:
            print("❌ No OpenAI API key found")
            print("💡 To use OpenAI:")
            print("   1. Get an API key from https://platform.openai.com/")
            print("   2. Set environment variable: OPENAI_API_KEY=your_key_here")
            print("   3. Or add it to Django settings.py")
            
            # Configure for local Ollama with simpler model
            print("\n🔄 Configuring for local Ollama instead...")
            
            # Delete existing configurations
            LLMConfig.objects.all().delete()
            
            # Create Ollama configuration with simpler model
            config = LLMConfig.objects.create(
                provider='local',
                base_url='http://localhost:11434',
                model_name='llama2',  # More common model
                temperature=0.1,
                max_tokens=500,
                system_prompt='You are a SQL expert. Convert natural language to SQL queries.',
                is_active=True
            )
            
            print(f"✅ Created Ollama configuration: {config}")
            print("⚠️ Note: You need to install Ollama and the llama2 model")
            print("   Visit: https://ollama.ai/")
            return True
            
    except Exception as e:
        print(f"❌ Error fixing LLM configuration: {e}")
        import traceback
        traceback.print_exc()
        return False

def fix_semantic_service():
    """Fix semantic service issues"""
    print("\n🧠 Fixing Semantic Service...")
    
    try:
        from services.semantic_service import SemanticService
        
        # Test with simple schema
        test_schema = {
            "tables": {
                "users": {
                    "columns": ["id", "name", "email", "created_at"]
                }
            }
        }
        
        semantic_service = SemanticService()
        success, result, clarification = semantic_service.get_enhanced_sql_from_openai(
            "Show all users", test_schema, "sqlite"
        )
        
        if success:
            print("✅ Semantic service is working")
            if clarification:
                print(f"   Returned clarification: {clarification}")
            else:
                print(f"   Generated SQL: {result}")
            return True
        else:
            print(f"❌ Semantic service still failing: {result}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing semantic service: {e}")
        return False

def fix_api_endpoints():
    """Fix API endpoint issues"""
    print("\n🌐 Fixing API Endpoints...")
    
    try:
        from django.test import Client
        from django.contrib.auth import get_user_model
        
        User = get_user_model()
        
        # Ensure test user exists
        test_user, created = User.objects.get_or_create(
            username='testuser',
            defaults={
                'email': 'test@example.com',
                'is_active': True
            }
        )
        
        if created:
            test_user.set_password('testpass123')
            test_user.save()
            print("✅ Created test user")
        
        client = Client()
        
        # Test login
        login_success = client.login(username='testuser', password='testpass123')
        if not login_success:
            print("❌ Test user login failed")
            return False
        
        # Test semantic page
        response = client.get('/datasets/semantic/')
        if response.status_code == 200:
            print("✅ Semantic page accessible")
        else:
            print(f"❌ Semantic page error: {response.status_code}")
            if hasattr(response, 'content'):
                error_content = response.content.decode('utf-8')[:500]
                print(f"   Error details: {error_content}")
        
        # Test data readiness endpoint
        response = client.get('/datasets/semantic/check-readiness/')
        if response.status_code == 200:
            print("✅ Data readiness endpoint working")
            try:
                data = response.json()
                print(f"   Response preview: {str(data)[:200]}...")
            except:
                print("   Response is not JSON")
        else:
            print(f"❌ Data readiness endpoint error: {response.status_code}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error fixing API endpoints: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_quick_setup_guide():
    """Create a quick setup guide for the user"""
    guide = """
# 🚀 Quick LLM Setup Guide

## Option 1: OpenAI (Recommended)
1. Get API key from https://platform.openai.com/
2. Set environment variable:
   ```
   set OPENAI_API_KEY=your_api_key_here
   ```
3. Restart Django server
4. Run this fix script again

## Option 2: Local Ollama
1. Install Ollama from https://ollama.ai/
2. Install a model: `ollama pull llama2`
3. Start Ollama service
4. The system will auto-configure

## Test Your Setup
- Visit the query page: http://localhost:8000/query/
- Try a simple query: "Show all data"
- Check for any JavaScript errors in browser console (F12)

## Common Issues
- **Token Error**: Usually means no valid LLM configuration
- **400 Errors**: Check Django logs for specific error details
- **Model Not Found**: Install the required Ollama model
"""
    
    with open('LLM_SETUP_GUIDE.md', 'w') as f:
        f.write(guide)
    
    print("📝 Created LLM_SETUP_GUIDE.md with detailed instructions")

def main():
    """Main fix function"""
    print("🛠️ ConvaBI LLM Issues Fix Tool")
    print("=" * 50)
    
    # Run fixes
    llm_fixed = fix_llm_configuration()
    semantic_fixed = fix_semantic_service() if llm_fixed else False
    api_fixed = fix_api_endpoints()
    
    # Create setup guide
    create_quick_setup_guide()
    
    print("\n" + "=" * 50)
    print("📋 Fix Results:")
    print(f"   LLM Configuration: {'✅' if llm_fixed else '❌'}")
    print(f"   Semantic Service: {'✅' if semantic_fixed else '❌'}")
    print(f"   API Endpoints: {'✅' if api_fixed else '❌'}")
    
    if llm_fixed and semantic_fixed:
        print("\n🎉 LLM issues fixed!")
        print("\n📋 Next Steps:")
        print("   1. Restart your Django server")
        print("   2. Clear browser cache and cookies")
        print("   3. Try a test query: 'Show all data'")
        print("   4. Check browser console (F12) for any JavaScript errors")
    
    elif llm_fixed:
        print("\n⚠️ LLM configured but semantic service needs attention")
        print("   This might resolve after restarting the Django server")
    
    else:
        print("\n❌ LLM configuration still needs work")
        print("   Please check the LLM_SETUP_GUIDE.md file for detailed instructions")

if __name__ == "__main__":
    main() 