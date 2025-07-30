#!/usr/bin/env python3
"""
Minimal OpenAI test without Django dependencies
"""

print("🧪 Minimal OpenAI Test - No Django Dependencies")

try:
    import openai
    print(f"✅ OpenAI imported successfully - Version: {openai.__version__}")
    
    print("\n🔧 Testing basic client creation...")
    
    # Test most basic client creation
    try:
        client = openai.OpenAI(api_key="test-key")
        print("✅ SUCCESS: Basic OpenAI client created successfully!")
        print(f"   Client type: {type(client)}")
    except Exception as e:
        print(f"❌ FAILED: Basic client creation failed: {e}")
        print(f"   Error type: {type(e)}")
        
        # Try to see what parameters are actually being passed
        import inspect
        try:
            sig = inspect.signature(openai.OpenAI.__init__)
            print(f"\n🔍 OpenAI.__init__ signature: {sig}")
        except Exception as sig_error:
            print(f"❌ Could not get signature: {sig_error}")
            
    print("\n🔧 Testing explicit parameter passing...")
    try:
        # Try with explicit keyword arguments
        client = openai.OpenAI(api_key="test-key", organization=None)
        print("✅ SUCCESS: Client with explicit organization=None created!")
    except Exception as e:
        print(f"❌ FAILED: Client with explicit parameters failed: {e}")
        
except ImportError as e:
    print(f"❌ FAILED: Could not import OpenAI: {e}")
except Exception as e:
    print(f"❌ FAILED: Unexpected error: {e}")

print("\n✨ Minimal OpenAI Test Complete!") 