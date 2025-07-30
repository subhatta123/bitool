#!/usr/bin/env python3
"""
Ultimate OpenAI fix - completely bypass the standard initialization
"""

print("🚀 Ultimate OpenAI Fix - Bypassing Standard Initialization")

import openai
import types

# Store the original class
OriginalOpenAI = openai.OpenAI

class FixedOpenAI:
    """Fixed OpenAI client that bypasses the problematic initialization"""
    
    def __init__(self, api_key=None, **kwargs):
        # Only keep supported parameters
        valid_params = {
            'api_key': api_key,
            'organization': kwargs.get('organization'),
            'project': kwargs.get('project'),
            'base_url': kwargs.get('base_url'),
            'timeout': kwargs.get('timeout'),
            'max_retries': kwargs.get('max_retries', 2),
            'default_headers': kwargs.get('default_headers'),
            'default_query': kwargs.get('default_query'),
            'http_client': kwargs.get('http_client'),
            '_strict_response_validation': kwargs.get('_strict_response_validation', False)
        }
        
        # Remove None values
        valid_params = {k: v for k, v in valid_params.items() if v is not None}
        
        print(f"🔧 Creating FixedOpenAI with params: {list(valid_params.keys())}")
        
        # Create the original client by directly calling object.__new__ and manually setting up
        self._client = object.__new__(OriginalOpenAI)
        
        # Manually initialize the client with only valid parameters
        try:
            # Call the original __init__ with filtered parameters
            OriginalOpenAI.__init__(self._client, **valid_params)
            print("✅ FixedOpenAI client created successfully!")
        except Exception as e:
            print(f"❌ FixedOpenAI creation failed: {e}")
            raise
    
    def __getattr__(self, name):
        """Delegate all attribute access to the wrapped client"""
        return getattr(self._client, name)

# Replace the OpenAI class
openai.OpenAI = FixedOpenAI

print("\n🧪 Testing Fixed OpenAI Client...")

try:
    client = openai.OpenAI(api_key="test-key")
    print("✅ SUCCESS: Fixed OpenAI client created!")
    print(f"   Client type: {type(client)}")
    print(f"   Has chat attribute: {hasattr(client, 'chat')}")
except Exception as e:
    print(f"❌ FAILED: Fixed client creation failed: {e}")

print("\n✨ Ultimate OpenAI Fix Complete!") 