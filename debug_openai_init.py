#!/usr/bin/env python3
"""
Debug script to intercept OpenAI client initialization
"""

print("🕵️ Debug OpenAI Initialization")

import openai
import inspect
from unittest.mock import patch

# Store original OpenAI.__init__
original_init = openai.OpenAI.__init__

def debug_init(self, *args, **kwargs):
    """Debug wrapper for OpenAI.__init__ to see what parameters are passed"""
    print(f"\n🔍 OpenAI.__init__ called with:")
    print(f"   args: {args}")
    print(f"   kwargs: {kwargs}")
    
    # Check for problematic parameters
    if 'proxies' in kwargs:
        print(f"❌ FOUND PROXIES PARAMETER: {kwargs['proxies']}")
        print("🔧 Removing proxies parameter...")
        kwargs.pop('proxies')
    
    try:
        return original_init(self, *args, **kwargs)
    except Exception as e:
        print(f"❌ Error in original_init: {e}")
        raise

# Monkey patch the OpenAI.__init__ method
openai.OpenAI.__init__ = debug_init

print("\n🧪 Testing OpenAI client creation with debug wrapper...")

try:
    client = openai.OpenAI(api_key="test-key")
    print("✅ SUCCESS: Client created successfully!")
    print(f"   Client type: {type(client)}")
except Exception as e:
    print(f"❌ FAILED: Client creation failed: {e}")

print("\n✨ Debug test complete!") 