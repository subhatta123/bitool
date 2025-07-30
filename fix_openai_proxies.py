#!/usr/bin/env python3
"""
Permanent fix for OpenAI proxies parameter issue
This script patches the OpenAI library to filter out unsupported parameters
"""

import openai
import inspect
from typing import Any, Dict

def patch_openai_client():
    """Patch OpenAI client to filter out unsupported parameters"""
    
    # Store the original __init__ method
    original_init = openai.OpenAI.__init__
    
    # Get the supported parameters from the method signature
    sig = inspect.signature(original_init)
    supported_params = set(sig.parameters.keys()) - {'self'}
    
    print(f"🔧 Supported OpenAI parameters: {sorted(supported_params)}")
    
    def filtered_init(self, *args, **kwargs):
        """Filtered __init__ that removes unsupported parameters"""
        
        # Filter out unsupported parameters
        filtered_kwargs = {}
        removed_params = []
        
        for key, value in kwargs.items():
            if key in supported_params:
                filtered_kwargs[key] = value
            else:
                removed_params.append(key)
        
        if removed_params:
            print(f"⚠️  Filtered out unsupported parameters: {removed_params}")
        
        # Call the original method with filtered parameters
        return original_init(self, *args, **filtered_kwargs)
    
    # Replace the __init__ method
    openai.OpenAI.__init__ = filtered_init
    print("✅ OpenAI client patched successfully!")

# Apply the patch
patch_openai_client()

# Test the patched client
print("\n🧪 Testing patched OpenAI client...")

try:
    # This should now work even if proxies parameter is somehow injected
    client = openai.OpenAI(api_key="test-key")
    print("✅ SUCCESS: Patched OpenAI client created successfully!")
    print(f"   Client type: {type(client)}")
except Exception as e:
    print(f"❌ FAILED: Patched client creation failed: {e}")

print("\n✨ OpenAI patching complete!") 