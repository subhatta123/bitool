"""
OpenAI Compatibility Fix
Simple and robust solution for the 'proxies' parameter issue
"""

import logging
import os
import sys
from typing import Tuple, Optional, Any

logger = logging.getLogger(__name__)

def create_safe_openai_client(api_key: str) -> Tuple[bool, Optional[Any], str]:
    """
    Create a safe OpenAI client that handles the proxies parameter issue
    Returns: (success, client, error_message)
    """
    try:
        import openai
        
        # Method 1: Simple initialization without any proxy settings
        try:
            # Clear any proxy environment variables that might interfere
            proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'no_proxy']
            for var in proxy_vars:
                if var in os.environ:
                    del os.environ[var]
            
            # Create client with minimal parameters
            client = openai.OpenAI(api_key=api_key)
            logger.info("OpenAI client created successfully with minimal initialization")
            return True, client, ""
        except Exception as e:
            logger.warning(f"Simple initialization failed: {e}")
        
        # Method 2: Try with custom httpx client (compatible with HTTPX 0.28.1)
        try:
            import httpx
            
            # Create httpx client with minimal parameters for HTTPX 0.28.1
            http_client = httpx.Client(
                timeout=60.0
            )
            
            client = openai.OpenAI(
                api_key=api_key,
                http_client=http_client
            )
            logger.info("OpenAI client created successfully with custom httpx client")
            return True, client, ""
        except Exception as e:
            logger.warning(f"Custom httpx client method failed: {e}")
        
        # Method 3: Try with base_url override (for different API endpoints)
        try:
            client = openai.OpenAI(
                api_key=api_key,
                base_url="https://api.openai.com/v1"
            )
            logger.info("OpenAI client created successfully with base_url")
            return True, client, ""
        except Exception as e:
            logger.warning(f"Base URL method failed: {e}")
        
        # Method 4: Last resort - try with minimal httpx client
        try:
            import httpx
            
            # Minimal httpx client for HTTPX 0.28.1 compatibility
            http_client = httpx.Client(
                timeout=120.0
            )
            
            client = openai.OpenAI(
                api_key=api_key,
                http_client=http_client
            )
            logger.info("OpenAI client created successfully with minimal httpx client")
            return True, client, ""
        except Exception as e:
            logger.warning(f"Minimal httpx client method failed: {e}")
        
        # All methods failed
        error_msg = "All OpenAI client initialization methods failed"
        logger.error(error_msg)
        return False, None, error_msg
        
    except ImportError:
        error_msg = "OpenAI library not installed"
        logger.error(error_msg)
        return False, None, error_msg
    except Exception as e:
        error_msg = f"Unexpected error creating OpenAI client: {str(e)}"
        logger.error(error_msg)
        return False, None, error_msg

def test_openai_compatibility() -> Tuple[bool, str]:
    """
    Test OpenAI compatibility and return status
    """
    try:
        import openai
        
        # Test basic import
        version = openai.__version__
        logger.info(f"OpenAI library version: {version}")
        
        # Test client creation with test key
        success, client, error = create_safe_openai_client("test-key")
        
        if success:
            return True, f"OpenAI compatibility test passed (version: {version})"
        else:
            return False, f"OpenAI compatibility test failed: {error}"
            
    except ImportError:
        return False, "OpenAI library not installed"
    except Exception as e:
        return False, f"OpenAI compatibility test error: {str(e)}"

def get_openai_version() -> str:
    """Get OpenAI library version"""
    try:
        import openai
        return openai.__version__
    except:
        return "Unknown"

def create_openai_client_with_fallback(api_key: str) -> Tuple[bool, Optional[Any], str]:
    """
    Create OpenAI client with comprehensive fallback strategies
    This is the main function to use for creating OpenAI clients
    """
    if not api_key or api_key == "test-key":
        return False, None, "Valid API key is required"
    
    success, client, error = create_safe_openai_client(api_key)
    
    if success:
        return True, client, ""
    else:
        return False, None, f"Failed to create OpenAI client: {error}. Please check your API key and network configuration." 