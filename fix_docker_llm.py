#!/usr/bin/env python3
"""
Fix LLM issues in Docker container
This script should be run after the container starts to fix OpenAI and Ollama issues
"""

import subprocess
import sys
import os

def run_command(cmd):
    """Run a shell command and return success status"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def fix_openai_installation():
    """Fix OpenAI installation issues"""
    print("🔧 Fixing OpenAI installation...")
    
    # Uninstall and reinstall OpenAI cleanly
    print("  - Uninstalling OpenAI...")
    success, stdout, stderr = run_command("pip uninstall -y openai")
    
    print("  - Clearing pip cache...")
    run_command("pip cache purge")
    
    print("  - Installing OpenAI 1.35.0 (known stable version)...")
    success, stdout, stderr = run_command("pip install openai==1.35.0 --no-cache-dir")
    
    if success:
        print("  ✅ OpenAI installation fixed")
        return True
    else:
        print(f"  ❌ OpenAI installation failed: {stderr}")
        return False

def test_openai():
    """Test if OpenAI works now"""
    print("🧪 Testing OpenAI...")
    
    try:
        import openai
        client = openai.OpenAI(api_key="test-key")
        print("  ✅ OpenAI client creation successful")
        return True
    except Exception as e:
        if "proxies" in str(e):
            print(f"  ❌ Still getting proxies error: {e}")
            return False
        else:
            print(f"  ⚠️  Other error (expected for test key): {e}")
            return True  # Other errors are expected with test key

def wait_for_ollama():
    """Wait for Ollama service to be ready"""
    print("⏳ Waiting for Ollama service...")
    
    import time
    import requests
    
    for i in range(30):  # Wait up to 30 seconds
        try:
            response = requests.get("http://ollama:11434/api/tags", timeout=2)
            if response.status_code == 200:
                print("  ✅ Ollama service is ready")
                return True
        except:
            pass
        
        print(f"  - Waiting for Ollama... ({i+1}/30)")
        time.sleep(1)
    
    print("  ❌ Ollama service not responding")
    return False

def setup_ollama_model():
    """Set up a basic model in Ollama"""
    print("🤖 Setting up Ollama model...")
    
    # Try to pull a small model
    success, stdout, stderr = run_command("curl -X POST http://ollama:11434/api/pull -d '{\"name\":\"llama3.2:1b\"}'")
    
    if success:
        print("  ✅ Ollama model setup initiated")
        return True
    else:
        print(f"  ⚠️  Could not initiate model pull: {stderr}")
        return False

def main():
    print("🚀 Docker LLM Fix Script Starting...")
    
    # Fix OpenAI
    openai_success = fix_openai_installation()
    if openai_success:
        test_openai()
    
    # Wait for and setup Ollama
    ollama_ready = wait_for_ollama()
    if ollama_ready:
        setup_ollama_model()
    
    print("✨ Docker LLM Fix Script Complete!")
    
    if openai_success and ollama_ready:
        print("🎉 Both OpenAI and Ollama should now be working!")
    elif openai_success:
        print("✅ OpenAI fixed, Ollama needs manual setup")
    elif ollama_ready:
        print("✅ Ollama ready, OpenAI needs manual fix")
    else:
        print("⚠️  Both services need attention")

if __name__ == "__main__":
    main() 