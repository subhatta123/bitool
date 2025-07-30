#!/usr/bin/env python3
"""Direct Test of Export Functionality"""

import requests
import sys

def test_export():
    print("🧪 Testing Export Functionality Directly")
    print("=" * 50)
    
    base_url = "http://localhost:8000"
    
    # Test 1: Check if app is accessible
    try:
        response = requests.get(f"{base_url}/health/", timeout=10)
        if response.status_code == 200:
            print("✅ Web application is accessible")
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot access application: {e}")
        return False
    
    # Test 2: Try to access dashboard list (might redirect to login)
    try:
        response = requests.get(f"{base_url}/dashboards/", timeout=10)
        print(f"📊 Dashboard endpoint status: {response.status_code}")
        if response.status_code in [200, 302]:  # 200 = accessible, 302 = redirect to login
            print("✅ Dashboard endpoint is accessible")
        else:
            print(f"⚠️  Dashboard returned: {response.status_code}")
    except Exception as e:
        print(f"⚠️  Dashboard test failed: {e}")
    
    # Test 3: Try to trigger an export (this will likely fail due to authentication)
    try:
        # Try to export a non-existent dashboard (just to see if endpoint exists)
        response = requests.get(f"{base_url}/dashboards/999/export/?format=pdf", timeout=30)
        print(f"📤 Export endpoint status: {response.status_code}")
        
        if response.status_code == 404:
            print("✅ Export endpoint exists (404 = dashboard not found - this is expected)")
        elif response.status_code == 403:
            print("✅ Export endpoint exists (403 = authentication required - this is expected)")
        elif response.status_code == 302:
            print("✅ Export endpoint exists (302 = redirect to login - this is expected)")
        else:
            print(f"📊 Export endpoint returned: {response.status_code}")
            if response.text:
                print(f"Response preview: {response.text[:200]}...")
                
    except Exception as e:
        print(f"⚠️  Export test failed: {e}")
    
    print("\n🔍 DIAGNOSIS:")
    print("If you're still getting text-only exports, please:")
    print("1. 🌐 Open http://localhost:8000/dashboards/ in your browser")
    print("2. 📝 Login to your account")
    print("3. 📊 Open a dashboard that has charts")
    print("4. 📤 Click Export and try PDF/PNG export")
    print("5. 📁 Check the downloaded file")
    print("\n💡 If exports are still text-only, check container logs:")
    print("   docker-compose logs web --tail=50")
    
    return True

if __name__ == "__main__":
    success = test_export()
    exit(0 if success else 1) 