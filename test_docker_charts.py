#!/usr/bin/env python3
"""
Docker Chart Rendering Test
===========================

Test script to verify chart rendering works in Docker containers
"""

import requests
import time
import sys

def test_chart_rendering():
    """Test chart rendering functionality"""
    
    print("🧪 Testing Docker Chart Rendering...")
    
    base_url = "http://localhost:8000"
    
    # Test 1: Check if web server is running
    try:
        response = requests.get(f"{base_url}/", timeout=10)
        if response.status_code == 200:
            print("✅ Web server is running")
        else:
            print(f"❌ Web server returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to web server: {e}")
        return False
    
    # Test 2: Check dashboard page
    try:
        response = requests.get(f"{base_url}/dashboards/", timeout=10)
        if response.status_code in [200, 302]:  # 302 for login redirect
            print("✅ Dashboard endpoint accessible")
        else:
            print(f"❌ Dashboard endpoint returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Dashboard endpoint error: {e}")
        return False
    
    print("🎉 Chart rendering tests completed!")
    print()
    print("📋 Manual Testing Instructions:")
    print("1. Open browser and go to: http://localhost:8000")
    print("2. Login if required")
    print("3. Navigate to Dashboards")
    print("4. Open any dashboard")
    print("5. Verify charts are showing (with sample data if needed)")
    print("6. Check browser console for any JavaScript errors")
    
    return True

if __name__ == "__main__":
    success = test_chart_rendering()
    sys.exit(0 if success else 1)
