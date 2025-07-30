#!/usr/bin/env python3
import os
import sys

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
import django
django.setup()

print("🧪 Testing Puppeteer in Container")
print("=" * 40)

try:
    from services.puppeteer_export_service import PuppeteerExportService
    print("✅ Puppeteer service imported successfully")
    
    service = PuppeteerExportService()
    print("✅ Puppeteer service created successfully")
    print(f"Base URL: {service.base_url}")
    print(f"Node path: {service.node_path}")
    
    # Test Node.js availability
    import subprocess
    result = subprocess.run(['node', '--version'], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"✅ Node.js available: {result.stdout.strip()}")
    else:
        print(f"❌ Node.js not available: {result.stderr}")
    
    # Test Puppeteer availability
    result = subprocess.run(['node', '-e', 'console.log(require("puppeteer").default || "Puppeteer available")'], 
                          capture_output=True, text=True)
    if result.returncode == 0:
        print("✅ Puppeteer module available")
    else:
        print(f"❌ Puppeteer not available: {result.stderr}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc() 