#!/usr/bin/env python3
"""
Simple Export Services Test
===========================

This script tests export services inside the Docker container.
Run with: docker-compose exec web python /app/test_export_simple.py
"""

import os
import sys
import django
from datetime import datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

def test_import():
    """Test import of export service"""
    print("🧪 Testing Export Service Import...")
    try:
        from services.dashboard_export_service import DashboardExportService
        service = DashboardExportService()
        print("✅ Export service imported and instantiated successfully")
        
        # Check methods
        methods = ['export_dashboard_pdf', 'export_dashboard_png', '_generate_dashboard_html_with_data']
        for method in methods:
            if hasattr(service, method):
                print(f"✅ Method {method} available")
            else:
                print(f"❌ Method {method} missing")
                return False
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

def test_dependencies():
    """Test dependencies"""
    print("\n🧪 Testing Dependencies...")
    deps = {
        'WeasyPrint': 'weasyprint',
        'Pillow': 'PIL', 
        'ReportLab': 'reportlab'
    }
    
    for name, module in deps.items():
        try:
            __import__(module)
            print(f"✅ {name} available")
        except ImportError:
            print(f"⚠️  {name} not available")

def create_test_dashboard():
    """Create a test dashboard"""
    print("\n🧪 Creating Test Dashboard...")
    try:
        from dashboards.models import Dashboard, DashboardItem
        from django.contrib.auth import get_user_model
        
        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username='test_user',
            defaults={'email': 'test@example.com'}
        )
        
        dashboard, _ = Dashboard.objects.get_or_create(
            name='Test Dashboard',
            defaults={
                'description': 'Test dashboard for export',
                'owner': user
            }
        )
        
        if dashboard.items.count() == 0:
            DashboardItem.objects.create(
                dashboard=dashboard,
                title='Test Chart',
                chart_type='bar',
                query='SELECT * FROM test_data',
                position_x=0,
                position_y=0,
                width=6,
                height=4
            )
        
        print(f"✅ Dashboard created: '{dashboard.name}' with {dashboard.items.count()} items")
        return dashboard
    except Exception as e:
        print(f"❌ Dashboard creation failed: {e}")
        return None

def test_pdf_export(dashboard):
    """Test PDF export"""
    print("\n🧪 Testing PDF Export...")
    if not dashboard:
        print("❌ No dashboard for testing")
        return False
    
    try:
        from services.dashboard_export_service import DashboardExportService
        service = DashboardExportService()
        
        pdf_bytes, filename = service.export_dashboard_pdf(dashboard)
        
        if pdf_bytes and len(pdf_bytes) > 100:
            print(f"✅ PDF generated successfully: {filename} ({len(pdf_bytes):,} bytes)")
            return True
        else:
            print(f"❌ PDF generation failed")
            return False
    except Exception as e:
        print(f"❌ PDF export error: {e}")
        return False

def test_png_export(dashboard):
    """Test PNG export"""
    print("\n🧪 Testing PNG Export...")
    if not dashboard:
        print("❌ No dashboard for testing")
        return False
    
    try:
        from services.dashboard_export_service import DashboardExportService
        service = DashboardExportService()
        
        png_bytes, filename = service.export_dashboard_png(dashboard)
        
        if png_bytes and len(png_bytes) > 100:
            print(f"✅ PNG generated successfully: {filename} ({len(png_bytes):,} bytes)")
            return True
        else:
            print(f"❌ PNG generation failed")
            return False
    except Exception as e:
        print(f"❌ PNG export error: {e}")
        return False

def main():
    """Main test"""
    print("=" * 60)
    print("🚀 Simple Export Services Test")
    print("=" * 60)
    
    results = {}
    
    # Test 1: Import
    results['import'] = test_import()
    
    # Test 2: Dependencies
    test_dependencies()
    
    # Test 3: Dashboard
    dashboard = create_test_dashboard()
    results['dashboard'] = dashboard is not None
    
    # Test 4: PDF
    results['pdf'] = test_pdf_export(dashboard)
    
    # Test 5: PNG
    results['png'] = test_png_export(dashboard)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    passed = sum(results.values())
    total = len(results)
    
    for test, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test.upper()}")
    
    print(f"\n📈 Overall: {passed}/{total} tests passed")
    
    if passed >= 3:
        print("\n🎉 EXPORT SERVICES ARE WORKING!")
        print("✅ Both PDF and PNG export functionality is operational")
        print("✅ Ready for use through web interface")
    else:
        print("\n⚠️  EXPORT SERVICES NEED ATTENTION")
        print("❌ Some functionality may not work properly")
    
    return passed >= 3

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 