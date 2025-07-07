#!/usr/bin/env python
"""
Test script to debug PDF export database connectivity
"""

import os
import sys
import django

# Add the Django project to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')

django.setup()

def test_database_paths():
    """Test various database paths"""
    from django.conf import settings
    
    possible_paths = [
        'data/integrated.duckdb',
        'django_dbchat/data/integrated.duckdb',
        os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb'),
        '../data/integrated.duckdb',
        'data_integration_storage/integrated_data.db'
    ]
    
    print("=== Testing Database Paths ===")
    print(f"Current working directory: {os.getcwd()}")
    print(f"BASE_DIR: {getattr(settings, 'BASE_DIR', 'Not set')}")
    
    for path in possible_paths:
        full_path = os.path.abspath(path)
        exists = os.path.exists(path)
        print(f"Path: {path}")
        print(f"  Full path: {full_path}")
        print(f"  Exists: {exists}")
        if exists:
            print(f"  Size: {os.path.getsize(path)} bytes")
        print()

def test_dashboard_items():
    """Test fetching dashboard items"""
    from dashboards.models import Dashboard, DashboardItem
    
    print("=== Testing Dashboard Items ===")
    
    dashboards = Dashboard.objects.all()
    print(f"Found {dashboards.count()} dashboards")
    
    for dashboard in dashboards:
        print(f"\nDashboard: {dashboard.name}")
        items = DashboardItem.objects.filter(dashboard=dashboard)
        print(f"  Items: {items.count()}")
        
        for item in items:
            print(f"    - {item.title}")
            print(f"      Type: {item.item_type}")
            print(f"      Query: {item.query[:100]}{'...' if len(item.query) > 100 else ''}")
            print(f"      Data Source: {item.data_source}")

def test_duckdb_connection():
    """Test DuckDB connection"""
    try:
        import duckdb
        print("=== Testing DuckDB Connection ===")
        
        # Try to find the database
        db_paths = [
            'data/integrated.duckdb',
            '../data/integrated.duckdb',
            'data_integration_storage/integrated_data.db'
        ]
        
        db_path = None
        for path in db_paths:
            if os.path.exists(path):
                db_path = path
                print(f"Found database at: {path}")
                break
        
        if not db_path:
            print("No database found!")
            return False
        
        # Connect and test
        conn = duckdb.connect(db_path)
        try:
            # List tables
            tables = conn.execute("SHOW TABLES").fetchall()
            print(f"Tables in database: {len(tables)}")
            for table in tables:
                print(f"  - {table[0]}")
            
            # Test a simple query
            if tables:
                table_name = tables[0][0]
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                print(f"Rows in {table_name}: {result[0]}")
                
                # Show sample data
                sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                print(f"Sample data from {table_name}:")
                for row in sample:
                    print(f"  {row}")
                
                return True
        finally:
            conn.close()
            
    except ImportError:
        print("DuckDB not installed!")
        return False
    except Exception as e:
        print(f"DuckDB connection failed: {e}")
        return False

def test_export_service():
    """Test the export service directly"""
    try:
        from services.dashboard_export_service import DashboardExportService
        from dashboards.models import Dashboard, DashboardItem
        
        print("=== Testing Export Service ===")
        
        # Get a dashboard
        dashboard = Dashboard.objects.first()
        if not dashboard:
            print("No dashboards found!")
            return
        
        print(f"Testing with dashboard: {dashboard.name}")
        
        # Get dashboard items
        items = list(DashboardItem.objects.filter(dashboard=dashboard).values())
        print(f"Dashboard items: {len(items)}")
        
        if items:
            export_service = DashboardExportService()
            
            # Test data processing for first item
            item = items[0]
            print(f"Testing item: {item['title']}")
            print(f"Query: {item['query']}")
            
            result = export_service._process_item_data(item)
            print(f"Query result: {len(result) if result else 0} rows")
            if result:
                print(f"Sample data: {result[0] if result else 'None'}")
            
    except Exception as e:
        print(f"Export service test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("PDF Export Debug Script")
    print("=" * 50)
    
    test_database_paths()
    test_dashboard_items()
    test_duckdb_connection()
    test_export_service()
    
    print("\nDebug completed!") 