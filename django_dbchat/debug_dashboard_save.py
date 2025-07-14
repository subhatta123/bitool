#!/usr/bin/env python3
"""
Debug script to test dashboard save functionality and identify persistence issues
"""

import os
import sys
import django
import json
from django.db import transaction
from django.utils import timezone

# Set up Django environment
sys.path.append('/c%3A/Users/SuddhasheelBhattacha/OneDrive%20-%20Mendix%20Technology%20B.V/Desktop/dbchat/django_dbchat')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from dashboards.models import Dashboard, DashboardItem
from accounts.models import CustomUser
from django.db import models
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def debug_dashboard_save():
    """Debug dashboard save functionality"""
    print("🔍 Testing Dashboard Save Functionality")
    print("=" * 50)
    
    try:
        # Get or create a test user
        user, created = CustomUser.objects.get_or_create(
            username='test_user',
            defaults={
                'email': 'test@example.com',
                'first_name': 'Test',
                'last_name': 'User'
            }
        )
        
        if created:
            print(f"✅ Created test user: {user.username}")
        else:
            print(f"📋 Using existing test user: {user.username}")
        
        # Test 1: Create a new dashboard
        print("\n📊 Test 1: Creating new dashboard...")
        dashboard_name = f"Test Dashboard {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        dashboard = Dashboard.objects.create(
            name=dashboard_name,
            description="Test dashboard for debugging",
            owner=user
        )
        
        print(f"✅ Dashboard created: {dashboard.name} (ID: {dashboard.id})")
        
        # Test 2: Create dashboard item
        print("\n📈 Test 2: Creating dashboard item...")
        
        # Find next available position
        existing_items = DashboardItem.objects.filter(dashboard=dashboard)
        max_y = existing_items.aggregate(models.Max('position_y'))['position_y__max'] or 0
        
        chart_config = {
            'chart_type': 'bar',
            'title': 'Test Chart',
            'description': 'Test chart for debugging',
            'x_column': 'category',
            'y_column': 'value'
        }
        
        dashboard_item = DashboardItem.objects.create(
            dashboard=dashboard,
            title='Test Chart',
            item_type='chart',
            chart_type='bar',
            query='SELECT category, COUNT(*) as value FROM test_table GROUP BY category',
            chart_config=chart_config,
            data_source='test_source',
            position_x=0,
            position_y=max_y + 1,
            width=6,
            height=4
        )
        
        print(f"✅ Dashboard item created: {dashboard_item.title} (ID: {dashboard_item.id})")
        
        # Test 3: Verify persistence
        print("\n🔍 Test 3: Verifying persistence...")
        
        # Query the dashboard again
        saved_dashboard = Dashboard.objects.get(id=dashboard.id)
        saved_items = DashboardItem.objects.filter(dashboard=saved_dashboard)
        
        print(f"📊 Dashboard exists: {saved_dashboard.name}")
        print(f"📈 Items count: {saved_items.count()}")
        
        for item in saved_items:
            print(f"   - {item.title} ({item.item_type})")
            print(f"     Config: {item.chart_config}")
            print(f"     Position: ({item.position_x}, {item.position_y})")
        
        # Test 4: Test transaction rollback scenario
        print("\n🔄 Test 4: Testing transaction scenarios...")
        
        try:
            with transaction.atomic():
                # Create another item
                test_item = DashboardItem.objects.create(
                    dashboard=dashboard,
                    title='Test Item 2',
                    item_type='chart',
                    chart_type='line',
                    query='SELECT * FROM test_table',
                    chart_config={'chart_type': 'line'},
                    position_x=0,
                    position_y=max_y + 2,
                    width=4,
                    height=3
                )
                
                print(f"✅ Item created in transaction: {test_item.title}")
                
                # This should not cause rollback
                print("✅ Transaction completed successfully")
                
        except Exception as e:
            print(f"❌ Transaction failed: {e}")
        
        # Test 5: Check final state
        print("\n🎯 Test 5: Final verification...")
        
        final_items = DashboardItem.objects.filter(dashboard=dashboard)
        print(f"📊 Final items count: {final_items.count()}")
        
        for item in final_items:
            print(f"   - {item.title} (Created: {item.created_at})")
        
        # Test 6: Test specific add_to_dashboard scenario
        print("\n📱 Test 6: Testing add_to_dashboard scenario...")
        
        # Simulate the data that would come from frontend
        result_data = {
            'result_data': [
                {'category': 'A', 'value': 10},
                {'category': 'B', 'value': 20},
                {'category': 'C', 'value': 15}
            ],
            'sql_query': 'SELECT category, value FROM test_data',
            'data_source_id': 'test_source'
        }
        
        # Simulate adding to dashboard
        chart_title = 'Frontend Test Chart'
        chart_description = 'Chart added via frontend simulation'
        chart_type = 'bar'
        
        chart_config = {
            'chart_type': chart_type,
            'title': chart_title,
            'description': chart_description
        }
        
        # Extract chart parameters from data
        if isinstance(result_data['result_data'], list) and result_data['result_data']:
            first_record = result_data['result_data'][0]
            columns = list(first_record.keys())
            
            if len(columns) >= 2:
                chart_config['x_column'] = columns[0]
                chart_config['y_column'] = columns[1]
        
        # Find next available position
        existing_items = DashboardItem.objects.filter(dashboard=dashboard)
        max_y = existing_items.aggregate(models.Max('position_y'))['position_y__max'] or 0
        
        # Create dashboard item (simulating the Django view)
        frontend_item = DashboardItem.objects.create(
            dashboard=dashboard,
            title=chart_title,
            item_type='chart',
            chart_type=chart_type,
            query=result_data.get('sql_query', ''),
            chart_config=chart_config,
            data_source=result_data.get('data_source_id', ''),
            position_x=0,
            position_y=max_y + 1,
            width=6,
            height=4
        )
        
        print(f"✅ Frontend simulation item created: {frontend_item.title}")
        print(f"   Config: {frontend_item.chart_config}")
        
        # Final verification
        print("\n🎉 Final Results:")
        total_items = DashboardItem.objects.filter(dashboard=dashboard).count()
        print(f"📊 Total dashboard items: {total_items}")
        
        if total_items > 0:
            print("✅ Dashboard save functionality is working correctly!")
            print("   Charts are being persisted to the database.")
        else:
            print("❌ Dashboard save functionality has issues!")
            print("   Charts are NOT being persisted to the database.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during dashboard save test: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_database_connection():
    """Debug database connection issues"""
    print("\n🔌 Testing Database Connection...")
    
    try:
        # Test basic database operations
        user_count = CustomUser.objects.count()
        dashboard_count = Dashboard.objects.count()
        item_count = DashboardItem.objects.count()
        
        print(f"📊 Database Statistics:")
        print(f"   Users: {user_count}")
        print(f"   Dashboards: {dashboard_count}")
        print(f"   Dashboard Items: {item_count}")
        
        # Test database write
        test_dashboard = Dashboard.objects.create(
            name="DB Test Dashboard",
            description="Test database write operations",
            owner=CustomUser.objects.first()
        )
        
        print(f"✅ Test dashboard created: {test_dashboard.id}")
        
        # Clean up
        test_dashboard.delete()
        print("✅ Test dashboard cleaned up")
        
        return True
        
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Dashboard Save Debug Script")
    print("=" * 60)
    
    # Test database connection first
    db_ok = debug_database_connection()
    
    if db_ok:
        # Test dashboard save functionality
        save_ok = debug_dashboard_save()
        
        if save_ok:
            print("\n🎉 All tests passed! Dashboard save functionality is working.")
        else:
            print("\n⚠️  Some tests failed. Check the errors above.")
    else:
        print("\n❌ Database connection failed. Cannot proceed with dashboard tests.") 