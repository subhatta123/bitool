#!/usr/bin/env python3
"""
Debug Date Format Issue
Investigate why CAST("Order_Date" AS DATE) fails with DD-MM-YYYY format
"""

import os
import sys
import django

# Set up Django environment
sys.path.append('/c%3A/Users/SuddhasheelBhattacha/OneDrive%20-%20Mendix%20Technology%20B.V/Desktop/dbchat/django_dbchat')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from services.integration_service import DataIntegrationService

def debug_date_format_issue():
    """Debug the date format casting issue"""
    print("🔍 Debugging Date Format Issue")
    print("=" * 60)
    
    integration_service = DataIntegrationService()
    
    # Check the problematic table
    table_name = 'source_id_360d52b4_dd6d_4317_b980_6bbb4c894a22'
    
    try:
        print(f"📊 Checking table: {table_name}")
        
        # Check schema
        schema = integration_service.integrated_db.execute(f'DESCRIBE "{table_name}"').fetchall()
        print("\n📋 Schema:")
        date_columns = []
        for row in schema:
            col_name, col_type = row[0], row[1]
            if 'date' in col_name.lower():
                date_columns.append((col_name, col_type))
                print(f"   🗓️  {col_name}: {col_type} ← DATE COLUMN")
            else:
                print(f"   📋 {col_name}: {col_type}")
        
        # Check actual date values
        print(f"\n📅 Sample date values:")
        for col_name, col_type in date_columns:
            print(f"\n   Column: {col_name} ({col_type})")
            try:
                date_sample = integration_service.integrated_db.execute(f'SELECT "{col_name}" FROM "{table_name}" LIMIT 5').fetchall()
                for i, row in enumerate(date_sample, 1):
                    value = row[0]
                    print(f"     Row {i}: '{value}' (Python type: {type(value).__name__})")
            except Exception as e:
                print(f"     Error sampling: {e}")
        
        # Test different date conversion approaches
        print(f"\n🧪 Testing Date Conversion Methods:")
        test_approaches = [
            ("Simple CAST", "CAST(\"Order_Date\" AS DATE)"),
            ("STRPTIME DD-MM-YYYY", "STRPTIME(\"Order_Date\", '%d-%m-%Y')"),
            ("STRPTIME MM-DD-YYYY", "STRPTIME(\"Order_Date\", '%m-%d-%Y')"),
            ("TRY_CAST", "TRY_CAST(\"Order_Date\" AS DATE)"),
        ]
        
        for approach_name, sql_expr in test_approaches:
            try:
                test_sql = f'SELECT {sql_expr} as converted_date FROM "{table_name}" LIMIT 2'
                print(f"\n   Testing {approach_name}:")
                print(f"     SQL: {sql_expr}")
                
                result = integration_service.integrated_db.execute(test_sql).fetchall()
                print(f"     ✅ SUCCESS:")
                for i, row in enumerate(result, 1):
                    converted_value = row[0]
                    print(f"       Row {i}: {converted_value} (type: {type(converted_value).__name__})")
                    
                # If this approach works, suggest it as the solution
                if approach_name != "Simple CAST":
                    print(f"     💡 SOLUTION FOUND: Use {sql_expr} instead of CAST")
                    return sql_expr
                    
            except Exception as e:
                print(f"     ❌ FAILED: {str(e)[:100]}...")
        
        print(f"\n❌ No working date conversion method found")
        return None
        
    except Exception as e:
        print(f"❌ Error checking table: {e}")
        return None

if __name__ == "__main__":
    working_method = debug_date_format_issue()
    print("=" * 60)
    if working_method:
        print(f"🎯 SOLUTION: Use {working_method} for date conversion")
        print("💡 Need to update SQL fixer to use this method")
    else:
        print("⚠️  No working date conversion method found")
        print("💡 May need to fix the stored date format or use different approach") 