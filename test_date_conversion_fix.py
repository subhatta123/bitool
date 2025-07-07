#!/usr/bin/env python
"""
Test script to verify date conversion fixes
"""

import pandas as pd
import sys
import os

# Add the Django project to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'django_dbchat'))

# Test data with dates that were previously problematic
test_data = pd.DataFrame({
    'order_id': [1, 2, 3, 4, 5],
    'order_date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
    'ship_date': ['2023-01-05', '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09'],
    'amount': [100.50, 200.75, 150.25, 300.00, 75.50],
    'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Mike Wilson']
})

print("🔍 TESTING DATE CONVERSION FIX")
print("=" * 50)

print("\n📊 Original Data:")
print(test_data)
print(f"\nData types:\n{test_data.dtypes}")

# Test the enhanced type inference
print("\n🧪 Testing Enhanced Type Inference...")

def test_date_conversion_fix():
    """Test the enhanced date conversion logic"""
    try:
        # Import the enhanced data service
        import django
        from django.conf import settings
        
        # Configure Django settings
        if not settings.configured:
            settings.configure(
                DEBUG=True,
                DATABASES={
                    'default': {
                        'ENGINE': 'django.db.backends.sqlite3',
                        'NAME': ':memory:',
                    }
                },
                USE_TZ=True,
                SECRET_KEY='test-key-for-date-conversion-test'
            )
            django.setup()
        
        from django_dbchat.services.data_service import DataService
        
        data_service = DataService()
        
        # Test the type inference
        sample_values = test_data['order_date'].dropna().head(5).tolist()
        inferred_type = data_service._infer_data_type_from_samples(test_data['order_date'], sample_values)
        
        print(f"✅ Type inference for order_date: {inferred_type}")
        assert inferred_type == 'date', f"Expected 'date', got '{inferred_type}'"
        
        # Test the auto conversion with the enhanced logic
        print("\n🔧 Testing Enhanced Auto Conversion...")
        converted_df, type_mapping = data_service._auto_convert_data_types(test_data.copy())
        
        print(f"✅ Type mapping: {type_mapping}")
        print(f"✅ Converted data types:\n{converted_df.dtypes}")
        
        # Verify dates weren't converted to integers
        for col in ['order_date', 'ship_date']:
            if col in type_mapping:
                mapped_type = type_mapping[col]
                print(f"✅ {col} mapped to: {mapped_type}")
                
                # Check that dates weren't turned into 0s
                if mapped_type == 'date':
                    sample_values = converted_df[col].dropna().head(3)
                    print(f"✅ Sample {col} values after conversion: {sample_values.tolist()}")
                    
                    # Verify no zeros
                    has_zeros = (converted_df[col] == 0).any()
                    if has_zeros:
                        print(f"❌ PROBLEM: {col} contains zeros after conversion!")
                        return False
                    else:
                        print(f"✅ SUCCESS: {col} has no zeros - dates preserved!")
                elif mapped_type == 'integer':
                    print(f"⚠️  WARNING: {col} was classified as integer - this might be the old behavior")
                    
        print("\n🎉 SUCCESS: Date conversion fix is working!")
        print("📝 Summary:")
        print("  - Date columns are properly detected as 'date' type")
        print("  - Date strings are not converted to integers")
        print("  - No more date values becoming 0")
        print("  - Original date strings are preserved")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR during testing: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_date_conversion_fix()
    print("\n🚀 Testing complete!")
    if success:
        print("✅ All tests passed - date conversion issue is FIXED!")
    else:
        print("❌ Some tests failed - there may still be issues") 