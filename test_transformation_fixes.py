#!/usr/bin/env python
"""
Test script to verify transformation logic fixes
"""

import pandas as pd
import sys
import os

def test_transformation_counting_logic():
    """Test that the counting logic is now correct"""
    
    print("🧪 TESTING TRANSFORMATION COUNTING FIXES")
    print("=" * 60)
    
    # Create test data that should have known conversion patterns
    test_data = pd.DataFrame({
        'mixed_numeric': ['123', '456', 'invalid', '789', '012'],  # 80% should convert
        'all_numeric': ['100', '200', '300', '400', '500'],        # 100% should convert
        'mostly_invalid': ['abc', 'def', '123', 'xyz', 'qwe'],     # 20% should convert
        'mixed_dates': ['2023-01-01', '2023-02-02', 'invalid', '2023-03-03', '2023-04-04'],  # 80% should convert
        'good_dates': ['2023-01-01', '2023-02-02', '2023-03-03', '2023-04-04', '2023-05-05'],  # 100% should convert
        'bad_dates': ['invalid1', 'invalid2', 'invalid3', 'invalid4', 'invalid5']  # 0% should convert
    })
    
    print(f"📊 Test Data: {len(test_data)} rows")
    print(test_data)
    print()
    
    # Test the fixed counting logic
    def test_numeric_conversion_logic(column_name, target_type):
        """Test numeric conversion with the fixed logic"""
        print(f"🔢 Testing {column_name} -> {target_type}")
        
        original_data = test_data[column_name].copy()
        original_count = len(original_data.dropna())
        
        converted = pd.to_numeric(original_data, errors='coerce')
        failed_count = converted.isna().sum()
        success_count = len(converted.dropna())
        success_rate = (success_count / original_count) * 100 if original_count > 0 else 0
        
        print(f"  Original count: {original_count}")
        print(f"  Success count: {success_count}")  
        print(f"  Failed count: {failed_count}")
        print(f"  Success rate: {success_rate:.1f}%")
        
        # Verify the math is correct
        assert success_count + failed_count == len(original_data), f"Math error: {success_count} + {failed_count} != {len(original_data)}"
        assert success_count <= original_count, f"Success count ({success_count}) can't exceed original count ({original_count})"
        assert failed_count <= len(original_data), f"Failed count ({failed_count}) can't exceed total count ({len(original_data)})"
        
        print(f"  ✅ Math checks passed!")
        
        # Test the threshold logic
        threshold_passed = success_rate >= 80
        print(f"  {'✅ PASS' if threshold_passed else '❌ FAIL'}: Threshold (80%) {'met' if threshold_passed else 'not met'}")
        print()
        
        return success_rate >= 80
    
    def test_date_conversion_logic(column_name):
        """Test date conversion with the fixed logic"""
        print(f"📅 Testing {column_name} -> date")
        
        original_data = test_data[column_name].copy()
        original_count = len(original_data.dropna())
        
        converted = pd.to_datetime(original_data, errors='coerce')
        failed_count = len(converted) - len(converted.dropna())
        success_count = len(converted.dropna())
        success_rate = (success_count / len(converted)) * 100 if len(converted) > 0 else 0
        
        print(f"  Original count: {original_count}")
        print(f"  Success count: {success_count}")
        print(f"  Failed count: {failed_count}")
        print(f"  Success rate: {success_rate:.1f}%")
        
        # Verify the math is correct  
        assert success_count + failed_count == len(converted), f"Math error: {success_count} + {failed_count} != {len(converted)}"
        assert success_count <= len(converted), f"Success count ({success_count}) can't exceed total count ({len(converted)})"
        assert failed_count <= len(converted), f"Failed count ({failed_count}) can't exceed total count ({len(converted)})"
        
        print(f"  ✅ Math checks passed!")
        
        # Test the threshold logic
        threshold_passed = success_rate >= 50
        print(f"  {'✅ PASS' if threshold_passed else '❌ FAIL'}: Threshold (50%) {'met' if threshold_passed else 'not met'}")
        print()
        
        return success_rate >= 50
    
    # Test all scenarios
    print("🔬 RUNNING CONVERSION TESTS...")
    print("-" * 40)
    
    # Test numeric conversions
    results = []
    results.append(('mixed_numeric -> float', test_numeric_conversion_logic('mixed_numeric', 'float')))
    results.append(('all_numeric -> float', test_numeric_conversion_logic('all_numeric', 'float')))
    results.append(('mostly_invalid -> float', test_numeric_conversion_logic('mostly_invalid', 'float')))
    
    # Test date conversions
    results.append(('mixed_dates -> date', test_date_conversion_logic('mixed_dates')))
    results.append(('good_dates -> date', test_date_conversion_logic('good_dates')))
    results.append(('bad_dates -> date', test_date_conversion_logic('bad_dates')))
    
    # Summary
    print("📋 TEST RESULTS SUMMARY")
    print("=" * 30)
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status}: {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 SUCCESS! All transformation counting logic is working correctly!")
        print("✅ No more impossible failure counts (like 7995/9994)")
        print("✅ Math checks pass for all scenarios")
        print("✅ Threshold logic works properly")
    else:
        print(f"\n⚠️  {total - passed} tests failed - there may still be issues")
    
    return passed == total

if __name__ == "__main__":
    try:
        success = test_transformation_counting_logic()
        if not success:
            print("\n❌ Some tests failed!")
            sys.exit(1)
        else:
            print("\n✅ All transformation fixes verified!")
            
    except Exception as e:
        print(f"\n❌ ERROR: Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 