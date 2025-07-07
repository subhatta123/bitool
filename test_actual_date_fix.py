#!/usr/bin/env python
"""
Test script using actual date formats from your Superstore data
"""

import pandas as pd
import numpy as np
import re

def test_actual_date_conversion():
    """Test date conversion with your actual data formats"""
    
    print("🧪 TESTING DATE CONVERSION WITH ACTUAL DATA FORMATS")
    print("=" * 60)
    
    # Your actual date formats from the CSV
    test_data = pd.DataFrame({
        'Order Date': [
            '08-11-2016',  # DD-MM-YYYY
            '12-06-2016',  # DD-MM-YYYY  
            '11-10-2015',  # DD-MM-YYYY
            '09-06-2014',  # DD-MM-YYYY
            '15-04-2017',  # DD-MM-YYYY
            '31-05-2015',  # DD-MM-YYYY (proves it's DD-MM-YYYY since no 31st month)
            '27-08-2014',  # DD-MM-YYYY
            'invalid-date', # Invalid
            '05-12-2016',  # DD-MM-YYYY
            '19-09-2017'   # DD-MM-YYYY
        ],
        'Ship Date': [
            '11-11-2016',  # DD-MM-YYYY
            '16-06-2016',  # DD-MM-YYYY
            '18-10-2015',  # DD-MM-YYYY
            '14-06-2014',  # DD-MM-YYYY
            '20-04-2017',  # DD-MM-YYYY
            '02-06-2015',  # DD-MM-YYYY
            '01-09-2014',  # DD-MM-YYYY
            'bad-date',    # Invalid
            '10-12-2016',  # DD-MM-YYYY
            '23-09-2017'   # DD-MM-YYYY
        ]
    })
    
    print("📊 Test Data:")
    print(test_data.head())
    print()
    
    def smart_date_conversion_fixed(series, column_name="column"):
        """Fixed date conversion logic that matches your ETL function"""
        
        print(f"🔧 Converting {column_name}")
        print("-" * 30)
        
        original_data = series.copy()
        original_count = len(original_data.dropna())
        
        # Try formats in order of likelihood for your data
        formats_to_try = [
            ('%d-%m-%Y', 'DD-MM-YYYY'),  # Most likely for your data
            ('%d/%m/%Y', 'DD/MM/YYYY'),
            ('%m/%d/%Y', 'MM/DD/YYYY'),  # US format
            ('%Y-%m-%d', 'YYYY-MM-DD'),  # ISO format
            ('%m-%d-%Y', 'MM-DD-YYYY'),
            (None, 'Auto-detect')  # Let pandas infer
        ]
        
        best_conversion = None
        best_format = None
        best_success_count = 0
        
        for fmt, description in formats_to_try:
            try:
                if fmt is None:
                    test_converted = pd.to_datetime(original_data, errors='coerce')
                else:
                    test_converted = pd.to_datetime(original_data, format=fmt, errors='coerce')
                
                # Count successful conversions (non-NaT values)
                success_count = len(test_converted.dropna())
                success_rate = (success_count / original_count) * 100 if original_count > 0 else 0
                
                print(f"  {description:15} ({fmt or 'auto':12}): {success_count:2}/{original_count} ({success_rate:5.1f}%)")
                
                if success_count > best_success_count:
                    best_conversion = test_converted
                    best_format = description
                    best_success_count = success_count
                
                # If we got >80% success, use this format
                if success_count >= original_count * 0.8:
                    print(f"  ✅ Using {description} (good success rate)")
                    break
                    
            except Exception as e:
                print(f"  ❌ {description:15}: ERROR - {e}")
                continue
        
        # Use the best conversion we found
        if best_conversion is None:
            print(f"  ❌ All formats failed!")
            return None, 0
        
        # Calculate final success rate
        failed_count = len(best_conversion) - len(best_conversion.dropna())
        success_rate = (len(best_conversion.dropna()) / len(best_conversion)) * 100 if len(best_conversion) > 0 else 0
        
        print(f"  🎯 Best: {best_format} with {success_rate:.1f}% success")
        print(f"  📊 Results: {len(best_conversion.dropna())}/{len(best_conversion)} successful, {failed_count} failed")
        
        # Show successful conversions
        if len(best_conversion.dropna()) > 0:
            successful_samples = best_conversion.dropna().head(3).tolist()
            print(f"  ✅ Sample results: {[str(d)[:10] for d in successful_samples]}")
        
        # Show failed values
        if failed_count > 0:
            failed_mask = best_conversion.isna()
            failed_values = original_data[failed_mask].tolist()
            print(f"  ❌ Failed values: {failed_values}")
        
        return best_conversion, success_rate
    
    # Test both date columns
    for col in ['Order Date', 'Ship Date']:
        result, success_rate = smart_date_conversion_fixed(test_data[col], col)
        print()
        
        # Apply the conversion if successful
        if result is not None and success_rate >= 50:
            test_data[f'{col}_converted'] = result
            print(f"✅ CONVERSION SUCCESSFUL: {col} converted with {success_rate:.1f}% success rate")
        else:
            print(f"❌ CONVERSION FAILED: {col} - success rate too low ({success_rate:.1f}%)")
        
        print("=" * 50)
    
    # Show final results
    print("\n📋 FINAL RESULTS:")
    print("=" * 20)
    
    for col in ['Order Date', 'Ship Date']:
        conv_col = f'{col}_converted'
        if conv_col in test_data.columns:
            print(f"\n{col}:")
            print(f"  Original: {test_data[col].tolist()}")
            print(f"  Converted: {[str(d)[:10] if pd.notna(d) else 'NaT' for d in test_data[conv_col]]}")
            
            # Verify the conversion worked
            non_null_converted = test_data[conv_col].dropna()
            if len(non_null_converted) > 0:
                print(f"  ✅ Sample converted dates: {[str(d)[:10] for d in non_null_converted.head(3)]}")
                print(f"  📊 Success: {len(non_null_converted)}/{len(test_data)} values converted")
    
    print("\n💡 CONCLUSIONS:")
    print("- DD-MM-YYYY format is correct for your data")
    print("- The conversion logic should work with proper format detection")
    print("- Invalid values correctly become NaT (Not a Time)")
    print("- Success rates should be >80% for your actual data")

if __name__ == "__main__":
    test_actual_date_conversion() 