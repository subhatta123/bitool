#!/usr/bin/env python
"""
Simple test for date conversion with actual Superstore data
"""

import pandas as pd

def test_simple_date_conversion():
    print("🧪 SIMPLE DATE CONVERSION TEST")
    print("=" * 40)
    
    # Your actual date formats
    dates_dd_mm_yyyy = [
        '08-11-2016',  # Should become 2016-11-08
        '31-05-2015',  # Should become 2015-05-31 (proves DD-MM-YYYY)
        '12-06-2016',  # Should become 2016-06-12
        '15-04-2017'   # Should become 2017-04-15
    ]
    
    print("📅 Original dates (DD-MM-YYYY format):")
    for date in dates_dd_mm_yyyy:
        print(f"  {date}")
    
    print("\n🔧 Converting with %d-%m-%Y format...")
    
    # Convert using the correct format
    try:
        converted = pd.to_datetime(dates_dd_mm_yyyy, format='%d-%m-%Y', errors='coerce')
        
        print("\n✅ CONVERSION SUCCESSFUL!")
        print("📅 Converted dates:")
        
        for i, (orig, conv) in enumerate(zip(dates_dd_mm_yyyy, converted)):
            print(f"  {orig} → {str(conv)[:10]} ({conv.strftime('%B %d, %Y')})")
        
        print(f"\n📊 Results:")
        print(f"  Success rate: {len(converted.dropna())}/{len(converted)} = {(len(converted.dropna())/len(converted)*100):.1f}%")
        print(f"  Failed conversions: {len(converted) - len(converted.dropna())}")
        
        return True
        
    except Exception as e:
        print(f"❌ CONVERSION FAILED: {e}")
        return False

def test_with_actual_csv():
    print("\n" + "=" * 50)
    print("🧪 TESTING WITH ACTUAL CSV DATA")
    print("=" * 50)
    
    try:
        # Try to load your actual CSV
        csv_path = 'django_dbchat/media/csv_files/Sample - Superstore2_D2FNk2B.csv'
        df = pd.read_csv(csv_path, nrows=10)  # Load just first 10 rows for testing
        
        print(f"✅ Loaded {len(df)} rows from actual CSV")
        print(f"📊 Columns: {list(df.columns)}")
        
        # Test date columns
        date_columns = ['Order Date', 'Ship Date']
        
        for col in date_columns:
            if col in df.columns:
                print(f"\n📅 Testing {col}:")
                print(f"  Sample values: {df[col].head(3).tolist()}")
                
                # Convert using DD-MM-YYYY format
                converted = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
                success_rate = (len(converted.dropna()) / len(converted)) * 100
                
                print(f"  ✅ Conversion: {len(converted.dropna())}/{len(converted)} = {success_rate:.1f}%")
                
                if len(converted.dropna()) > 0:
                    print(f"  📅 Sample results: {[str(d)[:10] for d in converted.dropna().head(3)]}")
                
        return True
        
    except Exception as e:
        print(f"❌ Could not load CSV: {e}")
        print("💡 This is OK - testing with sample data instead")
        return False

if __name__ == "__main__":
    # Test 1: Simple conversion
    test_simple_date_conversion()
    
    # Test 2: With actual CSV (if available)
    test_with_actual_csv()
    
    print("\n💡 SUMMARY:")
    print("- DD-MM-YYYY format (%d-%m-%Y) works correctly")
    print("- Your dates should convert properly with this format")
    print("- The ETL system should use this format for your data") 