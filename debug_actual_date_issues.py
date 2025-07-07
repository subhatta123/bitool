#!/usr/bin/env python
"""
Debug actual date conversion issues in your CSV data
"""

import pandas as pd
import re

def debug_date_conversion_issues():
    print("🔍 DEBUGGING DATE CONVERSION ISSUES")
    print("=" * 50)
    
    try:
        # Load your actual CSV file
        csv_path = 'django_dbchat/media/csv_files/Sample - Superstore2_D2FNk2B.csv'
        
        print(f"📂 Loading: {csv_path}")
        df = pd.read_csv(csv_path)
        
        print(f"✅ Loaded {len(df)} rows, {len(df.columns)} columns")
        print(f"📊 Columns: {list(df.columns)}")
        
        # Focus on date columns
        date_columns = ['Order Date', 'Ship Date']
        
        for col in date_columns:
            if col in df.columns:
                print(f"\n" + "="*60)
                print(f"🔍 ANALYZING: {col}")
                print("="*60)
                
                # Get all unique date values (sample)
                unique_dates = df[col].dropna().unique()
                print(f"📊 Total unique dates: {len(unique_dates)}")
                print(f"📊 Total rows: {len(df)}")
                print(f"📊 Non-null values: {len(df[col].dropna())}")
                
                # Sample date values
                print(f"\n📅 FIRST 20 DATE VALUES:")
                for i, date_val in enumerate(df[col].dropna().head(20)):
                    print(f"  {i+1:2d}. {date_val}")
                
                # Analyze date patterns
                print(f"\n🔍 PATTERN ANALYSIS:")
                patterns = {
                    'DD-MM-YYYY': r'^\d{1,2}-\d{1,2}-\d{4}$',
                    'MM-DD-YYYY': r'^\d{1,2}-\d{1,2}-\d{4}$',  # Same regex but different interpretation
                    'DD/MM/YYYY': r'^\d{1,2}/\d{1,2}/\d{4}$',
                    'MM/DD/YYYY': r'^\d{1,2}/\d{1,2}/\d{4}$',
                    'YYYY-MM-DD': r'^\d{4}-\d{1,2}-\d{1,2}$',
                    'Other': r'.*'
                }
                
                pattern_counts = {}
                sample_values = {}
                
                for pattern_name, regex in patterns.items():
                    matching_values = []
                    for val in df[col].dropna().head(100):  # Check first 100 values
                        if re.match(regex, str(val)):
                            matching_values.append(str(val))
                    
                    pattern_counts[pattern_name] = len(matching_values)
                    sample_values[pattern_name] = matching_values[:5]
                    
                    if len(matching_values) > 0:
                        print(f"  {pattern_name:12}: {len(matching_values):3d} matches - {sample_values[pattern_name]}")
                
                # Test specific DD-MM-YYYY conversion
                print(f"\n🧪 TESTING DD-MM-YYYY CONVERSION:")
                try:
                    test_sample = df[col].dropna().head(100)
                    converted = pd.to_datetime(test_sample, format='%d-%m-%Y', errors='coerce')
                    
                    success_count = len(converted.dropna())
                    total_count = len(test_sample)
                    success_rate = (success_count / total_count) * 100
                    
                    print(f"  ✅ Sample conversion: {success_count}/{total_count} = {success_rate:.1f}%")
                    
                    if success_count > 0:
                        print(f"  📅 Successful samples: {[str(d)[:10] for d in converted.dropna().head(5)]}")
                    
                    # Show failed values
                    failed_mask = converted.isna()
                    if failed_mask.any():
                        failed_values = test_sample[failed_mask].head(10).tolist()
                        print(f"  ❌ Failed samples: {failed_values}")
                        
                        # Analyze failed values
                        print(f"\n🔍 ANALYZING FAILED VALUES:")
                        for failed_val in failed_values[:5]:
                            val_str = str(failed_val)
                            print(f"    '{val_str}' - Length: {len(val_str)}, Type: {type(failed_val)}")
                            
                            # Check for common issues
                            if val_str.strip() != val_str:
                                print(f"      ⚠️  Has whitespace!")
                            if not re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', val_str):
                                print(f"      ⚠️  Doesn't match DD-MM-YYYY pattern!")
                            
                except Exception as e:
                    print(f"  ❌ Conversion failed: {e}")
                
                # Test with different formats
                print(f"\n🔄 TESTING OTHER FORMATS:")
                formats_to_test = [
                    ('%m-%d-%Y', 'MM-DD-YYYY'),
                    ('%d/%m/%Y', 'DD/MM/YYYY'),
                    ('%m/%d/%Y', 'MM/DD/YYYY'),
                    (None, 'Auto-detect')
                ]
                
                test_sample = df[col].dropna().head(100)
                for fmt, description in formats_to_test:
                    try:
                        if fmt is None:
                            converted = pd.to_datetime(test_sample, errors='coerce')
                        else:
                            converted = pd.to_datetime(test_sample, format=fmt, errors='coerce')
                        
                        success_count = len(converted.dropna())
                        success_rate = (success_count / len(test_sample)) * 100
                        print(f"  {description:12}: {success_rate:5.1f}% ({success_count}/{len(test_sample)})")
                        
                    except Exception as e:
                        print(f"  {description:12}: ERROR - {e}")
        
        # Overall recommendation
        print(f"\n💡 RECOMMENDATIONS:")
        print("=" * 30)
        print("1. Check for data quality issues (whitespace, mixed formats)")
        print("2. Verify the actual date format in your data")
        print("3. Consider data cleaning before transformation")
        
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        print("💡 Make sure the CSV file exists and is accessible")

def test_problematic_dates():
    """Test specific problematic date formats"""
    print(f"\n🧪 TESTING PROBLEMATIC DATES")
    print("=" * 40)
    
    # Test dates that might be causing issues
    problematic_dates = [
        '08-11-2016',  # Should work
        '31-05-2015',  # Should work (proves DD-MM-YYYY)
        '13-13-2016',  # Invalid (no 13th month)
        '32-01-2016',  # Invalid (no 32nd day)
        '',            # Empty
        'null',        # String null
        '00-00-0000',  # Zeros
        '1-1-2016',    # Single digits
        '01-01-16',    # 2-digit year
    ]
    
    print("Testing specific date values:")
    for date_str in problematic_dates:
        try:
            if date_str.strip():  # Skip empty strings
                result = pd.to_datetime(date_str, format='%d-%m-%Y', errors='coerce')
                status = "✅ SUCCESS" if pd.notna(result) else "❌ FAILED"
                print(f"  '{date_str:12}' → {status} → {result}")
            else:
                print(f"  '{date_str:12}' → ❌ EMPTY")
        except Exception as e:
            print(f"  '{date_str:12}' → ❌ ERROR: {e}")

if __name__ == "__main__":
    debug_date_conversion_issues()
    test_problematic_dates() 