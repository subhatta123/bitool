#!/usr/bin/env python
"""
Debug script to analyze actual date formats in your data
"""

import pandas as pd
import re
from datetime import datetime

def analyze_date_formats():
    """Analyze the actual date formats in your Superstore data"""
    
    print("🔍 ANALYZING DATE FORMATS IN YOUR DATA")
    print("=" * 50)
    
    # Load your actual CSV data
    try:
        # Try to find your CSV file
        csv_paths = [
            'django_dbchat/media/csv_files/Sample - Superstore2.csv',
            'media/csv_files/Sample - Superstore2.csv',
            'Sample - Superstore2.csv'
        ]
        
        df = None
        for path in csv_paths:
            try:
                df = pd.read_csv(path)
                print(f"✅ Loaded data from: {path}")
                break
            except FileNotFoundError:
                continue
        
        if df is None:
            print("❌ Could not find CSV file. Creating sample data instead...")
            # Create sample data similar to what might be in Superstore
            df = pd.DataFrame({
                'Order Date': ['1/3/2017', '1/3/2017', '6/11/2017', '6/11/2017', '6/11/2017'],
                'Ship Date': ['1/6/2017', '1/6/2017', '6/18/2017', '6/18/2017', '6/18/2017']
            })
            print("Created sample data for analysis")
        
        print(f"📊 Data shape: {df.shape}")
        
        # Analyze date columns
        date_columns = ['Order Date', 'Ship Date']
        
        for col in date_columns:
            if col in df.columns:
                print(f"\n📅 ANALYZING COLUMN: {col}")
                print("-" * 30)
                
                # Get sample values
                sample_values = df[col].dropna().head(20).tolist()
                print(f"Sample values: {sample_values[:10]}")
                
                # Analyze patterns
                patterns = {
                    'MM/DD/YYYY': r'^\d{1,2}/\d{1,2}/\d{4}$',
                    'DD/MM/YYYY': r'^\d{1,2}/\d{1,2}/\d{4}$',  # Same regex but different interpretation
                    'YYYY-MM-DD': r'^\d{4}-\d{1,2}-\d{1,2}$',
                    'DD-MM-YYYY': r'^\d{1,2}-\d{1,2}-\d{4}$',
                    'MM-DD-YYYY': r'^\d{1,2}-\d{1,2}-\d{4}$',
                    'DD/MM/YY': r'^\d{1,2}/\d{1,2}/\d{2}$',
                    'MM/DD/YY': r'^\d{1,2}/\d{1,2}/\d{2}$'
                }
                
                pattern_matches = {}
                for pattern_name, regex in patterns.items():
                    matches = sum(1 for val in sample_values if re.match(regex, str(val)))
                    pattern_matches[pattern_name] = matches
                    print(f"  {pattern_name}: {matches}/{len(sample_values)} matches")
                
                # Find most likely pattern
                best_pattern = max(pattern_matches, key=pattern_matches.get)
                print(f"  🎯 Most likely pattern: {best_pattern}")
                
                # Test different pandas date formats
                print(f"\n🧪 TESTING CONVERSION FORMATS:")
                
                formats_to_test = [
                    ('%m/%d/%Y', 'MM/DD/YYYY'),
                    ('%d/%m/%Y', 'DD/MM/YYYY'), 
                    ('%Y-%m-%d', 'YYYY-MM-DD'),
                    ('%d-%m-%Y', 'DD-MM-YYYY'),
                    ('%m-%d-%Y', 'MM-DD-YYYY'),
                    (None, 'Auto-detect')
                ]
                
                conversion_results = []
                
                for fmt, description in formats_to_test:
                    try:
                        if fmt is None:
                            converted = pd.to_datetime(df[col], errors='coerce')
                        else:
                            converted = pd.to_datetime(df[col], format=fmt, errors='coerce')
                        
                        success_count = len(converted.dropna())
                        total_count = len(converted)
                        success_rate = (success_count / total_count) * 100
                        
                        conversion_results.append((description, fmt, success_rate, success_count, total_count))
                        print(f"  {description:15} ({fmt or 'auto':12}): {success_rate:5.1f}% ({success_count:4}/{total_count})")
                        
                        # Show some successful conversions
                        if success_count > 0:
                            successful_samples = converted.dropna().head(3).tolist()
                            print(f"    ✅ Sample results: {[str(d)[:10] for d in successful_samples]}")
                        
                    except Exception as e:
                        print(f"  {description:15} ({fmt or 'auto':12}): ERROR - {e}")
                
                # Find best format
                if conversion_results:
                    best_result = max(conversion_results, key=lambda x: x[2])
                    print(f"\n🏆 BEST FORMAT: {best_result[0]} with {best_result[2]:.1f}% success")
                    
                    # Show failed samples if success rate is low
                    if best_result[2] < 80:
                        print(f"\n❌ ANALYZING FAILURES (success rate < 80%):")
                        best_fmt = best_result[1]
                        if best_fmt:
                            failed_conversion = pd.to_datetime(df[col], format=best_fmt, errors='coerce')
                        else:
                            failed_conversion = pd.to_datetime(df[col], errors='coerce')
                        
                        failed_mask = failed_conversion.isna()
                        failed_values = df[col][failed_mask].head(10).tolist()
                        print(f"Failed values: {failed_values}")
        
        # Overall recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        print("=" * 20)
        print("1. Check your actual data format - run this script on your real CSV")
        print("2. Most US data uses MM/DD/YYYY format")
        print("3. European data typically uses DD/MM/YYYY format")
        print("4. If conversion rates are low, data might have mixed formats")
        print("5. Consider cleaning data before conversion")
        
    except Exception as e:
        print(f"❌ Error analyzing data: {e}")
        import traceback
        traceback.print_exc()

def test_improved_date_conversion():
    """Test an improved date conversion approach"""
    
    print("\n🔧 TESTING IMPROVED DATE CONVERSION LOGIC")
    print("=" * 50)
    
    # Test data with various formats (realistic examples)
    test_data = pd.DataFrame({
        'mixed_formats': [
            '1/3/2017',      # MM/D/YYYY
            '12/31/2017',    # MM/DD/YYYY  
            '6/9/2018',      # M/D/YYYY
            '02/14/2019',    # MM/DD/YYYY
            '9/1/2020',      # M/D/YYYY
            'invalid',       # Invalid
            '13/25/2021',    # Invalid (impossible date)
            '3/15/2022'      # M/DD/YYYY
        ]
    })
    
    print("Test data:")
    print(test_data['mixed_formats'].tolist())
    
    def smart_date_conversion(series, specified_format=None):
        """Improved date conversion with smart format detection"""
        
        print(f"\n🤖 Smart Date Conversion (format: {specified_format or 'auto'})")
        
        # If specific format provided, try it first
        if specified_format:
            try:
                converted = pd.to_datetime(series, format=specified_format, errors='coerce')
                success_rate = (len(converted.dropna()) / len(converted)) * 100
                print(f"  Specified format {specified_format}: {success_rate:.1f}% success")
                if success_rate >= 80:
                    return converted, specified_format, success_rate
            except:
                pass
        
        # Try common formats in order of likelihood
        formats_to_try = [
            '%m/%d/%Y',   # Most common US format
            '%d/%m/%Y',   # European format
            '%Y-%m-%d',   # ISO format
            '%m-%d-%Y',   # US with dashes
            '%d-%m-%Y',   # European with dashes
            '%m/%d/%y',   # 2-digit year
            '%d/%m/%y',   # European 2-digit year
        ]
        
        best_conversion = None
        best_format = None
        best_rate = 0
        
        # Try pandas auto-detection first
        try:
            auto_converted = pd.to_datetime(series, errors='coerce')
            auto_rate = (len(auto_converted.dropna()) / len(auto_converted)) * 100
            print(f"  Auto-detection: {auto_rate:.1f}% success")
            if auto_rate > best_rate:
                best_conversion = auto_converted
                best_format = 'auto'
                best_rate = auto_rate
        except:
            pass
        
        # Try specific formats
        for fmt in formats_to_try:
            try:
                converted = pd.to_datetime(series, format=fmt, errors='coerce')
                success_rate = (len(converted.dropna()) / len(converted)) * 100
                print(f"  Format {fmt}: {success_rate:.1f}% success")
                
                if success_rate > best_rate:
                    best_conversion = converted
                    best_format = fmt
                    best_rate = success_rate
                    
            except Exception as e:
                print(f"  Format {fmt}: ERROR - {e}")
        
        return best_conversion, best_format, best_rate
    
    # Test the smart conversion
    result, format_used, success_rate = smart_date_conversion(test_data['mixed_formats'])
    
    print(f"\n🎯 RESULTS:")
    print(f"Best format: {format_used}")
    print(f"Success rate: {success_rate:.1f}%")
    print(f"Successful conversions: {len(result.dropna())}/{len(result)}")
    
    if len(result.dropna()) > 0:
        print(f"Sample conversions: {result.dropna().head(3).tolist()}")
    
    failed_mask = result.isna()
    if failed_mask.any():
        failed_values = test_data['mixed_formats'][failed_mask].tolist()
        print(f"Failed values: {failed_values}")

if __name__ == "__main__":
    analyze_date_formats()
    test_improved_date_conversion() 