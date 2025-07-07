#!/usr/bin/env python
"""
Check what's in the NULL date rows
"""

import pandas as pd

def check_null_dates():
    print("🔍 CHECKING NULL DATE ROWS")
    print("=" * 40)
    
    try:
        csv_path = 'django_dbchat/media/csv_files/Sample - Superstore2_D2FNk2B.csv'
        df = pd.read_csv(csv_path)
        
        print(f"📊 Total rows: {len(df)}")
        
        # Check Order Date nulls
        order_date_nulls = df['Order Date'].isna().sum()
        ship_date_nulls = df['Ship Date'].isna().sum()
        
        print(f"📅 Order Date nulls: {order_date_nulls}")
        print(f"📅 Ship Date nulls: {ship_date_nulls}")
        
        # Show a sample of rows with null dates
        print(f"\n🔍 SAMPLE OF NULL ORDER DATE ROWS:")
        null_order_rows = df[df['Order Date'].isna()].head(10)
        
        if len(null_order_rows) > 0:
            print("Columns with data in null rows:")
            for col in df.columns:
                non_null_count = null_order_rows[col].notna().sum()
                if non_null_count > 0:
                    print(f"  {col}: {non_null_count}/{len(null_order_rows)} non-null")
                    if col in ['Row ID', 'Order ID', 'Product Name']:
                        sample_values = null_order_rows[col].dropna().head(3).tolist()
                        print(f"    Sample: {sample_values}")
        else:
            print("✅ No null Order Date rows found!")
        
        # Check if it's just empty strings instead of actual nulls
        print(f"\n🔍 CHECKING FOR EMPTY STRINGS:")
        empty_order_dates = (df['Order Date'] == '').sum()
        empty_ship_dates = (df['Ship Date'] == '').sum()
        
        print(f"📅 Empty Order Date strings: {empty_order_dates}")
        print(f"📅 Empty Ship Date strings: {empty_ship_dates}")
        
        # Show data distribution
        print(f"\n📊 DATA DISTRIBUTION:")
        print(f"Order Date:")
        print(f"  Valid dates: {df['Order Date'].notna().sum()}")
        print(f"  Null values: {df['Order Date'].isna().sum()}")
        print(f"  Empty strings: {(df['Order Date'] == '').sum()}")
        
        print(f"Ship Date:")
        print(f"  Valid dates: {df['Ship Date'].notna().sum()}")
        print(f"  Null values: {df['Ship Date'].isna().sum()}")
        print(f"  Empty strings: {(df['Ship Date'] == '').sum()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_null_dates() 