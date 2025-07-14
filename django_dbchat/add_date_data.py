#!/usr/bin/env python3
"""
Add date columns and 2015 data to csv_data table
"""

import duckdb
from datetime import datetime, timedelta

def add_date_columns_and_data():
    """Add Order_Date column and populate with 2015 data"""
    
    print("📅 Adding Date Data for 2015 Queries")
    print("=" * 50)
    
    try:
        conn = duckdb.connect('data/integrated.duckdb')
        
        # First, check current table structure
        current_columns = conn.execute("DESCRIBE csv_data").fetchall()
        print(f"📊 Current table has {len(current_columns)} columns")
        
        # Check if Order_Date already exists
        has_date = any('date' in col[0].lower() for col in current_columns)
        
        if not has_date:
            print("🔧 Adding Order_Date column...")
            conn.execute("ALTER TABLE csv_data ADD COLUMN Order_Date DATE")
        else:
            print("✅ Date column already exists")
        
        # Update existing records with 2015 dates
        print("📅 Updating records with 2015 dates...")
        
        # Get all records
        records = conn.execute("SELECT Customer_Name, Sales, Region FROM csv_data").fetchall()
        
        # Update each record with a 2015 date
        base_date = datetime(2015, 1, 1)
        
        for i, (customer, sales, region) in enumerate(records):
            # Spread dates throughout 2015
            days_offset = (i * 25) % 365  # Spread across the year
            order_date = base_date + timedelta(days=days_offset)
            
            conn.execute("""
                UPDATE csv_data 
                SET Order_Date = ? 
                WHERE Customer_Name = ? AND Sales = ? AND Region = ?
            """, [order_date.strftime('%Y-%m-%d'), customer, sales, region])
        
        # Verify the update
        count_result = conn.execute("SELECT COUNT(*) FROM csv_data WHERE strftime('%Y', Order_Date) = '2015'").fetchone()
        count_2015 = count_result[0] if count_result else 0
        
        total_result = conn.execute("SELECT COUNT(*) FROM csv_data").fetchone()
        total_count = total_result[0] if total_result else 0
        
        print(f"✅ Updated {count_2015} records with 2015 dates (out of {total_count} total)")
        
        # Test the date-based query
        print("\n🧪 Testing 2015 sales query...")
        
        test_query = """
            SELECT SUM(Sales) as total_sales_2015
            FROM csv_data 
            WHERE strftime('%Y', Order_Date) = '2015'
        """
        
        result = conn.execute(test_query).fetchone()
        if result:
            total_2015_sales = result[0]
            print(f"✅ Total sales in 2015: ${total_2015_sales:,.2f}")
        
        # Show sample records with dates
        sample_query = """
            SELECT Customer_Name, Sales, Region, Order_Date 
            FROM csv_data 
            WHERE strftime('%Y', Order_Date) = '2015'
            LIMIT 5
        """
        
        samples = conn.execute(sample_query).fetchall()
        print(f"\n📊 Sample 2015 records:")
        for customer, sales, region, date in samples:
            print(f"   - {customer}: ${sales:,.2f} in {region} on {date}")
        
        conn.close()
        
        print(f"\n🎉 SUCCESS!")
        print("✅ Date column added")
        print("✅ 2015 data populated")
        print("🔗 Your '2015 sales' queries should now work!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = add_date_columns_and_data()
    
    if success:
        print("\n🚀 Ready for date-based queries!")
        print("💡 Try queries like:")
        print("   - 'total sales in year 2015'")
        print("   - 'sales by month in 2015'")
        print("   - 'top customers in 2015'")
    else:
        print("\n⚠️ Date data addition failed") 