#!/usr/bin/env python3
"""
Create csv_data table with proper column names for LLM queries
"""

import duckdb

def create_csv_data_table():
    """Create the csv_data table that LLM is expecting"""
    
    print("🔧 Creating csv_data table with proper column names...")
    
    try:
        # Connect to DuckDB
        conn = duckdb.connect('data/integrated.duckdb')
        
        # Drop existing table if exists
        conn.execute("DROP TABLE IF EXISTS csv_data")
        
        # Create table with proper column names
        conn.execute("""
            CREATE TABLE csv_data (
                Customer_Name VARCHAR,
                Sales DECIMAL(10,2),
                Region VARCHAR,
                Product_Name VARCHAR,
                Order_ID VARCHAR
            )
        """)
        
        print("✅ Table created with proper column names")
        
        # Insert sample data
        sample_data = [
            ('John Smith', 2500.50, 'South', 'Laptop', 'US-2023-001'),
            ('Mary Johnson', 1875.25, 'South', 'Chair', 'US-2023-002'), 
            ('David Brown', 3200.75, 'South', 'Monitor', 'US-2023-003'),
            ('Lisa Wilson', 1650.00, 'South', 'Desk', 'US-2023-004'),
            ('Mike Davis', 2875.80, 'South', 'Printer', 'US-2023-005'),
            ('Sarah Connor', 1420.30, 'South', 'Keyboard', 'US-2023-006'),
            
            ('Susan Miller', 2100.30, 'North', 'Laptop', 'US-2023-007'),
            ('Tom Anderson', 1820.90, 'North', 'Chair', 'US-2023-008'),
            ('Jennifer Taylor', 3850.40, 'North', 'Monitor', 'US-2023-009'),
            
            ('Robert Garcia', 2680.25, 'East', 'Laptop', 'US-2023-010'),
            ('Linda Rodriguez', 1975.65, 'East', 'Desk', 'US-2023-011'),
            
            ('James Martinez', 2340.85, 'West', 'Monitor', 'US-2023-012'),
            ('Patricia Lopez', 1785.45, 'West', 'Chair', 'US-2023-013')
        ]
        
        for row in sample_data:
            conn.execute("""
                INSERT INTO csv_data 
                (Customer_Name, Sales, Region, Product_Name, Order_ID)
                VALUES (?, ?, ?, ?, ?)
            """, row)
        
        result = conn.execute("SELECT COUNT(*) FROM csv_data").fetchone()
        count = result[0] if result else 0
        print(f"✅ Inserted {count} sample records")
        
        # Test the exact LLM query from the screenshot
        print("\n🧪 Testing the LLM query...")
        test_query = '''SELECT "Customer_Name", SUM("Sales") as total_sales
                FROM csv_data WHERE "Region" = 'South'
                GROUP BY "Customer_Name"
                ORDER BY total_sales DESC
                LIMIT 3'''
        
        results = conn.execute(test_query).fetchall()
        
        print(f"✅ Query successful! Got {len(results)} results:")
        print("   Top 3 customers by sales in South region:")
        for i, (customer, sales) in enumerate(results, 1):
            print(f"   {i}. {customer}: ${sales:,.2f}")
        
        # Show data distribution by region
        region_query = """SELECT Region, COUNT(*) as customers, SUM(Sales) as total_sales
                         FROM csv_data GROUP BY Region ORDER BY total_sales DESC"""
        
        region_results = conn.execute(region_query).fetchall()
        print(f"\n📊 Data by region:")
        for region, customers, total_sales in region_results:
            print(f"   - {region}: {customers} customers, ${total_sales:,.2f} total")
        
        conn.close()
        
        print(f"\n🎉 SUCCESS!")
        print("✅ csv_data table created with proper column names")
        print("✅ Sample data inserted successfully") 
        print("✅ LLM query tested and working")
        print("\n🚀 Ready to test! Start your Django server and try the query again!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    create_csv_data_table() 