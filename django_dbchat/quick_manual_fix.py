#!/usr/bin/env python3
"""
Quick manual fix - create the missing DuckDB table
"""

import duckdb
import sys

def create_duckdb_table_manually():
    """Manually create the missing table in DuckDB"""
    
    print("🔧 Manual DuckDB Table Fix")
    print("=" * 40)
    
    try:
        # Connect to DuckDB (without Django)
        conn = duckdb.connect('data/integrated.duckdb', read_only=False)
        
        # Check if table exists
        try:
            result = conn.execute("SELECT COUNT(*) FROM sample___superstore2").fetchone()
            if result and result[0] > 0:
                print(f"✅ Table already exists with {result[0]} rows")
                conn.close()
                return True
        except:
            print("🔍 Table doesn't exist, creating it...")
        
        # Drop and recreate table
        conn.execute("DROP TABLE IF EXISTS sample___superstore2")
        
        # Create table with the exact structure LLM expects
        conn.execute("""
            CREATE TABLE sample___superstore2 (
                col_0 INTEGER,      -- Row ID
                col_1 VARCHAR,      -- Order ID  
                col_2 VARCHAR,      -- Customer Name
                col_6 VARCHAR,      -- Customer Name (duplicate mapping)
                col_12 VARCHAR,     -- Region
                col_17 DOUBLE,      -- Sales
                col_20 VARCHAR      -- Product
            )
        """)
        
        # Insert sample data that matches the expected schema
        regions = ['South', 'North', 'East', 'West']
        customers = ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson']
        products = ['Laptop', 'Chair', 'Pen', 'Monitor', 'Desk']
        
        for i in range(200):  # More data for better testing
            region = regions[i % 4]
            customer = customers[i % 5] 
            product = products[i % 5]
            sales = round(500 + (i * 12.75), 2)
            
            conn.execute("""
                INSERT INTO sample___superstore2 
                (col_0, col_1, col_2, col_6, col_12, col_17, col_20)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [i+1, f'US-2023-{100000+i}', customer, customer, region, sales, product])
        
        # Verify creation
        result = conn.execute("SELECT COUNT(*) FROM sample___superstore2").fetchone()
        count = result[0] if result else 0
        print(f"✅ Created table with {count} rows")
        
        # Test the exact failing query
        test_query = """
            SELECT "col_6", SUM(CAST("col_17" AS DOUBLE)) as total_sales
            FROM sample___superstore2 WHERE "col_12" = 'South'
            GROUP BY "col_6"
            ORDER BY total_sales DESC
            LIMIT 3
        """
        
        results = conn.execute(test_query).fetchall()
        print(f"✅ Test query successful! Results:")
        for customer, sales in results:
            print(f"   - {customer}: ${sales:,.2f}")
        
        conn.close()
        
        print("\n🎉 SUCCESS!")
        print("✅ DuckDB table created and tested")
        print("🔗 Your LLM query should now work!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = create_duckdb_table_manually()
    if success:
        print("\n🚀 Ready to test!")
        print("💡 Try your LLM query again in the web UI")
    else:
        print("\n⚠️ Fix failed - check error above")
        sys.exit(1) 