#!/usr/bin/env python3
"""
Clean up problematic tables with hardcoded superstore_data references
These tables are causing errors when the dynamic LLM service tries to analyze them
"""

import duckdb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_hardcoded_tables():
    """Remove tables that contain hardcoded references to superstore_data"""
    print("🧹 Cleaning Up Hardcoded Table References")
    print("=" * 60)
    
    # These tables contain hardcoded SQL queries referencing "superstore_data"
    problematic_tables = [
        'col_mapping',           # Contains hardcoded reference to superstore_data
        'col_mapping_fixed_dates',  # Contains hardcoded reference to superstore_data
        'csv_data',              # Legacy hardcoded compatibility view
        'sample___superstore2'   # Contains hardcoded reference to superstore_data
    ]
    
    try:
        db_path = 'data/integrated.duckdb'
        conn = duckdb.connect(db_path)
        
        print(f"📊 Connected to DuckDB: {db_path}")
        
        # Show all current tables
        all_tables = conn.execute("SHOW TABLES").fetchall()
        current_tables = [t[0] for t in all_tables]
        print(f"📋 Current tables: {current_tables}")
        
        cleaned_count = 0
        
        for table_name in problematic_tables:
            if table_name in current_tables:
                print(f"\n🗑️  Removing problematic table: {table_name}")
                try:
                    # Try both DROP TABLE and DROP VIEW to handle both cases
                    try:
                        conn.execute(f"DROP VIEW IF EXISTS {table_name}")
                        print(f"   ✅ Dropped view: {table_name}")
                    except:
                        pass
                    
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        print(f"   ✅ Dropped table: {table_name}")
                    except:
                        pass
                    
                    cleaned_count += 1
                    
                except Exception as e:
                    print(f"   ❌ Error removing {table_name}: {e}")
            else:
                print(f"✅ Table {table_name} does not exist (already clean)")
        
        # Verify cleanup
        print(f"\n🔍 Verifying cleanup...")
        remaining_tables = conn.execute("SHOW TABLES").fetchall()
        remaining_table_names = [t[0] for t in remaining_tables]
        
        print(f"📋 Remaining tables: {remaining_table_names}")
        
        # Check if any problematic tables remain
        remaining_problematic = [t for t in problematic_tables if t in remaining_table_names]
        
        if remaining_problematic:
            print(f"⚠️  Still problematic tables: {remaining_problematic}")
        else:
            print(f"✅ All problematic tables successfully removed!")
        
        conn.close()
        
        print(f"\n📊 CLEANUP SUMMARY:")
        print(f"   Tables cleaned: {cleaned_count}")
        print(f"   Remaining tables: {len(remaining_table_names)}")
        
        return len(remaining_problematic) == 0
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_system_health():
    """Verify the system no longer has hardcoded reference issues"""
    print(f"\n🏥 Verifying System Health...")
    print("=" * 60)
    
    try:
        db_path = 'data/integrated.duckdb'
        conn = duckdb.connect(db_path)
        
        # Get all remaining tables
        all_tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in all_tables]
        
        print(f"📊 Testing table analysis (like dynamic LLM service does)...")
        
        healthy_tables = []
        problematic_tables = []
        
        for table_name in table_names:
            print(f"   Testing {table_name}...", end="")
            try:
                # Try the same operations that were failing before
                schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
                sample_data = conn.execute(f"SELECT * FROM {table_name} LIMIT 1").fetchall()
                
                print(f" ✅")
                healthy_tables.append(table_name)
                
            except Exception as e:
                print(f" ❌ Error: {str(e)[:50]}...")
                problematic_tables.append(table_name)
        
        print(f"\n📋 HEALTH CHECK RESULTS:")
        print(f"   Healthy tables: {len(healthy_tables)}")
        print(f"   Problematic tables: {len(problematic_tables)}")
        
        if problematic_tables:
            print(f"   🚨 Still problematic: {problematic_tables}")
        else:
            print(f"   🎉 All tables are healthy!")
        
        conn.close()
        return len(problematic_tables) == 0
        
    except Exception as e:
        print(f"❌ Error during health check: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Hardcoded Table Reference Cleanup")
    print("=" * 70)
    
    # Step 1: Clean up problematic tables
    cleanup_success = cleanup_hardcoded_tables()
    
    # Step 2: Verify system health
    health_success = verify_system_health()
    
    print(f"\n" + "=" * 70)
    print("🎯 CLEANUP RESULTS:")
    print(f"   Cleanup completed: {'✅' if cleanup_success else '❌'}")
    print(f"   System healthy: {'✅' if health_success else '❌'}")
    
    if cleanup_success and health_success:
        print(f"\n🎉 SUCCESS!")
        print("✅ Hardcoded 'superstore_data' references eliminated")
        print("✅ Problematic metadata tables removed")
        print("✅ Dynamic LLM service should work without errors")
        print("✅ System ready for production use")
    else:
        print(f"\n⚠️  Issues remain")
        print("💡 Check error messages above")
        print("💡 May need manual intervention")
    
    print("=" * 70) 