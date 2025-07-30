#!/usr/bin/env python3
"""
Comprehensive SQL Server connection diagnostic for Docker environment
"""

import socket
import sys
import subprocess
import os

def test_network_connectivity(host, port):
    """Test basic network connectivity"""
    print(f"🌐 Testing network connectivity to {host}:{port}")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, int(port)))
        sock.close()
        
        if result == 0:
            print(f"✅ Network connectivity successful")
            return True
        else:
            print(f"❌ Cannot reach {host}:{port} - Connection refused (Error: {result})")
            return False
            
    except socket.gaierror as e:
        print(f"❌ DNS resolution failed for {host}: {e}")
        return False
    except Exception as e:
        print(f"❌ Network test failed: {e}")
        return False

def check_sql_drivers():
    """Check if SQL Server drivers are available"""
    print("🔍 Checking SQL Server drivers...")
    
    try:
        import pyodbc
        drivers = pyodbc.drivers()
        print(f"📋 Available ODBC drivers: {drivers}")
        
        sql_drivers = [d for d in drivers if 'SQL Server' in d]
        if sql_drivers:
            print(f"✅ SQL Server drivers found: {sql_drivers}")
            return sql_drivers[0]
        else:
            print("❌ No SQL Server drivers found!")
            print("💡 Available drivers:", drivers)
            return None
            
    except ImportError:
        print("❌ pyodbc module not found!")
        return None
    except Exception as e:
        print(f"❌ Error checking drivers: {e}")
        return None

def test_sql_connection(host, port, database, driver, username=None, password=None):
    """Test SQL Server connection with detailed error reporting"""
    print(f"🔗 Testing SQL Server connection to {host}:{port}/{database}")
    
    try:
        import pyodbc
        
        # Try different connection methods
        connection_methods = []
        
        if username and password:
            # SQL Server authentication
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
                f"Encrypt=no;"
            )
            connection_methods.append(("SQL Server Auth", conn_str))
        
        # Windows authentication (might work in some cases)
        conn_str_trusted = (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no;"
        )
        connection_methods.append(("Windows Auth", conn_str_trusted))
        
        # Try without specifying database first
        conn_str_no_db = (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no;"
        )
        if username and password:
            conn_str_no_db += f"UID={username};PWD={password};"
        else:
            conn_str_no_db += "Trusted_Connection=yes;"
        connection_methods.append(("No Database", conn_str_no_db))
        
        for method_name, conn_str in connection_methods:
            print(f"\n🔧 Trying {method_name}...")
            print(f"   Connection string: {conn_str.replace(password or 'PWD=', 'PWD=***')}")
            
            try:
                conn = pyodbc.connect(conn_str, timeout=15)
                cursor = conn.cursor()
                
                # Test basic query
                cursor.execute("SELECT @@VERSION, @@SERVERNAME")
                row = cursor.fetchone()
                
                print(f"✅ {method_name} successful!")
                print(f"📊 Server: {row[1] if row[1] else 'Unknown'}")
                print(f"📊 Version: {row[0][:100] if row[0] else 'Unknown'}...")
                
                # List databases
                try:
                    cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
                    databases = [row[0] for row in cursor.fetchall()]
                    print(f"📋 Available databases: {databases}")
                except Exception as e:
                    print(f"⚠️ Could not list databases: {e}")
                
                cursor.close()
                conn.close()
                return True
                
            except pyodbc.Error as e:
                print(f"❌ {method_name} failed: {e}")
                continue
            except Exception as e:
                print(f"❌ {method_name} unexpected error: {e}")
                continue
        
        return False
        
    except ImportError:
        print("❌ pyodbc module not available")
        return False

def check_environment():
    """Check Docker environment and network settings"""
    print("🐳 Checking Docker environment...")
    
    # Check if we're in Docker
    if os.path.exists('/.dockerenv'):
        print("✅ Running inside Docker container")
    else:
        print("⚠️ Not running in Docker container")
    
    # Check network settings
    try:
        import socket
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        print(f"🏠 Container hostname: {hostname}")
        print(f"🌐 Container IP: {local_ip}")
    except Exception as e:
        print(f"⚠️ Could not get network info: {e}")

def main():
    """Main diagnostic function"""
    
    print("🚀 SQL Server Docker Connection Diagnostic")
    print("=" * 60)
    
    # Check environment
    check_environment()
    print()
    
    # Check drivers
    driver = check_sql_drivers()
    if not driver:
        print("\n❌ Cannot proceed without SQL Server drivers!")
        print("💡 Try installing: apt-get update && apt-get install -y msodbcsql18")
        return
    
    print()
    
    # Test different hosts
    test_hosts = [
        "host.docker.internal",
        "192.168.1.6",
        "localhost",
        "127.0.0.1"
    ]
    
    port = "1433"
    database = "mendix"
    
    # Test credentials - adjust these
    test_credentials = [
        ("sa", "YourPassword"),  # Replace with actual SA password
        ("sa", None),  # SA without password
        (None, None),  # Windows auth
    ]
    
    successful_hosts = []
    
    for host in test_hosts:
        print(f"\n🔍 Testing host: {host}")
        print("-" * 40)
        
        # Test network connectivity first
        if not test_network_connectivity(host, port):
            continue
        
        # Test SQL connection with different credentials
        for username, password in test_credentials:
            print(f"\n   👤 Testing with user: {username or 'Windows Auth'}")
            
            if test_sql_connection(host, port, database, driver, username, password):
                successful_hosts.append((host, username))
                print(f"✅ SUCCESS: {host} with {username or 'Windows Auth'}")
                break
    
    print("\n" + "=" * 60)
    if successful_hosts:
        print("🎯 SUCCESSFUL CONNECTIONS:")
        for host, user in successful_hosts:
            print(f"   ✅ {host} (user: {user or 'Windows Auth'})")
    else:
        print("❌ NO SUCCESSFUL CONNECTIONS")
        print("\n🔧 TROUBLESHOOTING STEPS:")
        print("1. Ensure SQL Server allows remote connections:")
        print("   - SQL Server Configuration Manager > Network Configuration")
        print("   - Enable TCP/IP protocol")
        print("2. Check SQL Server authentication mode:")
        print("   - SQL Server and Windows Authentication mode")
        print("3. Verify firewall settings:")
        print("   - Allow port 1433 through Windows Firewall")
        print("4. Check SQL Server service is running")
        print("5. Install SQL Server drivers in Docker:")
        print("   - Add to Dockerfile: RUN apt-get install -y msodbcsql18")

if __name__ == "__main__":
    main() 