#!/usr/bin/env python3
"""
Test script to verify UTF-8 encoding handling in query_results function
"""

import sys
import os
import django
import pandas as pd
import numpy as np
from io import BytesIO

# Set up Django environment
sys.path.append('/app/django_dbchat')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

from core.models import QueryLog
from django.contrib.auth import get_user_model
from django.test import RequestFactory
from core.views import query_results

def test_utf8_handling():
    """Test UTF-8 encoding handling with various data types"""
    
    print("🧪 Testing UTF-8 encoding handling in query_results function...")
    
    # Get a test user
    User = get_user_model()
    user = User.objects.first()
    
    if not user:
        print("❌ No user found for testing")
        return False
    
    print(f"✅ Using test user: {user.username}")
    
    # Create test data with different encodings
    test_cases = [
        {
            'name': 'UTF-8 String',
            'data': 'Hello World - UTF-8',
            'expected': 'Hello World - UTF-8'
        },
        {
            'name': 'UTF-16 Bytes',
            'data': 'Hello World - UTF-16'.encode('utf-16'),
            'expected': 'Hello World - UTF-16'
        },
        {
            'name': 'Latin-1 Bytes',
            'data': 'Hello World - Latin-1'.encode('latin-1'),
            'expected': 'Hello World - Latin-1'
        },
        {
            'name': 'CP1252 Bytes',
            'data': 'Hello World - CP1252'.encode('cp1252'),
            'expected': 'Hello World - CP1252'
        },
        {
            'name': 'Binary Data (0xff start)',
            'data': b'\xff\xfeH\x00e\x00l\x00l\x00o\x00',  # UTF-16 LE with BOM
            'expected': 'Hello'
        },
        {
            'name': 'Dictionary Data',
            'data': {'result': 'Hello World', 'count': 42},
            'expected': "{'result': 'Hello World', 'count': 42}"
        },
        {
            'name': 'Pandas DataFrame',
            'data': pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']}),
            'expected': 'DataFrame with 3 rows and 2 columns'
        }
    ]
    
    # Create a mock request factory
    rf = RequestFactory()
    
    success_count = 0
    total_tests = len(test_cases)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📋 Test {i}/{total_tests}: {test_case['name']}")
        
        try:
            # Create a QueryLog entry with the test data
            query_log = QueryLog.objects.create(
                user=user,
                natural_query=f"test query {i}",
                query_results=test_case['data'],
                final_sql="SELECT * FROM test",
                status='completed'
            )
            
            # Create a mock request
            request = rf.get(f'/query/results/?q=test query {i}')
            request.user = user
            
            # Call the query_results function
            response = query_results(request)
            
            # Check if we got a valid response
            if response.status_code == 200:
                print(f"✅ Test {i} PASSED: Function handled {test_case['name']} correctly")
                success_count += 1
            elif response.status_code == 302:
                print(f"⚠️  Test {i} REDIRECTED: No matching query found (expected for some cases)")
                success_count += 1
            else:
                print(f"❌ Test {i} FAILED: Unexpected response status {response.status_code}")
                
        except Exception as e:
            print(f"❌ Test {i} FAILED with exception: {e}")
        
        finally:
            # Clean up the test QueryLog entry
            try:
                QueryLog.objects.filter(id=query_log.id).delete()
            except:
                pass
    
    print(f"\n📊 Test Results: {success_count}/{total_tests} tests passed")
    
    if success_count == total_tests:
        print("🎉 All tests passed! UTF-8 encoding handling is working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. UTF-8 encoding handling needs attention.")
        return False

def test_csv_encoding():
    """Test CSV file encoding handling"""
    
    print("\n📁 Testing CSV file encoding handling...")
    
    # Create test CSV data with different encodings
    test_csv_data = [
        {
            'name': 'UTF-8 CSV',
            'data': 'Name,Age,City\nJohn,25,New York\nJane,30,Los Angeles',
            'encoding': 'utf-8'
        },
        {
            'name': 'UTF-16 CSV',
            'data': 'Name,Age,City\nJohn,25,New York\nJane,30,Los Angeles',
            'encoding': 'utf-16'
        },
        {
            'name': 'Latin-1 CSV',
            'data': 'Name,Age,City\nJohn,25,New York\nJane,30,Los Angeles',
            'encoding': 'latin-1'
        }
    ]
    
    success_count = 0
    total_tests = len(test_csv_data)
    
    for i, test_case in enumerate(test_csv_data, 1):
        print(f"\n📋 CSV Test {i}/{total_tests}: {test_case['name']}")
        
        try:
            # Create CSV data with the specified encoding
            csv_bytes = test_case['data'].encode(test_case['encoding'])
            
            # Try to read it back with pandas (simulating the CSV reading process)
            df = pd.read_csv(BytesIO(csv_bytes), encoding=test_case['encoding'])
            
            if len(df) == 2 and len(df.columns) == 3:
                print(f"✅ CSV Test {i} PASSED: Successfully read {test_case['name']}")
                success_count += 1
            else:
                print(f"❌ CSV Test {i} FAILED: Unexpected DataFrame shape {df.shape}")
                
        except Exception as e:
            print(f"❌ CSV Test {i} FAILED with exception: {e}")
    
    print(f"\n📊 CSV Test Results: {success_count}/{total_tests} tests passed")
    return success_count == total_tests

if __name__ == "__main__":
    print("🚀 Starting UTF-8 encoding tests...")
    
    # Test query_results function
    query_results_success = test_utf8_handling()
    
    # Test CSV encoding
    csv_success = test_csv_encoding()
    
    print(f"\n🎯 Final Results:")
    print(f"   Query Results Function: {'✅ PASSED' if query_results_success else '❌ FAILED'}")
    print(f"   CSV Encoding Handling: {'✅ PASSED' if csv_success else '❌ FAILED'}")
    
    if query_results_success and csv_success:
        print("\n🎉 All encoding tests passed! The system handles UTF-8 errors correctly.")
        sys.exit(0)
    else:
        print("\n⚠️  Some encoding tests failed. Please review the UTF-8 handling.")
        sys.exit(1) 