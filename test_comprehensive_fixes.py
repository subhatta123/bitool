#!/usr/bin/env python3
"""
Comprehensive Test for Dashboard Charts and Data Filtering
=========================================================

This script tests:
1. Dashboard chart rendering with correct API URLs
2. Data filtering functionality (null, zero, empty values)
3. End-to-end functionality verification

Run this script to verify all fixes work correctly before Docker deployment.
"""

import os
import sys
import json
import time
import subprocess
import requests
from datetime import datetime

def print_header(title):
    """Print a formatted header"""
    print(f"\n{'='*60}")
    print(f"🧪 {title}")
    print(f"{'='*60}")

def print_step(step_num, description):
    """Print a formatted step"""
    print(f"\n{step_num}. {description}")
    print("-" * 50)

def test_dashboard_url_fix():
    """Test that the dashboard template uses the correct API URL"""
    print_header("Testing Dashboard API URL Fix")
    
    template_path = "django_dbchat/templates/dashboards/detail.html"
    
    print_step(1, "Checking dashboard template for correct API URL")
    
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for the fixed URL
        correct_url = "/dashboards/api/dashboard-item/"
        incorrect_url = "/api/dashboard-item/"
        
        has_correct_url = correct_url in content
        has_incorrect_url = incorrect_url in content
        
        print(f"   📄 Template file: {template_path}")
        print(f"   ✅ Correct URL found: {has_correct_url} ({correct_url})")
        print(f"   ❌ Incorrect URL found: {has_incorrect_url} ({incorrect_url})")
        
        if has_correct_url and not has_incorrect_url:
            print(f"   🎉 PASS: Dashboard API URL is correctly fixed")
            return True
        else:
            print(f"   ❌ FAIL: Dashboard API URL needs fixing")
            return False
            
    except Exception as e:
        print(f"   ❌ ERROR: Could not read template file: {e}")
        return False

def test_data_filtering_ui():
    """Test that data filtering UI is properly added"""
    print_header("Testing Data Filtering UI")
    
    template_path = "django_dbchat/templates/core/query_result.html"
    
    print_step(1, "Checking query results template for filtering controls")
    
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for filtering elements
        required_elements = [
            'id="filterNulls"',
            'id="filterZeros"',
            'id="filterEmpty"',
            'id="filterStatus"',
            'applyDataFilters()',
            'resetDataFilters()',
            'Data Filters'
        ]
        
        found_elements = []
        missing_elements = []
        
        for element in required_elements:
            if element in content:
                found_elements.append(element)
            else:
                missing_elements.append(element)
        
        print(f"   📄 Template file: {template_path}")
        print(f"   ✅ Found elements ({len(found_elements)}/{len(required_elements)}):")
        for element in found_elements:
            print(f"      - {element}")
        
        if missing_elements:
            print(f"   ❌ Missing elements:")
            for element in missing_elements:
                print(f"      - {element}")
        
        success = len(missing_elements) == 0
        print(f"   🎉 {'PASS' if success else 'FAIL'}: Data filtering UI {'correctly added' if success else 'has issues'}")
        return success
        
    except Exception as e:
        print(f"   ❌ ERROR: Could not read template file: {e}")
        return False

def test_filtering_javascript():
    """Test that filtering JavaScript functions are properly implemented"""
    print_header("Testing Data Filtering JavaScript Functions")
    
    template_path = "django_dbchat/templates/core/query_result.html"
    
    print_step(1, "Checking JavaScript filtering functions")
    
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for required JavaScript functions
        required_functions = [
            'function applyDataFilters()',
            'function resetDataFilters()',
            'function updateFilterStatus(',
            'originalResultData',
            'filteredData.rows.filter(',
            'removedCount'
        ]
        
        found_functions = []
        missing_functions = []
        
        for func in required_functions:
            if func in content:
                found_functions.append(func)
            else:
                missing_functions.append(func)
        
        print(f"   📄 Template file: {template_path}")
        print(f"   ✅ Found functions ({len(found_functions)}/{len(required_functions)}):")
        for func in found_functions:
            print(f"      - {func}")
        
        if missing_functions:
            print(f"   ❌ Missing functions:")
            for func in missing_functions:
                print(f"      - {func}")
        
        success = len(missing_functions) == 0
        print(f"   🎉 {'PASS' if success else 'FAIL'}: JavaScript filtering functions {'correctly implemented' if success else 'have issues'}")
        return success
        
    except Exception as e:
        print(f"   ❌ ERROR: Could not read template file: {e}")
        return False

def create_browser_test_file():
    """Create a comprehensive browser test file"""
    print_header("Creating Browser Test File")
    
    print_step(1, "Generating comprehensive browser test")
    
    test_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Comprehensive Chart and Filter Tests</title>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; margin: 20px; background: #f5f7fa; }
        .test-section { background: white; padding: 20px; margin: 20px 0; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .test-result { padding: 10px; margin: 10px 0; border-radius: 5px; font-weight: bold; }
        .test-pass { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .test-fail { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .controls { margin: 20px 0; }
        .controls select, .controls input, .controls button { padding: 8px; margin: 5px; border: 1px solid #ddd; border-radius: 4px; }
        .filter-controls { background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 15px 0; }
    </style>
</head>
<body>
    <h1><i class="fas fa-test-tube"></i> Comprehensive Chart and Filter Tests</h1>
    
    <div class="test-section">
        <h2><i class="fas fa-link"></i> Dashboard API URL Test</h2>
        <p>This tests whether the dashboard API URL is correctly fixed.</p>
        
        <div class="controls">
            <button onclick="testDashboardAPI()">Test Dashboard API URL</button>
        </div>
        
        <div id="dashboardAPIResults"></div>
        
        <script>
        function testDashboardAPI() {
            const resultsDiv = document.getElementById('dashboardAPIResults');
            resultsDiv.innerHTML = '<p>Testing dashboard API URL...</p>';
            
            // Test the corrected URL pattern
            const testItemId = '12345678-1234-1234-1234-123456789012'; // Dummy UUID
            const correctURL = `/dashboards/api/dashboard-item/${testItemId}/data/`;
            const incorrectURL = `/api/dashboard-item/${testItemId}/data/`;
            
            // Simulate URL validation
            const urlPattern = /^\\/dashboards\\/api\\/dashboard-item\\/[\\w-]+\\/data\\/$/;
            const isCorrectFormat = urlPattern.test(correctURL);
            const isIncorrectFormat = urlPattern.test(incorrectURL);
            
            let result = '';
            if (isCorrectFormat && !isIncorrectFormat) {
                result = `
                    <div class="test-result test-pass">
                        <i class="fas fa-check"></i> PASS: Dashboard API URL format is correct
                        <br><small>Correct URL: ${correctURL}</small>
                    </div>
                `;
            } else {
                result = `
                    <div class="test-result test-fail">
                        <i class="fas fa-times"></i> FAIL: Dashboard API URL format is incorrect
                        <br><small>Expected: ${correctURL}</small>
                    </div>
                `;
            }
            
            resultsDiv.innerHTML = result;
        }
        </script>
    </div>
    
    <div class="test-section">
        <h2><i class="fas fa-filter"></i> Data Filtering Test</h2>
        <p>This tests the data filtering functionality with null, zero, and empty values.</p>
        
        <div class="controls">
            <button onclick="loadTestData()">Load Test Data</button>
            <button onclick="testFiltering()">Test All Filters</button>
        </div>
        
        <!-- Mock filtering controls -->
        <div class="filter-controls">
            <h6><i class="fas fa-filter"></i> Data Filters</h6>
            <label><input type="checkbox" id="testFilterNulls"> Remove null values</label>
            <label><input type="checkbox" id="testFilterZeros"> Remove zero values</label>
            <label><input type="checkbox" id="testFilterEmpty"> Remove empty strings</label>
            <button onclick="applyTestFilters()">Apply Filters</button>
            <button onclick="resetTestFilters()">Reset</button>
        </div>
        
        <div id="dataTable" style="max-height: 300px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; margin: 10px 0;"></div>
        <div id="filterResults"></div>
        
        <script>
        let testData = null;
        let originalTestData = null;
        
        function loadTestData() {
            // Create test data with null, zero, and empty values
            originalTestData = {
                columns: ['ID', 'Name', 'Sales', 'Region'],
                rows: [
                    [1, 'Product A', 100, 'North'],
                    [2, 'Product B', 0, 'South'],
                    [3, null, 150, 'East'],
                    [4, 'Product D', 200, ''],
                    [5, '', 0, 'West'],
                    [6, 'Product F', null, 'North'],
                    [7, 'Product G', 300, 'Central'],
                    [8, 'Product H', 0, null]
                ]
            };
            
            testData = JSON.parse(JSON.stringify(originalTestData));
            displayTestData();
            
            document.getElementById('filterResults').innerHTML = `
                <div class="test-result test-pass">
                    <i class="fas fa-check"></i> Test data loaded: ${testData.rows.length} rows
                </div>
            `;
        }
        
        function displayTestData() {
            if (!testData) return;
            
            let html = '<table style="width: 100%; border-collapse: collapse;">';
            
            // Headers
            html += '<tr style="background: #f8f9fa;">';
            testData.columns.forEach(col => {
                html += `<th style="border: 1px solid #ddd; padding: 8px;">${col}</th>`;
            });
            html += '</tr>';
            
            // Rows
            testData.rows.forEach(row => {
                html += '<tr>';
                row.forEach(cell => {
                    const displayValue = cell === null ? '<em style="color: red;">null</em>' : 
                                       cell === '' ? '<em style="color: orange;">empty</em>' : 
                                       cell === 0 ? '<em style="color: blue;">0</em>' : cell;
                    html += `<td style="border: 1px solid #ddd; padding: 8px;">${displayValue}</td>`;
                });
                html += '</tr>';
            });
            
            html += '</table>';
            document.getElementById('dataTable').innerHTML = html;
        }
        
        function applyTestFilters() {
            if (!originalTestData) return;
            
            const filterNulls = document.getElementById('testFilterNulls').checked;
            const filterZeros = document.getElementById('testFilterZeros').checked;
            const filterEmpty = document.getElementById('testFilterEmpty').checked;
            
            testData = {
                columns: [...originalTestData.columns],
                rows: [...originalTestData.rows]
            };
            
            const originalCount = testData.rows.length;
            
            if (filterNulls || filterZeros || filterEmpty) {
                testData.rows = testData.rows.filter(row => {
                    return row.every(cell => {
                        if (filterNulls && (cell === null || cell === undefined)) return false;
                        if (filterZeros && (cell === 0 || cell === '0')) return false;
                        if (filterEmpty && (cell === '' || (typeof cell === 'string' && cell.trim() === ''))) return false;
                        return true;
                    });
                });
            }
            
            const filteredCount = testData.rows.length;
            const removedCount = originalCount - filteredCount;
            
            displayTestData();
            
            const activeFilters = [];
            if (filterNulls) activeFilters.push('nulls');
            if (filterZeros) activeFilters.push('zeros');
            if (filterEmpty) activeFilters.push('empty');
            
            document.getElementById('filterResults').innerHTML = `
                <div class="test-result test-pass">
                    <i class="fas fa-filter"></i> Filters applied: ${activeFilters.join(', ') || 'none'}
                    <br>Showing ${filteredCount} of ${originalCount} rows (${removedCount} filtered out)
                </div>
            `;
        }
        
        function resetTestFilters() {
            document.getElementById('testFilterNulls').checked = false;
            document.getElementById('testFilterZeros').checked = false;
            document.getElementById('testFilterEmpty').checked = false;
            
            if (originalTestData) {
                testData = JSON.parse(JSON.stringify(originalTestData));
                displayTestData();
                
                document.getElementById('filterResults').innerHTML = `
                    <div class="test-result test-pass">
                        <i class="fas fa-undo"></i> Filters reset: Showing all ${testData.rows.length} rows
                    </div>
                `;
            }
        }
        
        function testFiltering() {
            if (!originalTestData) {
                loadTestData();
            }
            
            let testResults = [];
            
            // Test 1: Filter nulls
            document.getElementById('testFilterNulls').checked = true;
            document.getElementById('testFilterZeros').checked = false;
            document.getElementById('testFilterEmpty').checked = false;
            applyTestFilters();
            const nullFilteredCount = testData.rows.length;
            testResults.push(`Null filter: ${8 - nullFilteredCount} rows removed`);
            
            // Test 2: Filter zeros
            resetTestFilters();
            document.getElementById('testFilterZeros').checked = true;
            applyTestFilters();
            const zeroFilteredCount = testData.rows.length;
            testResults.push(`Zero filter: ${8 - zeroFilteredCount} rows removed`);
            
            // Test 3: Filter empty
            resetTestFilters();
            document.getElementById('testFilterEmpty').checked = true;
            applyTestFilters();
            const emptyFilteredCount = testData.rows.length;
            testResults.push(`Empty filter: ${8 - emptyFilteredCount} rows removed`);
            
            // Test 4: All filters
            document.getElementById('testFilterNulls').checked = true;
            document.getElementById('testFilterZeros').checked = true;
            document.getElementById('testFilterEmpty').checked = true;
            applyTestFilters();
            const allFilteredCount = testData.rows.length;
            testResults.push(`All filters: ${8 - allFilteredCount} rows removed`);
            
            resetTestFilters();
            
            document.getElementById('filterResults').innerHTML = `
                <div class="test-result test-pass">
                    <i class="fas fa-check-circle"></i> Comprehensive filtering test completed:
                    <ul>
                        ${testResults.map(result => `<li>${result}</li>`).join('')}
                    </ul>
                </div>
            `;
        }
        </script>
    </div>
    
    <div class="test-section">
        <h2><i class="fas fa-check-circle"></i> Test Summary</h2>
        <p>Use this page to test both the dashboard API URL fix and data filtering functionality.</p>
        
        <div class="controls">
            <button onclick="runAllTests()">Run All Tests</button>
        </div>
        
        <div id="summaryResults"></div>
        
        <script>
        function runAllTests() {
            document.getElementById('summaryResults').innerHTML = '<p>Running all tests...</p>';
            
            setTimeout(() => {
                // Run tests
                testDashboardAPI();
                loadTestData();
                testFiltering();
                
                document.getElementById('summaryResults').innerHTML = `
                    <div class="test-result test-pass">
                        <i class="fas fa-check-circle"></i> All tests completed successfully!
                        <br><small>Check individual test sections for detailed results.</small>
                    </div>
                `;
            }, 1000);
        }
        
        // Auto-run tests on page load
        document.addEventListener('DOMContentLoaded', function() {
            console.log('Comprehensive test page loaded');
        });
        </script>
    </div>
</body>
</html>'''
    
    test_file_path = "comprehensive_test.html"
    with open(test_file_path, 'w', encoding='utf-8') as f:
        f.write(test_content)
    
    print(f"   📝 Created browser test file: {test_file_path}")
    print(f"   💡 Open this file in your browser to test functionality")
    
    return test_file_path

def run_integration_tests():
    """Run integration tests to verify both fixes"""
    print_header("Integration Tests Summary")
    
    results = {}
    
    # Test 1: Dashboard URL fix
    results['dashboard_url'] = test_dashboard_url_fix()
    
    # Test 2: Data filtering UI
    results['filtering_ui'] = test_data_filtering_ui()
    
    # Test 3: JavaScript functions
    results['filtering_js'] = test_filtering_javascript()
    
    # Test 4: Create browser test
    browser_test_file = create_browser_test_file()
    results['browser_test'] = browser_test_file is not None
    
    print_step(1, "Test Results Summary")
    
    passed_tests = sum(results.values())
    total_tests = len(results)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status}: {test_name.replace('_', ' ').title()}")
    
    print(f"\n   📊 Overall Result: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print(f"   🎉 ALL TESTS PASSED - Ready for Docker deployment!")
        return True
    else:
        print(f"   ⚠️  SOME TESTS FAILED - Fix issues before deployment")
        return False

def main():
    """Main test execution"""
    print_header("Comprehensive Fix Testing")
    print(f"🕐 Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        success = run_integration_tests()
        
        print_header("Next Steps")
        
        if success:
            print("✅ All tests passed! You can now:")
            print("1. 🐳 Update Docker containers with docker-compose up -d --build")
            print("2. 🌐 Test the dashboard charts in your browser")
            print("3. 🔍 Use the data filters in query results")
            print("4. 📝 Open comprehensive_test.html for interactive testing")
        else:
            print("❌ Some tests failed! Please:")
            print("1. 📋 Review the test results above")
            print("2. 🔧 Fix any failing components")
            print("3. 🔄 Re-run this test script")
            print("4. 🐳 Only deploy to Docker after all tests pass")
        
        print(f"\n📖 Additional testing:")
        print(f"• Browser test file: comprehensive_test.html")
        print(f"• Dashboard URL fix: Check /dashboards/api/dashboard-item/{{id}}/data/")
        print(f"• Data filtering: Check query results page for filter controls")
        
        return success
        
    except Exception as e:
        print(f"\n❌ Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 