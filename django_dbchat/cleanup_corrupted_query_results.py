#!/usr/bin/env python3
"""
Cleanup script for corrupted query_results data in QueryLog
Fixes UTF-8 decoding errors by cleaning up binary data in JSONField
"""

import os
import sys
import django
from django.conf import settings

# Setup Django environment
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

import logging
from core.models import QueryLog

logger = logging.getLogger(__name__)

def cleanup_corrupted_query_results():
    """Clean up corrupted query_results data"""
    
    print("🔧 Starting cleanup of corrupted query_results data...")
    
    corrupted_count = 0
    fixed_count = 0
    total_count = 0
    
    try:
        # Get all QueryLog entries
        query_logs = QueryLog.objects.all()
        total_count = query_logs.count()
        
        print(f"📊 Found {total_count} query log entries to check")
        
        for query_log in query_logs:
            try:
                # Try to access the query_results field
                query_results_data = query_log.query_results
                
                # Test if the data is accessible and valid
                if query_results_data is None:
                    # This is fine, leave as is
                    continue
                elif isinstance(query_results_data, dict):
                    # This is what we expect, test JSON serialization
                    import json
                    try:
                        json.dumps(query_results_data)
                        # Data is valid, continue
                        continue
                    except (TypeError, ValueError) as e:
                        print(f"❌ Found non-serializable dict in query {query_log.id}: {e}")
                        corrupted_count += 1
                        # Replace with safe data
                        query_log.query_results = {
                            'error': 'Data was corrupted and cleaned up',
                            'cleaned_at': str(timezone.now()),
                            'original_query': query_log.natural_query[:100] if query_log.natural_query else ''
                        }
                        query_log.save()
                        fixed_count += 1
                elif isinstance(query_results_data, bytes):
                    print(f"❌ Found binary data in query {query_log.id}")
                    corrupted_count += 1
                    # Try to decode if possible, otherwise replace
                    try:
                        decoded_data = query_results_data.decode('utf-8')
                        # Try to parse as JSON
                        import json
                        parsed_data = json.loads(decoded_data)
                        query_log.query_results = parsed_data
                        print(f"✅ Successfully recovered binary data for query {query_log.id}")
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        query_log.query_results = {
                            'error': 'Binary data was corrupted and could not be recovered',
                            'cleaned_at': str(timezone.now()),
                            'original_query': query_log.natural_query[:100] if query_log.natural_query else ''
                        }
                        print(f"🔧 Replaced corrupted binary data for query {query_log.id}")
                    query_log.save()
                    fixed_count += 1
                else:
                    # Other data type, convert to safe format
                    print(f"⚠️  Found unexpected data type {type(query_results_data)} in query {query_log.id}")
                    corrupted_count += 1
                    query_log.query_results = {
                        'error': f'Unexpected data type {type(query_results_data).__name__} was cleaned up',
                        'cleaned_at': str(timezone.now()),
                        'original_query': query_log.natural_query[:100] if query_log.natural_query else '',
                        'original_data_str': str(query_results_data)[:500]  # First 500 chars
                    }
                    query_log.save()
                    fixed_count += 1
                    
            except Exception as e:
                print(f"❌ Error accessing query_results for query {query_log.id}: {e}")
                corrupted_count += 1
                try:
                    # Force reset the field to a safe value
                    query_log.query_results = {
                        'error': 'Data was completely corrupted and reset',
                        'cleaned_at': str(timezone.now()),
                        'original_query': query_log.natural_query[:100] if query_log.natural_query else '',
                        'exception': str(e)
                    }
                    query_log.save()
                    fixed_count += 1
                    print(f"🔧 Reset corrupted field for query {query_log.id}")
                except Exception as save_error:
                    print(f"❌ Could not fix query {query_log.id}: {save_error}")
        
        print(f"\n📈 Cleanup Summary:")
        print(f"   Total entries checked: {total_count}")
        print(f"   Corrupted entries found: {corrupted_count}")
        print(f"   Entries fixed: {fixed_count}")
        
        if corrupted_count == 0:
            print("✅ No corrupted data found!")
        else:
            print(f"✅ Cleanup completed! Fixed {fixed_count}/{corrupted_count} corrupted entries")
            
    except Exception as e:
        print(f"❌ Fatal error during cleanup: {e}")
        import traceback
        traceback.print_exc()

def test_query_results_access():
    """Test if query_results can be accessed without errors"""
    print("\n🧪 Testing query_results access...")
    
    try:
        recent_queries = QueryLog.objects.order_by('-created_at')[:10]
        
        for query_log in recent_queries:
            try:
                query_results_data = query_log.query_results
                print(f"✅ Query {query_log.id}: {type(query_results_data).__name__}")
            except Exception as e:
                print(f"❌ Query {query_log.id}: {e}")
                
    except Exception as e:
        print(f"❌ Error testing queries: {e}")

if __name__ == '__main__':
    from django.utils import timezone
    
    print("🚀 QueryLog Cleanup Utility")
    print("=" * 50)
    
    # Run cleanup
    cleanup_corrupted_query_results()
    
    # Test access
    test_query_results_access()
    
    print("\n🎉 Cleanup complete!") 