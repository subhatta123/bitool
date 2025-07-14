#!/usr/bin/env python3
"""
Debug script to test semantic table API response
"""
import os
import sys
import django
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

def test_semantic_table_api():
    """Test the semantic table API to see the response format"""
    print("Testing Semantic Table API...")
    
    try:
        from datasets.models import SemanticTable
        from django.test import Client
        from django.contrib.auth import get_user_model
        
        User = get_user_model()
        
        # Get or create test user
        test_user, created = User.objects.get_or_create(
            username='testuser',
            defaults={'email': 'test@example.com'}
        )
        if created:
            test_user.set_password('testpass123')
            test_user.save()
        
        # Check what semantic tables exist
        tables = SemanticTable.objects.all()
        print(f"Found {tables.count()} semantic tables:")
        
        for table in tables:
            print(f"  ID: {table.id}")
            print(f"  Name: {table.name}")
            print(f"  Display Name: {table.display_name}")
            print(f"  Table Name: {getattr(table, 'table_name', 'N/A')}")
            print(f"  Description: {table.description}")
            print("  ---")
        
        if tables.exists():
            # Test the API endpoint
            client = Client()
            client.login(username='testuser', password='testpass123')
            
            first_table = tables.first()
            print(f"\nTesting API for table ID: {first_table.id}")
            
            response = client.get(f'/datasets/api/semantic/table/{first_table.id}/')
            print(f"Response status: {response.status_code}")
            print(f"Response content: {response.content.decode()}")
            
            if response.status_code == 200:
                import json
                data = response.json()
                print(f"Parsed JSON: {json.dumps(data, indent=2)}")
            
        else:
            print("No semantic tables found")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_semantic_table_api() 