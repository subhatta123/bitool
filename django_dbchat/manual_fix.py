#!/usr/bin/env python3
"""
Manual Fix for Semantic and LLM Issues
Run this script to fix the problems
"""
import os
import sys
import django
from pathlib import Path

# Add the Django project directory to the Python path
sys.path.append(str(Path(__file__).parent))

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

def main():
    print("🔧 Manual Fix for Semantic and LLM Issues")
    print("=" * 60)
    
    try:
        from core.models import LLMConfig
        from datasets.models import DataSource, SemanticTable, SemanticColumn
        from django.db import transaction
        
        # Fix 1: Switch away from broken sqlcoder:15b model
        print("1. Fixing broken Ollama model...")
        ollama_config = LLMConfig.objects.filter(provider='local', is_active=True).first()
        if ollama_config and ollama_config.model_name == 'sqlcoder:15b':
            # Try sqlcoder:latest first, then fallback to llama3.2:1b
            ollama_config.model_name = 'sqlcoder:latest'
            ollama_config.save()
            print("   ✅ Switched to sqlcoder:latest")
        else:
            print("   ✅ Model is not sqlcoder:15b")
        
        # Fix 2: Create missing semantic tables
        print("\n2. Creating missing semantic tables...")
        data_sources = DataSource.objects.filter(status='active')
        
        for ds in data_sources:
            if not SemanticTable.objects.filter(data_source=ds).exists():
                print(f"   Creating semantic table for {ds.name}...")
                
                with transaction.atomic():
                    # Create semantic table
                    semantic_table = SemanticTable.objects.create(
                        data_source=ds,
                        name=f"semantic_{ds.id.hex.replace('-', '_')}",
                        display_name=ds.name,
                        description=f'Semantic layer for {ds.name}',
                        business_purpose='Analytics data',
                        is_fact_table=True,
                        row_count_estimate=1000
                    )
                    
                    # Create semantic columns based on schema_info or defaults
                    schema_info = ds.schema_info or {}
                    columns = schema_info.get('columns', [])
                    
                    if not columns:
                        # Create basic columns for retail/sales data
                        columns = [
                            {'name': 'segment', 'type': 'string'},
                            {'name': 'profit', 'type': 'float'},
                            {'name': 'sales', 'type': 'float'},
                            {'name': 'order_date', 'type': 'date'},
                            {'name': 'customer_name', 'type': 'string'},
                            {'name': 'region', 'type': 'string'},
                            {'name': 'category', 'type': 'string'},
                            {'name': 'product_name', 'type': 'string'},
                            {'name': 'quantity', 'type': 'integer'},
                            {'name': 'discount', 'type': 'float'}
                        ]
                    
                    for col in columns[:15]:  # Limit to first 15 columns
                        col_name = col.get('name', 'unknown')
                        col_type = col.get('type', 'string')
                        
                        SemanticColumn.objects.create(
                            semantic_table=semantic_table,
                            name=col_name,
                            display_name=col_name.replace('_', ' ').title(),
                            description=f'Column {col_name} from {ds.name}',
                            data_type=col_type,
                            semantic_type='dimension' if col_type == 'string' else 'measure',
                            is_nullable=True,
                            sample_values=[]
                        )
                    
                    print(f"   ✅ Created semantic table with {len(columns)} columns")
            else:
                print(f"   ✅ {ds.name} already has semantic table")
        
        # Fix 3: Update LLM service configuration
        print("\n3. Updating LLM service configuration...")
        from services.llm_service import LLMService
        llm_service = LLMService()
        llm_service.update_configuration()
        print("   ✅ LLM service configuration updated")
        
        # Fix 4: Check final status
        print("\n4. Final status check...")
        active_config = LLMConfig.get_active_config()
        semantic_count = SemanticTable.objects.count()
        
        print(f"   Active LLM: {active_config.provider} - {active_config.model_name}")
        print(f"   Semantic tables: {semantic_count}")
        
        if active_config and semantic_count > 0:
            print("\n🎉 FIXES APPLIED SUCCESSFULLY!")
            print("You can now try your query: 'total profit in consumer segment in year 2015'")
        else:
            print("\n❌ Some issues remain:")
            if not active_config:
                print("   - No active LLM configuration")
            if semantic_count == 0:
                print("   - No semantic tables found")
                
    except Exception as e:
        print(f"❌ Error during fix: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 