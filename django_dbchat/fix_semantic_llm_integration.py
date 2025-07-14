#!/usr/bin/env python3
"""
Fix to integrate semantic layer context into LLM preprocessing pipeline
"""

import os
import sys
import django

# Set up Django environment
sys.path.append('/c%3A/Users/SuddhasheelBhattacha/OneDrive%20-%20Mendix%20Technology%20B.V/Desktop/dbchat/django_dbchat')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbchat_project.settings')
django.setup()

def fix_dynamic_llm_service():
    """
    CRITICAL FIX: Integrate semantic layer context into LLM service
    """
    
    fix_content = '''
    def generate_sql(self, query: str, data_source=None) -> Tuple[bool, str]:
        """
        Generate SQL for any data source with semantic layer preprocessing
        FIXED: Now includes ETL-transformed semantic context as system prompt
        """
        try:
            # --- STEP 1: SEMANTIC LAYER PREPROCESSING (NEW) ---
            from .semantic_service import SemanticService
            semantic_service = SemanticService()
            
            # Discover the data environment
            environment = self.discover_data_environment()
            if not environment['available_tables']:
                return False, "No data tables found"
            
            target_table = environment['best_table']
            if not target_table:
                return False, "No usable data found"
            
            # --- STEP 2: GET SEMANTIC SCHEMA WITH ETL TYPES (NEW) ---
            # Check if semantic layer exists for this table
            semantic_schema = semantic_service.get_semantic_schema_for_table(target_table)
            
            if semantic_schema:
                # Use semantic layer with ETL-transformed types and business context
                schema_description = semantic_service.generate_enhanced_schema_prompt(
                    semantic_schema, connection_type="integrated_duckdb"
                )
                logger.info(f"Using semantic layer schema for table: {target_table}")
            else:
                # Fallback to raw analysis but trigger semantic layer generation
                analysis = environment['table_analyses'][target_table]
                schema_description = self.generate_smart_schema_description(analysis, query)
                logger.warning(f"No semantic layer found for {target_table}, using raw schema")
            
            # --- STEP 3: COLUMN MAPPING WITH SEMANTIC AWARENESS (ENHANCED) ---
            if semantic_schema:
                # Use semantic column names for mapping
                schema_columns = [col['name'] for col in semantic_schema.get('columns', [])]
            else:
                # Fallback to raw columns
                analysis = environment['table_analyses'][target_table]
                schema_columns = analysis['columns']
            
            from .column_mapper import ColumnMapper
            column_mapper = ColumnMapper(schema_columns)
            mapping = column_mapper.map_all_terms(query)
            mapped_query = column_mapper.rewrite_query(query, mapping)
            
            # --- STEP 4: ENHANCED PROMPT WITH SEMANTIC CONTEXT (NEW) ---
            if semantic_schema:
                # Add business glossary and semantic context to prompt
                business_context = semantic_service.get_semantic_context_for_query(mapped_query)
                enhanced_schema_description = f"""
{schema_description}

BUSINESS CONTEXT:
{business_context}

ETL DATA TYPE INFORMATION:
{semantic_service._format_etl_type_context(semantic_schema)}

SEMANTIC COLUMN DESCRIPTIONS:
{semantic_service._format_column_descriptions(semantic_schema)}
"""
            else:
                enhanced_schema_description = schema_description
            
            # --- STEP 5: UNIVERSAL PROMPT WITH SEMANTIC PREPROCESSING (ENHANCED) ---
            prompt = self._create_universal_prompt(mapped_query, enhanced_schema_description, self.preferred_provider)
            
            # Generate SQL based on provider
            if self.preferred_provider == 'openai':
                success, sql = self._generate_sql_openai(prompt)
            else:
                success, sql = self._generate_sql_ollama(prompt)
            
            # --- STEP 6: VALIDATION WITH SEMANTIC AWARENESS (ENHANCED) ---
            if not success:
                logger.error(f"LLM generation failed, checking semantic layer availability")
                if not semantic_schema:
                    # Try to generate semantic layer and retry
                    logger.info(f"Attempting to generate semantic layer for {target_table}")
                    semantic_success = semantic_service.auto_generate_metadata_from_table(target_table)
                    if semantic_success:
                        return self.generate_sql(query, data_source)  # Retry with semantic layer
            
            return success, sql
            
        except Exception as e:
            logger.error(f"Failed to generate SQL with semantic preprocessing: {e}")
            return False, f"SQL generation failed: {str(e)}"
    '''
    
    print("🔧 CRITICAL FIX: Dynamic LLM Service Semantic Integration")
    print("=" * 70)
    print("This fix adds semantic layer preprocessing to LLM pipeline:")
    print("✅ Step 1: Check for semantic layer before LLM query")
    print("✅ Step 2: Use ETL-transformed types from semantic layer")
    print("✅ Step 3: Include business context in system prompt")
    print("✅ Step 4: Add semantic column descriptions to LLM context")
    print("✅ Step 5: Fallback to semantic layer generation if missing")
    print("\n📋 Implementation needed in: django_dbchat/services/dynamic_llm_service.py")
    print(f"\n📝 Code to add/replace in generate_sql method:")
    print(fix_content)

def fix_semantic_service_methods():
    """
    CRITICAL FIX: Add missing methods to semantic service for LLM integration
    """
    
    missing_methods = '''
    def get_semantic_schema_for_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get semantic schema with ETL-transformed types for a specific table
        CRITICAL: This provides the ETL context that LLM needs
        """
        try:
            from datasets.models import SemanticTable, SemanticColumn
            
            # Find semantic table
            semantic_table = SemanticTable.objects.filter(name=table_name).first()
            if not semantic_table:
                return None
            
            # Get all columns with semantic metadata
            columns = SemanticColumn.objects.filter(semantic_table=semantic_table)
            
            schema = {
                'table_name': table_name,
                'display_name': semantic_table.display_name,
                'description': semantic_table.description,
                'business_purpose': semantic_table.business_purpose,
                'columns': []
            }
            
            for col in columns:
                column_info = {
                    'name': col.name,
                    'display_name': col.display_name,
                    'description': col.description,
                    'data_type': col.data_type,
                    'semantic_type': col.semantic_type,
                    'is_measure': col.is_measure,
                    'is_dimension': col.is_dimension,
                    'sample_values': col.sample_values,
                    'business_rules': col.business_rules,
                    'etl_enriched': col.etl_enriched
                }
                schema['columns'].append(column_info)
            
            return schema
            
        except Exception as e:
            logger.error(f"Error getting semantic schema for {table_name}: {e}")
            return None
    
    def _format_etl_type_context(self, semantic_schema: Dict[str, Any]) -> str:
        """
        Format ETL-transformed data type information for LLM context
        CRITICAL: This tells LLM about proper data types after ETL transformations
        """
        etl_info = []
        
        for col in semantic_schema.get('columns', []):
            if col.get('etl_enriched', False):
                etl_info.append(f"- {col['name']}: {col['data_type']} (ETL-transformed from original)")
            else:
                etl_info.append(f"- {col['name']}: {col['data_type']} (original type)")
        
        if etl_info:
            return "ETL-Transformed Data Types:\n" + "\n".join(etl_info)
        else:
            return "No ETL transformations applied to this table."
    
    def _format_column_descriptions(self, semantic_schema: Dict[str, Any]) -> str:
        """
        Format semantic column descriptions for LLM context
        CRITICAL: This provides business context for each column
        """
        descriptions = []
        
        for col in semantic_schema.get('columns', []):
            desc_parts = [f'"{col["name"]}"']
            
            if col.get('display_name') != col.get('name'):
                desc_parts.append(f"(Business Name: {col['display_name']})")
            
            if col.get('description'):
                desc_parts.append(f"- {col['description']}")
            
            if col.get('semantic_type'):
                desc_parts.append(f"[{col['semantic_type']}]")
            
            if col.get('is_measure'):
                desc_parts.append("(Measure)")
            elif col.get('is_dimension'):
                desc_parts.append("(Dimension)")
            
            descriptions.append(" ".join(desc_parts))
        
        return "Semantic Column Context:\n" + "\n".join(descriptions)
    
    def auto_generate_metadata_from_table(self, table_name: str) -> bool:
        """
        Auto-generate semantic layer metadata for a table if missing
        CRITICAL: Ensures semantic layer exists before LLM queries
        """
        try:
            from services.integration_service import DataIntegrationService
            integration_service = DataIntegrationService()
            
            # Generate semantic layer for this table
            success = self.auto_generate_metadata_from_data_integration(integration_service)
            
            if success:
                logger.info(f"Successfully generated semantic layer for {table_name}")
                return True
            else:
                logger.warning(f"Failed to generate semantic layer for {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error auto-generating semantic metadata for {table_name}: {e}")
            return False
    '''
    
    print("\n🔧 REQUIRED ADDITION: Semantic Service Methods")
    print("=" * 70)
    print("These methods need to be added to SemanticService class:")
    print("✅ get_semantic_schema_for_table() - Gets ETL-transformed schema")
    print("✅ _format_etl_type_context() - Formats ETL type info for LLM")
    print("✅ _format_column_descriptions() - Formats business context for LLM")
    print("✅ auto_generate_metadata_from_table() - Creates semantic layer if missing")
    print("\n📋 Implementation needed in: django_dbchat/services/semantic_service.py")
    print(f"\n📝 Methods to add to SemanticService class:")
    print(missing_methods)

def create_integration_workflow():
    """
    CRITICAL FIX: Create proper workflow for ETL → Semantic → LLM integration
    """
    
    workflow_code = '''
    # In datasets/views.py - ETL Operations View
    
    def execute_etl_transformation(request):
        """
        FIXED: Now triggers semantic layer regeneration after ETL
        """
        # ... existing ETL code ...
        
        # After ETL completes successfully:
        if etl_success:
            # CRITICAL: Regenerate semantic layer with ETL-transformed data
            from services.semantic_service import SemanticService
            semantic_service = SemanticService()
            
            # Clear existing semantic layer for this table
            semantic_service.clear_semantic_layer_for_table(output_table_name)
            
            # Regenerate with ETL-transformed data
            semantic_success = semantic_service.auto_generate_metadata_from_table(output_table_name)
            
            if semantic_success:
                logger.info(f"Semantic layer regenerated for ETL output: {output_table_name}")
            else:
                logger.warning(f"Failed to regenerate semantic layer for: {output_table_name}")
    
    # In core/views.py - Query Processing View
    
    def process_query(request):
        """
        FIXED: Now ensures semantic layer exists before LLM processing
        """
        # ... existing code ...
        
        # CRITICAL: Check semantic layer exists before LLM query
        from services.semantic_service import SemanticService
        semantic_service = SemanticService()
        
        # Get the target table for the query
        from services.dynamic_llm_service import DynamicLLMService
        llm_service = DynamicLLMService()
        environment = llm_service.discover_data_environment()
        
        if environment['best_table']:
            target_table = environment['best_table']
            
            # Ensure semantic layer exists
            semantic_schema = semantic_service.get_semantic_schema_for_table(target_table)
            if not semantic_schema:
                logger.info(f"No semantic layer for {target_table}, generating...")
                semantic_service.auto_generate_metadata_from_table(target_table)
        
        # Now proceed with LLM query (will use semantic context)
        success, sql = llm_service.generate_sql(query)
    '''
    
    print("\n🔧 WORKFLOW INTEGRATION: ETL → Semantic → LLM")
    print("=" * 70)
    print("Proper workflow integration needed:")
    print("✅ ETL completion triggers semantic layer regeneration")
    print("✅ Query processing ensures semantic layer exists")
    print("✅ LLM queries use semantic context as system prompt")
    print("\n📋 Implementation needed in: django_dbchat/datasets/views.py and django_dbchat/core/views.py")
    print(f"\n📝 Workflow integration code:")
    print(workflow_code)

if __name__ == "__main__":
    print("🚨 CRITICAL ISSUE: Semantic Layer NOT Integrated with LLM Context")
    print("=" * 80)
    print("\n🎯 PROBLEM IDENTIFIED:")
    print("   ❌ ETL transformations are saved but NOT sent to LLM as system context")
    print("   ❌ Semantic layer exists but LLM bypasses it and uses raw DuckDB schema")
    print("   ❌ Business context and ETL-transformed types are missing from LLM prompts")
    print("   ❌ No preprocessing workflow to ensure semantic context before queries")
    
    print("\n🔧 REQUIRED FIXES:")
    
    # Fix 1: LLM Service Integration
    fix_dynamic_llm_service()
    
    # Fix 2: Semantic Service Methods
    fix_semantic_service_methods()
    
    # Fix 3: Workflow Integration
    create_integration_workflow()
    
    print("\n" + "=" * 80)
    print("🎯 IMPLEMENTATION PRIORITY:")
    print("1. Add missing methods to SemanticService")
    print("2. Modify DynamicLLMService.generate_sql() to use semantic preprocessing")
    print("3. Update ETL workflow to regenerate semantic layer after transformations")
    print("4. Update query processing to ensure semantic layer exists")
    print("\n✅ After these fixes: ETL-transformed schema will be sent as LLM system context")
    print("=" * 80) 