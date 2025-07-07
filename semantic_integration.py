"""
Integration of Semantic Layer with ConvaBI Application
Enhanced LLM query generation using semantic metadata
"""

import streamlit as st
from semantic_layer import SemanticLayer
from typing import Dict, Any, Optional
import database
import re
import json
import pandas as pd

def get_enhanced_sql_from_openai(natural_language_query: str, 
                                data_schema: Dict[str, Any], 
                                db_type: str = "sqlserver", 
                                target_table: Optional[str] = None,
                                use_semantic_layer: bool = True):
    """
    Enhanced SQL generation with semantic layer integration and postal code disambiguation
    """
    try:
        # First check for postal code queries and handle them specifically
        query_lower = natural_language_query.lower()
        
        # Detect postal code queries
        postal_keywords = ['pincode', 'pin code', 'postal code', 'zip code', 'zip', 'area code']
        geographic_keywords = ['south', 'north', 'east', 'west', 'region']
        
        is_postal_query = any(keyword in query_lower for keyword in postal_keywords)
        has_geographic_filter = any(keyword in query_lower for keyword in geographic_keywords)
        
        if is_postal_query:
            print(f"[SEMANTIC] Detected postal code query: {natural_language_query}")
            return handle_postal_code_query(natural_language_query, data_schema, db_type, target_table)
        
        # Apply semantic layer enhancement if available
        if use_semantic_layer:
            try:
                semantic_context = get_semantic_context_for_query(natural_language_query)
                if semantic_context:
                    enhanced_query = enhance_query_with_semantic_context(natural_language_query, semantic_context)
                    print(f"[SEMANTIC] Enhanced query: {enhanced_query}")
                    natural_language_query = enhanced_query
            except Exception as e:
                print(f"[SEMANTIC] Could not apply semantic enhancement: {e}")
        
        # Fall back to basic SQL generation
        from app import get_basic_sql_from_openai
        return get_basic_sql_from_openai(natural_language_query, data_schema, db_type, target_table)
        
    except Exception as e:
        print(f"[SEMANTIC] Error in enhanced SQL generation: {e}")
        # Fall back to basic generation
        from app import get_basic_sql_from_openai
        return get_basic_sql_from_openai(natural_language_query, data_schema, db_type, target_table)

def handle_postal_code_query(query, data_schema, db_type, target_table):
    """
    Specifically handle postal code queries to prevent confusion with product data
    """
    print(f"[POSTAL] Handling postal code query: {query}")
    
    # Check if data has postal code columns
    postal_columns = find_postal_code_columns(data_schema)
    
    if not postal_columns:
        # Create a helpful error message with suggestions
        error_response = {
            "type": "error",
            "content": """❌ **No Postal Code Data Found**

Your data doesn't contain postal code columns. Here are some options:

1. **Check Column Names**: Postal codes might be under different names like:
   - `postal_code`, `zip_code`, `pincode`, `area_code`
   - `customer_postal`, `shipping_zip`, `address_code`

2. **Add Postal Data**: Use Data Integration to add postal code information

3. **Sample Data**: I can create sample postal codes for testing

4. **Alternative Query**: Try asking about geographic regions instead of postal codes

**Example queries that might work:**
- "Which cities are in the South region?"
- "Show sales by state in the South"
- "List customers by region"
"""
        }
        return error_response
    
    # Generate SQL focusing on postal code columns
    query_lower = query.lower()
    
    # Extract geographic filters
    region_filter = ""
    if 'south' in query_lower:
        region_filter = "region = 'South'"
    elif 'north' in query_lower:
        region_filter = "region = 'North'"
    elif 'east' in query_lower:
        region_filter = "region = 'East'"
    elif 'west' in query_lower:
        region_filter = "region = 'West'"
    
    # Build SQL query for postal codes
    table_name = target_table if target_table and target_table != "All Tables / Auto-detect" else "integrated_data"
    
    # Use the first available postal column
    postal_col = postal_columns[0]
    
    if region_filter:
        sql_query = f"""
        SELECT DISTINCT {postal_col} as postal_code, region, city, state
        FROM {table_name}
        WHERE {region_filter}
        ORDER BY {postal_col}
        LIMIT 50
        """
    else:
        sql_query = f"""
        SELECT DISTINCT {postal_col} as postal_code, region, city, state, COUNT(*) as count
        FROM {table_name}
        GROUP BY {postal_col}, region, city, state
        ORDER BY count DESC, {postal_col}
        LIMIT 50
        """
    
    print(f"[POSTAL] Generated SQL: {sql_query}")
    return sql_query

def find_postal_code_columns(data_schema):
    """
    Find postal code related columns in the data schema
    """
    postal_columns = []
    
    if isinstance(data_schema, dict):  # Database schema
        for table, columns in data_schema.items():
            for col_name in columns.keys():
                if is_postal_code_column(col_name):
                    postal_columns.append(col_name)
    elif isinstance(data_schema, list):  # CSV/integrated data schema
        for col_info in data_schema:
            col_name = col_info.get('name', '')
            if is_postal_code_column(col_name):
                postal_columns.append(col_name)
    
    return postal_columns

def is_postal_code_column(col_name):
    """
    Check if a column name indicates postal code data
    """
    col_lower = col_name.lower()
    postal_patterns = [
        'postal', 'zip', 'pincode', 'pin_code', 'area_code',
        'postcode', 'postal_code', 'zip_code'
    ]
    return any(pattern in col_lower for pattern in postal_patterns)

def get_semantic_context_for_query(query):
    """
    Get semantic context from the database for the given query
    """
    try:
        conn = database.get_db_connection()
        if not conn:
            return None
        
        cursor = conn.cursor()
        
        # Get relevant business terms
        query_words = query.lower().split()
        relevant_terms = []
        
        for word in query_words:
            try:
                cursor.execute("""
                    SELECT term, definition, category
                    FROM semantic_glossary
                    WHERE LOWER(term) LIKE %s OR LOWER(definition) LIKE %s
                """, (f'%{word}%', f'%{word}%'))
                terms = cursor.fetchall()
                relevant_terms.extend(terms)
            except Exception:
                pass
        
        # Get query guidance
        guidance = []
        try:
            cursor.execute("""
                SELECT query_pattern, guidance
                FROM query_guidance
                WHERE %s LIKE CONCAT('%%', query_pattern, '%%')
            """, (query.lower(),))
            guidance = cursor.fetchall()
        except Exception:
            pass
        
        conn.close()
        
        return {
            'terms': relevant_terms,
            'guidance': guidance
        }
        
    except Exception as e:
        print(f"[SEMANTIC] Error getting context: {e}")
        return None

def enhance_query_with_semantic_context(query, context):
    """
    Enhance the natural language query with semantic context
    """
    enhanced_query = query
    
    # Add guidance if available
    if context.get('guidance'):
        for pattern, guidance in context['guidance']:
            if pattern in query.lower():
                enhanced_query += f"\n\nImportant context: {guidance}"
                break
    
    # Add relevant business term definitions
    if context.get('terms'):
        term_definitions = []
        for term, definition, category in context['terms'][:3]:  # Limit to top 3
            term_definitions.append(f"{term}: {definition}")
        
        if term_definitions:
            enhanced_query += f"\n\nBusiness context: {'; '.join(term_definitions)}"
    
    return enhanced_query

def generate_basic_schema_prompt(data_schema: Dict[str, Any], db_type: str) -> str:
    """Generate basic schema prompt (current approach)"""
    schema_prompt_part = "Schema:\n"
    
    if isinstance(data_schema, dict):  # For databases
        for table, columns in data_schema.items():
            schema_prompt_part += f"Table {table}:\n"
            for col_name, col_type in columns.items():
                schema_prompt_part += f"  - {col_name} ({col_type})\n"
    elif isinstance(data_schema, list):  # For CSV and integrated data
        if db_type == "integrated":
            schema_prompt_part += "Integrated Data Columns (query as 'integrated_data'):\n"
        else:
            schema_prompt_part += "CSV Columns (query as 'csv_data'):\n"
        for col_info in data_schema:
            schema_prompt_part += f"  - {col_info['name']} ({col_info['type']})\n"
    
    return schema_prompt_part

def load_semantic_layer_from_db() -> Optional[SemanticLayer]:
    """Load semantic layer configuration from database"""
    try:
        conn = database.get_db_connection()
        if conn:
            try:
                semantic_layer = SemanticLayer.load_from_database(conn)
                return semantic_layer
            finally:
                conn.close()
    except Exception as e:
        print(f"Could not load semantic layer from database: {e}")
    
    return None

def initialize_semantic_layer_on_startup():
    """Initialize semantic layer when app starts"""
    if 'semantic_layer' not in st.session_state:
        semantic_layer = load_semantic_layer_from_db()
        if semantic_layer:
            st.session_state.semantic_layer = semantic_layer
            print(f"[SEMANTIC LAYER] Loaded from database: {len(semantic_layer.tables)} tables")
        else:
            st.session_state.semantic_layer = SemanticLayer()
            print("[SEMANTIC LAYER] Initialized new semantic layer")

def auto_update_semantic_layer_on_data_change():
    """Auto-update semantic layer when data sources change"""
    try:
        import data_integration
        integration_engine = data_integration.data_integration_engine
        
        # Check if we have data sources but no semantic layer
        summary = integration_engine.get_data_sources_summary()
        semantic_layer = st.session_state.get('semantic_layer')
        
        if summary['total_sources'] > 0 and (not semantic_layer or not semantic_layer.tables):
            print("[SEMANTIC LAYER] Auto-updating semantic layer for new data sources")
            
            if not semantic_layer:
                semantic_layer = SemanticLayer()
            
            # Auto-generate metadata
            success = semantic_layer.auto_generate_metadata_from_data_integration(integration_engine)
            
            if success:
                # Save to database
                conn = database.get_db_connection()
                if conn:
                    try:
                        saved = semantic_layer.save_to_database(conn)
                        if saved:
                            st.session_state.semantic_layer = semantic_layer
                            print(f"[SEMANTIC LAYER] Auto-updated and saved: {len(semantic_layer.tables)} tables")
                    except Exception as e:
                        print(f"[SEMANTIC LAYER] Failed to save auto-update: {e}")
                    finally:
                        conn.close()
    except Exception as e:
        print(f"[SEMANTIC LAYER] Auto-update failed: {e}")

def get_semantic_enhanced_prompt_for_clarification(natural_language_query: str, 
                                                 data_schema: Dict[str, Any], 
                                                 db_type: str = "sqlserver",
                                                 target_table: Optional[str] = None):
    """
    Generate enhanced prompt for the query clarifier module
    """
    # Try to get semantic layer
    semantic_layer = st.session_state.get("semantic_layer")
    if not semantic_layer:
        semantic_layer = load_semantic_layer_from_db()
        if semantic_layer:
            st.session_state.semantic_layer = semantic_layer
    
    # Generate enhanced schema prompt if available
    if semantic_layer and semantic_layer.tables:
        return semantic_layer.generate_enhanced_schema_prompt(data_schema, db_type)
    else:
        return generate_basic_schema_prompt(data_schema, db_type)

def enhance_existing_query_generation():
    """
    Enhance the existing get_sql_from_openai function to use semantic layer
    This function modifies the session state to use enhanced prompts
    """
    # Initialize semantic layer if not already done
    initialize_semantic_layer_on_startup()
    
    # Auto-update if needed
    auto_update_semantic_layer_on_data_change()

# Integration hooks for existing app functions
# Semantic layer patching functions removed - using direct integration instead

def apply_semantic_layer_integration():
    """
    Apply semantic layer integration to the current session
    """
    try:
        print("[SEMANTIC] Applying semantic layer integration...")
        
        # Check if semantic tables exist and initialize if needed
        conn = database.get_db_connection()
        if not conn:
            print("[SEMANTIC] Could not connect to database")
            return False
        
        cursor = conn.cursor()
        
        # Create semantic tables if they don't exist
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS semantic_glossary (
                    id SERIAL PRIMARY KEY,
                    term VARCHAR(255) UNIQUE NOT NULL,
                    definition TEXT NOT NULL,
                    category VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_guidance (
                    id SERIAL PRIMARY KEY,
                    query_pattern VARCHAR(255) UNIQUE NOT NULL,
                    guidance TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add essential postal code terms if not present
            postal_terms = [
                ("Pincode", "Postal code or ZIP code for geographic areas - NOT product codes", "Geography"),
                ("Sales", "Revenue amount from transactions", "Finance"),
                ("Product Name", "Name of a product being sold", "Product"),
                ("Region", "Geographic sales territory", "Geography")
            ]
            
            for term, definition, category in postal_terms:
                try:
                    cursor.execute("""
                        INSERT INTO semantic_glossary (term, definition, category)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (term) DO UPDATE SET 
                        definition = EXCLUDED.definition
                    """, (term, definition, category))
                except Exception:
                    pass  # Term might already exist
            
            # Add postal code query guidance
            guidance_rules = [
                ("pincode", "User wants postal codes/ZIP codes (geographic), not product codes or sales data. Look for postal_code, zip_code, or similar geographic columns."),
                ("sales", "Revenue/financial data, not geographic information.")
            ]
            
            for pattern, guidance in guidance_rules:
                try:
                    cursor.execute("""
                        INSERT INTO query_guidance (query_pattern, guidance)
                        VALUES (%s, %s)
                        ON CONFLICT (query_pattern) DO UPDATE SET 
                        guidance = EXCLUDED.guidance
                    """, (pattern, guidance))
                except Exception:
                    pass
            
            conn.commit()
            print("[SEMANTIC] Semantic layer integration applied successfully")
            return True
            
        except Exception as e:
            print(f"[SEMANTIC] Error creating semantic tables: {e}")
            return False
        finally:
            conn.close()
            
    except Exception as e:
        print(f"[SEMANTIC] Error in semantic layer integration: {e}")
        return False 