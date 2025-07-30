#!/usr/bin/env python3
"""
Dynamic LLM Service - Enhanced with Llama 3.2b Support
Works with ANY dataset, data types, business domains, and table structures
Now with optimized Llama 3.2b support and improved OpenAI integration
"""

import json
import logging
import requests
import duckdb
import re
from typing import Tuple, Dict, Any, Optional, List
from django.conf import settings
from .column_mapper import ColumnMapper

logger = logging.getLogger(__name__)

class DynamicLLMService:
    """
    Enhanced dynamic LLM service with Llama 3.2b support
    Zero hardcoding - pure runtime detection and adaptation
    """
    
    def __init__(self):
        # CRITICAL FIX: Use the same path resolution as DataService
        try:
            from django.conf import settings
            import os
            self.duckdb_path = os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb')
            logger.info(f"DynamicLLMService using DuckDB path: {self.duckdb_path}")
        except:
            self.duckdb_path = 'data/integrated.duckdb'
            
        self._load_llm_config()
        # Initialize universal SQL syntax instructions
        self._init_universal_sql_syntax()
    
    def _load_llm_config(self):
        """Load LLM configuration from database or use defaults"""
        try:
            from core.models import LLMConfig
            
            active_config = LLMConfig.get_active_config()
            if active_config:
                logger.info(f"Loading LLM config: {active_config.provider} - {active_config.model_name}")
                
                if active_config.provider == 'openai':
                    self.openai_api_key = active_config.api_key
                    self.openai_model = active_config.model_name or 'gpt-4o'
                    self.preferred_provider = 'openai'
                    self.ollama_url = None  # Clear Ollama settings when using OpenAI
                elif active_config.provider == 'local':
                    self.ollama_url = active_config.base_url or 'http://ollama:11434'  # Use Docker hostname
                    self.ollama_model = active_config.model_name or 'llama3.2:3b'
                    self.preferred_provider = 'ollama'
                    self.openai_api_key = None  # Clear OpenAI settings when using Ollama
                
                self.temperature = getattr(active_config, 'temperature', 0.1)
                self.max_tokens = getattr(active_config, 'max_tokens', 1000)
                self.system_prompt = getattr(active_config, 'system_prompt', '')
                self.config = active_config
                
                logger.info(f"Successfully configured DynamicLLMService for {self.preferred_provider}")
                
            else:
                # Fallback defaults - prefer OpenAI if available, otherwise Ollama
                logger.warning("No LLM configuration found in database, using fallback")
                
                # Try to use OpenAI from environment first
                import os
                openai_key = os.getenv('OPENAI_API_KEY')
                if openai_key:
                    self.openai_api_key = openai_key
                    self.openai_model = 'gpt-4o'
                    self.preferred_provider = 'openai'
                    logger.info("Using OpenAI from environment variable")
                else:
                    # Fall back to Ollama
                    self.ollama_url = 'http://ollama:11434'  # Use Docker hostname
                    self.ollama_model = 'llama3.2:3b'
                    self.preferred_provider = 'ollama'
                    logger.info("Falling back to Ollama")
                
                self.temperature = 0.1
                self.max_tokens = 1000
                self.config = None
                
        except Exception as e:
            logger.error(f"Error loading LLM config, using fallback: {e}")
            # Emergency fallback
            self.ollama_url = 'http://ollama:11434'  # Use Docker hostname
            self.ollama_model = 'llama3.2:3b'
            self.preferred_provider = 'ollama'
            self.temperature = 0.1
            self.max_tokens = 1000
            self.config = None
    
    def _init_universal_sql_syntax(self):
        """Initialize universal SQL syntax instructions for all providers"""
        self.universal_sql_syntax = """
CRITICAL SQL SYNTAX REQUIREMENTS:
1. Use DuckDB SQL syntax with proper identifier quoting
2. Use double quotes for column names with spaces or special characters
3. Use single quotes for string literals
4. Always use proper JOIN syntax with ON clauses
5. Use LIMIT clause for "top N" queries
6. End all queries with semicolon
7. Use proper aggregation functions (SUM, COUNT, AVG, etc.)
8. Use proper date/time functions compatible with DuckDB
9. Use proper string functions (UPPER, LOWER, SUBSTRING, etc.)
10. Ensure all column references exist in the schema
"""
    
    def generate_sql(self, query: str, data_source=None) -> Tuple[bool, str]:
        """
        Generate SQL using enhanced data environment discovery
        FIXED: Prioritize table that matches the specific data source
        """
        try:
            # Use the enhanced LLMService for generation
            from .llm_service import LLMService
            
            # Discover the data environment
            environment = self.discover_data_environment()
            if not environment['available_tables']:
                return False, "No data tables found"
            
            # CRITICAL FIX: If we have a specific data source, prefer the matching table
            target_table = environment['best_table']
            if data_source and hasattr(data_source, 'id'):
                data_source_id = str(data_source.id).replace('-', '')
                logger.info(f"Looking for table matching data source ID: {data_source_id}")
                
                # Look for tables that contain the data source ID
                for table_name in environment['available_tables']:
                    table_clean = table_name.replace('-', '').replace('_', '')
                    if data_source_id in table_clean:
                        target_table = table_name
                        logger.info(f"Found matching table for data source: {target_table}")
                        break
            
            if not target_table:
                return False, "No usable data found"
            
            logger.info(f"Using target table: {target_table}")
            
            # Get table analysis
            analysis = environment['table_analyses'][target_table]
            
            # CRITICAL FIX: Try template-based SQL generation first for common patterns
            template_sql = self._try_template_sql_generation(query, target_table, analysis)
            if template_sql:
                logger.info(f"Generated SQL using template for query: '{query}'")
                return True, template_sql
            
            # Generate schema description
            schema_description = self.generate_smart_schema_description(analysis, query, target_table)
            
            # Create schema info for LLMService
            schema_info = {
                "tables": {
                    target_table: {
                        "columns": [
                            {"name": col, "type": analysis['column_types'].get(col, 'VARCHAR')}
                            for col in analysis['columns']
                        ]
                    }
                }
            }
            
            # Generate enhanced prompt for this data
            enhanced_prompt = self._create_enhanced_prompt(query, schema_info, target_table, analysis)
            
            # Use our own provider logic instead of delegating to LLMService
            logger.info(f"Generating SQL using {self.preferred_provider}")
            
            if self.preferred_provider == 'openai' and self.openai_api_key:
                success, sql = self._generate_sql_openai(enhanced_prompt)
            elif self.preferred_provider == 'ollama' and self.ollama_url:
                success, sql = self._generate_sql_ollama(enhanced_prompt)
            else:
                # Fallback - try both providers
                logger.warning("Primary provider not available, trying fallback")
                
                # Try OpenAI first if we have a key
                if self.openai_api_key:
                    logger.info("Trying OpenAI as fallback")
                    success, sql = self._generate_sql_openai(enhanced_prompt)
                    if success:
                        logger.info("OpenAI fallback successful")
                    else:
                        logger.warning(f"OpenAI fallback failed: {sql}")
                else:
                    success = False
                    sql = "No OpenAI key available"
                
                # If OpenAI failed, try Ollama
                if not success and self.ollama_url:
                    logger.info("Trying Ollama as fallback")
                    success, sql = self._generate_sql_ollama(enhanced_prompt)
                    if success:
                        logger.info("Ollama fallback successful")
                    else:
                        logger.warning(f"Ollama fallback failed: {sql}")
            
            if success:
                logger.info(f"Generated SQL successfully using table {target_table}")
                # Post-process SQL to fix any generic table names
                sql = self._fix_table_names_in_sql(sql, target_table)
                return True, sql
            else:
                return False, sql
            
        except Exception as e:
            logger.error(f"Error in generate_sql: {e}")
            return False, f"Error generating SQL: {str(e)}"
    
    def _post_process_sql(self, sql: str, target_table: str) -> str:
        """Post-process generated SQL for better compatibility"""
        if not sql:
            return sql
        
        # Ensure the correct table name is used
        # Replace common table name patterns
        sql = re.sub(r'\bFROM\s+[\w_]+\b', f'FROM "{target_table}"', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bJOIN\s+[\w_]+\b', f'JOIN "{target_table}"', sql, flags=re.IGNORECASE)
        
        # Ensure proper column quoting for columns with spaces
        # This is a basic implementation - could be enhanced further
        
        return sql
    
    def _test_sql_execution(self, sql: str) -> Tuple[bool, str]:
        """Test SQL execution against the database"""
        try:
            with duckdb.connect(self.duckdb_path) as conn:
                # Test with LIMIT 1 to avoid large results
                test_sql = sql.rstrip(';') + ' LIMIT 1;'
                result = conn.execute(test_sql).fetchall()
                return True, f"SQL executed successfully, returned {len(result)} rows"
        except Exception as e:
            return False, str(e)
    
    def _fix_common_sql_issues(self, sql: str, target_table: str) -> str:
        """Fix common SQL issues"""
        fixed_sql = sql
        
        # Fix table name references
        fixed_sql = re.sub(r'\bFROM\s+\w+\b', f'FROM "{target_table}"', fixed_sql, flags=re.IGNORECASE)
        
        # Fix common column name issues
        # Add more fixes as needed based on common patterns
        
        return fixed_sql
    
    def discover_data_environment(self) -> Dict:
        """
        Discover the complete data environment with enhanced analysis
        FIXED: Better table selection logic and fallback handling
        """
        try:
            # CRITICAL FIX: Use the same path resolution as DataService
            logger.info(f"DynamicLLMService connecting to DuckDB at: {self.duckdb_path}")
            
            # Add retry logic for DuckDB connections due to potential file locking
            max_retries = 3
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    with duckdb.connect(self.duckdb_path) as conn:
                        # Get all tables
                        tables = conn.execute("SHOW TABLES").fetchall()
                        
                        if not tables:
                            logger.warning("No tables found in DuckDB")
                            return {
                                'available_tables': [],
                                'best_table': None,
                                'table_analyses': {}
                            }
                        
                        logger.info(f"Found {len(tables)} tables in DuckDB: {[t[0] for t in tables]}")
                        table_analyses = {}
                        
                        for table_row in tables:
                            table_name = table_row[0]
                            try:
                                # Get schema
                                schema = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
                                columns = [row[0] for row in schema]
                                column_types = {row[0]: row[1] for row in schema}
                                
                                # Get sample data
                                sample_data = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 5').fetchall()
                                
                                # Get row count
                                row_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                                
                                table_analyses[table_name] = {
                                    'columns': columns,
                                    'column_types': column_types,
                                    'sample_data': sample_data,
                                    'row_count': row_count,
                                    'schema': schema
                                }
                                
                                logger.info(f"Analyzed table {table_name}: {row_count} rows, {len(columns)} columns")
                                
                            except Exception as e:
                                logger.warning(f"Could not analyze table {table_name}: {e}")
                                continue
                        
                        # If we got here, the connection worked - return the results
                        available_tables = list(table_analyses.keys())
                        best_table = self._select_best_table(table_analyses) if available_tables else None
                        
                        return {
                            'available_tables': available_tables,
                            'best_table': best_table,
                            'table_analyses': table_analyses
                        }
                        
                except Exception as duck_error:
                    last_error = duck_error
                    if "already open" in str(duck_error).lower():
                        logger.warning(f"DuckDB file locked, attempt {attempt + 1}/{max_retries}: {duck_error}")
                        if attempt < max_retries - 1:
                            import time
                            time.sleep(0.5)  # Wait before retry
                            continue
                    else:
                        # Non-locking error, don't retry
                        break
            
            # If all retries failed, return fallback
            logger.error(f"Failed to connect to DuckDB after {max_retries} attempts: {last_error}")
            return {
                'available_tables': [],
                'best_table': None,
                'table_analyses': {}
            }
            
        except Exception as e:
            logger.error(f"Error discovering data environment: {e}")
            return {
                'available_tables': [],
                'best_table': None,
                'table_analyses': {}
            }
    
    def _select_best_table(self, table_analyses: Dict[str, Any]) -> Optional[str]:
        """
        Select the best table based on a scoring system.
        Prioritizes tables with actual data and meaningful columns.
        """
        best_table = None
        best_score = 0
        
        for table_name, analysis in table_analyses.items():
            score = 0
            
            # CRITICAL FIX: Empty tables get negative score
            if analysis['row_count'] == 0:
                score -= 100
                logger.info(f"Table {table_name}: EMPTY TABLE - penalized heavily")
                continue
            
            # Base score for having data
            if analysis['row_count'] > 0:
                score += 10
            
            # Score for number of columns (more = better structure)
            score += len(analysis['columns']) * 2
            
            # Score for data volume (more = more useful)
            score += min(analysis['row_count'] / 1000, 50)  # Cap at 50 points, higher cap
            
            # PRIORITY: Prefer data source tables (ds_xxxx format)
            if table_name.startswith('ds_'):
                score += 100  # Much higher priority
            
            # PRIORITY: Prefer source tables 
            elif table_name.startswith('source_'):
                score += 80
            
            # Avoid metadata/summary tables
            table_lower = table_name.lower()
            if any(skip_word in table_lower for skip_word in ['metadata', 'summary', 'history', 'metrics']):
                score -= 50  # Heavy penalty for metadata tables
            
            logger.info(f"Table {table_name}: score={score}, rows={analysis['row_count']}, cols={len(analysis['columns'])}")
            
            if score > best_score:
                best_score = score
                best_table = table_name
        
        # CRITICAL FIX: If no table scored well, just use the first data table with rows
        if not best_table:
            logger.warning("No table scored well, using fallback selection")
            for table_name, analysis in table_analyses.items():
                if analysis['row_count'] > 0:
                    best_table = table_name
                    logger.info(f"Fallback: selected {table_name} with {analysis['row_count']} rows")
                    break
        
        # LAST RESORT: If still no table, use the first available table
        if not best_table and table_analyses:
            best_table = list(table_analyses.keys())[0]
            logger.warning(f"Last resort: using first table {best_table}")
        
        logger.info(f"Selected best table: {best_table} (score: {best_score})")
        
        return best_table
    
    def generate_smart_schema_description(self, analysis: Dict[str, Any], query: str, target_table: str = None) -> str:
        """
        Generate enhanced schema description for LLM with STRICT SQL-only instructions
        """
        columns = analysis['columns']
        column_types = analysis['column_types']
        sample_data = analysis['sample_data']
        
        # Build schema description with actual table name
        schema_lines = []
        table_name = target_table if target_table else "your_table"
        schema_lines.append(f"TABLE: {table_name}")
        schema_lines.append("COLUMNS:")
        
        for i, column in enumerate(columns):
            col_type = column_types.get(column, 'VARCHAR')
            sample_values = []
            
            # Extract sample values for this column
            for row in sample_data:
                if i < len(row) and row[i] is not None:
                    sample_values.append(str(row[i]))
            
            sample_str = ", ".join(sample_values[:3]) if sample_values else "NULL"
            schema_lines.append(f"  {column} ({col_type}) - Sample: {sample_str}")
        
        schema_description = "\n".join(schema_lines)
        
        # CRITICAL FIX: ULTRA-STRICT instructions to force SQL-only response with correct table name
        context = f"""
{schema_description}

🚨 CRITICAL MANDATORY INSTRUCTIONS - FOLLOW EXACTLY 🚨
- RESPOND WITH ONLY SQL CODE
- USE TABLE NAME: {table_name}
- NO EXPLANATIONS WHATSOEVER
- NO MARKDOWN CODE BLOCKS  
- NO DESCRIPTIVE TEXT
- NO COMMENTS IN SQL
- START IMMEDIATELY WITH SELECT/INSERT/UPDATE/DELETE
- END WITH SEMICOLON

SIMPLE QUERY EXAMPLES:
✅ "count records" → SELECT COUNT(*) FROM {table_name};
✅ "show all data" → SELECT * FROM {table_name} LIMIT 100;
✅ "sum of column A" → SELECT SUM(A) FROM {table_name};

USER QUERY: {query}

FORBIDDEN RESPONSES:
❌ "To generate the SQL..."
❌ "Based on the schema..."
❌ "The query would be..."
❌ Any explanation text
❌ Complex CASE statements for simple counts

REQUIRED FORMAT:
✅ SELECT column FROM {table_name} WHERE condition;

{self.universal_sql_syntax}

SQL:"""
        
        return context
    
    def _create_enhanced_prompt(self, query: str, schema_info: Dict[str, Any], target_table: str, analysis: Dict[str, Any]) -> str:
        """
        Create an enhanced prompt for the LLM, incorporating schema and query.
        """
        columns = analysis['columns']
        column_types = analysis['column_types']
        sample_data = analysis['sample_data']
        
        # Build schema description
        schema_description = self.generate_smart_schema_description(analysis, query, target_table)
        
        # Add sample data for context
        sample_context = ""
        if sample_data:
            sample_context = "Sample data for context:\n"
            for i, row in enumerate(sample_data[:3]): # Show first 3 rows
                sample_context += f"Row {i+1}: {row}\n"
            sample_context += "\n"
        
        # Add specific query context
        query_context = f"USER QUERY: {query}\n\n"
        
        # Combine all parts with explicit table name
        enhanced_prompt = f"""
{schema_description}
{sample_context}
{query_context}

🚨 CRITICAL MANDATORY INSTRUCTIONS - FOLLOW EXACTLY 🚨
- RESPOND WITH ONLY SQL CODE
- USE TABLE NAME: {target_table}
- NO EXPLANATIONS WHATSOEVER
- NO MARKDOWN CODE BLOCKS  
- NO DESCRIPTIVE TEXT
- NO COMMENTS IN SQL
- START IMMEDIATELY WITH SELECT/INSERT/UPDATE/DELETE
- END WITH SEMICOLON

SIMPLE QUERY EXAMPLES:
✅ "count records" → SELECT COUNT(*) FROM {target_table};
✅ "show all data" → SELECT * FROM {target_table} LIMIT 100;
✅ "sum of column A" → SELECT SUM(A) FROM {target_table};

FORBIDDEN RESPONSES:
❌ "To generate the SQL..."
❌ "Based on the schema..."
❌ "The query would be..."
❌ Any explanation text
❌ Complex CASE statements for simple counts

REQUIRED FORMAT:
✅ SELECT column FROM {target_table} WHERE condition;

{self.universal_sql_syntax}

SQL:"""
        
        return enhanced_prompt
    
    def _try_template_sql_generation(self, query: str, target_table: str, analysis: Dict[str, Any]) -> Optional[str]:
        """
        Try to generate SQL using templates for common query patterns before using LLM
        This prevents the LLM from overcomplicating simple queries
        """
        query_lower = query.lower().strip()
        
        # Pattern 1: Total records / count all records
        total_records_patterns = [
            'total records', 'count records', 'number of records', 'how many records',
            'count all', 'total rows', 'count rows', 'all records', 'record count',
            'total entries', 'count entries', 'size of dataset', 'dataset size'
        ]
        
        if any(pattern in query_lower for pattern in total_records_patterns):
            logger.info(f"Using template for total records query: {query}")
            return f"SELECT COUNT(*) AS total_records FROM {target_table};"
        
        # Pattern 2: Show all data / display all
        show_all_patterns = [
            'show all', 'display all', 'select all', 'all data', 'everything',
            'show me all', 'display everything', 'all rows', 'full dataset'
        ]
        
        if any(pattern in query_lower for pattern in show_all_patterns):
            logger.info(f"Using template for show all query: {query}")
            return f"SELECT * FROM {target_table} LIMIT 100;"
        
        # Pattern 3: Simple column selection
        columns = analysis.get('columns', [])
        for column in columns:
            if column.lower() in query_lower and len(query_lower.split()) <= 4:
                if any(word in query_lower for word in ['show', 'display', 'get', 'select']):
                    logger.info(f"Using template for column selection: {query}")
                    return f"SELECT {column} FROM {target_table} LIMIT 50;"
        
        # Pattern 4: Count specific values (but not for "total records")
        if 'count' in query_lower and not any(pattern in query_lower for pattern in total_records_patterns):
            # Let LLM handle complex counting
            return None
            
        # No template match found
        return None
    
    def _fix_table_names_in_sql(self, sql: str, target_table: str) -> str:
        """
        Post-process generated SQL to replace generic table names with actual table name
        """
        if not sql or not target_table:
            return sql
        
        # Common generic table names that LLMs might use
        generic_names = ['table', 'data', 'A', 'B', 'T', 'your_table', 'dataset', 'records']
        
        sql_upper = sql.upper()
        sql_fixed = sql
        
        for generic_name in generic_names:
            # Replace in FROM clauses
            patterns = [
                f' FROM {generic_name.upper()}',
                f' FROM {generic_name.lower()}',
                f' FROM {generic_name}',
                f'FROM {generic_name.upper()}',
                f'FROM {generic_name.lower()}',
                f'FROM {generic_name}',
                f' JOIN {generic_name.upper()}',
                f' JOIN {generic_name.lower()}',
                f' JOIN {generic_name}',
            ]
            
            for pattern in patterns:
                if pattern.upper() in sql_upper:
                    # Replace while preserving case structure
                    replacement = pattern.replace(generic_name.upper(), target_table).replace(generic_name.lower(), target_table).replace(generic_name, target_table)
                    sql_fixed = sql_fixed.replace(pattern, f' FROM {target_table}' if 'FROM' in pattern else f' JOIN {target_table}')
        
        logger.info(f"SQL post-processing: '{sql.strip()}' -> '{sql_fixed.strip()}'")
        return sql_fixed
    
    def _generate_sql_ollama(self, prompt: str) -> Tuple[bool, str]:
        """Generate SQL using Ollama with enhanced Llama 3.2b support"""
        try:
            if not self.config:
                # Fallback configuration
                additional_settings = {}
                stop_sequences = []
            else:
                additional_settings = self.config.additional_settings or {}
                stop_sequences = additional_settings.get('stop_sequences', [])
            
            # Prepare optimized prompt for Llama 3.2b
            if self.config and self.config.is_llama32_model():
                optimized_prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

You are an expert SQL query generator. Convert natural language questions into accurate DuckDB SQL queries.

RULES:
1. Generate ONLY valid SQL queries
2. Use exact column names from schema
3. Use proper DuckDB syntax
4. End with semicolon
5. Use LIMIT for "top N" queries<|eot_id|><|start_header_id|>user<|end_header_id|>

{prompt}<|eot_id|><|start_header_id|>assistant<|end_header_id|>

"""
            else:
                optimized_prompt = f"""You are an expert SQL query generator.

{prompt}

Generate the SQL query:"""
            
            # Prepare request payload
            payload = {
                "model": self.ollama_model,
                "prompt": optimized_prompt,
                "stream": False,
                "options": {
                    "temperature": self.temperature,
                    "top_p": 0.9,
                    "num_predict": self.max_tokens,
                    "stop": stop_sequences
                }
            }
            
            # Add Llama 3.2 specific optimizations
            if self.config and self.config.is_llama32_model():
                payload["options"].update({
                    "num_ctx": additional_settings.get('context_window', 8192),
                    "repeat_penalty": 1.1,
                    "tfs_z": 1.0,
                    "mirostat": 0,
                    "mirostat_eta": 0.1,
                    "mirostat_tau": 5.0
                })
            
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                sql_query = result.get('response', '').strip()
                
                if sql_query:
                    # Enhanced SQL cleaning for Llama 3.2
                    sql_query = self._clean_sql_response(sql_query)
                    
                    logger.info(f"Generated SQL using Ollama ({self.ollama_model}): {sql_query[:100]}...")
                    return True, sql_query
                else:
                    return False, "Ollama returned empty response"
            else:
                return False, f"Ollama API error: {response.status_code}"
                
        except Exception as e:
            return False, f"Ollama error: {str(e)}"
    
    def _generate_sql_openai(self, prompt: str) -> Tuple[bool, str]:
        """Generate SQL using OpenAI with enhanced configuration"""
        try:
            if not self.config or not self.config.api_key:
                return False, "OpenAI API key not configured"
            
            # Use safe OpenAI client initialization
            try:
                from .openai_compatibility_fix import create_openai_client_with_fallback
                success, client, error_msg = create_openai_client_with_fallback(self.config.api_key)
                if not success or not client:
                    return False, f"Failed to create OpenAI client: {error_msg}"
            except ImportError:
                # Fallback to direct OpenAI client creation
                try:
                    import openai
                    client = openai.OpenAI(api_key=self.config.api_key)
                except Exception as client_error:
                    return False, f"Failed to create OpenAI client: {str(client_error)}"
            
            response = client.chat.completions.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            
            sql_query = response.choices[0].message.content
            if sql_query:
                sql_query = sql_query.strip()
                sql_query = self._clean_sql_response(sql_query)
                
                logger.info(f"Generated SQL using OpenAI ({self.openai_model}): {sql_query[:100]}...")
                return True, sql_query
            else:
                return False, "OpenAI returned empty response"
            
        except Exception as e:
            return False, f"OpenAI error: {str(e)}"
    
    def _clean_sql_response(self, sql_query: str) -> str:
        """Enhanced SQL response cleaning"""
        if not sql_query:
            return sql_query
        
        # Handle Llama 3.2 specific formatting
        if '<|eot_id|>' in sql_query:
            sql_query = sql_query.split('<|eot_id|>')[0]
        
        # Remove markdown formatting
        sql_query = re.sub(r'^```sql\n?', '', sql_query, flags=re.MULTILINE)
        sql_query = re.sub(r'\n?```$', '', sql_query, flags=re.MULTILINE)
        
        # Remove explanatory text
        sql_query = re.sub(r'\n\n.*', '', sql_query, flags=re.DOTALL)
        
        # Remove common prefixes
        sql_query = re.sub(r'^(SQL Query:|Query:|SELECT\s+)?\s*', '', sql_query, flags=re.IGNORECASE)
        
        # Ensure proper SQL structure
        sql_query = sql_query.strip()
        if sql_query and not re.match(r'^(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|DROP|ALTER)\s+', sql_query, re.IGNORECASE):
            if not sql_query.upper().startswith('SELECT'):
                sql_query = 'SELECT ' + sql_query
        
        # Ensure semicolon at end
        if sql_query and not sql_query.endswith(';'):
            sql_query += ';'
        
        return sql_query.strip()
    
    def test_llm_connection(self) -> Tuple[bool, str]:
        """Test the current LLM configuration"""
        try:
            from .llm_service import LLMService
            
            llm_service = LLMService()
            
            if self.preferred_provider == 'openai':
                if not self.config or not self.config.api_key:
                    return False, "OpenAI API key not configured"
                return llm_service.test_openai_connection(self.config.api_key)
            else:
                return llm_service.test_ollama_connection(model=self.ollama_model)
                
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current model configuration"""
        try:
            info = {
                'provider': self.preferred_provider,
                'model': self.ollama_model if self.preferred_provider == 'ollama' else self.openai_model,
                'temperature': self.temperature,
                'max_tokens': self.max_tokens,
                'is_llama32': False,
                'config_source': 'database' if self.config else 'fallback'
            }
            
            if self.config:
                info['is_llama32'] = self.config.is_llama32_model()
                info['display_name'] = self.config.get_model_config().get('display_name', info['model'])
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting model info: {e}")
            return {'error': str(e)}
    
    def switch_provider(self, provider: str, **kwargs) -> Tuple[bool, str]:
        """Switch between providers"""
        try:
            from .llm_service import LLMService
            
            llm_service = LLMService()
            
            if provider == 'openai':
                api_key = kwargs.get('api_key')
                model = kwargs.get('model', 'gpt-4o')
                user = kwargs.get('user')
                
                if not api_key:
                    return False, "OpenAI API key is required"
                
                return llm_service.switch_to_openai(api_key, model, user)
                
            elif provider == 'ollama':
                model = kwargs.get('model', 'llama3.2:3b')
                user = kwargs.get('user')
                
                return llm_service.switch_to_llama32(model, user)
                
            else:
                return False, f"Unsupported provider: {provider}"
                
        except Exception as e:
            return False, f"Failed to switch provider: {str(e)}"
    
    def validate_prompt_syntax_compliance(self, prompt: str) -> Tuple[bool, List[str]]:
        """
        Validate that a prompt contains required DuckDB syntax instructions
        """
        required_elements = [
            'double quotes',
            'DuckDB',
            'semicolon',
            'SQL',
        ]
        
        missing_elements = []
        prompt_lower = prompt.lower()
        
        for element in required_elements:
            if element not in prompt_lower:
                missing_elements.append(element)
        
        is_compliant = len(missing_elements) == 0
        return is_compliant, missing_elements
    
    def generate_sql_with_provider(self, query: str, provider_name: str, data_source=None) -> Tuple[bool, str]:
        """
        Generate SQL using a specific LLM provider
        """
        try:
            # Temporarily switch provider
            original_provider = self.preferred_provider
            self.preferred_provider = provider_name.lower()
            
            # Generate SQL
            success, sql = self.generate_sql(query, data_source)
            
            # Restore original provider
            self.preferred_provider = original_provider
            
            return success, sql
            
        except Exception as e:
            return False, f"Provider-specific generation failed: {str(e)}" 