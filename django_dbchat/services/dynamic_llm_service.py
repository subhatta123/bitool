#!/usr/bin/env python3
"""
Dynamic LLM Service - Zero Hardcoding
Works with ANY dataset, data types, business domains, and table structures
Automatically adapts to any data source without assumptions
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
    Completely dynamic LLM service that adapts to any dataset
    Zero hardcoding - pure runtime detection and adaptation
    """
    
    def __init__(self):
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
                logger.info(f"Loading LLM config: {active_config.provider}")
                
                if active_config.provider == 'openai':
                    self.openai_api_key = active_config.api_key
                    self.openai_model = active_config.model_name or 'gpt-3.5-turbo'
                    self.preferred_provider = 'openai'
                elif active_config.provider == 'local':
                    self.ollama_url = active_config.base_url or 'http://localhost:11434'
                    self.ollama_model = active_config.model_name or 'sqlcoder:15b'
                    self.preferred_provider = 'ollama'
                
                self.temperature = getattr(active_config, 'temperature', 0.1)
                self.max_tokens = getattr(active_config, 'max_tokens', 500)
            else:
                # Fallback defaults
                self.ollama_url = 'http://localhost:11434'
                self.ollama_model = 'sqlcoder:15b'
                self.preferred_provider = 'ollama'
                self.temperature = 0.1
                self.max_tokens = 500
                
        except Exception as e:
            logger.warning(f"Using fallback LLM config: {e}")
            self.ollama_url = 'http://localhost:11434'
            self.ollama_model = 'sqlcoder:15b'
            self.preferred_provider = 'ollama'
            self.temperature = 0.1
            self.max_tokens = 500
    
    def _init_universal_sql_syntax(self):
        """
        Initialize comprehensive DuckDB SQL syntax instructions that prevent common errors
        UNIVERSAL: Ensures DuckDB compatibility and prevents malformed SQL generation
        """
        self.universal_sql_syntax = {
            'critical_rules': [
                'Use DOUBLE QUOTES (") for column and table names, NEVER backticks (`)',
                'This database uses DuckDB/PostgreSQL syntax, NOT MySQL syntax',
                'Column names with spaces MUST use double quotes: "column_name"',
                'Table names with special characters MUST use double quotes: "table_name"',
                'NEVER use MySQL backtick syntax: `column_name`',
                'ALIASES: Use simple names without quotes: as total_sales, NOT as "Total_Sales"',
                'ORDER BY: Use quoted column names: ORDER BY "column_name" DESC, NOT ORDER BY "column_name DESC"',
                'AGGREGATIONS: Use proper syntax: SUM("column_name") as result_name',
                'NEVER create double-quoted identifiers: ""column"" is INVALID',
                'NEVER put direction (ASC/DESC) inside quotes in ORDER BY',
                'NEVER put LIMIT inside quotes or ORDER BY clause',
            ],
            'examples': {
                'correct': [
                    'SELECT "column_one", "column_two" FROM "table_name"',
                    'WHERE "field_name" = \'value\' AND "status_field" = \'active\'',
                    'GROUP BY "grouping_column", "classification_field"',
                    'ORDER BY "sorting_column" DESC',
                    'SUM("sales") as total_sales',
                    'ORDER BY "total_sales" DESC LIMIT 10',
                    'COUNT("customer_id") as customer_count',
                ],
                'incorrect': [
                    'SELECT `column_one`, `column_two` FROM `table_name`  -- WRONG: Uses backticks',
                    'SELECT column one FROM table_name  -- WRONG: Unquoted spaces',
                    'WHERE field_name = value  -- WRONG: Unquoted identifiers',
                    'ORDER BY "column_name DESC"  -- WRONG: Direction inside quotes',
                    'SUM("sales") as "Total_Sales"  -- WRONG: Quoted alias',
                    'ORDER BY "total_sales DESC LIMIT 10"  -- WRONG: Everything in quotes',
                    '""column_name""  -- WRONG: Double quotes',
                ]
            },
            'format_template': '''
CRITICAL DUCKDB SQL SYNTAX REQUIREMENTS:
{rules}

CORRECT EXAMPLES:
{correct_examples}

NEVER DO THIS (INCORRECT):
{incorrect_examples}

DATABASE: DuckDB (PostgreSQL-compatible syntax)
RULE: Always use double quotes (") for identifiers, NEVER backticks (`)
ALIAS RULE: Use simple names without quotes for aliases
ORDER BY RULE: Only quote the column name, put direction outside quotes
'''
        }
    
    def _get_universal_sql_instructions(self) -> str:
        """
        Get universal SQL syntax instructions that apply to ALL LLM models
        UNIVERSAL: This ensures consistent DuckDB syntax regardless of LLM provider
        """
        rules_text = '\n'.join([f"- {rule}" for rule in self.universal_sql_syntax['critical_rules']])
        
        correct_examples = '\n'.join([f"✅ {ex}" for ex in self.universal_sql_syntax['examples']['correct']])
        
        incorrect_examples = '\n'.join([f"❌ {ex}" for ex in self.universal_sql_syntax['examples']['incorrect']])
        
        return self.universal_sql_syntax['format_template'].format(
            rules=rules_text,
            correct_examples=correct_examples,
            incorrect_examples=incorrect_examples
        )
    
    def _create_sqlcoder_prompt(self, query: str, schema_description: str) -> str:
        """
        Create optimized prompt for SQLCoder models with comprehensive DuckDB syntax
        UNIVERSAL: Uses standardized SQL syntax instructions with specific error prevention
        """
        universal_syntax = self._get_universal_sql_instructions()
        
        return f"""### Instruction:
You are a SQL code assistant. Convert the natural language question to SQL query.
Use ONLY the specified table. Do NOT create JOINs or reference other tables.

{universal_syntax}

CRITICAL DUCKDB REQUIREMENTS:
- Use double quotes for identifiers: "column_name"
- Use simple aliases without quotes: as total_sales
- ORDER BY: ORDER BY "column_name" DESC (direction outside quotes)
- NEVER create double-quoted identifiers: ""column"" is invalid
- NEVER put ASC/DESC inside quotes in ORDER BY
- NEVER put LIMIT inside quotes or ORDER BY clause

### Input:
{schema_description}

Question: {query}

### Response:
SELECT"""
    
    def _create_generic_prompt(self, query: str, schema_description: str) -> str:
        """
        Create generic prompt for other LLM models with comprehensive DuckDB syntax
        UNIVERSAL: Uses standardized SQL syntax instructions with specific error prevention
        """
        universal_syntax = self._get_universal_sql_instructions()
        
        return f"""You are an expert SQL query generator. Convert natural language questions into accurate SQL queries.

{universal_syntax}

{schema_description}

Question: {query}

CRITICAL DUCKDB REQUIREMENTS:
- Use double quotes for identifiers: "column_name"
- Use simple aliases without quotes: as total_sales
- ORDER BY: ORDER BY "column_name" DESC (direction outside quotes)
- NEVER create double-quoted identifiers: ""column"" is invalid
- NEVER put ASC/DESC inside quotes in ORDER BY
- NEVER put LIMIT inside quotes or ORDER BY clause

Generate ONLY the SQL query using proper DuckDB syntax:"""
    
    def _create_openai_prompt(self, query: str, schema_description: str) -> str:
        """
        Create OpenAI-specific prompt with comprehensive DuckDB syntax
        UNIVERSAL: Uses standardized SQL syntax instructions with specific error prevention
        """
        universal_syntax = self._get_universal_sql_instructions()
        
        return f"""You are an expert SQL assistant specializing in DuckDB queries.

{universal_syntax}

TASK: Convert the natural language question into a precise SQL query.

{schema_description}

QUESTION: {query}

CRITICAL REQUIREMENTS:
- Generate ONLY valid DuckDB SQL
- Use double quotes for identifiers: "column_name"
- Use simple aliases without quotes: as total_sales
- ORDER BY: ORDER BY "column_name" DESC (direction outside quotes)
- NEVER create double-quoted identifiers: ""column"" is invalid
- NEVER put ASC/DESC inside quotes in ORDER BY
- NEVER put LIMIT inside quotes or ORDER BY clause
- No explanations, just the SQL query
- Ensure query is syntactically correct for DuckDB

SQL QUERY:"""
    
    def _create_claude_prompt(self, query: str, schema_description: str) -> str:
        """
        Create Claude-specific prompt with comprehensive DuckDB syntax
        UNIVERSAL: Uses standardized SQL syntax instructions with specific error prevention
        """
        universal_syntax = self._get_universal_sql_instructions()
        
        return f"""I need help generating a DuckDB SQL query.

{universal_syntax}

Schema Information:
{schema_description}

User Question: {query}

CRITICAL DUCKDB REQUIREMENTS:
- Use double quotes for identifiers: "column_name"
- Use simple aliases without quotes: as total_sales
- ORDER BY: ORDER BY "column_name" DESC (direction outside quotes)
- NEVER create double-quoted identifiers: ""column"" is invalid
- NEVER put ASC/DESC inside quotes in ORDER BY
- NEVER put LIMIT inside quotes or ORDER BY clause

Please generate a syntactically correct DuckDB SQL query that:
1. Uses double quotes for all identifiers
2. Follows PostgreSQL/DuckDB syntax rules
3. Addresses the user's question precisely
4. Avoids all common syntax errors

SQL Query:"""
    
    def add_custom_llm_provider(self, provider_name: str, prompt_template: str) -> None:
        """
        Add a new LLM provider while ensuring universal DuckDB syntax compliance
        UNIVERSAL: Any new provider automatically gets DuckDB syntax instructions
        
        Args:
            provider_name: Name of the new LLM provider
            prompt_template: Template with placeholders {universal_syntax}, {schema}, {query}
        """
        if not hasattr(self, 'custom_providers'):
            self.custom_providers = {}
        
        # Validate that the template includes universal syntax placeholder
        if '{universal_syntax}' not in prompt_template:
            raise ValueError(f"Custom provider template must include {{universal_syntax}} placeholder")
        
        self.custom_providers[provider_name.lower()] = prompt_template
        logger.info(f"Added custom LLM provider '{provider_name}' with universal DuckDB syntax compliance")
    
    def _create_custom_provider_prompt(self, provider_name: str, query: str, schema_description: str) -> str:
        """
        Create prompt for custom LLM provider with universal syntax enforcement
        UNIVERSAL: Ensures all custom providers get DuckDB syntax instructions
        """
        if provider_name.lower() not in getattr(self, 'custom_providers', {}):
            raise ValueError(f"Custom provider '{provider_name}' not found")
        
        template = self.custom_providers[provider_name.lower()]
        universal_syntax = self._get_universal_sql_instructions()
        
        return template.format(
            universal_syntax=universal_syntax,
            schema=schema_description,
            query=query
        )
    
    def validate_prompt_syntax_compliance(self, prompt: str) -> Tuple[bool, List[str]]:
        """
        Validate that a prompt contains required DuckDB syntax instructions
        UNIVERSAL: Ensures all prompts enforce proper SQL syntax
        """
        required_elements = [
            'double quotes',
            'DuckDB',
            'backticks',
            'PostgreSQL',
        ]
        
        missing_elements = []
        prompt_lower = prompt.lower()
        
        for element in required_elements:
            if element not in prompt_lower:
                missing_elements.append(element)
        
        is_compliant = len(missing_elements) == 0
        return is_compliant, missing_elements
    
    def get_all_supported_providers(self) -> Dict[str, str]:
        """
        Get all supported LLM providers with their syntax compliance status
        UNIVERSAL: Shows all providers that support DuckDB syntax
        """
        providers = {
            'openai': 'Built-in with universal DuckDB syntax',
            'ollama': 'Built-in with universal DuckDB syntax',
            'claude': 'Built-in with universal DuckDB syntax',
            'sqlcoder': 'Built-in with universal DuckDB syntax',
            'generic': 'Built-in with universal DuckDB syntax',
        }
        
        # Add custom providers
        custom_providers = getattr(self, 'custom_providers', {})
        for provider_name in custom_providers:
            providers[provider_name] = 'Custom provider with universal DuckDB syntax'
        
        return providers
    
    def _create_universal_prompt(self, query: str, schema_description: str, llm_provider: str = "generic") -> str:
        """
        Create universal prompt that works with ANY LLM model or provider
        UNIVERSAL: Guaranteed DuckDB syntax compliance for all LLMs
        """
        # Check for custom providers first
        custom_providers = getattr(self, 'custom_providers', {})
        if llm_provider.lower() in custom_providers:
            return self._create_custom_provider_prompt(llm_provider, query, schema_description)
        
        # Provider-specific optimizations while maintaining universal syntax
        if llm_provider.lower() in ['openai', 'gpt']:
            return self._create_openai_prompt(query, schema_description)
        elif llm_provider.lower() in ['claude', 'anthropic']:
            return self._create_claude_prompt(query, schema_description)
        elif 'sqlcoder' in llm_provider.lower():
            return self._create_sqlcoder_prompt(query, schema_description)
        else:
            return self._create_generic_prompt(query, schema_description)
    
    def discover_data_environment(self) -> Dict[str, Any]:
        """Discover all available data sources and tables without assumptions"""
        
        environment = {
            'data_sources': [],
            'available_tables': [],
            'table_analyses': {},
            'best_table': None
        }
        
        try:
            # Discover Django data sources
            from datasets.models import DataSource
            
            data_sources = DataSource.objects.all()
            for ds in data_sources:
                environment['data_sources'].append({
                    'id': str(ds.id),
                    'name': ds.name,
                    'table_name': ds.table_name,
                    'source_type': ds.source_type
                })
            
            # Discover DuckDB tables
            conn = duckdb.connect(self.duckdb_path)
            tables = conn.execute("SHOW TABLES").fetchall()
            environment['available_tables'] = [t[0] for t in tables]
            
            # Analyze each table to understand structure
            for table_name in environment['available_tables']:
                analysis = self._analyze_table_structure(conn, table_name)
                if analysis['has_data']:
                    environment['table_analyses'][table_name] = analysis
            
            # ENHANCED: Find the best table prioritizing semantic layer availability
            if environment['table_analyses']:
                from .semantic_service import SemanticService
                semantic_service = SemanticService()
                
                # Check which tables have semantic layers and boost their scores
                for table_name in environment['table_analyses']:
                    analysis = environment['table_analyses'][table_name]
                    
                    # Check if semantic layer exists for this table
                    semantic_schema = semantic_service.get_semantic_schema_for_table(table_name)
                    if semantic_schema:
                        # BOOST: Give significant score boost to tables with semantic layers
                        analysis['score'] += 1000  # Large boost to prioritize semantic tables
                        analysis['has_semantic_layer'] = True
                        logger.info(f"Table {table_name} has semantic layer, boosting score to {analysis['score']}")
                    else:
                        analysis['has_semantic_layer'] = False
                
                # Now select the best table (semantic layer tables will have much higher scores)
                best_table = max(environment['table_analyses'].keys(), 
                               key=lambda t: environment['table_analyses'][t]['score'])
                environment['best_table'] = best_table
                
                logger.info(f"Selected best table: {best_table} (score: {environment['table_analyses'][best_table]['score']}, semantic: {environment['table_analyses'][best_table].get('has_semantic_layer', False)})")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error discovering data environment: {e}")
        
        return environment
    
    def _analyze_table_structure(self, conn, table_name: str) -> Dict[str, Any]:
        """Analyze any table structure without domain assumptions"""
        
        analysis = {
            'table_name': table_name,
            'columns': [],
            'column_types': {},
            'date_columns': [],
            'numeric_columns': [],
            'text_columns': [],
            'categorical_columns': [],
            'id_columns': [],
            'has_data': False,
            'row_count': 0,
            'sample_values': {},
            'score': 0
        }
        
        try:
            # Get schema
            schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
            
            for row in schema:
                col_name = row[0]
                col_type = row[1].upper()
                
                analysis['columns'].append(col_name)
                analysis['column_types'][col_name] = col_type
                
                # Categorize by SQL data type
                if any(hint in col_type for hint in ['DATE', 'TIME', 'TIMESTAMP']):
                    analysis['date_columns'].append(col_name)
                elif any(hint in col_type for hint in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC']):
                    analysis['numeric_columns'].append(col_name)
                else:
                    analysis['text_columns'].append(col_name)
                
                # Semantic categorization based on column names (language-agnostic)
                col_lower = col_name.lower()
                if any(id_hint in col_lower for id_hint in ['id', 'key', 'code', 'number']):
                    analysis['id_columns'].append(col_name)
            
            # Get row count and basic stats
            analysis['row_count'] = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            analysis['has_data'] = analysis['row_count'] > 0
            
            if analysis['has_data']:
                # Auto-detect date columns in text format
                self._detect_text_date_columns(conn, table_name, analysis)
                
                # Get sample values for understanding content
                self._sample_column_values(conn, table_name, analysis)
                
                # Calculate table usefulness score
                analysis['score'] = (
                    len(analysis['columns']) * 2 +  # More columns = more useful
                    len(analysis['numeric_columns']) * 3 +  # Numeric data = more analytical value
                    len(analysis['date_columns']) * 4 +  # Date data = time series potential
                    min(analysis['row_count'] / 1000, 10)  # More data = better (capped at 10)
                )
            
        except Exception as e:
            logger.error(f"Error analyzing table {table_name}: {e}")
        
        return analysis
    
    def _detect_text_date_columns(self, conn, table_name: str, analysis: Dict[str, Any]):
        """Auto-detect date columns in text format (any locale/format)"""
        
        # Common date patterns across different locales
        date_patterns = [
            r'^\d{1,2}[-/]\d{1,2}[-/]\d{4}$',  # DD-MM-YYYY or MM-DD-YYYY
            r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$',  # YYYY-MM-DD
            r'^\d{1,2}[-/]\d{1,2}[-/]\d{2}$',   # DD-MM-YY or MM-DD-YY
            r'^\d{4}\d{2}\d{2}$',               # YYYYMMDD
            r'^\w{3}\s+\d{1,2},?\s+\d{4}$',    # Jan 15, 2023
        ]
        
        for col in analysis['text_columns'][:]:  # Copy to modify during iteration
            try:
                # Sample values from this column
                sample = conn.execute(f"SELECT {col} FROM {table_name} WHERE {col} IS NOT NULL LIMIT 20").fetchall()
                if not sample:
                    continue
                
                values = [str(row[0]).strip() for row in sample if row[0]]
                if not values:
                    continue
                
                # Count how many values match date patterns
                date_like_count = 0
                for value in values:
                    for pattern in date_patterns:
                        if re.match(pattern, value):
                            date_like_count += 1
                            break
                
                # If 60% or more look like dates, classify as date column
                if date_like_count >= len(values) * 0.6:
                    analysis['date_columns'].append(col)
                    analysis['text_columns'].remove(col)
                    logger.info(f"Auto-detected date column: {col} in {table_name}")
                    
            except Exception:
                continue
    
    def _sample_column_values(self, conn, table_name: str, analysis: Dict[str, Any]):
        """Sample values from each column to understand content"""
        
        try:
            # Get sample values for understanding data content
            for col in analysis['columns'][:10]:  # Limit to first 10 columns for performance
                try:
                    sample_query = f"SELECT DISTINCT {col} FROM {table_name} WHERE {col} IS NOT NULL LIMIT 5"
                    sample_values = [row[0] for row in conn.execute(sample_query).fetchall()]
                    analysis['sample_values'][col] = sample_values
                except:
                    analysis['sample_values'][col] = []
                    
        except Exception as e:
            logger.error(f"Error sampling values from {table_name}: {e}")
    
    def generate_smart_schema_description(self, analysis: Dict[str, Any], user_query: str) -> str:
        """
        Generate intelligent schema description based on actual data structure
        ENHANCED: Include exact column names to prevent column mapping errors
        """
        
        table_name = analysis['table_name']
        columns = analysis['columns']
        
        # CRITICAL: Build column descriptions with EXACT names from database
        column_descriptions = []
        for col in columns[:20]:  # Limit for prompt size
            col_type = analysis['column_types'].get(col, 'TEXT')
            col_lower = col.lower()
            
            # Semantic hints based on column names and sample values
            hints = []
            
            if col in analysis['date_columns']:
                hints.append("date/time")
            elif col in analysis['numeric_columns']:
                # UNIVERSAL: Pattern-based semantic analysis without business domain assumptions
                from .universal_schema_service import universal_schema_service
                patterns = universal_schema_service.discover_column_patterns(col)
                
                if 'monetary' in patterns:
                    hints.append("monetary")
                elif 'quantity' in patterns:
                    hints.append("quantity")
                else:
                    hints.append("numeric")
            elif col in analysis['id_columns']:
                hints.append("identifier")
            else:
                # Analyze sample values for text categorization
                sample_values = analysis['sample_values'].get(col, [])
                if sample_values:
                    # Check if categorical (limited distinct values)
                    if len(sample_values) <= 5 and len(str(sample_values[0])) < 50:
                        hints.append("classification")
                    else:
                        hints.append("text")
                else:
                    hints.append("text")
            
            hint_text = f" ({', '.join(hints)})" if hints else ""
            
            # CRITICAL: Show exact column name with semantic description
            semantic_description = self._get_semantic_column_description(col)
            if semantic_description != col:
                column_descriptions.append(f'"{col}": {col_type}{hint_text} -- Use exactly "{col}" (represents {semantic_description})')
            else:
                column_descriptions.append(f'"{col}": {col_type}{hint_text}')
        
        # Detect if query needs special handling
        query_lower = user_query.lower()
        special_hints = []
        
        if analysis['date_columns'] and any(time_word in query_lower for time_word in ['year', 'month', 'date', 'time', 'when', 'before', 'after']):
            date_cols = [f'"{col}"' for col in analysis['date_columns'][:3]]
            special_hints.append(f"Date columns available: {', '.join(date_cols)}")
        
        if analysis['numeric_columns'] and any(math_word in query_lower for math_word in ['sum', 'total', 'average', 'count', 'max', 'min']):
            numeric_cols = [f'"{col}"' for col in analysis['numeric_columns'][:5]]
            special_hints.append(f"Numeric columns available: {', '.join(numeric_cols)}")
        
        # Build the schema description with EXACT column names
        schema_parts = [
            f'Table: "{table_name}" (single table containing all data)',
            f"Total columns: {len(columns)} | Rows: {analysis['row_count']:,}",
            f"Structure: {len(analysis['date_columns'])} date, {len(analysis['numeric_columns'])} numeric, {len(analysis['text_columns'])} text columns",
            "",
            "EXACT COLUMN NAMES (use these exact names with double quotes):",
            *[f"  - {desc}" for desc in column_descriptions],
        ]
        
        if special_hints:
            schema_parts.extend(["", "Query hints:"] + [f"  - {hint}" for hint in special_hints])
        
        # Format column names for the rules section
        quoted_columns = [f'"{col}"' for col in columns[:10]]
        column_list = ", ".join(quoted_columns)
        
        schema_parts.extend([
            "",
            "CRITICAL COLUMN USAGE RULES:",
            f"- Use ONLY these exact column names: {column_list}",
            f'- Table name: "{table_name}"',
            "- Do NOT guess column names - use only the exact names listed above",
            "- Do NOT create JOINs or reference other tables",
            "- All data is in this single flat table",
        ])
        
        return "\n".join(schema_parts)
    
    def _get_semantic_column_description(self, column_name: str) -> str:
        """
        UNIVERSAL: Get semantic description based on patterns, not business domain assumptions
        Works with ANY dataset, ANY business domain, ANY language
        """
        from .universal_schema_service import universal_schema_service
        
        patterns = universal_schema_service.discover_column_patterns(column_name)
        
        # Generate description based on discovered patterns
        if 'identifier' in patterns:
            return "unique identifier"
        elif 'name' in patterns:
            return "name/title field"
        elif 'monetary' in patterns:
            return "monetary value"
        elif 'quantity' in patterns:
            return "quantity/count value"
        elif 'temporal' in patterns:
            return "date/time information"
        elif 'geographic' in patterns:
            return "location/geographic data"
        elif 'categorical' in patterns:
            return "classification/grouping"
        elif 'contact' in patterns:
            return "contact information"
        else:
            # Default: return the column name as-is for generic patterns
            return column_name
    
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
                
                # Try to generate semantic layer for future queries
                try:
                    semantic_service.auto_generate_metadata_from_table(target_table)
                except Exception as sem_e:
                    logger.warning(f"Failed to auto-generate semantic layer: {sem_e}")
            
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
                
                enhanced_schema_description = f"""{schema_description}

BUSINESS CONTEXT:
{business_context}

ETL DATA TYPE INFORMATION:
{semantic_service._format_etl_type_context(semantic_schema)}

SEMANTIC COLUMN DESCRIPTIONS:
{semantic_service._format_column_descriptions(semantic_schema)}

BUSINESS GLOSSARY:
{semantic_service._format_business_glossary()}"""
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
                    # Try to generate semantic layer and retry once
                    logger.info(f"Attempting to generate semantic layer for {target_table}")
                    semantic_success = semantic_service.auto_generate_metadata_from_table(target_table)
                    if semantic_success:
                        logger.info(f"Retrying SQL generation with new semantic layer")
                        return self.generate_sql(query, data_source)  # Retry with semantic layer
            
            # --- FALLBACK: If LLM output references unmapped columns, suggest closest match ---
            if not success or any(col not in schema_columns for col in mapping.values()):
                # Find unmapped columns in the SQL
                unmapped = []
                for col in mapping.values():
                    if col not in schema_columns:
                        unmapped.append(col)
                if unmapped:
                    suggestions = {col: column_mapper.map_term(col) for col in unmapped}
                    return False, f"LLM referenced unmapped columns: {unmapped}. Suggestions: {suggestions}"
            
            return success, sql
            
        except Exception as e:
            logger.error(f"Failed to generate SQL with semantic preprocessing: {e}")
            return False, f"SQL generation failed: {str(e)}"
    
    def _generate_sql_ollama(self, prompt: str) -> Tuple[bool, str]:
        """Generate SQL using Ollama"""
        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": self.temperature,
                        "top_p": 0.9,
                        "stop": ["\n\n", "Question:", "Schema:", "###", "Instruction:", "Input:"]
                    }
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                sql_query = result.get('response', '').strip()
                
                # Handle SQLCoder response completion
                if 'sqlcoder' in self.ollama_model.lower() and sql_query:
                    if not sql_query.upper().startswith('SELECT'):
                        sql_query = 'SELECT ' + sql_query
                
                # Clean up the response
                sql_query = self._clean_sql_response(sql_query)
                
                logger.info(f"Generated SQL using Ollama: {sql_query[:100]}...")
                return True, sql_query
            else:
                return False, f"Ollama API error: {response.status_code}"
                
        except Exception as e:
            return False, f"Ollama error: {str(e)}"
    
    def _generate_sql_openai(self, prompt: str) -> Tuple[bool, str]:
        """Generate SQL using OpenAI"""
        try:
            import openai
            
            client = openai.OpenAI(api_key=self.openai_api_key)
            
            response = client.chat.completions.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            
            sql_query = response.choices[0].message.content
            if sql_query:
                sql_query = sql_query.strip()
            else:
                sql_query = ""
            
            sql_query = self._clean_sql_response(sql_query)
            
            logger.info(f"Generated SQL using OpenAI: {sql_query[:100]}...")
            return True, sql_query
            
        except Exception as e:
            return False, f"OpenAI error: {str(e)}"
    
    def _clean_sql_response(self, sql_query: str) -> str:
        """
        Clean up SQL response from LLM and ensure DuckDB compatibility
        FIXED: Convert MySQL backticks to DuckDB double quotes
        """
        # Remove common prefixes/suffixes
        prefixes_to_remove = [
            "```sql", "```", "SQL:", "Query:", "Answer:", "Here's the SQL:", "Here is the SQL:"
        ]
        
        cleaned = sql_query.strip()
        
        # Remove prefixes
        for prefix in prefixes_to_remove:
            if cleaned.lower().startswith(prefix.lower()):
                cleaned = cleaned[len(prefix):].strip()
        
        # Remove trailing backticks
        if cleaned.endswith('```'):
            cleaned = cleaned[:-3].strip()
        
        # CRITICAL FIX: Convert MySQL backticks to DuckDB double quotes
        cleaned = self._normalize_sql_syntax_for_duckdb(cleaned)
        
        # FIXED: Use dedicated SQL fixer for comprehensive syntax repair
        from .sql_fixer import sql_fixer
        cleaned = sql_fixer.fix_sql_syntax(cleaned)
        
        # Ensure query ends with semicolon
        if not cleaned.endswith(';'):
            cleaned += ';'
        
        return cleaned
    
    def _normalize_sql_syntax_for_duckdb(self, sql_query: str) -> str:
        """
        Normalize SQL syntax to be DuckDB-compatible
        FIXED: Convert MySQL backticks to PostgreSQL/DuckDB double quotes
        """
        try:
            # Step 1: Convert backticks to double quotes for identifiers
            # Pattern: `identifier` -> "identifier"
            normalized = re.sub(r'`([^`]+)`', r'"\1"', sql_query)
            
            # Step 2: Ensure table names are properly quoted if they contain special characters
            # Look for table names that might need quoting (contain spaces, start with numbers, etc.)
            def quote_table_if_needed(match):
                table_name = match.group(1)
                # If table name contains spaces, hyphens, or starts with number, quote it
                if (' ' in table_name or '-' in table_name or 
                    table_name[0].isdigit() or '_' in table_name):
                    return f'FROM "{table_name}"'
                return match.group(0)
            
            # Apply table name quoting for FROM clauses
            normalized = re.sub(r'FROM\s+([^\s,)]+)', quote_table_if_needed, normalized, flags=re.IGNORECASE)
            
            # Step 3: Ensure proper quoting for column names with spaces
            def quote_column_if_needed(match):
                prefix = match.group(1)  # SELECT, WHERE, GROUP BY, etc.
                column_name = match.group(2)
                
                # If column contains spaces and isn't already quoted, quote it
                if ' ' in column_name and not (column_name.startswith('"') and column_name.endswith('"')):
                    return f'{prefix}"{column_name}"'
                return match.group(0)
            
            # Apply column quoting for various SQL clauses - FIXED: Don't break ORDER BY
            sql_keywords = ['SELECT', 'WHERE', 'GROUP BY', 'HAVING']  # Removed ORDER BY to prevent breaking it
            for keyword in sql_keywords:
                pattern = f'({keyword}\\s+)([^",()\\s]+(?:\\s+[^",()\\s]+)*)'
                normalized = re.sub(pattern, quote_column_if_needed, normalized, flags=re.IGNORECASE)
            
            # Step 4: Log the normalization for debugging
            if normalized != sql_query:
                logger.info(f"Normalized SQL syntax for DuckDB:")
                logger.info(f"  Original: {sql_query}")
                logger.info(f"  DuckDB:   {normalized}")
            
            return normalized
            
        except Exception as e:
            logger.error(f"Error normalizing SQL syntax: {e}")
            # If normalization fails, at least convert backticks to double quotes
            return re.sub(r'`([^`]+)`', r'"\1"', sql_query)
    
    def get_environment_info(self) -> Dict[str, Any]:
        """Get information about the current data environment for debugging"""
        
        environment = self.discover_data_environment()
        
        return {
            'data_sources_count': len(environment['data_sources']),
            'available_tables': environment['available_tables'],
            'best_table': environment['best_table'],
            'table_analyses': {
                table: {
                    'columns': len(analysis['columns']),
                    'rows': analysis['row_count'],
                    'score': analysis['score']
                }
                for table, analysis in environment['table_analyses'].items()
            }
        }

    def generate_sql_with_provider(self, query: str, provider_name: str, data_source=None) -> Tuple[bool, str]:
        """
        Generate SQL using a specific LLM provider with universal syntax compliance
        UNIVERSAL: Any provider will generate DuckDB-compatible SQL
        
        Args:
            query: Natural language query
            provider_name: Name of LLM provider to use
            data_source: Optional data source specification
        """
        try:
            # Discover the data environment
            environment = self.discover_data_environment()
            
            if not environment['available_tables']:
                return False, "No data tables found"
            
            target_table = environment['best_table']
            if not target_table:
                return False, "No usable data found"
            
            analysis = environment['table_analyses'][target_table]
            schema_description = self.generate_smart_schema_description(analysis, query)
            
            # UNIVERSAL: Generate prompt with universal syntax compliance
            prompt = self._create_universal_prompt(query, schema_description, provider_name)
            
            # Validate prompt compliance
            is_compliant, missing_elements = self.validate_prompt_syntax_compliance(prompt)
            if not is_compliant:
                logger.warning(f"Prompt may be missing SQL syntax elements: {missing_elements}")
            
            # Route to appropriate generation method
            if provider_name.lower() in ['openai', 'gpt']:
                return self._generate_sql_openai(prompt)
            elif provider_name.lower() in getattr(self, 'custom_providers', {}):
                # For custom providers, use Ollama as default execution method
                return self._generate_sql_ollama(prompt)
            else:
                return self._generate_sql_ollama(prompt)
                
        except Exception as e:
            logger.error(f"Failed to generate SQL with provider '{provider_name}': {e}")
            return False, f"SQL generation failed: {str(e)}" 