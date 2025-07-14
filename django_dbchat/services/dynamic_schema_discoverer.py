#!/usr/bin/env python3
"""
Dynamic Schema Discoverer - Zero Hardcoding
Discovers table schemas, column patterns, and data relationships without any hardcoded assumptions
Works with ANY dataset regardless of business domain or data structure
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class DynamicSchemaDiscoverer:
    """
    Universal schema discovery service that works with any dataset
    ZERO HARDCODING: No business domain assumptions or hardcoded column names
    """
    
    def __init__(self):
        # Define generic patterns that can apply to any domain
        self.column_patterns = {
            'identifier': [
                r'.*id.*', r'.*key.*', r'.*code.*', r'.*number.*', r'.*ref.*',
                r'.*identifier.*', r'.*reference.*'
            ],
            'name': [
                r'.*name.*', r'.*title.*', r'.*label.*', r'.*description.*'
            ],
            'monetary': [
                r'.*amount.*', r'.*price.*', r'.*cost.*', r'.*value.*', r'.*revenue.*',
                r'.*income.*', r'.*expense.*', r'.*fee.*', r'.*charge.*', r'.*rate.*'
            ],
            'quantity': [
                r'.*count.*', r'.*qty.*', r'.*quantity.*', r'.*units.*', r'.*items.*',
                r'.*volume.*', r'.*total.*', r'.*sum.*'
            ],
            'temporal': [
                r'.*date.*', r'.*time.*', r'.*created.*', r'.*updated.*', r'.*modified.*',
                r'.*start.*', r'.*end.*', r'.*expiry.*', r'.*deadline.*'
            ],
            'geographic': [
                r'.*location.*', r'.*address.*', r'.*city.*', r'.*state.*', r'.*country.*',
                r'.*region.*', r'.*area.*', r'.*zone.*', r'.*territory.*', r'.*postal.*'
            ],
            'categorical': [
                r'.*type.*', r'.*category.*', r'.*class.*', r'.*group.*', r'.*segment.*',
                r'.*status.*', r'.*state.*', r'.*level.*', r'.*grade.*', r'.*rank.*'
            ],
            'contact': [
                r'.*email.*', r'.*phone.*', r'.*contact.*', r'.*mobile.*', r'.*tel.*'
            ]
        }
    
    def discover_table_schema(self, table_name: str, connection) -> Dict[str, Any]:
        """
        Dynamically discover table schema without any hardcoded assumptions
        UNIVERSAL: Works with any table structure or business domain
        """
        try:
            # Get basic schema information
            schema_info = connection.execute(f"DESCRIBE {table_name}").fetchall()
            
            schema = {
                'table_name': table_name,
                'columns': [],
                'column_types': {},
                'column_patterns': {},
                'semantic_categories': {},
                'sample_data': {},
                'data_statistics': {},
                'relationships': {},
                'has_data': False,
                'row_count': 0
            }
            
            # Extract column information
            for row in schema_info:
                col_name = row[0]
                col_type = row[1].upper()
                
                schema['columns'].append(col_name)
                schema['column_types'][col_name] = col_type
                
                # Categorize column by pattern (no hardcoding)
                pattern_category = self._categorize_column_by_pattern(col_name)
                schema['column_patterns'][col_name] = pattern_category
                
                # Determine semantic category
                semantic_category = self._determine_semantic_category(col_name, col_type)
                schema['semantic_categories'][col_name] = semantic_category
            
            # Get data statistics
            schema['row_count'] = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            schema['has_data'] = schema['row_count'] > 0
            
            if schema['has_data']:
                # Sample data for pattern analysis
                self._analyze_column_data(connection, table_name, schema)
                
                # Discover relationships between columns
                self._discover_column_relationships(connection, table_name, schema)
            
            logger.info(f"Discovered schema for {table_name}: {len(schema['columns'])} columns, {schema['row_count']} rows")
            return schema
            
        except Exception as e:
            logger.error(f"Error discovering schema for {table_name}: {e}")
            return {'error': str(e)}
    
    def _categorize_column_by_pattern(self, column_name: str) -> List[str]:
        """
        Categorize column by name patterns without hardcoded business logic
        UNIVERSAL: Pattern-based, not domain-specific
        """
        col_lower = column_name.lower()
        categories = []
        
        for category, patterns in self.column_patterns.items():
            for pattern in patterns:
                if re.match(pattern, col_lower):
                    categories.append(category)
                    break
        
        return categories if categories else ['unknown']
    
    def _determine_semantic_category(self, column_name: str, column_type: str) -> str:
        """
        Determine semantic category based on name and type patterns
        UNIVERSAL: No business domain assumptions
        """
        col_lower = column_name.lower()
        type_upper = column_type.upper()
        
        # Type-based categorization first
        if any(t in type_upper for t in ['DATE', 'TIME', 'TIMESTAMP']):
            return 'temporal'
        elif any(t in type_upper for t in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC']):
            return 'numeric'
        elif any(t in type_upper for t in ['BOOL', 'BOOLEAN']):
            return 'boolean'
        
        # Pattern-based categorization
        patterns = self._categorize_column_by_pattern(column_name)
        if patterns and patterns[0] != 'unknown':
            return patterns[0]
        
        # Default to text
        return 'text'
    
    def _analyze_column_data(self, connection, table_name: str, schema: Dict[str, Any]):
        """
        Analyze actual column data to understand patterns and content
        UNIVERSAL: Content-based analysis, no business assumptions
        """
        try:
            for col in schema['columns'][:10]:  # Limit for performance
                try:
                    # Get sample values
                    sample_query = f'SELECT DISTINCT "{col}" FROM {table_name} WHERE "{col}" IS NOT NULL LIMIT 10'
                    sample_values = [row[0] for row in connection.execute(sample_query).fetchall()]
                    schema['sample_data'][col] = sample_values
                    
                    # Get basic statistics
                    stats_query = f'''
                        SELECT 
                            COUNT(*) as total_count,
                            COUNT("{col}") as non_null_count,
                            COUNT(DISTINCT "{col}") as unique_count
                        FROM {table_name}
                    '''
                    stats = connection.execute(stats_query).fetchone()
                    schema['data_statistics'][col] = {
                        'total_count': stats[0],
                        'non_null_count': stats[1],
                        'unique_count': stats[2],
                        'null_percentage': ((stats[0] - stats[1]) / stats[0] * 100) if stats[0] > 0 else 0,
                        'uniqueness_ratio': (stats[2] / stats[1]) if stats[1] > 0 else 0
                    }
                    
                except Exception as col_error:
                    logger.debug(f"Error analyzing column {col}: {col_error}")
                    schema['sample_data'][col] = []
                    schema['data_statistics'][col] = {}
                    
        except Exception as e:
            logger.error(f"Error analyzing column data: {e}")
    
    def _discover_column_relationships(self, connection, table_name: str, schema: Dict[str, Any]):
        """
        Discover relationships between columns without hardcoded assumptions
        UNIVERSAL: Pattern-based relationship discovery
        """
        try:
            relationships = {}
            columns = schema['columns']
            
            # Find potential key relationships
            for col in columns:
                stats = schema['data_statistics'].get(col, {})
                uniqueness = stats.get('uniqueness_ratio', 0)
                
                if uniqueness > 0.9:  # Likely unique identifier
                    relationships[col] = {'type': 'primary_key_candidate', 'confidence': uniqueness}
                elif uniqueness > 0.7:  # Likely foreign key or secondary identifier
                    relationships[col] = {'type': 'identifier_candidate', 'confidence': uniqueness}
            
            # Find potential grouping columns (low uniqueness, high frequency)
            for col in columns:
                stats = schema['data_statistics'].get(col, {})
                uniqueness = stats.get('uniqueness_ratio', 0)
                
                if uniqueness < 0.1 and stats.get('non_null_count', 0) > 10:  # Likely categorical
                    relationships[col] = {'type': 'grouping_candidate', 'confidence': 1 - uniqueness}
            
            schema['relationships'] = relationships
            
        except Exception as e:
            logger.error(f"Error discovering relationships: {e}")
            schema['relationships'] = {}
    
    def generate_universal_schema_description(self, schema: Dict[str, Any], query_context: str = "") -> str:
        """
        Generate schema description without any hardcoded business terms
        UNIVERSAL: Pure pattern-based description
        """
        table_name = schema['table_name']
        columns = schema['columns']
        
        # Build column descriptions with discovered patterns
        column_descriptions = []
        for col in columns[:20]:  # Limit for prompt size
            col_type = schema['column_types'].get(col, 'TEXT')
            patterns = schema['column_patterns'].get(col, ['unknown'])
            semantic_cat = schema['semantic_categories'].get(col, 'text')
            
            # Get sample values for context
            samples = schema['sample_data'].get(col, [])
            sample_text = f" (examples: {', '.join(map(str, samples[:3]))[:50]}...)" if samples else ""
            
            # Build pattern description
            pattern_text = f" [patterns: {', '.join(patterns)}]" if patterns != ['unknown'] else ""
            
            column_descriptions.append(f'"{col}": {col_type} ({semantic_cat}){pattern_text}{sample_text}')
        
        # Build relationships description
        relationships_desc = ""
        if schema.get('relationships'):
            key_candidates = [col for col, rel in schema['relationships'].items() if 'key' in rel['type']]
            grouping_candidates = [col for col, rel in schema['relationships'].items() if 'grouping' in rel['type']]
            
            if key_candidates:
                relationships_desc += f"\nIdentifier columns: {', '.join([f'\"{col}\"' for col in key_candidates])}"
            if grouping_candidates:
                relationships_desc += f"\nGrouping columns: {', '.join([f'\"{col}\"' for col in grouping_candidates])}"
        
        # Format column names for rules
        quoted_columns = [f'"{col}"' for col in columns[:15]]
        column_list = ", ".join(quoted_columns)
        
        schema_parts = [
            f'Table: "{table_name}" (single table containing all data)',
            f"Total columns: {len(columns)} | Rows: {schema['row_count']:,}",
            "",
            "EXACT COLUMN NAMES (use these exact names with double quotes):",
            *[f"  - {desc}" for desc in column_descriptions],
            relationships_desc,
            "",
            "CRITICAL COLUMN USAGE RULES:",
            f"- Use ONLY these exact column names: {column_list}",
            f'- Table name: "{table_name}"',
            "- Do NOT guess or invent column names",
            "- Do NOT create JOINs or reference other tables",
            "- All data is in this single flat table",
        ]
        
        return "\n".join(filter(None, schema_parts))
    
    def find_columns_by_pattern(self, schema: Dict[str, Any], pattern_type: str) -> List[str]:
        """
        Find columns matching a specific pattern type
        UNIVERSAL: Pattern-based search, no hardcoded business logic
        """
        matching_columns = []
        
        for col, patterns in schema.get('column_patterns', {}).items():
            if pattern_type in patterns:
                matching_columns.append(col)
        
        return matching_columns
    
    def get_column_semantic_description(self, column_name: str, schema: Dict[str, Any]) -> str:
        """
        Get semantic description of a column based on discovered patterns
        UNIVERSAL: No hardcoded business domain assumptions
        """
        patterns = schema.get('column_patterns', {}).get(column_name, ['unknown'])
        semantic_cat = schema.get('semantic_categories', {}).get(column_name, 'text')
        
        # Generate description based on patterns
        if 'identifier' in patterns:
            return f"unique identifier or reference code"
        elif 'name' in patterns:
            return f"name or title field"
        elif 'monetary' in patterns:
            return f"monetary value or amount"
        elif 'quantity' in patterns:
            return f"quantity or count value"
        elif 'temporal' in patterns:
            return f"date or time information"
        elif 'geographic' in patterns:
            return f"location or geographic information"
        elif 'categorical' in patterns:
            return f"category or classification"
        elif 'contact' in patterns:
            return f"contact information"
        else:
            return f"{semantic_cat} data"
    
    def discover_query_patterns(self, schema: Dict[str, Any], query: str) -> Dict[str, Any]:
        """
        Discover relevant patterns in the query without hardcoded business logic
        UNIVERSAL: Works with any query type or business domain
        """
        query_lower = query.lower()
        
        analysis = {
            'intent': 'unknown',
            'relevant_columns': [],
            'aggregation_type': None,
            'filtering_hints': [],
            'grouping_hints': [],
            'sorting_hints': []
        }
        
        # Detect aggregation intent
        if any(agg in query_lower for agg in ['sum', 'total', 'count', 'average', 'max', 'min']):
            analysis['intent'] = 'aggregation'
            if 'sum' in query_lower or 'total' in query_lower:
                analysis['aggregation_type'] = 'sum'
            elif 'count' in query_lower:
                analysis['aggregation_type'] = 'count'
            elif 'average' in query_lower or 'avg' in query_lower:
                analysis['aggregation_type'] = 'average'
        
        # Detect top/bottom intent
        elif any(word in query_lower for word in ['top', 'bottom', 'highest', 'lowest', 'best', 'worst']):
            analysis['intent'] = 'ranking'
        
        # Find relevant columns by matching query terms with column names
        for col in schema['columns']:
            col_lower = col.lower()
            # Check for partial matches in column names
            col_words = re.split(r'[_\s]+', col_lower)
            query_words = re.split(r'[_\s]+', query_lower)
            
            if any(qword in col_words for qword in query_words if len(qword) > 2):
                analysis['relevant_columns'].append(col)
        
        return analysis 