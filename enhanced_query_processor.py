"""
Enhanced Query Processor
Processes natural language queries with awareness of multiple data sources,
suggests joins, and can perform cross-source analysis
"""

import streamlit as st
import pandas as pd
import json
import re
from typing import Dict, List, Tuple, Optional, Any
import data_integration

class EnhancedQueryProcessor:
    """Enhanced query processor with multi-source awareness"""
    
    def __init__(self):
        self.integration_engine = data_integration.data_integration_engine
    
    def analyze_query_for_multi_source(self, query: str) -> Dict[str, Any]:
        """
        Analyze a natural language query to determine if it requires multiple data sources
        and suggest appropriate ETL operations
        """
        analysis = {
            'requires_multiple_sources': False,
            'suggested_sources': [],
            'suggested_joins': [],
            'suggested_etl_operations': [],
            'confidence': 0.0,
            'reasoning': []
        }
        
        # Get available data sources
        summary = self.integration_engine.get_data_sources_summary()
        available_sources = summary['sources']
        
        if len(available_sources) < 2:
            analysis['reasoning'].append("Only one or no data sources available")
            return analysis
        
        # Keywords that might indicate cross-source queries
        cross_source_keywords = [
            'join', 'combine', 'merge', 'relate', 'link', 'connect',
            'both', 'together', 'across', 'between', 'and', 'with',
            'match', 'correlate', 'compare'
        ]
        
        query_lower = query.lower()
        
        # Check for cross-source indicators
        cross_source_indicators = sum(1 for keyword in cross_source_keywords if keyword in query_lower)
        
        # Check if multiple source names are mentioned
        mentioned_sources = []
        for source in available_sources:
            source_name_variants = [
                source['name'].lower(),
                source['name'].lower().replace('_', ' '),
                source['name'].lower().replace('-', ' ')
            ]
            
            for variant in source_name_variants:
                if variant in query_lower:
                    mentioned_sources.append(source)
                    break
        
        # Calculate confidence
        confidence = 0.0
        
        if len(mentioned_sources) >= 2:
            confidence += 0.6
            analysis['reasoning'].append(f"Multiple sources mentioned: {[s['name'] for s in mentioned_sources]}")
        
        if cross_source_indicators > 0:
            confidence += min(0.4, cross_source_indicators * 0.1)
            analysis['reasoning'].append(f"Cross-source keywords found: {cross_source_indicators}")
        
        # Check for specific relationship patterns
        relationship_patterns = [
            r'(\w+)\s+and\s+(\w+)',  # "customers and orders"
            r'(\w+)\s+with\s+(\w+)',  # "products with sales"
            r'(\w+)\s+by\s+(\w+)',   # "revenue by customer"
            r'(\w+)\s+from\s+(\w+)'  # "data from table1 and table2"
        ]
        
        for pattern in relationship_patterns:
            matches = re.findall(pattern, query_lower)
            if matches:
                confidence += 0.2
                analysis['reasoning'].append(f"Relationship pattern found: {matches}")
        
        analysis['confidence'] = min(confidence, 1.0)
        analysis['requires_multiple_sources'] = confidence > 0.3
        analysis['suggested_sources'] = mentioned_sources if mentioned_sources else available_sources[:2]
        
        # Generate join suggestions if applicable
        if analysis['requires_multiple_sources']:
            suggested_joins = self.integration_engine.get_suggested_joins()
            analysis['suggested_joins'] = suggested_joins[:3]  # Top 3 suggestions
            
            # Suggest ETL operations
            if analysis['suggested_joins']:
                analysis['suggested_etl_operations'] = [
                    {
                        'type': 'join',
                        'description': f"Join {join['source1_name']} with {join['source2_name']}",
                        'confidence': join['confidence'],
                        'join_type': join['join_type']
                    }
                    for join in analysis['suggested_joins']
                ]
        
        return analysis
    
    def suggest_etl_pipeline(self, query: str, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest a complete ETL pipeline for the query"""
        pipeline = []
        
        if not analysis['requires_multiple_sources']:
            return pipeline
        
        # Step 1: Data integration (joins)
        for join_suggestion in analysis['suggested_joins']:
            step = {
                'step_number': len(pipeline) + 1,
                'operation': 'join',
                'description': f"Join {join_suggestion['source1_name']} with {join_suggestion['source2_name']}",
                'parameters': {
                    'join_type': join_suggestion['join_type'],
                    'left_table': f"source_{join_suggestion['relationship'].source1_id}",
                    'right_table': f"source_{join_suggestion['relationship'].source2_id}",
                    'left_column': join_suggestion['relationship'].source1_column,
                    'right_column': join_suggestion['relationship'].source2_column
                },
                'output_name': f"joined_{join_suggestion['source1_name']}_{join_suggestion['source2_name']}",
                'confidence': join_suggestion['confidence']
            }
            pipeline.append(step)
        
        # Step 2: Analysis operations (based on query intent)
        query_lower = query.lower()
        
        # Check for aggregation needs
        agg_keywords = ['total', 'sum', 'average', 'count', 'max', 'min', 'group by']
        if any(keyword in query_lower for keyword in agg_keywords):
            step = {
                'step_number': len(pipeline) + 1,
                'operation': 'aggregate',
                'description': "Perform aggregation based on query requirements",
                'parameters': {
                    'detected_functions': [kw for kw in agg_keywords if kw in query_lower]
                },
                'confidence': 0.7
            }
            pipeline.append(step)
        
        # Step 3: Filtering (if conditions are mentioned)
        filter_keywords = ['where', 'filter', 'only', 'exclude', 'include']
        if any(keyword in query_lower for keyword in filter_keywords):
            step = {
                'step_number': len(pipeline) + 1,
                'operation': 'filter',
                'description': "Apply filters based on query conditions",
                'parameters': {
                    'detected_conditions': [kw for kw in filter_keywords if kw in query_lower]
                },
                'confidence': 0.6
            }
            pipeline.append(step)
        
        return pipeline
    
    def execute_etl_pipeline(self, pipeline: List[Dict[str, Any]]) -> Optional[str]:
        """Execute the suggested ETL pipeline"""
        current_table = None
        
        for step in pipeline:
            try:
                if step['operation'] == 'join':
                    operation_name = f"AutoETL_{step['output_name']}"
                    
                    operation_id = self.integration_engine.create_etl_operation(
                        name=operation_name,
                        operation_type='join',
                        source_tables=[step['parameters']['left_table'], step['parameters']['right_table']],
                        parameters={
                            'join_type': step['parameters']['join_type'],
                            'left_column': step['parameters']['left_column'],
                            'right_column': step['parameters']['right_column']
                        }
                    )
                    
                    # Get the output table name
                    for operation in self.integration_engine.etl_operations:
                        if operation.id == operation_id:
                            current_table = operation.output_table_name
                            break
                
                # Add more operation types as needed
                
            except Exception as e:
                st.error(f"Failed to execute ETL step {step['step_number']}: {e}")
                return None
        
        return current_table
    
    def get_enhanced_prompt(self, query: str, schema: Any, analysis: Dict[str, Any]) -> str:
        """Generate an enhanced prompt that includes multi-source context"""
        
        if not analysis['requires_multiple_sources']:
            # Use standard prompt for single-source queries
            return self._get_standard_prompt(query, schema)
        
        # Multi-source prompt
        prompt_parts = [
            "You are an expert AI assistant that converts natural language questions into SQL queries.",
            "The user has MULTIPLE DATA SOURCES that may need to be joined or combined.",
            ""
        ]
        
        # Add available sources information
        summary = self.integration_engine.get_data_sources_summary()
        if summary['total_sources'] > 0:
            prompt_parts.append("AVAILABLE DATA SOURCES:")
            for source in summary['sources']:
                prompt_parts.append(f"- {source['name']} ({source['type']}) - Table: source_{source['id']}")
            prompt_parts.append("")
        
        # Add schema information
        if isinstance(schema, list):  # CSV/API format
            prompt_parts.append("CURRENT ACTIVE SCHEMA:")
            prompt_parts.append("Columns:")
            for col_info in schema:
                prompt_parts.append(f"  - {col_info['name']} ({col_info['type']})")
        else:
            prompt_parts.append("CURRENT ACTIVE SCHEMA:")
            prompt_parts.append(str(schema))
        
        prompt_parts.append("")
        
        # Add join suggestions if available
        if analysis['suggested_joins']:
            prompt_parts.append("SUGGESTED RELATIONSHIPS:")
            for join in analysis['suggested_joins'][:3]:
                prompt_parts.append(
                    f"- {join['source1_name']}.{join['relationship'].source1_column} "
                    f"can be joined with {join['source2_name']}.{join['relationship'].source2_column} "
                    f"(Confidence: {join['confidence']:.1%})"
                )
            prompt_parts.append("")
        
        # Analysis context
        prompt_parts.extend([
            f"QUERY ANALYSIS:",
            f"- Requires multiple sources: {analysis['requires_multiple_sources']}",
            f"- Confidence: {analysis['confidence']:.1%}",
            f"- Reasoning: {'; '.join(analysis['reasoning'])}",
            ""
        ])
        
        # Instructions
        prompt_parts.extend([
            f"USER QUESTION: {query}",
            "",
            "INSTRUCTIONS:",
            "1. If this query requires data from multiple sources, suggest using JOIN operations",
            "2. Use the table names in the format 'source_[id]' for different data sources",
            "3. If joins are needed, explain what should be joined and why",
            "4. Generate the most appropriate SQL query or suggest ETL operations needed",
            "",
            "RESPONSE FORMAT:",
            "Provide either:",
            "A) A direct SQL query if data is available in current source, OR",
            "B) ETL suggestions in the format: 'ETL_NEEDED: [description of required operations]'",
            "",
            "SQL Query or ETL Suggestion:"
        ])
        
        return "\n".join(prompt_parts)
    
    def _get_standard_prompt(self, query: str, schema: Any) -> str:
        """Generate standard prompt for single-source queries"""
        # This would be similar to the existing get_sql_from_openai prompt
        # but we can enhance it as needed
        return f"""
        You are an expert AI assistant that converts natural language questions into SQL queries.
        
        Schema: {schema}
        
        User Question: {query}
        
        Generate a syntactically correct SQL query to answer the question.
        Only return the SQL query, with no other explanatory text.
        """

# Global instance
enhanced_query_processor = EnhancedQueryProcessor() 