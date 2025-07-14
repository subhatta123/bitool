#!/usr/bin/env python3
"""
Universal Schema Service - Zero Hardcoding
Replaces all hardcoded business logic with dynamic pattern discovery
"""

import re
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class UniversalSchemaService:
    """
    Universal service that eliminates ALL hardcoding from schema operations
    Works with ANY dataset, business domain, or data structure
    """
    
    def __init__(self):
        # Generic patterns that work across all domains
        self.universal_patterns = {
            'identifier': [r'.*id.*', r'.*key.*', r'.*code.*', r'.*ref.*'],
            'name': [r'.*name.*', r'.*title.*', r'.*label.*'],
            'monetary': [r'.*amount.*', r'.*price.*', r'.*cost.*', r'.*value.*'],
            'quantity': [r'.*count.*', r'.*qty.*', r'.*quantity.*', r'.*total.*'],
            'temporal': [r'.*date.*', r'.*time.*', r'.*created.*', r'.*modified.*'],
            'geographic': [r'.*location.*', r'.*address.*', r'.*city.*', r'.*region.*'],
            'categorical': [r'.*type.*', r'.*category.*', r'.*class.*', r'.*status.*']
        }
    
    def discover_column_patterns(self, column_name: str) -> List[str]:
        """Discover column patterns without any business domain assumptions"""
        col_lower = column_name.lower()
        patterns = []
        
        for pattern_type, pattern_list in self.universal_patterns.items():
            for pattern in pattern_list:
                if re.match(pattern, col_lower):
                    patterns.append(pattern_type)
                    break
        
        return patterns if patterns else ['generic']
    
    def create_universal_column_mapping(self, available_columns: List[str], query_context: str = "") -> Dict[str, str]:
        """
        Create column mapping based on actual available columns, not hardcoded names
        ZERO HARDCODING: Uses pattern matching instead of business assumptions
        """
        mapping = {}
        
        # Find columns by pattern, not hardcoded business terms
        for col in available_columns:
            patterns = self.discover_column_patterns(col)
            
            # Create semantic aliases based on patterns
            if 'identifier' in patterns:
                mapping[f'identifier_column'] = col
            if 'name' in patterns:
                mapping[f'name_column'] = col
            if 'monetary' in patterns:
                mapping[f'monetary_column'] = col
            if 'quantity' in patterns:
                mapping[f'quantity_column'] = col
            if 'temporal' in patterns:
                mapping[f'temporal_column'] = col
            if 'geographic' in patterns:
                mapping[f'geographic_column'] = col
            if 'categorical' in patterns:
                mapping[f'categorical_column'] = col
        
        return mapping
    
    def get_universal_examples(self, columns: List[str]) -> Dict[str, List[str]]:
        """
        Generate universal examples based on actual column names
        ZERO HARDCODING: Examples use actual column names, not fictional ones
        """
        if not columns:
            return {'correct': [], 'incorrect': []}
        
        # Use first few actual columns for examples
        sample_cols = columns[:3]
        quoted_cols = [f'"{col}"' for col in sample_cols]
        
        return {
            'correct': [
                f'SELECT {", ".join(quoted_cols)} FROM "table_name"',
                f'WHERE "{sample_cols[0]}" = \'value\'' if sample_cols else 'WHERE "column" = \'value\'',
                f'GROUP BY "{sample_cols[0]}"' if sample_cols else 'GROUP BY "column"',
                f'ORDER BY "{sample_cols[0]}" DESC' if sample_cols else 'ORDER BY "column" DESC',
            ],
            'incorrect': [
                f'SELECT `{sample_cols[0]}` FROM table  -- WRONG: Uses backticks' if sample_cols else 'SELECT `column` FROM table  -- WRONG',
                f'SELECT {sample_cols[0]} FROM table  -- WRONG: Unquoted' if sample_cols else 'SELECT column FROM table  -- WRONG',
                'WHERE column = value  -- WRONG: Unquoted identifiers',
            ]
        }

# Create global instance
universal_schema_service = UniversalSchemaService() 