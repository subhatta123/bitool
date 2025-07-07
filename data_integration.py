
"""
Multi-Source Data Integration and AI-Driven ETL Module
Handles multiple data sources, schema analysis, join detection, and ETL operations
"""

import pandas as pd
import sqlite3
import json
import re
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import streamlit as st
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DataSource:
    """Represents a data source in the integration system"""
    id: str
    name: str
    type: str
    connection_info: Dict[str, Any]
    schema: Dict[str, Any]
    status: str
    created_at: str
    last_updated: str

@dataclass
class DataRelationship:
    """Represents a detected relationship between data sources"""
    source1_id: str
    source1_table: str
    source1_column: str
    source2_id: str
    source2_table: str
    source2_column: str
    relationship_type: str
    confidence_score: float
    suggested_join_type: str

@dataclass
class ETLOperation:
    """Represents an ETL operation"""
    id: str
    name: str
    operation_type: str
    source_tables: List[str]
    parameters: Dict[str, Any]
    output_table_name: str
    sql_query: str
    created_at: str

class DataIntegrationEngine:
    """Main engine for data integration and ETL operations"""
    
    def __init__(self):
        self.data_sources: Dict[str, DataSource] = {}
        self.relationships: List[DataRelationship] = []
        self.etl_operations: List[ETLOperation] = []
        self.integrated_db: Optional[sqlite3.Connection] = None
        self._init_integrated_database()
    
    def _init_integrated_database(self):
        try:
            self.integrated_db = sqlite3.connect(':memory:', check_same_thread=False)
            logger.info("Integrated database initialized")
        except Exception as e:
            logger.error(f"Failed to initialize integrated database: {e}")
    
    def add_data_source(self, name: str, source_type: str, connection_info: Dict, data: Optional[pd.DataFrame] = None) -> str:
        source_id = f"{source_type}_{len(self.data_sources) + 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        if data is None:
            data = pd.DataFrame()
        
        data, original_to_cleaned_mapping = self._clean_column_names(data)
        schema = self._analyze_schema(data, source_type, connection_info, original_to_cleaned_mapping)
        
        data_source = DataSource(
            id=source_id, name=name, type=source_type, connection_info=connection_info,
            schema=schema, status='connected', created_at=datetime.now().isoformat(),
            last_updated=datetime.now().isoformat()
        )
        
        self.data_sources[source_id] = data_source
        self._load_cleaned_data_to_integrated_db(source_id, data)
        self._detect_relationships(source_id)
        
        logger.info(f"Added data source: {name} ({source_id})")
        return source_id
    
    def remove_data_source(self, source_id: str) -> bool:
        """Remove a data source and its data"""
        if source_id not in self.data_sources:
            return False
        
        try:
            # Remove from integrated database
            if self.integrated_db:
                table_name = f"source_{source_id}"
                cursor = self.integrated_db.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.integrated_db.commit()
            
            # Remove from data sources
            del self.data_sources[source_id]
            
            # Remove relationships involving this source
            self.relationships = [rel for rel in self.relationships 
                                if rel.source1_id != source_id and rel.source2_id != source_id]
            
            # Remove ETL operations involving this source
            self.etl_operations = [op for op in self.etl_operations 
                                 if not any(f"source_{source_id}" in table for table in op.source_tables)]
            
            logger.info(f"Removed data source: {source_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove data source {source_id}: {e}")
            return False
    
    def _clean_column_names(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
        cleaned_data = data.copy()
        original_columns = cleaned_data.columns.tolist()
        cleaned_columns = []
        
        for col in original_columns:
            cleaned_col = re.sub(r'[^a-zA-Z0-9_]', '_', str(col))
            if cleaned_col and cleaned_col[0].isdigit():
                cleaned_col = f"col_{cleaned_col}"
            if not cleaned_col or cleaned_col == '_':
                cleaned_col = f"column_{len(cleaned_columns)}"
            cleaned_columns.append(cleaned_col)
        
        column_mapping = dict(zip(original_columns, cleaned_columns))
        if cleaned_columns != original_columns:
            cleaned_data.rename(columns=column_mapping, inplace=True)
        return cleaned_data, column_mapping
    
    def _load_cleaned_data_to_integrated_db(self, source_id: str, data: pd.DataFrame):
        if not self.integrated_db:
            logger.error("Integrated database not connected.")
            return
        try:
            table_name = f"source_{source_id}"
            data.to_sql(table_name, self.integrated_db, if_exists='replace', index=False)
            logger.info(f"Loaded {len(data)} rows into table {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data for source {source_id}: {e}")
            raise
    
    def _analyze_schema(self, data: pd.DataFrame, source_type: str, connection_info: Dict, column_mapping: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        if column_mapping is None:
            column_mapping = {}
        schema = {'tables': {'main_table': {'columns': {}, 'row_count': len(data), 'sample_data': [], 'original_column_mapping': column_mapping}}}
        if len(data) > 0:
            schema['tables']['main_table']['sample_data'] = data.head(3).to_dict('records')
        
        for col in data.columns:
            col_info = {'type': str(data[col].dtype), 'null_count': data[col].isnull().sum(), 'unique_count': data[col].nunique(), 'sample_values': data[col].dropna().head(5).tolist()}
            if col_info['unique_count'] == len(data) and col_info['null_count'] == 0:
                col_info['potential_key'] = True
            
            original_col = next((orig for orig, cleaned in column_mapping.items() if cleaned == col), col)
            check_name = original_col.lower()
            if any(keyword in check_name for keyword in ['id', 'key', 'ref']):
                col_info['potential_foreign_key'] = True
            schema['tables']['main_table']['columns'][col] = col_info
        return schema
    
    def _detect_relationships(self, new_source_id: str):
        new_source = self.data_sources.get(new_source_id)
        if not new_source: return

        for existing_id, existing_source in self.data_sources.items():
            if existing_id == new_source_id: continue
            relationships = self._find_column_relationships(new_source_id, new_source.schema, existing_id, existing_source.schema)
            self.relationships.extend(relationships)
    
    def _find_column_relationships(self, source1_id: str, schema1: Dict, source2_id: str, schema2: Dict) -> List[DataRelationship]:
        """Find potential relationships between columns in two data sources"""
        relationships = []
        
        # Get main table columns from both schemas
        table1_columns = schema1.get('tables', {}).get('main_table', {}).get('columns', {})
        table2_columns = schema2.get('tables', {}).get('main_table', {}).get('columns', {})
        
        for col1_name, col1_info in table1_columns.items():
            for col2_name, col2_info in table2_columns.items():
                confidence = self._calculate_relationship_confidence(col1_name, col1_info, col2_name, col2_info)
                
                if confidence > 0.6:  # Threshold for potential relationship
                    relationship_type = self._determine_relationship_type(col1_info, col2_info)
                    suggested_join = self._suggest_join_type(relationship_type, col1_info, col2_info)
                    
                    relationship = DataRelationship(
                        source1_id=source1_id,
                        source1_table='main_table',
                        source1_column=col1_name,
                        source2_id=source2_id,
                        source2_table='main_table', 
                        source2_column=col2_name,
                        relationship_type=relationship_type,
                        confidence_score=confidence,
                        suggested_join_type=suggested_join
                    )
                    relationships.append(relationship)
                    
        return relationships

    def _calculate_relationship_confidence(self, col1_name: str, col1_info: Dict, col2_name: str, col2_info: Dict) -> float:
        """Calculate confidence score for potential relationship between two columns"""
        confidence = 0.0
        
        # Name similarity (most important factor)
        name_similarity = self._calculate_name_similarity(col1_name, col2_name)
        confidence += name_similarity * 0.5
        
        # Type compatibility
        if self._are_types_compatible(col1_info.get('type', ''), col2_info.get('type', '')):
            confidence += 0.3
        
        # Check for key indicators
        if col1_info.get('potential_key') or col1_info.get('potential_foreign_key'):
            confidence += 0.1
        if col2_info.get('potential_key') or col2_info.get('potential_foreign_key'):
            confidence += 0.1
        
        # Unique value ratio similarity
        unique1 = col1_info.get('unique_count', 0)
        unique2 = col2_info.get('unique_count', 0)
        if unique1 > 0 and unique2 > 0:
            ratio_diff = abs(unique1 - unique2) / max(unique1, unique2)
            confidence += (1 - ratio_diff) * 0.1
        
        return min(confidence, 1.0)

    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between column names"""
        name1_lower = name1.lower()
        name2_lower = name2.lower()
        
        # Exact match
        if name1_lower == name2_lower:
            return 1.0
        
        # Common key patterns
        key_patterns = ['id', 'key', 'ref', 'code', 'num', 'number']
        for pattern in key_patterns:
            if pattern in name1_lower and pattern in name2_lower:
                return 0.8
        
        # Substring match
        if name1_lower in name2_lower or name2_lower in name1_lower:
            return 0.7
        
        # Common prefixes/suffixes
        for i in range(3, min(len(name1_lower), len(name2_lower)) + 1):
            if name1_lower[:i] == name2_lower[:i] or name1_lower[-i:] == name2_lower[-i:]:
                return 0.6
        
        return 0.0

    def _are_types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two column types are compatible for joining"""
        # Normalize type names
        type1_norm = type1.lower()
        type2_norm = type2.lower()
        
        # Exact match
        if type1_norm == type2_norm:
            return True
        
        # Integer types
        int_types = ['int', 'integer', 'bigint', 'smallint']
        if any(t in type1_norm for t in int_types) and any(t in type2_norm for t in int_types):
            return True
        
        # String types
        str_types = ['str', 'string', 'varchar', 'char', 'text', 'object']
        if any(t in type1_norm for t in str_types) and any(t in type2_norm for t in str_types):
            return True
        
        # Float types
        float_types = ['float', 'double', 'decimal', 'numeric']
        if any(t in type1_norm for t in float_types) and any(t in type2_norm for t in float_types):
            return True
        
        return False

    def _determine_relationship_type(self, col1_info: Dict, col2_info: Dict) -> str:
        """Determine the type of relationship between two columns"""
        unique1 = col1_info.get('unique_count', 0)
        unique2 = col2_info.get('unique_count', 0)
        
        # Check if either column is a potential key
        is_key1 = col1_info.get('potential_key', False)
        is_key2 = col2_info.get('potential_key', False)
        
        if is_key1 and is_key2:
            return 'one_to_one'
        elif is_key1 and not is_key2:
            return 'one_to_many'
        elif not is_key1 and is_key2:
            return 'many_to_one'
        else:
            # Use unique count ratio as heuristic
            if unique1 == unique2:
                return 'one_to_one'
            elif unique1 > unique2 * 0.8:
                return 'one_to_many'
            elif unique2 > unique1 * 0.8:
                return 'many_to_one'
            else:
                return 'many_to_many'

    def _suggest_join_type(self, relationship_type: str, col1_info: Dict, col2_info: Dict) -> str:
        """Suggest appropriate join type based on relationship"""
        null_count1 = col1_info.get('null_count', 0)
        null_count2 = col2_info.get('null_count', 0)
        
        # If either column has many nulls, suggest LEFT or RIGHT join
        if null_count1 > 0 and null_count2 == 0:
            return 'RIGHT'
        elif null_count2 > 0 and null_count1 == 0:
            return 'LEFT'
        elif null_count1 > 0 or null_count2 > 0:
            return 'FULL'
        else:
            return 'INNER'

    def get_suggested_joins(self) -> List[Dict[str, Any]]:
        """Get AI-suggested joins between data sources"""
        suggestions = []
        
        for relationship in self.relationships:
            # Get source names
            source1 = self.data_sources.get(relationship.source1_id)
            source2 = self.data_sources.get(relationship.source2_id)
            
            if not source1 or not source2:
                continue
            
            suggestion = {
                'relationship': relationship,
                'source1_name': source1.name,
                'source2_name': source2.name,
                'confidence': relationship.confidence_score,
                'join_type': relationship.suggested_join_type,
                'suggestion_text': f"Join {source1.name}.{relationship.source1_column} with {source2.name}.{relationship.source2_column}"
            }
            suggestions.append(suggestion)
        
        # Sort by confidence score
        suggestions.sort(key=lambda x: x['confidence'], reverse=True)
        return suggestions
    
    def create_etl_operation(self, name: str, operation_type: str, source_tables: List[str], parameters: Dict[str, Any]) -> str:
        operation_id = f"etl_{len(self.etl_operations) + 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        sql_query = self._generate_etl_sql(operation_type, source_tables, parameters)
        
        operation = ETLOperation(
            id=operation_id, name=name, operation_type=operation_type,
            source_tables=source_tables, parameters=parameters,
            output_table_name=f"etl_output_{operation_id}",
            sql_query=sql_query, created_at=datetime.now().isoformat()
        )
        self.etl_operations.append(operation)
        self._execute_etl_operation(operation)
        return operation_id
    
    def _generate_etl_sql(self, operation_type: str, source_tables: List[str], parameters: Dict[str, Any]) -> str:
        """Generate SQL for ETL operations"""
        if operation_type.lower() == 'join':
            return self._generate_join_sql(source_tables, parameters)
        elif operation_type.lower() == 'union':
            return self._generate_union_sql(source_tables, parameters)
        elif operation_type.lower() == 'aggregate':
            return self._generate_aggregate_sql(source_tables, parameters)
        else:
            raise ValueError(f"Unsupported ETL operation type: {operation_type}")
        return ""

    def _generate_join_sql(self, source_tables: List[str], parameters: Dict[str, Any]) -> str:
        if len(source_tables) < 2: raise ValueError("JOIN requires at least 2 tables")
        left_table, right_table = source_tables[0], source_tables[1]
        left_column, right_column = parameters.get('left_column'), parameters.get('right_column')
        if not left_column or not right_column: raise ValueError("JOIN requires left and right columns")
        
        left_column_quoted = f'"{left_column}"' if isinstance(left_column, str) and ' ' in left_column else left_column
        right_column_quoted = f'"{right_column}"' if isinstance(right_column, str) and ' ' in right_column else right_column
        
        return f"SELECT * FROM {left_table} t1 {parameters.get('join_type', 'INNER')} JOIN {right_table} t2 ON t1.{left_column_quoted} = t2.{right_column_quoted}"
    
    def _generate_union_sql(self, source_tables: List[str], parameters: Dict[str, Any]) -> str:
        """Generate SQL for UNION operation"""
        if len(source_tables) < 2:
            raise ValueError("UNION requires at least 2 tables")
        
        union_type = parameters.get('union_type', 'UNION')
        return f" {union_type} ".join([f"SELECT * FROM {table}" for table in source_tables])

    def _generate_aggregate_sql(self, source_tables: List[str], parameters: Dict[str, Any]) -> str:
        """Generate SQL for aggregate operation"""
        if len(source_tables) != 1:
            raise ValueError("AGGREGATE requires exactly 1 table")
        
        table = source_tables[0]
        group_by_columns = parameters.get('group_by_columns', [])
        aggregate_functions = parameters.get('aggregate_functions', {})
        
        # Build SELECT clause
        select_parts = []
        for col in group_by_columns:
            select_parts.append(col)
        
        for col, func in aggregate_functions.items():
            select_parts.append(f"{func}({col}) as {func.lower()}_{col}")
        
        sql = f"SELECT {', '.join(select_parts)} FROM {table}"
        
        if group_by_columns:
            sql += f" GROUP BY {', '.join(group_by_columns)}"
        
        return sql

    def _execute_etl_operation(self, operation: ETLOperation):
        if not self.integrated_db:
            logger.error("Integrated database not connected.")
            return
        try:
            result_df = pd.read_sql_query(operation.sql_query, self.integrated_db)
            result_df.to_sql(operation.output_table_name, self.integrated_db, if_exists='replace', index=False)
            logger.info(f"Executed ETL {operation.id}, produced {len(result_df)} rows")
        except Exception as e:
            logger.error(f"Failed to execute ETL {operation.id}: {e}")
            raise
    
    def get_integrated_data(self, table_name: Optional[str] = None) -> pd.DataFrame:
        if not self.integrated_db:
            return pd.DataFrame()
        if table_name:
            return pd.read_sql_query(f"SELECT * FROM {table_name}", self.integrated_db)
        
        cursor = self.integrated_db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return pd.DataFrame({'available_tables': [row[0] for row in cursor.fetchall()]})
        
    def update_data_for_table(self, table_name: str, new_data: pd.DataFrame) -> bool:
        if not self.integrated_db:
            logger.error("Integrated database not connected.")
            return False
        try:
            new_data.to_sql(table_name, self.integrated_db, if_exists='replace', index=False)
            source_id_to_update = table_name.replace('source_', '') if table_name.startswith('source_') else None
            if source_id_to_update and source_id_to_update in self.data_sources:
                source = self.data_sources[source_id_to_update]
                source.schema = self._analyze_schema(new_data, source.type, source.connection_info)
                source.last_updated = datetime.now().isoformat()
            return True
        except Exception as e:
            logger.error(f"Failed to update data for table {table_name}: {e}")
            return False
            
    def get_data_sources_summary(self) -> Dict[str, Any]:
        """Get summary of all data sources and operations"""
        sources_by_type = {}
        sources_list = []
        
        for source_id, source in self.data_sources.items():
            # Count by type
            source_type = source.type
            sources_by_type[source_type] = sources_by_type.get(source_type, 0) + 1
            
            # Create source info for UI
            table_count = len(source.schema.get('tables', {}))
            source_info = {
                'id': source.id,
                'name': source.name,
                'type': source.type,
                'status': source.status,
                'table_count': table_count,
                'created_at': source.created_at,
                'last_updated': source.last_updated
            }
            sources_list.append(source_info)
        
        return {
            'total_sources': len(self.data_sources),
            'sources_by_type': sources_by_type,
            'total_relationships': len(self.relationships),
            'total_etl_operations': len(self.etl_operations),
            'sources': sources_list
        }

data_integration_engine = DataIntegrationEngine()