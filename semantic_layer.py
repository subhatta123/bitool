"""
Semantic Layer for ConvaBI Application
Provides business-friendly metadata and context to improve LLM query generation
"""

import json
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import streamlit as st
import database

class DataType(Enum):
    DIMENSION = "dimension"
    MEASURE = "measure" 
    IDENTIFIER = "identifier"
    DATE = "date"

@dataclass
class SemanticColumn:
    """Enhanced column metadata with business context"""
    name: str
    display_name: str
    description: str
    data_type: str
    semantic_type: DataType
    sample_values: List[str] = field(default_factory=list)
    related_columns: List[str] = field(default_factory=list)
    business_rules: List[str] = field(default_factory=list)
    common_filters: List[str] = field(default_factory=list)
    aggregation_default: Optional[str] = None
    is_nullable: bool = True
    
    def __post_init__(self):
        if self.sample_values is None:
            self.sample_values = []
        if self.related_columns is None:
            self.related_columns = []
        if self.business_rules is None:
            self.business_rules = []
        if self.common_filters is None:
            self.common_filters = []
        
        # Ensure is_nullable is a Python boolean, not numpy.bool_
        if self.is_nullable is not None:
            try:
                # Force conversion to Python boolean, handling numpy types
                type_name = type(self.is_nullable).__name__
                if 'numpy' in str(type(self.is_nullable)) or 'bool_' in type_name:
                    # Handle numpy scalar types that have .item() method
                    item_method = getattr(self.is_nullable, 'item', None)
                    if item_method is not None:
                        self.is_nullable = bool(item_method())
                    else:
                        self.is_nullable = bool(self.is_nullable)
                else:
                    self.is_nullable = bool(self.is_nullable)
                print(f"[SEMANTIC COLUMN] Converted is_nullable to Python bool: {self.is_nullable} (type: {type(self.is_nullable)})")
            except Exception as convert_error:
                print(f"[SEMANTIC COLUMN] Error converting is_nullable to bool: {convert_error}, defaulting to True")
                self.is_nullable = True
        
@dataclass
class SemanticTable:
    """Enhanced table metadata with business context"""
    name: str
    display_name: str
    description: str
    business_purpose: str
    columns: Dict[str, SemanticColumn]
    relationships: List[Dict[str, str]] = field(default_factory=list)
    common_queries: List[str] = field(default_factory=list)

@dataclass
class SemanticRelationship:
    """Defines relationships between tables"""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_type: str  # "one_to_many", "many_to_one", "one_to_one", "many_to_many"
    description: str

@dataclass
class SemanticMetric:
    """Defines business metrics and calculations"""
    name: str
    display_name: str
    description: str
    formula: str
    category: str
    tables_involved: List[str]

class SemanticLayer:
    """
    Semantic layer that enriches raw database schema with business context
    """
    
    def __init__(self):
        self.tables: Dict[str, SemanticTable] = {}
        self.relationships: List[SemanticRelationship] = []
        self.metrics: Dict[str, SemanticMetric] = {}
        self.business_glossary: Dict[str, str] = {}
        
    def register_table(self, table: SemanticTable):
        """Register a table with its semantic metadata"""
        self.tables[table.name] = table
        
    def register_relationship(self, relationship: SemanticRelationship):
        """Register a relationship between tables"""
        self.relationships.append(relationship)
        
    def add_metric(self, metric: SemanticMetric):
        """Add a business metric definition"""
        self.metrics[metric.name] = metric
        
    def add_business_term(self, term: str, definition: str):
        """Add a business glossary term"""
        self.business_glossary[term] = definition

    def generate_enhanced_schema_prompt(self, raw_schema: Dict[str, Any], 
                                      connection_type: str = "database") -> str:
        """
        Generate an enhanced schema prompt with business context for LLM
        """
        prompt_parts = ["=== ENHANCED DATA SCHEMA WITH BUSINESS CONTEXT ===\n"]
        
        # Add business glossary context
        if self.business_glossary:
            prompt_parts.append("BUSINESS GLOSSARY:")
            for term, definition in self.business_glossary.items():
                prompt_parts.append(f"- {term}: {definition}")
            prompt_parts.append("")
        
        # Add business metrics
        if self.metrics:
            prompt_parts.append("COMMON BUSINESS METRICS:")
            for metric_name, metric in self.metrics.items():
                prompt_parts.append(f"- {metric.display_name}: {metric.formula}")
                if metric.description:
                    prompt_parts.append(f"  Description: {metric.description}")
            prompt_parts.append("")
        
        # Enhanced table information
        if isinstance(raw_schema, dict):
            # Database schema format
            for table_name, columns in raw_schema.items():
                self._generate_table_prompt(table_name, columns, prompt_parts)
        elif isinstance(raw_schema, list):
            # CSV/API/Integrated data format
            table_name = "integrated_data" if connection_type == "integrated" else "csv_data"
            columns = {col['name']: col['type'] for col in raw_schema}
            self._generate_table_prompt(table_name, columns, prompt_parts)
        
        # Add relationship summary
        if self.relationships:
            prompt_parts.append("TABLE RELATIONSHIPS:")
            for rel in self.relationships:
                prompt_parts.append(f"- {rel.from_table}.{rel.from_column} → {rel.to_table}.{rel.to_column} ({rel.relationship_type})")
                if rel.description:
                    prompt_parts.append(f"  {rel.description}")
            prompt_parts.append("")
        
        prompt_parts.append("=== QUERY GENERATION GUIDELINES ===")
        prompt_parts.append("1. Use exact column names as shown (with quotes for spaces)")
        prompt_parts.append("2. Follow the relationship patterns for JOINs")
        prompt_parts.append("3. Use recommended aggregations for measures")
        prompt_parts.append("4. Apply business rules and common filters")
        prompt_parts.append("5. Consider business context when interpreting questions")
        prompt_parts.append("")
        
        return "\n".join(prompt_parts)
    
    def _generate_table_prompt(self, table_name: str, columns: Dict[str, str], prompt_parts: List[str]):
        """Generate prompt section for a single table"""
        semantic_table = self.tables.get(table_name)
        
        if semantic_table:
            prompt_parts.append(f"TABLE: {semantic_table.display_name} ({table_name})")
            prompt_parts.append(f"Purpose: {semantic_table.description}")
            if semantic_table.business_purpose:
                prompt_parts.append(f"Business Purpose: {semantic_table.business_purpose}")
        else:
            prompt_parts.append(f"TABLE: {table_name}")
            
        prompt_parts.append("COLUMNS:")
        
        # Enhanced column information
        for col_name, col_type in columns.items():
            semantic_col = None
            if semantic_table and col_name in semantic_table.columns:
                semantic_col = semantic_table.columns[col_name]
            
            if semantic_col:
                prompt_parts.append(f"  • {semantic_col.display_name} ({col_name})")
                prompt_parts.append(f"    Type: {col_type} | Semantic: {semantic_col.semantic_type.value}")
                prompt_parts.append(f"    Description: {semantic_col.description}")
                
                if semantic_col.sample_values:
                    prompt_parts.append(f"    Sample Values: {', '.join(semantic_col.sample_values[:5])}")
                
                if semantic_col.common_filters:
                    prompt_parts.append(f"    Common Filters: {', '.join(semantic_col.common_filters)}")
                    
                if semantic_col.aggregation_default:
                    prompt_parts.append(f"    Default Aggregation: {semantic_col.aggregation_default}")
                    
                if semantic_col.business_rules:
                    prompt_parts.append(f"    Business Rules: {'; '.join(semantic_col.business_rules)}")
            else:
                # Fallback for columns without semantic metadata
                prompt_parts.append(f"  • {col_name}: {col_type}")
        
        # Add table relationships
        table_relationships = [r for r in self.relationships 
                             if r.from_table == table_name or r.to_table == table_name]
        if table_relationships:
            prompt_parts.append("  RELATIONSHIPS:")
            for rel in table_relationships:
                if rel.from_table == table_name:
                    prompt_parts.append(f"    → {rel.to_table}.{rel.to_column} ({rel.relationship_type})")
                else:
                    prompt_parts.append(f"    ← {rel.from_table}.{rel.from_column} ({rel.relationship_type})")
        
        # Add common queries
        if semantic_table and semantic_table.common_queries:
            prompt_parts.append("  COMMON QUERIES:")
            for query in semantic_table.common_queries:
                prompt_parts.append(f"    - {query}")
        
        prompt_parts.append("")
    
    def auto_generate_metadata_from_data_integration(self, integration_engine) -> bool:
        """Auto-generate semantic metadata from data integration sources"""
        try:
            summary = integration_engine.get_data_sources_summary()
            
            if summary['total_sources'] == 0:
                return False
            
            # Add common business terms
            self.add_business_term("Customer", "Individual or organization that purchases products")
            self.add_business_term("Revenue", "Total income generated from sales before costs")
            self.add_business_term("Profit", "Revenue minus costs and expenses")
            self.add_business_term("Order", "A transaction where a customer purchases products")
            
            # Process each data source
            for source_info in summary['sources']:
                try:
                    table_name = f"source_{source_info['id']}"
                    table_data = integration_engine.get_integrated_data(table_name)
                    
                    if table_data.empty:
                        continue
                    
                    # Generate semantic metadata for this table
                    semantic_table = self._create_semantic_table_from_dataframe(
                        table_name, source_info['name'], table_data, source_info['type']
                    )
                    
                    self.register_table(semantic_table)
                    
                    # Try to detect relationships
                    self._auto_detect_relationships(table_name, table_data)
                    
                except Exception as e:
                    print(f"Error processing source {source_info['name']}: {e}")
                    continue
            
            # Add common metrics
            self._add_common_metrics()
            
            return True
            
        except Exception as e:
            print(f"Error auto-generating semantic metadata: {e}")
            return False
    
    def _create_semantic_table_from_dataframe(self, table_name: str, display_name: str, 
                                            df: pd.DataFrame, source_type: str) -> SemanticTable:
        """Create semantic table metadata from DataFrame"""
        columns = {}
        
        for col_name in df.columns:
            col_data = df[col_name]
            data_type = str(col_data.dtype)
            
            # Infer semantic type
            semantic_type = self._infer_semantic_type(col_name, col_data)
            
            # Generate description
            description = self._generate_column_description(col_name, semantic_type, data_type)
            
            # Get sample values
            sample_values = self._get_sample_values(col_data)
            
            # Generate common filters
            common_filters = self._generate_common_filters(col_name, col_data, semantic_type)
            
            # Generate business rules
            business_rules = self._generate_business_rules(col_name, col_data, semantic_type)
            
            # Default aggregation
            aggregation_default = self._get_default_aggregation(semantic_type, data_type)
            
            # Ensure all values are Python types, not numpy types
            is_nullable_value = bool(col_data.isnull().any()) if hasattr(col_data, 'isnull') else True
            
            semantic_col = SemanticColumn(
                name=col_name,
                display_name=col_name.replace("_", " ").title(),
                description=description,
                data_type=data_type,
                semantic_type=semantic_type,
                sample_values=sample_values,
                common_filters=common_filters,
                business_rules=business_rules,
                aggregation_default=aggregation_default,
                is_nullable=is_nullable_value  # Ensure this is a Python bool
            )
            
            columns[col_name] = semantic_col
        
        # Generate common queries for this table
        common_queries = self._generate_common_queries(table_name, columns, source_type)
        
        return SemanticTable(
            name=table_name,
            display_name=display_name,
            description=f"Data from {source_type} source: {display_name}",
            business_purpose=self._infer_business_purpose(table_name, columns, source_type),
            columns=columns,
            common_queries=common_queries
        )
    
    def _infer_semantic_type(self, col_name: str, col_data: pd.Series) -> DataType:
        """Infer semantic type from column name and data"""
        col_name_lower = col_name.lower()
        
        # Identifier patterns
        if "id" in col_name_lower or col_name_lower.endswith("_key"):
            return DataType.IDENTIFIER
        
        # Date patterns
        if any(word in col_name_lower for word in ["date", "time", "created", "updated", "timestamp"]):
            return DataType.DATE
        
        # Measure patterns
        if any(word in col_name_lower for word in ["amount", "price", "cost", "revenue", "sales", "profit", "total", "sum", "count", "value"]):
            return DataType.MEASURE
        
        # Check data type
        if pd.api.types.is_numeric_dtype(col_data) and not col_name_lower.endswith("_id"):
            return DataType.MEASURE
        
        # Default to dimension
        return DataType.DIMENSION
    
    def _generate_column_description(self, col_name: str, semantic_type: DataType, data_type: str) -> str:
        """Generate a description for a column"""
        base_name = col_name.replace("_", " ").lower()
        
        if semantic_type == DataType.IDENTIFIER:
            return f"Unique identifier for {base_name.replace(' id', '')}"
        elif semantic_type == DataType.DATE:
            return f"Date/time when {base_name.replace(' date', '').replace(' time', '')} occurred"
        elif semantic_type == DataType.MEASURE:
            return f"Numerical value representing {base_name}"
        else:
            return f"Descriptive attribute for {base_name}"
    
    def _get_sample_values(self, col_data: pd.Series, max_samples: int = 5) -> List[str]:
        """Get sample values from column data"""
        try:
            # Get unique non-null values
            unique_values = col_data.dropna().unique()
            
            # Convert to strings and take first few, ensuring Python types
            sample_values = []
            for val in unique_values[:max_samples]:
                # Convert numpy types to Python types before converting to string
                converted_val = self._convert_numpy_types(val)
                sample_values.append(str(converted_val))
            
            return sample_values
        except:
            return []
    
    def _generate_common_filters(self, col_name: str, col_data: pd.Series, semantic_type: DataType) -> List[str]:
        """Generate common filter patterns for a column"""
        filters = []
        col_name_lower = col_name.lower()
        
        if semantic_type == DataType.DATE:
            filters.extend([
                f"YEAR({col_name}) = 2023",
                f"{col_name} >= '2023-01-01'",
                f"{col_name} < CURRENT_DATE"
            ])
        elif semantic_type == DataType.DIMENSION:
            # Get common values for filter suggestions
            try:
                top_values = col_data.value_counts().head(3).index.tolist()
                for value in top_values:
                    if isinstance(value, str) and len(value) < 20:
                        filters.append(f"{col_name} = '{value}'")
            except:
                pass
            
            filters.append(f"{col_name} IS NOT NULL")
            
        elif semantic_type == DataType.MEASURE:
            filters.extend([
                f"{col_name} > 0",
                f"{col_name} IS NOT NULL"
            ])
        
        return filters[:3]  # Limit to 3 filters
    
    def _generate_business_rules(self, col_name: str, col_data: pd.Series, semantic_type: DataType) -> List[str]:
        """Generate business rules for a column"""
        rules = []
        
        # Check for nulls
        if col_data.isnull().any():
            rules.append("Can contain null values")
        else:
            rules.append("Cannot be null")
        
        # Type-specific rules
        if semantic_type == DataType.IDENTIFIER:
            rules.append("Should be unique")
            if pd.api.types.is_numeric_dtype(col_data):
                rules.append("Always positive")
        elif semantic_type == DataType.MEASURE:
            if pd.api.types.is_numeric_dtype(col_data):
                min_val = col_data.min()
                if min_val >= 0:
                    rules.append("Always positive or zero")
                rules.append("Numeric values only")
        elif semantic_type == DataType.DATE:
            rules.append("Valid date format required")
        
        return rules
    
    def _get_default_aggregation(self, semantic_type: DataType, data_type: str) -> Optional[str]:
        """Get default aggregation for a column"""
        if semantic_type == DataType.MEASURE:
            return "SUM"
        elif semantic_type == DataType.IDENTIFIER:
            return "COUNT"
        elif semantic_type == DataType.DATE:
            return "MIN"
        
        return None
    
    def _generate_common_queries(self, table_name: str, columns: Dict[str, SemanticColumn], source_type: str) -> List[str]:
        """Generate common query patterns for a table"""
        queries = []
        
        # Find key column types
        dimensions = [col for col, meta in columns.items() if meta.semantic_type == DataType.DIMENSION]
        measures = [col for col, meta in columns.items() if meta.semantic_type == DataType.MEASURE]
        dates = [col for col, meta in columns.items() if meta.semantic_type == DataType.DATE]
        
        # Generate query patterns
        if measures:
            measure = measures[0]
            queries.append(f"Total {measure.replace('_', ' ')}")
            
            if dimensions:
                dim = dimensions[0]
                queries.append(f"{measure.replace('_', ' ').title()} by {dim.replace('_', ' ')}")
        
        if dates:
            date_col = dates[0]
            queries.append(f"Data trends over {date_col.replace('_', ' ')}")
        
        if dimensions:
            queries.append(f"Count by {dimensions[0].replace('_', ' ')}")
        
        return queries[:4]  # Limit to 4 queries
    
    def _infer_business_purpose(self, table_name: str, columns: Dict[str, SemanticColumn], source_type: str) -> str:
        """Infer the business purpose of a table"""
        col_names = [col.lower() for col in columns.keys()]
        
        # Pattern matching for business purposes
        if any(word in " ".join(col_names) for word in ["customer", "client"]):
            return "Customer relationship and contact management"
        elif any(word in " ".join(col_names) for word in ["order", "sale", "revenue", "transaction"]):
            return "Sales and transaction tracking"
        elif any(word in " ".join(col_names) for word in ["product", "item", "inventory"]):
            return "Product and inventory management"
        elif any(word in " ".join(col_names) for word in ["employee", "staff", "user"]):
            return "Human resources and user management"
        else:
            return f"Business data from {source_type} source"
    
    def _auto_detect_relationships(self, table_name: str, df: pd.DataFrame):
        """Auto-detect potential relationships with other tables"""
        # This is a simplified version - could be enhanced with more sophisticated logic
        for col_name in df.columns:
            if col_name.lower().endswith("_id") and col_name.lower() != f"{table_name.replace('source_', '')}_id":
                # Potential foreign key
                related_table_name = col_name.lower().replace("_id", "")
                
                # Check if we have a table that might be referenced
                for existing_table_name in self.tables.keys():
                    if related_table_name in existing_table_name.lower():
                        relationship = SemanticRelationship(
                            from_table=table_name,
                            from_column=col_name,
                            to_table=existing_table_name,
                            to_column=col_name,  # Assume same column name
                            relationship_type="many_to_one",
                            description=f"Links {table_name} to {existing_table_name}"
                        )
                        
                        # Check if relationship already exists
                        if not any(r.from_table == relationship.from_table and 
                                 r.from_column == relationship.from_column and
                                 r.to_table == relationship.to_table 
                                 for r in self.relationships):
                            self.register_relationship(relationship)
    
    def _add_common_metrics(self):
        """Add common business metrics"""
        # Revenue metrics
        revenue_metric = SemanticMetric(
            name="total_revenue",
            display_name="Total Revenue",
            description="Sum of all revenue/sales amounts",
            formula="SUM(sales_amount) OR SUM(revenue) OR SUM(amount)",
            category="Financial",
            tables_involved=[]
        )
        self.add_metric(revenue_metric)
        
        # Customer metrics
        customer_metric = SemanticMetric(
            name="customer_count",
            display_name="Customer Count",
            description="Total number of unique customers",
            formula="COUNT(DISTINCT customer_id)",
            category="Customer",
            tables_involved=[]
        )
        self.add_metric(customer_metric)
        
        # Average metrics
        avg_metric = SemanticMetric(
            name="average_order_value",
            display_name="Average Order Value",
            description="Average value per order/transaction",
            formula="AVG(order_amount) OR AVG(sales_amount)",
            category="Financial",
            tables_involved=[]
        )
        self.add_metric(avg_metric)
    
    def _convert_numpy_types(self, value):
        """Convert numpy types to Python types for database compatibility"""
        try:
            import numpy as np
            
            # Handle None values first
            if value is None:
                return None
            
            # Handle pandas NA and NaT
            try:
                if pd.isna(value):
                    return None
            except (TypeError, ValueError):
                # pd.isna() might fail on some types, that's OK
                pass
            
            # Check type name and module to detect numpy types more reliably
            type_name = type(value).__name__
            type_module = getattr(type(value), '__module__', '')
            
            # Comprehensive numpy boolean detection
            if ('bool' in type_name and 'numpy' in type_module) or \
               type_name in ['bool_', 'bool8'] or \
               (hasattr(np, 'bool_') and isinstance(value, np.bool_)) or \
               (hasattr(value, 'dtype') and 'bool' in str(value.dtype)):
                return bool(value)
            
            # Comprehensive numpy integer detection
            elif ('int' in type_name and 'numpy' in type_module) or \
                 any(int_type in type_name for int_type in ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64']) or \
                 (hasattr(np, 'integer') and isinstance(value, np.integer)):
                return int(value)
            
            # Comprehensive numpy float detection
            elif ('float' in type_name and 'numpy' in type_module) or \
                 any(float_type in type_name for float_type in ['float16', 'float32', 'float64', 'float128']) or \
                 (hasattr(np, 'floating') and isinstance(value, np.floating)):
                # Check for NaN or infinity
                try:
                    if np.isnan(value) or np.isinf(value):
                        return None
                except (TypeError, ValueError):
                    pass
                return float(value)
            
            # Handle numpy string types
            elif ('str' in type_name and 'numpy' in type_module) or \
                 type_name in ['str_', 'unicode_', 'bytes_'] or \
                 (hasattr(np, 'str_') and isinstance(value, np.str_)):
                return str(value)
            
            # Handle numpy arrays
            elif type_name == 'ndarray' or (hasattr(np, 'ndarray') and isinstance(value, np.ndarray)):
                return [self._convert_numpy_types(item) for item in value.tolist()]
            
            # Handle pandas Timestamp
            elif 'Timestamp' in type_name:
                return value.isoformat() if hasattr(value, 'isoformat') else str(value)
            
            # Handle pandas Period
            elif 'Period' in type_name:
                return str(value)
            
            # Handle lists recursively
            elif isinstance(value, list):
                return [self._convert_numpy_types(item) for item in value]
            
            # Handle dictionaries recursively
            elif isinstance(value, dict):
                return {key: self._convert_numpy_types(val) for key, val in value.items()}
            
            # Handle tuples
            elif isinstance(value, tuple):
                return tuple(self._convert_numpy_types(item) for item in value)
            
            # Handle sets
            elif isinstance(value, set):
                return [self._convert_numpy_types(item) for item in value]
            
            # For any other unknown numpy types, try to convert to Python equivalent
            elif 'numpy' in type_module:
                try:
                    # Try to extract the Python value using .item() method
                    if hasattr(value, 'item'):
                        return self._convert_numpy_types(value.item())
                    # Try to convert to Python type based on value
                    elif hasattr(value, 'dtype'):
                        if 'bool' in str(value.dtype):
                            return bool(value)
                        elif 'int' in str(value.dtype):
                            return int(value)
                        elif 'float' in str(value.dtype):
                            return float(value)
                        else:
                            return str(value)
                    else:
                        return str(value)
                except Exception as convert_error:
                    print(f"[NUMPY CONVERT] Failed to convert numpy type {type_name}: {convert_error}")
                    return str(value)
            
            # Return as-is for standard Python types
            else:
                return value
                
        except Exception as e:
            print(f"[NUMPY CONVERT] Error converting value {value} of type {type(value)}: {e}")
            # Return string representation as fallback
            return str(value) if value is not None else None

    def save_to_database(self, connection) -> bool:
        """Save semantic layer configuration to database"""
        try:
            cursor = connection.cursor()
            
            # Create semantic layer tables if they don't exist
            self._create_semantic_tables(cursor)
            
            # Clear existing data for this update
            cursor.execute("DELETE FROM semantic_relationships")
            cursor.execute("DELETE FROM semantic_columns")
            cursor.execute("DELETE FROM semantic_tables")
            cursor.execute("DELETE FROM semantic_metrics")
            cursor.execute("DELETE FROM semantic_glossary")
            
            print(f"[SEMANTIC SAVE] Saving {len(self.tables)} tables to database...")
            
            # Save tables
            for table in self.tables.values():
                # Convert all table data
                table_name = self._convert_numpy_types(table.name)
                display_name = self._convert_numpy_types(table.display_name)
                description = self._convert_numpy_types(table.description)
                business_purpose = self._convert_numpy_types(table.business_purpose)
                common_queries = self._convert_numpy_types(table.common_queries)
                
                cursor.execute("""
                    INSERT INTO semantic_tables (table_name, display_name, description, business_purpose, common_queries)
                    VALUES (%s, %s, %s, %s, %s)
                """, (table_name, display_name, description, business_purpose, json.dumps(common_queries)))
                
                print(f"[SEMANTIC SAVE] Saved table: {table_name} with {len(table.columns)} columns")
                
                # Save columns
                for col in table.columns.values():
                    try:
                        # Convert all column data thoroughly
                        col_name = self._convert_numpy_types(col.name)
                        col_display_name = self._convert_numpy_types(col.display_name)
                        col_description = self._convert_numpy_types(col.description)
                        col_data_type = self._convert_numpy_types(col.data_type)
                        semantic_type_value = self._convert_numpy_types(col.semantic_type.value)
                        sample_values = self._convert_numpy_types(col.sample_values)
                        common_filters = self._convert_numpy_types(col.common_filters)
                        business_rules = self._convert_numpy_types(col.business_rules)
                        aggregation_default = self._convert_numpy_types(col.aggregation_default)
                        is_nullable = self._convert_numpy_types(col.is_nullable)
                        
                        # Extra safety for is_nullable field
                        if is_nullable is not None:
                            try:
                                # Force conversion to Python boolean
                                is_nullable = bool(is_nullable)
                                print(f"[SEMANTIC SAVE] Converted is_nullable for {col_name}: {is_nullable} (type: {type(is_nullable)})")
                            except Exception as bool_error:
                                print(f"[SEMANTIC SAVE] Error converting is_nullable to bool for {col_name}: {bool_error}")
                                is_nullable = True  # Safe default
                        else:
                            is_nullable = True  # Default value
                        
                        # Additional validation for all fields
                        data_to_insert = [
                            table_name, col_name, col_display_name, col_description, semantic_type_value,
                            col_data_type, json.dumps(sample_values), json.dumps(common_filters),
                            json.dumps(business_rules), aggregation_default, is_nullable
                        ]
                        
                        # Debug logging
                        print(f"[SEMANTIC SAVE] Inserting column {col_name} with is_nullable={is_nullable} (type: {type(is_nullable)})")
                        
                        cursor.execute("""
                            INSERT INTO semantic_columns (table_name, column_name, display_name, description, 
                                                        semantic_type, data_type, sample_values, common_filters, 
                                                        business_rules, aggregation_default, is_nullable)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, data_to_insert)
                        
                        print(f"[SEMANTIC SAVE] Successfully saved column: {col_name}")
                        
                    except Exception as col_error:
                        print(f"[SEMANTIC SAVE] Error saving column {col.name}: {col_error}")
                        print(f"[SEMANTIC SAVE] Column original data types:")
                        print(f"  - is_nullable: {type(col.is_nullable)} = {col.is_nullable}")
                        print(f"  - semantic_type: {type(col.semantic_type)} = {col.semantic_type}")
                        print(f"  - name: {type(col.name)} = {col.name}")
                        
                        # Check if it's a database adaptation error
                        if "can't adapt type" in str(col_error):
                            print(f"[SEMANTIC SAVE] Database adaptation error - checking data types in insert values")
                            try:
                                # Try to identify which field is causing the issue
                                test_values = [
                                    self._convert_numpy_types(table.name),
                                    self._convert_numpy_types(col.name), 
                                    self._convert_numpy_types(col.display_name),
                                    self._convert_numpy_types(col.description), 
                                    self._convert_numpy_types(col.semantic_type.value),
                                    self._convert_numpy_types(col.data_type), 
                                    json.dumps(self._convert_numpy_types(col.sample_values)), 
                                    json.dumps(self._convert_numpy_types(col.common_filters)),
                                    json.dumps(self._convert_numpy_types(col.business_rules)), 
                                    self._convert_numpy_types(col.aggregation_default), 
                                    bool(self._convert_numpy_types(col.is_nullable)) if col.is_nullable is not None else True
                                ]
                                
                                for i, val in enumerate(test_values):
                                    print(f"    [{i}] {type(val)} = {val}")
                                    
                            except Exception as debug_error:
                                print(f"[SEMANTIC SAVE] Error during debug analysis: {debug_error}")
                        
                        # Continue with other columns instead of failing completely
                        continue
            
            # Save relationships
            for rel in self.relationships:
                try:
                    cursor.execute("""
                        INSERT INTO semantic_relationships (from_table, from_column, to_table, to_column, relationship_type, description)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        self._convert_numpy_types(rel.from_table),
                        self._convert_numpy_types(rel.from_column),
                        self._convert_numpy_types(rel.to_table),
                        self._convert_numpy_types(rel.to_column),
                        self._convert_numpy_types(rel.relationship_type),
                        self._convert_numpy_types(rel.description)
                    ))
                except Exception as rel_error:
                    print(f"[SEMANTIC SAVE] Error saving relationship: {rel_error}")
                    continue
            
            # Save metrics
            for metric in self.metrics.values():
                try:
                    cursor.execute("""
                        INSERT INTO semantic_metrics (metric_name, display_name, description, formula, category, tables_involved)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        self._convert_numpy_types(metric.name),
                        self._convert_numpy_types(metric.display_name),
                        self._convert_numpy_types(metric.description),
                        self._convert_numpy_types(metric.formula),
                        self._convert_numpy_types(metric.category),
                        json.dumps(self._convert_numpy_types(metric.tables_involved))
                    ))
                except Exception as metric_error:
                    print(f"[SEMANTIC SAVE] Error saving metric {metric.name}: {metric_error}")
                    continue
            
            # Save glossary
            for term, definition in self.business_glossary.items():
                try:
                    cursor.execute("""
                        INSERT INTO semantic_glossary (term, definition, category)
                        VALUES (%s, %s, %s)
                    """, (
                        self._convert_numpy_types(term),
                        self._convert_numpy_types(definition),
                        "Business"
                    ))
                except Exception as glossary_error:
                    print(f"[SEMANTIC SAVE] Error saving glossary term {term}: {glossary_error}")
                    continue
            
            connection.commit()
            print(f"[SEMANTIC SAVE] Successfully saved semantic layer to database")
            return True
            
        except Exception as e:
            print(f"[SEMANTIC SAVE] Error saving semantic layer: {e}")
            print(f"[SEMANTIC SAVE] Error type: {type(e).__name__}")
            
            # Provide detailed debugging information
            if "can't adapt type" in str(e):
                print(f"[SEMANTIC SAVE] Data type adaptation error - this suggests numpy types weren't properly converted")
                print(f"[SEMANTIC SAVE] Error details: {str(e)}")
            elif "not supported" in str(e):
                print(f"[SEMANTIC SAVE] Database doesn't support a data type being inserted")
            
            try:
                connection.rollback()
                print(f"[SEMANTIC SAVE] Database transaction rolled back")
            except Exception as rollback_error:
                print(f"[SEMANTIC SAVE] Error during rollback: {rollback_error}")
            
            return False
    
    def _create_semantic_tables(self, cursor):
        """Create semantic layer tables if they don't exist"""
        # Tables creation
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS semantic_tables (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) UNIQUE NOT NULL,
                display_name VARCHAR(255) NOT NULL,
                description TEXT,
                business_purpose TEXT,
                common_queries JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS semantic_columns (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                display_name VARCHAR(255) NOT NULL,
                description TEXT NOT NULL,
                semantic_type VARCHAR(50) NOT NULL,
                data_type VARCHAR(100),
                sample_values JSONB,
                common_filters JSONB,
                business_rules JSONB,
                aggregation_default VARCHAR(50),
                is_nullable BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_name, column_name)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS semantic_relationships (
                id SERIAL PRIMARY KEY,
                from_table VARCHAR(255) NOT NULL,
                from_column VARCHAR(255) NOT NULL,
                to_table VARCHAR(255) NOT NULL,
                to_column VARCHAR(255) NOT NULL,
                relationship_type VARCHAR(50) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS semantic_metrics (
                id SERIAL PRIMARY KEY,
                metric_name VARCHAR(255) UNIQUE NOT NULL,
                display_name VARCHAR(255) NOT NULL,
                description TEXT,
                formula TEXT NOT NULL,
                category VARCHAR(100),
                tables_involved JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS semantic_glossary (
                id SERIAL PRIMARY KEY,
                term VARCHAR(255) UNIQUE NOT NULL,
                definition TEXT NOT NULL,
                category VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    @classmethod
    def load_from_database(cls, connection):
        """Load semantic layer configuration from database"""
        semantic_layer = cls()
        
        try:
            cursor = connection.cursor()
            
            # Load tables
            cursor.execute("SELECT table_name, display_name, description, business_purpose, common_queries FROM semantic_tables")
            tables_data = cursor.fetchall()
            
            for table_name, display_name, description, business_purpose, common_queries in tables_data:
                # Load columns for this table
                cursor.execute("""
                    SELECT column_name, display_name, description, semantic_type, data_type, 
                           sample_values, common_filters, business_rules, aggregation_default, is_nullable
                    FROM semantic_columns WHERE table_name = %s
                """, (table_name,))
                
                columns = {}
                for col_data in cursor.fetchall():
                    col_name, col_display, col_desc, sem_type, col_data_type, sample_vals, filters, rules, agg_default, nullable = col_data
                    
                    columns[col_name] = SemanticColumn(
                        name=col_name,
                        display_name=col_display,
                        description=col_desc,
                        data_type=col_data_type,
                        semantic_type=DataType(sem_type),
                        sample_values=sample_vals or [],
                        common_filters=filters or [],
                        business_rules=rules or [],
                        aggregation_default=agg_default,
                        is_nullable=nullable
                    )
                
                semantic_table = SemanticTable(
                    name=table_name,
                    display_name=display_name,
                    description=description,
                    business_purpose=business_purpose,
                    columns=columns,
                    common_queries=common_queries or []
                )
                
                semantic_layer.register_table(semantic_table)
            
            # Load relationships
            cursor.execute("""
                SELECT from_table, from_column, to_table, to_column, relationship_type, description 
                FROM semantic_relationships
            """)
            for rel_data in cursor.fetchall():
                from_table, from_col, to_table, to_col, rel_type, description = rel_data
                relationship = SemanticRelationship(
                    from_table=from_table,
                    from_column=from_col,
                    to_table=to_table,
                    to_column=to_col,
                    relationship_type=rel_type,
                    description=description
                )
                semantic_layer.register_relationship(relationship)
            
            # Load metrics
            cursor.execute("SELECT metric_name, display_name, description, formula, category, tables_involved FROM semantic_metrics")
            for metric_data in cursor.fetchall():
                name, display_name, description, formula, category, tables_involved = metric_data
                metric = SemanticMetric(
                    name=name,
                    display_name=display_name,
                    description=description,
                    formula=formula,
                    category=category,
                    tables_involved=tables_involved or []
                )
                semantic_layer.add_metric(metric)
            
            # Load glossary
            cursor.execute("SELECT term, definition FROM semantic_glossary")
            for term, definition in cursor.fetchall():
                semantic_layer.add_business_term(term, definition)
            
            return semantic_layer
            
        except Exception as e:
            print(f"Error loading semantic layer: {e}")
            return semantic_layer 