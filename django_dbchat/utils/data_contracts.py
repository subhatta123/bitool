"""
Data Contract Definitions for ConvaBI Application
Provides Pydantic models for all service interfaces and data validation
"""

from typing import List, Dict, Any, Optional, Union, Tuple
from pydantic import BaseModel, Field, validator
from datetime import datetime
from enum import Enum

class ConnectionType(str, Enum):
    """Supported connection types"""
    CSV = "csv"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    SQLITE = "sqlite"
    API = "api"
    EXCEL = "excel"
    JSON = "json"

class DataType(str, Enum):
    """Supported data types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIME = "time"
    JSON = "json"
    BINARY = "binary"

class ETLOperationType(str, Enum):
    """ETL operation types"""
    JOIN = "join"
    UNION = "union"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    TRANSFORM = "transform"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"

class WorkflowStep(str, Enum):
    """Workflow steps"""
    DATA_LOADED = "data_loaded"
    ETL_COMPLETED = "etl_completed"
    SEMANTICS_COMPLETED = "semantics_completed"
    QUERY_ENABLED = "query_enabled"
    DASHBOARD_ENABLED = "dashboard_enabled"

class ConnectionInfo(BaseModel):
    """Database connection information contract"""
    type: ConnectionType
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    file_path: Optional[str] = None
    path: Optional[str] = None
    
    @validator('port')
    def validate_port(cls, v):
        if v is not None and (v < 1 or v > 65535):
            raise ValueError('Port must be between 1 and 65535')
        return v
    
    @validator('password')
    def validate_password(cls, v):
        if v is not None and len(v) > 1000:
            raise ValueError('Password too long')
        return v

class ColumnInfo(BaseModel):
    """Database column information contract"""
    name: str = Field(..., min_length=1, max_length=200)
    type: DataType
    nullable: bool = True
    primary_key: bool = False
    foreign_key: bool = False
    unique_count: Optional[int] = None
    sample_values: List[str] = Field(default_factory=list)
    
    @validator('sample_values')
    def validate_sample_values(cls, v):
        if len(v) > 10:
            return v[:10]  # Limit to 10 sample values
        return v

class TableInfo(BaseModel):
    """Database table information contract"""
    name: str = Field(..., min_length=1, max_length=200)
    columns: List[ColumnInfo]
    row_count: Optional[int] = None
    estimated_size_mb: Optional[float] = None

class SchemaInfo(BaseModel):
    """Database schema information contract"""
    tables: List[TableInfo]
    connection_type: ConnectionType
    total_tables: int = 0
    total_columns: int = 0
    
    @validator('total_tables', always=True)
    def validate_total_tables(cls, v, values):
        if 'tables' in values:
            return len(values['tables'])
        return v
    
    @validator('total_columns', always=True)
    def validate_total_columns(cls, v, values):
        if 'tables' in values:
            return sum(len(table.columns) for table in values['tables'])
        return v

class BusinessMetric(BaseModel):
    """Business metric definition contract"""
    name: str = Field(..., min_length=1, max_length=200)
    display_name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    calculation: str = Field(..., min_length=1)
    metric_type: str = Field(..., pattern=r'^(simple|calculated|ratio|growth)$')
    unit: Optional[str] = None
    format_string: Optional[str] = None
    
    @validator('calculation')
    def validate_calculation(cls, v):
        # Basic SQL validation
        dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
        v_upper = v.upper()
        for keyword in dangerous_keywords:
            if keyword in v_upper:
                raise ValueError(f'Dangerous keyword {keyword} not allowed in calculations')
        return v

class ETLParameters(BaseModel):
    """ETL operation parameters contract"""
    source_tables: List[str] = Field(..., min_items=1)
    operation_type: ETLOperationType
    join_column_left: Optional[str] = None
    join_column_right: Optional[str] = None
    join_type: Optional[str] = Field(None, pattern=r'^(INNER|LEFT|RIGHT|FULL)$')
    union_type: Optional[str] = Field(None, pattern=r'^(UNION|UNION ALL)$')
    group_by_columns: List[str] = Field(default_factory=list)
    aggregate_functions: Dict[str, str] = Field(default_factory=dict)
    
    @validator('source_tables')
    def validate_source_tables(cls, v):
        for table in v:
            if not table or not table.strip():
                raise ValueError('Table names cannot be empty')
            # SQL injection prevention
            import re
            if not re.match(r'^[a-zA-Z0-9_]+$', table):
                raise ValueError(f'Invalid table name: {table}')
        return v

class ETLResult(BaseModel):
    """ETL operation result contract"""
    operation_id: str
    success: bool
    row_count: Optional[int] = None
    output_table: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    error_message: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)

class WorkflowStatus(BaseModel):
    """Workflow status tracking contract"""
    data_loaded: bool = False
    etl_completed: bool = False
    semantics_completed: bool = False
    query_enabled: bool = False
    dashboard_enabled: bool = False
    last_updated: Optional[datetime] = None
    
    def is_complete(self) -> bool:
        """Check if all workflow steps are complete"""
        return all([
            self.data_loaded,
            self.etl_completed,
            self.semantics_completed,
            self.query_enabled,
            self.dashboard_enabled
        ])
    
    def get_next_step(self) -> Optional[WorkflowStep]:
        """Get the next workflow step to complete"""
        if not self.data_loaded:
            return WorkflowStep.DATA_LOADED
        elif not self.etl_completed:
            return WorkflowStep.ETL_COMPLETED
        elif not self.semantics_completed:
            return WorkflowStep.SEMANTICS_COMPLETED
        elif not self.query_enabled:
            return WorkflowStep.QUERY_ENABLED
        elif not self.dashboard_enabled:
            return WorkflowStep.DASHBOARD_ENABLED
        return None

class QueryRequest(BaseModel):
    """Natural language query request contract"""
    query: str = Field(..., min_length=1, max_length=5000)
    data_source_id: str
    use_cache: bool = True
    clarification_context: Optional[str] = None
    
    @validator('query')
    def validate_query(cls, v):
        v = v.strip()
        if not v:
            raise ValueError('Query cannot be empty')
        return v

class QueryResponse(BaseModel):
    """Query response contract"""
    success: bool
    sql_query: Optional[str] = None
    result_data: Optional[List[Dict[str, Any]]] = None
    row_count: Optional[int] = None
    execution_time_seconds: Optional[float] = None
    error_message: Optional[str] = None
    needs_clarification: bool = False
    clarification_question: Optional[str] = None
    cached: bool = False

class DataQualityMetrics(BaseModel):
    """Data quality assessment contract"""
    completeness: float = Field(..., ge=0.0, le=1.0)
    uniqueness: float = Field(..., ge=0.0, le=1.0)
    validity: float = Field(..., ge=0.0, le=1.0)
    consistency: float = Field(..., ge=0.0, le=1.0)
    overall_score: float = Field(..., ge=0.0, le=1.0)
    
    @validator('overall_score', always=True)
    def calculate_overall_score(cls, v, values):
        scores = [
            values.get('completeness', 0),
            values.get('uniqueness', 0),
            values.get('validity', 0),
            values.get('consistency', 0)
        ]
        return sum(scores) / len(scores) if scores else 0.0

class ErrorResponse(BaseModel):
    """Standardized error response contract"""
    error: str
    error_code: Optional[str] = None
    details: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)
    request_id: Optional[str] = None

class ServiceResponse(BaseModel):
    """Generic service response contract"""
    success: bool
    data: Optional[Any] = None
    error: Optional[ErrorResponse] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

# Validation utilities
def validate_connection_info(data: Dict[str, Any]) -> ConnectionInfo:
    """Validate connection information against contract"""
    return ConnectionInfo(**data)

def validate_etl_parameters(data: Dict[str, Any]) -> ETLParameters:
    """Validate ETL parameters against contract"""
    return ETLParameters(**data)

def validate_business_metric(data: Dict[str, Any]) -> BusinessMetric:
    """Validate business metric against contract"""
    return BusinessMetric(**data)

def validate_workflow_status(data: Dict[str, Any]) -> WorkflowStatus:
    """Validate workflow status against contract"""
    return WorkflowStatus(**data)

# Data type validation functions
def validate_semantic_data_type(data_type: str) -> bool:
    """
    Check if a given type string is valid according to the DataType enum.
    
    Args:
        data_type: Data type string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not data_type:
        return False
    return data_type.lower() in [dt.value.lower() for dt in DataType]


def normalize_data_type(data_type: str) -> str:
    """
    Convert various type representations to the canonical DataType enum values.
    
    Args:
        data_type: Data type string to normalize
        
    Returns:
        Normalized data type from DataType enum, defaults to 'string' for unknown types
    """
    if not data_type:
        return DataType.STRING.value
    
    dtype_lower = data_type.lower().strip()
    
    # Direct enum value match
    for dt in DataType:
        if dtype_lower == dt.value.lower():
            return dt.value
    
    # Mapping for common variations
    type_mappings = {
        # String variations
        'str': DataType.STRING.value,
        'varchar': DataType.STRING.value,
        'char': DataType.STRING.value,
        'text': DataType.STRING.value,
        'object': DataType.STRING.value,  # Important: object -> string
        
        # Integer variations
        'int': DataType.INTEGER.value,
        'int64': DataType.INTEGER.value,
        'int32': DataType.INTEGER.value,
        'bigint': DataType.INTEGER.value,
        'smallint': DataType.INTEGER.value,
        
        # Float variations
        'double': DataType.FLOAT.value,
        'float64': DataType.FLOAT.value,
        'float32': DataType.FLOAT.value,
        'decimal': DataType.FLOAT.value,
        'numeric': DataType.FLOAT.value,
        'real': DataType.FLOAT.value,
        
        # Boolean variations
        'bool': DataType.BOOLEAN.value,
        'bit': DataType.BOOLEAN.value,
        
        # DateTime variations
        'timestamp': DataType.DATETIME.value,
        'datetime64': DataType.DATETIME.value,
        'datetime64[ns]': DataType.DATETIME.value,
        
        # Time variations
        'timedelta': DataType.TIME.value,
        'timedelta64': DataType.TIME.value,
        
        # JSON variations
        'jsonb': DataType.JSON.value,
        
        # Binary variations
        'blob': DataType.BINARY.value,
        'bytes': DataType.BINARY.value,
    }
    
    return type_mappings.get(dtype_lower, DataType.STRING.value)


def get_pandas_to_semantic_mapping() -> Dict[str, str]:
    """
    Get mapping of pandas dtypes to semantic types for consistency across the application.
    
    Returns:
        Dictionary mapping pandas dtypes to DataType enum values
    """
    return {
        'object': DataType.STRING.value,
        'int64': DataType.INTEGER.value,
        'int32': DataType.INTEGER.value,
        'int16': DataType.INTEGER.value,
        'int8': DataType.INTEGER.value,
        'Int64': DataType.INTEGER.value,  # Nullable integer
        'float64': DataType.FLOAT.value,
        'float32': DataType.FLOAT.value,
        'Float64': DataType.FLOAT.value,  # Nullable float
        'bool': DataType.BOOLEAN.value,
        'boolean': DataType.BOOLEAN.value,
        'datetime64[ns]': DataType.DATETIME.value,
        'datetime64': DataType.DATETIME.value,
        'timedelta64[ns]': DataType.TIME.value,
        'category': DataType.STRING.value,
        'string': DataType.STRING.value,
    }


def validate_schema_info(schema_info: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate schema information to ensure no object types are present in semantic fields.
    
    Args:
        schema_info: Schema information dictionary
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    if not isinstance(schema_info, dict):
        errors.append("Schema info must be a dictionary")
        return False, errors
    
    # Check tables structure
    if 'tables' in schema_info:
        for table_name, table_info in schema_info['tables'].items():
            if not isinstance(table_info, dict):
                continue
                
            if 'columns' in table_info:
                for col_name, col_info in table_info['columns'].items():
                    if isinstance(col_info, dict) and 'type' in col_info:
                        col_type = col_info['type']
                        
                        # Check for object type in semantic field
                        if col_type == 'object':
                            errors.append(f"Column '{col_name}' in table '{table_name}' has 'object' type - should be semantic type")
                        
                        # Validate semantic type
                        elif not validate_semantic_data_type(col_type):
                            errors.append(f"Column '{col_name}' has invalid semantic type: '{col_type}'")
    
    return len(errors) == 0, errors


# Contract versioning
CONTRACT_VERSION = "1.0.0"

def get_contract_info() -> Dict[str, Any]:
    """Get contract version and metadata"""
    return {
        "version": CONTRACT_VERSION,
        "contracts": [
            "ConnectionInfo",
            "ColumnInfo", 
            "TableInfo",
            "SchemaInfo",
            "BusinessMetric",
            "ETLParameters",
            "ETLResult",
            "WorkflowStatus",
            "QueryRequest",
            "QueryResponse",
            "DataQualityMetrics",
            "ErrorResponse",
            "ServiceResponse"
        ],
        "supported_types": {
            "connection_types": [e.value for e in ConnectionType],
            "data_types": [e.value for e in DataType],
            "etl_operations": [e.value for e in ETLOperationType],
            "workflow_steps": [e.value for e in WorkflowStep]
        },
        "validation_functions": [
            "validate_semantic_data_type",
            "normalize_data_type", 
            "get_pandas_to_semantic_mapping",
            "validate_schema_info"
        ]
    } 