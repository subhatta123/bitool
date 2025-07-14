#!/usr/bin/env python3
"""
Universal Data Loader Service
Handles data loading for transformations from any source type
"""

import pandas as pd
import logging
from typing import Tuple, Optional, Dict, Any
from datasets.models import DataSource

logger = logging.getLogger(__name__)

class UniversalDataLoader:
    """Universal data loader for all data source types"""
    
    def __init__(self):
        self.connection = None
    
    def load_data_for_transformation(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """
        Load data from any data source type for ETL transformations
        
        Args:
            data_source: DataSource object
            
        Returns:
            Tuple[success, dataframe, message]
        """
        try:
            source_type = data_source.source_type
            logger.info(f"Loading data for transformation from {source_type} source: {data_source.name}")
            
            if source_type == 'csv':
                return self._load_from_csv(data_source)
            elif source_type == 'etl_result':
                return self._load_from_etl_result(data_source)
            elif source_type in ['postgresql', 'mysql', 'oracle', 'sqlserver', 'sqlite']:
                return self._load_from_database(data_source)
            elif source_type == 'api':
                return self._load_from_api(data_source)
            elif source_type == 'excel':
                return self._load_from_excel(data_source)
            elif source_type == 'json':
                return self._load_from_json(data_source)
            else:
                # Try unified data access as fallback
                return self._load_from_unified_access(data_source)
                
        except Exception as e:
            logger.error(f"Error loading data from {data_source.source_type} source: {e}")
            return False, None, f"Failed to load data: {str(e)}"
    
    def _load_from_csv(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Load data from CSV file"""
        try:
            file_path = data_source.connection_info.get('file_path')
            if not file_path:
                # Try unified data access for CSV
                return self._load_from_unified_access(data_source)
            
            # Try multiple CSV loading strategies
            from datasets.views import DataService
            data_service = DataService()
            
            # Try to resolve file path
            full_file_path = data_service.resolve_csv_path(file_path)
            
            if not full_file_path:
                logger.warning(f"CSV file not found: {file_path}, trying PostgreSQL backup")
                
                # Try to load from PostgreSQL unified storage
                try:
                    from django.db import connection
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT data FROM unified_data_storage WHERE data_source_name = %s ORDER BY created_at DESC LIMIT 1",
                            [data_source.name]
                        )
                        result = cursor.fetchone()
                        
                        if result:
                            import json
                            data_rows = json.loads(result[0])
                            df = pd.DataFrame(data_rows)
                            logger.info(f"Successfully loaded {len(df)} rows from PostgreSQL unified storage")
                            return True, df, f"Loaded {len(df)} rows from backup storage"
                        else:
                            return self._generate_sample_from_schema(data_source)
                            
                except Exception as e:
                    logger.error(f"Error loading from PostgreSQL backup: {e}")
                    return self._generate_sample_from_schema(data_source)
            else:
                # Load from actual CSV file using enhanced parsing options if available
                parsing_options = data_source.connection_info.get('parsing_options', {})
                enhanced_processing = data_source.connection_info.get('enhanced_processing', False)
                
                if enhanced_processing and parsing_options:
                    # Use enhanced CSV processor for consistent parsing
                    logger.info(f"Loading CSV with enhanced parsing options: {parsing_options}")
                    
                    try:
                        from services.enhanced_csv_processor import EnhancedCSVProcessor
                        processor = EnhancedCSVProcessor()
                        
                        success, df, message = processor.process_csv_with_options(full_file_path, parsing_options)
                        
                        if success and df is not None:
                            logger.info(f"Successfully loaded {len(df)} rows from CSV file with enhanced parsing")
                            return True, df, f"Loaded {len(df)} rows from CSV file with enhanced parsing"
                        else:
                            logger.warning(f"Enhanced parsing failed: {message}, falling back to basic CSV loading")
                    except Exception as e:
                        logger.warning(f"Enhanced CSV processor failed: {e}, falling back to basic CSV loading")
                
                # Fallback to basic CSV loading with detected options
                csv_options = {
                    'delimiter': parsing_options.get('delimiter', ','),
                    'encoding': parsing_options.get('encoding', 'utf-8'),
                    'header': 0 if parsing_options.get('has_header', True) else None
                }
                
                df = pd.read_csv(full_file_path, **csv_options)
                logger.info(f"Successfully loaded {len(df)} rows from CSV file")
                return True, df, f"Loaded {len(df)} rows from CSV file"
                
        except Exception as e:
            logger.error(f"Error loading CSV data: {e}")
            return False, None, f"CSV loading error: {str(e)}"
    
    def _load_from_etl_result(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Load data from ETL result table in DuckDB"""
        try:
            # Get table name from data source
            table_name = data_source.table_name
            if not table_name:
                table_name = data_source.connection_info.get('source_table_name')
            
            if not table_name:
                return False, None, "ETL result table name not found in data source"
            
            logger.info(f"Loading ETL result data from table: {table_name}")
            
            # Get DuckDB connection
            from datasets.data_access_layer import unified_data_access
            unified_data_access._ensure_duckdb_connection()
            conn = unified_data_access.duckdb_connection
            
            if not conn:
                return False, None, "Could not connect to DuckDB"
            
            # Check if table exists
            try:
                # Handle schema-qualified table names
                if '.' in table_name:
                    # Already qualified
                    full_table_name = table_name
                else:
                    # Try to find in different schemas
                    schemas_to_try = ['integrated', 'main']
                    full_table_name = None
                    
                    for schema in schemas_to_try:
                        test_name = f"{schema}.{table_name}"
                        try:
                            conn.execute(f"SELECT 1 FROM {test_name} LIMIT 1")
                            full_table_name = test_name
                            break
                        except:
                            continue
                    
                    if not full_table_name:
                        full_table_name = table_name  # Fallback to unqualified
                
                # Load data from table
                query = f"SELECT * FROM {full_table_name}"
                df = conn.execute(query).df()
                
                logger.info(f"Successfully loaded {len(df)} rows from ETL result table: {full_table_name}")
                return True, df, f"Loaded {len(df)} rows from ETL result table"
                
            except Exception as e:
                logger.error(f"Error querying ETL result table {table_name}: {e}")
                return False, None, f"Could not access ETL result table: {str(e)}"
                
        except Exception as e:
            logger.error(f"Error loading ETL result data: {e}")
            return False, None, f"ETL result loading error: {str(e)}"
    
    def _load_from_database(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Load data from database source"""
        try:
            conn_info = data_source.connection_info
            db_type = data_source.source_type
            
            # Build connection string based on database type
            if db_type == 'postgresql':
                import psycopg2
                import sqlalchemy
                
                conn_str = f"postgresql://{conn_info['user']}:{conn_info['password']}@{conn_info['host']}:{conn_info.get('port', 5432)}/{conn_info['database']}"
                engine = sqlalchemy.create_engine(conn_str)
                
                # Get table name
                table_name = conn_info.get('table_name', data_source.table_name)
                if not table_name:
                    return False, None, "Database table name not specified"
                
                # Load data
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, engine)
                
                logger.info(f"Successfully loaded {len(df)} rows from PostgreSQL table: {table_name}")
                return True, df, f"Loaded {len(df)} rows from PostgreSQL table"
                
            elif db_type == 'mysql':
                import pymysql
                import sqlalchemy
                
                conn_str = f"mysql+pymysql://{conn_info['user']}:{conn_info['password']}@{conn_info['host']}:{conn_info.get('port', 3306)}/{conn_info['database']}"
                engine = sqlalchemy.create_engine(conn_str)
                
                table_name = conn_info.get('table_name', data_source.table_name)
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, engine)
                
                logger.info(f"Successfully loaded {len(df)} rows from MySQL table: {table_name}")
                return True, df, f"Loaded {len(df)} rows from MySQL table"
                
            elif db_type == 'oracle':
                import cx_Oracle
                import sqlalchemy
                
                dsn = f"{conn_info['host']}:{conn_info.get('port', 1521)}/{conn_info['service_name']}"
                conn_str = f"oracle+cx_oracle://{conn_info['user']}:{conn_info['password']}@{dsn}"
                engine = sqlalchemy.create_engine(conn_str)
                
                table_name = conn_info.get('table_name', data_source.table_name)
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, engine)
                
                logger.info(f"Successfully loaded {len(df)} rows from Oracle table: {table_name}")
                return True, df, f"Loaded {len(df)} rows from Oracle table"
                
            elif db_type == 'sqlite':
                import sqlite3
                
                db_path = conn_info.get('database_path')
                if not db_path:
                    return False, None, "SQLite database path not specified"
                
                conn = sqlite3.connect(db_path)
                table_name = conn_info.get('table_name', data_source.table_name)
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, conn)
                conn.close()
                
                logger.info(f"Successfully loaded {len(df)} rows from SQLite table: {table_name}")
                return True, df, f"Loaded {len(df)} rows from SQLite table"
                
            else:
                return False, None, f"Database type {db_type} not supported yet"
                
        except Exception as e:
            logger.error(f"Error loading database data: {e}")
            return False, None, f"Database loading error: {str(e)}"
    
    def _load_from_api(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Load data from API source"""
        try:
            import requests
            
            conn_info = data_source.connection_info
            api_url = conn_info.get('api_url')
            
            if not api_url:
                return False, None, "API URL not specified"
            
            # Prepare headers
            headers = {}
            if conn_info.get('api_key'):
                headers['Authorization'] = f"Bearer {conn_info['api_key']}"
            
            # Make API request
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse response based on content type
            if 'application/json' in response.headers.get('content-type', ''):
                data = response.json()
                
                # Handle different JSON structures
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                elif isinstance(data, dict):
                    if 'data' in data:
                        df = pd.DataFrame(data['data'])
                    elif 'results' in data:
                        df = pd.DataFrame(data['results'])
                    else:
                        df = pd.DataFrame([data])
                else:
                    return False, None, "Unsupported JSON structure from API"
                    
                logger.info(f"Successfully loaded {len(df)} rows from API: {api_url}")
                return True, df, f"Loaded {len(df)} rows from API"
                
            else:
                return False, None, "API response is not JSON format"
                
        except Exception as e:
            logger.error(f"Error loading API data: {e}")
            return False, None, f"API loading error: {str(e)}"
    
    def _load_from_excel(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Load data from Excel file"""
        try:
            file_path = data_source.connection_info.get('file_path')
            if not file_path:
                return False, None, "Excel file path not specified"
            
            # Load Excel file
            sheet_name = data_source.connection_info.get('sheet_name', 0)
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            logger.info(f"Successfully loaded {len(df)} rows from Excel file")
            return True, df, f"Loaded {len(df)} rows from Excel file"
            
        except Exception as e:
            logger.error(f"Error loading Excel data: {e}")
            return False, None, f"Excel loading error: {str(e)}"
    
    def _load_from_json(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Load data from JSON file"""
        try:
            import json
            
            file_path = data_source.connection_info.get('file_path')
            if not file_path:
                return False, None, "JSON file path not specified"
            
            # Load JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                return False, None, "Unsupported JSON structure"
            
            logger.info(f"Successfully loaded {len(df)} rows from JSON file")
            return True, df, f"Loaded {len(df)} rows from JSON file"
            
        except Exception as e:
            logger.error(f"Error loading JSON data: {e}")
            return False, None, f"JSON loading error: {str(e)}"
    
    def _load_from_unified_access(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Load data using unified data access layer as fallback"""
        try:
            from datasets.data_access_layer import unified_data_access
            
            success, df, message = unified_data_access.get_data_source_data(data_source)
            
            if success and df is not None and not df.empty:
                logger.info(f"Successfully loaded {len(df)} rows using unified data access")
                return True, df, f"Loaded {len(df)} rows using unified data access"
            else:
                return self._generate_sample_from_schema(data_source)
                
        except Exception as e:
            logger.error(f"Error loading with unified data access: {e}")
            return self._generate_sample_from_schema(data_source)
    
    def _generate_sample_from_schema(self, data_source: DataSource) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """Generate sample data from schema as last resort"""
        try:
            schema_info = data_source.schema_info
            if not schema_info or 'columns' not in schema_info:
                return False, None, "No schema information available to generate sample data"
            
            # Generate sample data from schema
            sample_data = {}
            for col in schema_info['columns'][:10]:  # Max 10 columns
                col_name = col['name']
                col_type = col.get('type', 'VARCHAR')
                
                if 'int' in col_type.lower():
                    sample_data[col_name] = [1, 2, 3, 4, 5]
                elif 'float' in col_type.lower() or 'decimal' in col_type.lower():
                    sample_data[col_name] = [1.1, 2.2, 3.3, 4.4, 5.5]
                elif 'date' in col_type.lower():
                    sample_data[col_name] = ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
                else:
                    sample_data[col_name] = ['Sample1', 'Sample2', 'Sample3', 'Sample4', 'Sample5']
            
            df = pd.DataFrame(sample_data)
            logger.warning(f"Generated sample data with {len(df)} rows from schema")
            return True, df, f"Generated {len(df)} sample rows from schema (original data not accessible)"
            
        except Exception as e:
            logger.error(f"Error generating sample data: {e}")
            return False, None, f"Could not generate sample data: {str(e)}"

# Global instance
universal_data_loader = UniversalDataLoader() 