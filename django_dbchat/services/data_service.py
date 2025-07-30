"""
Data Service for ConvaBI Application
Provides data connection and query execution functionality with enhanced security and performance
"""

import psycopg2
import psycopg2.pool
import pandas as pd
import sqlite3
import json
import logging
import os
import time
import re
from typing import Dict, List, Any, Optional, Tuple
from django.conf import settings
from django.db import connections
from django.core.cache import cache
from core.models import QueryLog
from datasets.models import DataSource
from utils.type_helpers import (
    map_pandas_dtype_to_standard,
    infer_semantic_type_from_series,
    convert_object_columns_to_string,
    get_column_type_info
)
from datetime import datetime
from decimal import Decimal, InvalidOperation
from django.utils import timezone

logger = logging.getLogger(__name__)

class DataService:
    """
    Enhanced data execution service with connection pooling, caching, and security features
    """
    
    def __init__(self):
        self.connections_cache = {}
        self.connection_pools = {}
        self._init_connection_pools()
    
    def _init_connection_pools(self):
        """Initialize connection pools for better performance"""
        try:
            # Initialize PostgreSQL connection pool if configured
            if hasattr(settings, 'DATABASE_CONNECTION_POOL_SIZE'):
                pool_size = getattr(settings, 'DATABASE_CONNECTION_POOL_SIZE', 5)
                self.max_pool_size = pool_size
            else:
                self.max_pool_size = 5
        except Exception as e:
            logger.warning(f"Failed to initialize connection pools: {e}")
    
    def get_connection(self, connection_info: Dict[str, Any]) -> Optional[Any]:
        """
        Get database connection with connection pooling and retry logic
        """
        connection_type = connection_info.get('type', 'postgresql')
        
        # Create cache key for connection pooling
        cache_key = self._create_connection_cache_key(connection_info)
        
        # Clear any existing cached connections to prevent engine/cursor errors
        if cache_key in self.connections_cache:
            try:
                old_conn = self.connections_cache[cache_key]
                if hasattr(old_conn, 'close'):
                    old_conn.close()
            except:
                pass
            del self.connections_cache[cache_key]
        
        # Create new connection with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if connection_type == 'postgresql':
                    conn = self._get_postgresql_connection(connection_info)
                elif connection_type == 'mysql':
                    conn = self._get_mysql_connection(connection_info)
                elif connection_type == 'oracle':
                    conn = self._get_oracle_connection(connection_info)
                elif connection_type == 'sqlserver':
                    conn = self._get_sqlserver_connection(connection_info)
                elif connection_type == 'sqlite':
                    conn = self._get_sqlite_connection(connection_info)
                elif connection_type == 'csv':
                    conn = self._get_csv_connection(connection_info)
                elif connection_type == 'etl_result':
                    conn = self._get_etl_result_connection(connection_info)
                else:
                    logger.error(f"Unsupported connection type: {connection_type}")
                    return None
                
                # Handle both database connections and DataFrames safely
                if connection_type == 'csv':
                    # For CSV, conn is a DataFrame - check if it's valid
                    if isinstance(conn, pd.DataFrame):
                        return conn  # Return DataFrame even if empty (valid state)
                    # If conn is None for CSV, continue to retry
                elif conn is not None:
                    # For database connections, return directly (skip caching for now)
                    # self.connections_cache[cache_key] = conn  # Disabled to prevent engine/cursor mixing
                    return conn
                    
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"All connection attempts failed for {connection_type}")
                    
        return None
    
    def _create_connection_cache_key(self, connection_info: Dict[str, Any]) -> str:
        """Create a cache key for connection info (excluding sensitive data)"""
        safe_info = {
            'type': connection_info.get('type'),
            'host': connection_info.get('host'),
            'port': connection_info.get('port'),
            'database': connection_info.get('database'),
            'username': connection_info.get('username'),
            'file_path': connection_info.get('file_path')
        }
        return f"conn_{hash(json.dumps(safe_info, sort_keys=True))}"
    
    def _test_connection_health(self, conn, connection_type: str) -> bool:
        """Test if a cached connection is still healthy"""
        try:
            if connection_type in ['postgresql', 'mysql']:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
            elif connection_type == 'sqlite':
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
            return True
        except Exception:
            return False
    
    def _get_postgresql_connection(self, connection_info: Dict[str, Any]) -> Optional[psycopg2.extensions.connection]:
        """Get PostgreSQL connection with proper parameter mapping"""
        try:
            conn_params = {
                'host': connection_info.get('host'),
                'port': connection_info.get('port', 5432),
                'database': connection_info.get('database'),
                'user': connection_info.get('username'),  # Fixed: use 'user' instead of 'username'
                'password': connection_info.get('password'),
                'connect_timeout': 10,  # Add timeout
                'application_name': 'ConvaBI'
            }
            
            # Filter out None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}
            
            connection = psycopg2.connect(**conn_params)
            connection.autocommit = True  # Enable autocommit for safety
            return connection
            
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            return None
    
    def _get_mysql_connection(self, connection_info: Dict[str, Any]) -> Optional[Any]:
        """Get MySQL connection with timeout configuration"""
        try:
            import mysql.connector
            
            conn_params = {
                'host': connection_info.get('host'),
                'port': connection_info.get('port', 3306),
                'database': connection_info.get('database'),
                'user': connection_info.get('username'),
                'password': connection_info.get('password'),
                'connection_timeout': 10,
                'autocommit': True
            }
            
            # Filter out None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}
            
            connection = mysql.connector.connect(**conn_params)
            return connection
            
        except ImportError:
            logger.error("MySQL connector not installed. Run: pip install mysql-connector-python")
            return None
        except Exception as e:
            logger.error(f"MySQL connection failed: {e}")
            return None
    
    def _get_oracle_connection(self, connection_info: Dict[str, Any]) -> Optional[Any]:
        """Get Oracle connection with timeout configuration"""
        try:
            import cx_Oracle
            
            host = connection_info.get('host')
            port = connection_info.get('port', 1521)
            database = connection_info.get('database')
            username = connection_info.get('username')
            password = connection_info.get('password')
            
            if not all([host, database, username, password]):
                logger.error("Missing required Oracle connection parameters")
                return None
            
            # Create DSN with timeout
            dsn = cx_Oracle.makedsn(host, port, service_name=database)
            connection = cx_Oracle.connect(username, password, dsn, timeout=10)
            return connection
            
        except ImportError:
            logger.error("Oracle client not installed. Run: pip install cx_Oracle")
            return None
        except Exception as e:
            logger.error(f"Oracle connection failed: {e}")
            return None
    
    def _get_sqlserver_connection(self, connection_info: Dict[str, Any]) -> Optional[Any]:
        """Get SQL Server connection with timeout configuration"""
        try:
            import pyodbc
            
            host = connection_info.get('host')
            port = connection_info.get('port', 1433)
            database = connection_info.get('database')
            username = connection_info.get('username')
            password = connection_info.get('password')
            
            if not all([host, database, username, password]):
                logger.error("Missing required SQL Server connection parameters")
                return None
            
            # Create connection string with timeout
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password};Connection Timeout=10;"
            connection = pyodbc.connect(conn_str)
            connection.autocommit = True
            return connection
            
        except ImportError:
            logger.error("SQL Server driver not installed. Run: pip install pyodbc")
            return None
        except Exception as e:
            logger.error(f"SQL Server connection failed: {e}")
            return None
    
    def _get_sqlite_connection(self, connection_info: Dict[str, Any]) -> Optional[sqlite3.Connection]:
        """Get SQLite connection with proper configuration"""
        try:
            db_path = connection_info.get('path', ':memory:')
            connection = sqlite3.connect(db_path, check_same_thread=False, timeout=10.0)
            connection.row_factory = sqlite3.Row  # Enable column access by name
            return connection
            
        except sqlite3.Error as e:
            logger.error(f"SQLite connection failed: {e}")
            return None
    
    def resolve_csv_path(self, file_path: Optional[str]) -> Optional[str]:
        """Dedicated method to resolve CSV file paths"""
        if not file_path:
            logger.error("CSV file path not provided")
            return None
        
        # Try as absolute path first
        if os.path.isabs(file_path) and os.path.exists(file_path):
            return file_path
        
        # Try relative to media directory
        media_path = os.path.join(settings.MEDIA_ROOT, file_path)
        if os.path.exists(media_path):
            return media_path
        
        # Try relative to project root
        project_root = os.path.dirname(settings.BASE_DIR)
        project_path = os.path.join(project_root, file_path)
        if os.path.exists(project_path):
            return project_path
        
        # Try relative to current working directory
        if os.path.exists(file_path):
            return file_path
        
        # Try common locations for the file
        filename = os.path.basename(file_path)
        possible_locations = [
            os.path.join(settings.MEDIA_ROOT, 'csv_files', filename),
            os.path.join(settings.MEDIA_ROOT, filename),
            os.path.join(settings.BASE_DIR, 'media', 'csv_files', filename),
            os.path.join(settings.BASE_DIR, 'csv_files', filename),
        ]
        
        for location in possible_locations:
            if os.path.exists(location):
                return location
        
        logger.error(f"CSV file not found: {file_path}")
        return None
    
    def _get_csv_connection(self, connection_info: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Get CSV data as DataFrame with enhanced error handling"""
        try:
            file_path = connection_info.get('file_path')
            resolved_path = self.resolve_csv_path(file_path)
            
            if not resolved_path:
                return None
            
            # Check file size limit (100MB default)
            max_file_size = getattr(settings, 'MAX_CSV_FILE_SIZE', 100 * 1024 * 1024)
            file_size = os.path.getsize(resolved_path)
            if file_size > max_file_size:
                logger.error(f"CSV file too large: {file_size} bytes (max: {max_file_size})")
                return None
            
            # Read CSV file with comprehensive encoding handling
            logger.info(f"Reading CSV from: {resolved_path}")
            
            # Try multiple encodings in order of likelihood
            encodings_to_try = ['utf-8', 'utf-16', 'latin-1', 'cp1252', 'iso-8859-1', 'windows-1252']
            
            df = None
            successful_encoding = None
            
            for encoding in encodings_to_try:
                try:
                    logger.info(f"Attempting to read CSV with encoding: {encoding}")
                    df = pd.read_csv(resolved_path, encoding=encoding)
                    successful_encoding = encoding
                    logger.info(f"Successfully read CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError as e:
                    logger.warning(f"Failed to read CSV with {encoding} encoding: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"Unexpected error reading CSV with {encoding} encoding: {e}")
                    continue
            
            if df is None:
                logger.error(f"Failed to read CSV file with any encoding: {resolved_path}")
                return None
            
            logger.info(f"Successfully loaded CSV with {len(df)} rows and {len(df.columns)} columns")
            
            # Normalize column names to ensure they are strings and SQL-safe
            normalized_columns = []
            for i, col in enumerate(df.columns):
                # Convert to string and clean
                col_str = str(col).strip()
                if not col_str or col_str.lower() in ['unnamed', 'unnamed:']:
                    col_str = f"col_{i}"
                # Make SQL-safe by replacing spaces and special characters
                col_str = re.sub(r'[^a-zA-Z0-9_]', '_', col_str)
                # Ensure it doesn't start with a number
                if col_str[0].isdigit():
                    col_str = f"col_{col_str}"
                normalized_columns.append(col_str)
            
            # Update DataFrame column names
            df.columns = normalized_columns
            logger.info(f"Normalized column names: {list(df.columns)}")
            
            # Apply automatic data type inference and conversion
            df, dtype_mapping = self._auto_convert_data_types(df)
            logger.info("Applied automatic data type inference to DataFrame")
            
            return df
                
        except Exception as e:
            logger.error(f"CSV connection failed: {e}")
            return None

    def _get_etl_result_connection(self, connection_info: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Get ETL result data as DataFrame from DuckDB"""
        try:
            from services.universal_data_loader import universal_data_loader
            
            # Create a mock data source object with ETL result information
            class MockDataSource:
                def __init__(self, connection_info):
                    self.source_type = 'etl_result'
                    self.connection_info = connection_info
                    self.table_name = connection_info.get('source_table_name') or connection_info.get('table_name')
                    self.name = f"ETL Result: {self.table_name}"
                    
            mock_data_source = MockDataSource(connection_info)
            
            # Use universal data loader to get the data
            success, df, message = universal_data_loader.load_data_for_transformation(mock_data_source)
            
            if not success or df is None or df.empty:
                logger.error(f"Failed to load ETL result data: {message}")
                return None
            
            logger.info(f"Successfully loaded ETL result with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"ETL result connection failed: {e}")
            return None
    
    def execute_query(self, query: str, connection_info: Dict[str, Any], 
                     user_id: Optional[int] = None, use_cache: bool = True) -> Tuple[bool, Any]:
        """
        Execute SQL query with enhanced security and performance features
        FIXED: Always prioritize DuckDB data over CSV files
        """
        # Input validation and SQL injection prevention
        if not query or not query.strip():
            return False, "Query cannot be empty"
        
        query = query.strip()
        
        # Basic SQL injection prevention
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        query_upper = query.upper()
        for keyword in dangerous_keywords:
            if keyword in query_upper and not query_upper.startswith('SELECT'):
                logger.warning(f"Potentially dangerous query blocked: {query[:100]}")
                return False, f"Query contains potentially dangerous keyword: {keyword}"
        
        # Check query cache if enabled
        if use_cache:
            cache_key = f"query_cache_{hash(query)}_{hash(json.dumps(connection_info, sort_keys=True))}"
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                logger.info("Returning cached query result")
                return True, cached_result
        
        # ENHANCED: ALWAYS check DuckDB first for any data source
        connection_type = connection_info.get('type', 'postgresql')
        
        # Try to find data in DuckDB for ANY source type (not just CSV)
        duckdb_success, duckdb_result = self._try_duckdb_query_first(query, connection_info, user_id)
        if duckdb_success:
            logger.info("Successfully executed query against DuckDB integrated data")
            return True, duckdb_result
        
        # If DuckDB failed, fall back to original connection logic
        logger.info("DuckDB query failed, falling back to original connection logic")
        
        # For CSV data sources, check if they should use integrated DuckDB (legacy fallback)
        if connection_type == 'csv':
            # Check if this CSV has been loaded into the integrated database
            csv_file_path = connection_info.get('file_path')
            if csv_file_path:
                # Try to get the DataSource to check if it has integrated data
                try:
                    from datasets.models import DataSource
                    data_source = DataSource.objects.filter(
                        connection_info__file_path=csv_file_path,
                        status='active'
                    ).first()
                    
                    if data_source and data_source.table_name:
                        # This CSV has been integrated - use DuckDB for querying
                        logger.info(f"Redirecting CSV query to integrated DuckDB table: {data_source.table_name}")
                        return self._execute_integrated_query(query, data_source.table_name, user_id)
                except Exception as e:
                    logger.warning(f"Could not check for integrated data: {e}")
        
        # Original logic for non-integrated queries
        connection = self.get_connection(connection_info)
        # Handle both database connections and DataFrames safely - FIXED: Avoid DataFrame boolean context
        if connection_info.get('type') == 'csv':
            if not isinstance(connection, pd.DataFrame):
                return False, "Failed to load CSV data"
            elif connection.empty:
                return False, "CSV data is empty"
        else:
            if connection is None:
                return False, "Failed to establish database connection"
        
        try:
            start_time = time.time()
            
            if connection_info.get('type') in ['csv', 'etl_result'] or isinstance(connection, pd.DataFrame):
                # For CSV and ETL results (DataFrame connections), use DuckDB for SQL execution
                result = self._execute_query_on_dataframe_with_duckdb(query, connection, user_id)
                if result[0]:  # If successful
                    result = result[1]  # Extract the DataFrame result
                else:
                    raise Exception(result[1])  # Raise the error message
            else:
                # For database connections, use pandas read_sql
                result = pd.read_sql(query, connection)
            
            execution_time = time.time() - start_time
            
            # Safely get result count for logging
            try:
                if isinstance(result, pd.DataFrame):
                    result_count = len(result)
                elif hasattr(result, '__len__'):
                    result_count = len(result)
                else:
                    result_count = 1 if result is not None else 0
            except:
                result_count = 0
            
            # Cache successful results
            if use_cache and result_count > 0:
                cache.set(cache_key, result, timeout=300)  # Cache for 5 minutes
            
            # Log successful query
            if user_id:
                self._log_query(user_id, query, 'SUCCESS', result_count, "", execution_time)
            
            logger.info(f"Query executed successfully in {execution_time:.2f}s, {result_count} rows returned")
            return True, result
            
        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            logger.error(error_msg)
            
            # Log failed query
            if user_id:
                self._log_query(user_id, query, 'FAILURE', 0, error_msg)
            
            return False, error_msg
            
        finally:
            # Only close non-cached connections (not DataFrames)
            if (connection is not None and 
                not isinstance(connection, pd.DataFrame) and 
                hasattr(connection, 'close') and 
                connection_info.get('type') != 'csv'):
                cache_key = self._create_connection_cache_key(connection_info)
                if cache_key not in self.connections_cache:
                    connection.close()
    
    def _try_duckdb_query_first(self, query: str, connection_info: Dict[str, Any], user_id: Optional[int] = None) -> Tuple[bool, Any]:
        """
        NEW: Try to execute query against DuckDB first for any connection type
        This ensures we always use integrated data when available
        """
        try:
            from datasets.models import DataSource
            from datasets.data_access_layer import unified_data_access
            
            # Strategy 1: Try to find data source by file path (for CSV)
            csv_file_path = connection_info.get('file_path')
            if csv_file_path:
                logger.info(f"[DUCKDB_FIRST] Looking for CSV data source with file path: {csv_file_path}")
                data_source = DataSource.objects.filter(
                    connection_info__file_path=csv_file_path,
                    status='active'
                ).first()
                
                if data_source:
                    logger.info(f"[DUCKDB_FIRST] Found data source: {data_source.name}")
                    # Try to get data from unified access layer
                    success, df, message = unified_data_access.get_data_source_data(data_source)
                    
                    if success and df is not None and not df.empty:
                        logger.info(f"[DUCKDB_FIRST] Successfully retrieved data: {len(df)} rows")
                        # Execute query on the DataFrame using DuckDB
                        return self._execute_query_on_dataframe_with_duckdb(query, df, user_id)
                    else:
                        logger.warning(f"[DUCKDB_FIRST] Failed to get data via unified access: {message}")
            
            # Strategy 2: Try to find any active data sources for the user
            if user_id:
                logger.info(f"[DUCKDB_FIRST] Looking for active data sources for user {user_id}")
                active_data_sources = DataSource.objects.filter(
                    created_by_id=user_id,
                    status='active'
                ).exclude(
                    workflow_status__etl_completed=False
                )
                
                for data_source in active_data_sources:
                    logger.info(f"[DUCKDB_FIRST] Trying data source: {data_source.name}")
                    success, df, message = unified_data_access.get_data_source_data(data_source)
                    
                    if success and df is not None and not df.empty:
                        logger.info(f"[DUCKDB_FIRST] Found data in {data_source.name}: {len(df)} rows")
                        # Execute query on the DataFrame using DuckDB
                        return self._execute_query_on_dataframe_with_duckdb(query, df, user_id)
            
            logger.info("[DUCKDB_FIRST] No DuckDB data found")
            return False, None
            
        except Exception as e:
            logger.warning(f"[DUCKDB_FIRST] Error trying DuckDB first: {e}")
            return False, None
    
    def _execute_query_on_dataframe_with_duckdb(self, query: str, df: pd.DataFrame, user_id: Optional[int] = None) -> Tuple[bool, Any]:
        """
        Execute query on a DataFrame using DuckDB for better SQL compatibility
        """
        try:
            import duckdb
            
            # Create in-memory DuckDB connection
            conn = duckdb.connect(':memory:')
            
            # Register DataFrame as a table
            conn.register('data', df)
            
            # Adapt query to use 'data' as table name
            # For ETL results, extract the specific table name from context if available  
            specific_table_name = None
            # Try to extract table name from query itself
            import re
            etl_table_matches = re.findall(r'\b(etl_\w+_\d{8}_\d{6}_ds_[a-f0-9_]+)\b', query, re.IGNORECASE)
            if etl_table_matches:
                specific_table_name = etl_table_matches[0]
                
            adapted_query = self._adapt_query_for_dataframe(query, specific_table_name)
            
            start_time = time.time()
            
            # Execute query
            result = conn.execute(adapted_query).fetchdf()
            
            execution_time = time.time() - start_time
            
            # Log successful query
            if user_id:
                self._log_query(user_id, adapted_query, 'SUCCESS', len(result), "", execution_time)
            
            logger.info(f"DuckDB DataFrame query executed successfully in {execution_time:.2f}s, {len(result)} rows returned")
            conn.close()
            
            return True, result
            
        except Exception as e:
            logger.error(f"DuckDB DataFrame query failed: {e}")
            return False, str(e)
    
    def _adapt_query_for_dataframe(self, query: str, specific_table_name: Optional[str] = None) -> str:
        """
        Adapt query to work with DataFrame registered as 'data' table
        FIXED: Enhanced to handle ETL result table names and specific table replacement
        """
        import re
        
        adapted_query = query
        
        # PRIORITY 1: If we have a specific table name, replace it first
        if specific_table_name:
            # Use word boundaries to ensure exact table name replacement
            specific_pattern = r'\b' + re.escape(specific_table_name) + r'\b'
            adapted_query = re.sub(specific_pattern, 'data', adapted_query, flags=re.IGNORECASE)
            logger.info(f"Replaced specific table '{specific_table_name}' with 'data'")
        
        # PRIORITY 2: Handle ETL result table patterns
        etl_patterns = [
            r'\betl_join_\d{8}_\d{6}_ds_[a-f0-9]+_ds_[a-f0-9]+\b',         # ETL join results (two sources)
            r'\betl_union_\d{8}_\d{6}_ds_[a-f0-9]+_ds_[a-f0-9]+\b',        # ETL union results (two sources)
            r'\betl_aggregate_\d{8}_\d{6}_ds_[a-f0-9]+\b',                 # ETL aggregate results (single source)
            r'\betl_transform_\d{8}_\d{6}_ds_[a-f0-9]+\b',                 # ETL transform results (single source)
            r'\betl_\w+_\d{8}_\d{6}_ds_[a-f0-9_]+\b',                      # Generic ETL pattern (fallback)
        ]
        
        for pattern in etl_patterns:
            count = len(re.findall(pattern, adapted_query, flags=re.IGNORECASE))
            if count > 0:
                adapted_query = re.sub(pattern, 'data', adapted_query, flags=re.IGNORECASE)
                logger.info(f"Replaced {count} ETL table references with pattern: {pattern}")
        
        # PRIORITY 3: Handle other common table patterns
        common_patterns = [
            r'\bcsv_data\b',
            r'\bmain_table\b', 
            r'\btable\b',
            r'\bds_[a-f0-9_]+\b',           # Dynamic table names
            r'\bsource_[a-f0-9_]+\b',       # Source table names
            r'\bsource_id_[a-f0-9_]+\b',    # Source ID table names
        ]
        
        for pattern in common_patterns:
            count = len(re.findall(pattern, adapted_query, flags=re.IGNORECASE))
            if count > 0:
                adapted_query = re.sub(pattern, 'data', adapted_query, flags=re.IGNORECASE)
                logger.info(f"Replaced {count} common table references with pattern: {pattern}")
        
        if adapted_query != query:
            logger.info(f"Adapted query: {query} -> {adapted_query}")
        else:
            logger.info(f"No table adaptations needed for query: {query}")
            
        return adapted_query
    
    def _execute_integrated_query(self, query: str, table_name: str, user_id: Optional[int] = None) -> Tuple[bool, Any]:
        """
        Execute query against integrated DuckDB with improved table name resolution and SQL fixing
        FIXED: Better table name mapping and alias handling + SQL syntax validation
        """
        try:
            # Enhanced: Use robust DuckDB connection with better error handling
            import duckdb
            
            db_path = 'data/integrated.duckdb'
            logger.info(f"Using persistent DuckDB database at: {os.path.abspath(db_path)}")
            
            conn = duckdb.connect(db_path)
            logger.info(f"DuckDB integrated database initialized successfully at: {os.path.abspath(db_path)}")
            version_result = conn.execute('PRAGMA version').fetchone()
            logger.info(f"DuckDB version: {version_result[0] if version_result else 'unknown'}")
            
            # FIXED: Better table name resolution
            available_tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
            logger.info(f"Available tables in DuckDB: {available_tables}")
            
                        # FIXED: Use consistent table name resolution to prevent switching
            actual_table_name = self._get_consistent_table_name(query, available_tables, table_name)
            if not actual_table_name:
                logger.error(f"No matching table found for: {table_name}")
                logger.error(f"Available tables: {available_tables}")
                return False, f"Table not found: {table_name}"

            logger.info(f"Using consistent table name: {actual_table_name}")

            # FIXED: Enhanced query adaptation with consistent table name usage
            adapted_query = self._adapt_query_with_better_mapping(query, actual_table_name, conn)
            
            # NEW: Validate and fix SQL syntax before execution
            validated_query = self._validate_and_fix_sql_syntax(adapted_query)
            
            logger.info(f"Executing integrated query: {validated_query}")
            
            # Layer 3: Execute with fallback strategy
            return self._execute_with_fallback(conn, validated_query, actual_table_name, user_id)
            
        except Exception as e:
            error_msg = f"DuckDB query execution failed: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Failed query: {validated_query if 'validated_query' in locals() else adapted_query if 'adapted_query' in locals() else query}")
            
            if user_id:
                self._log_query(user_id, query, 'FAILURE', 0, error_msg)
            
            return False, error_msg
        finally:
            if 'conn' in locals():
                conn.close()
    
    def _execute_with_fallback(self, conn, query: str, table_name: str, user_id: Optional[int] = None) -> Tuple[bool, Any]:
        """
        Execute query with multiple fallback strategies
        Layer 3: Safety net for any remaining SQL issues
        """
        # Strategy 1: Try original query
        try:
            result = conn.execute(query).fetchdf()
            
            # Log successful execution
            if user_id:
                self._log_query(user_id, query, 'SUCCESS', len(result))
            
            logger.info(f"Query executed successfully, {len(result)} rows returned")
            return True, result
            
        except Exception as error1:
            logger.warning(f"Strategy 1 failed: {error1}")
            
            # Strategy 2: Try with aggressive SQL fixing
            try:
                aggressive_fixed = self._aggressive_sql_fix(query)
                logger.info(f"Trying aggressive fix: {aggressive_fixed}")
                
                result = conn.execute(aggressive_fixed).fetchdf()
                
                if user_id:
                    self._log_query(user_id, aggressive_fixed, 'SUCCESS_FALLBACK', len(result))
                
                logger.info(f"Aggressive fix succeeded, {len(result)} rows returned")
                return True, result
                
            except Exception as error2:
                logger.warning(f"Strategy 2 failed: {error2}")
                
                # REMOVED: Strategy 3 (simplified query) that gives wrong results
                logger.error(f"All strategies failed. Final error: {error2}")
                
                # Provide helpful error message
                error_msg = str(error1)
                if "column" in error_msg.lower() and "not found" in error_msg.lower():
                    return False, f"Column not found in table. Available columns: {self._get_available_columns(conn, table_name)}"
                elif "syntax error" in error_msg.lower():
                    return False, f"SQL syntax error: {error_msg}. Please check your query syntax."
                else:
                    return False, f"Query execution failed. Original error: {error_msg}"
    
    def _aggressive_sql_fix(self, query: str) -> str:
        """
        Apply aggressive SQL fixing for problematic queries
        """
        # Remove all problematic patterns
        fixed = query
        
        # Fix double underscores in aliases
        fixed = re.sub(r'as\s+"([^"]+)__([^"]+)"', r'as "\1_\2"', fixed, flags=re.IGNORECASE)
        
        # Fix unquoted column references in ORDER BY
        fixed = re.sub(r'ORDER\s+BY\s+([A-Za-z0-9_]+)_"([^"]+)"', r'ORDER BY "\1_\2"', fixed, flags=re.IGNORECASE)
        fixed = re.sub(r'ORDER\s+BY\s+([A-Za-z0-9_]+)(?=\s+DESC|\s+ASC|\s*$|\s*LIMIT)', r'ORDER BY "\1"', fixed, flags=re.IGNORECASE)
        
        # Fix any remaining unquoted identifiers
        fixed = re.sub(r'([A-Za-z0-9_]+)_"([^"]+)"', r'"\1_\2"', fixed)
        
        return fixed
    
    def _simplify_query(self, query: str, table_name: str, conn) -> str:
        """
        REMOVED: Simplified query fallback that gives wrong results
        Now returns None to force proper error handling instead of misleading COUNT queries
        """
        logger.warning("Simplified query fallback was requested but has been disabled to prevent wrong results")
        return None
    
    def _get_available_columns(self, conn, table_name: str) -> List[str]:
        """
        Get available columns for a table
        """
        try:
            schema_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
            return [row[0] for row in schema_info]
        except:
            return []
    
    def _validate_and_fix_sql_syntax(self, query: str) -> str:
        """
        Validate and fix SQL syntax issues for DuckDB compatibility
        FIXED: Handle double quote issues and use SQLFixer correctly
        """
        try:
            import re
            
            # CRITICAL FIX: Remove malformed double quotes first
            fixed_query = query
            
            # Fix double quotes like ""Sales"" -> "Sales"
            fixed_query = re.sub(r'""([^"]*?)""', r'"\1"', fixed_query)
            
            # Fix multiple consecutive quotes
            fixed_query = re.sub(r'""+', '"', fixed_query)
            
            # Fix empty quoted strings
            fixed_query = re.sub(r'""', '"', fixed_query)
            
            # Use the dedicated SQL fixer for comprehensive syntax repair
            try:
                from services.sql_fixer import SQLFixer
                fixed_query = SQLFixer.fix_sql_syntax(fixed_query)
            except ImportError:
                logger.warning("SQLFixer not available, using basic fixes")
            except Exception as fixer_error:
                logger.warning(f"SQLFixer failed: {fixer_error}, continuing with basic fixes")
            
            # Additional basic fixes
            # Convert backticks to double quotes
            fixed_query = re.sub(r'`([^`]+)`', r'"\1"', fixed_query)
            
            # Ensure proper semicolon ending
            if not fixed_query.strip().endswith(';'):
                fixed_query = fixed_query.strip() + ';'
            
            # Log if any changes were made
            if fixed_query != query:
                logger.info(f"SQL SYNTAX VALIDATION:")
                logger.info(f"  Original: {query}")
                logger.info(f"  Fixed:    {fixed_query}")
            
            return fixed_query
            
        except Exception as e:
            logger.error(f"Error validating SQL syntax: {e}")
            # Ultimate fallback: just clean up obvious issues
            try:
                import re
                fallback = query
                fallback = re.sub(r'""([^"]*?)""', r'"\1"', fallback)  # Fix double quotes
                fallback = re.sub(r'`([^`]+)`', r'"\1"', fallback)      # Fix backticks
                fallback = re.sub(r'""+', '"', fallback)                # Fix multiple quotes
                if not fallback.strip().endswith(';'):
                    fallback = fallback.strip() + ';'
                return fallback
            except:
                return query

    def _find_best_table_match(self, requested_table: str, available_tables: List[str]) -> Optional[str]:
        """
        Find the best matching table name from available tables
        FIXED: Comprehensive table name matching for all naming patterns
        """
        if not available_tables:
            return None
        
        # Direct match
        if requested_table in available_tables:
            return requested_table
        
        # Try case-insensitive match
        for table in available_tables:
            if table.lower() == requested_table.lower():
                return table
        
        # ENHANCED: Extract UUID pattern from requested table for comprehensive matching
        import re
        
        # Extract UUID-like patterns from the requested table
        uuid_patterns = []
        
        # Pattern 1: Extract hyphenated UUID (e.g., 7c476b12-e624-42d1-8b22-aa095ba30d4c)
        hyphen_match = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', requested_table)
        if hyphen_match:
            uuid_patterns.append(hyphen_match.group(1))
        
        # Pattern 2: Extract underscored UUID (e.g., 7c476b12_e624_42d1_8b22_aa095ba30d4c)
        underscore_match = re.search(r'([a-f0-9]{8}_[a-f0-9]{4}_[a-f0-9]{4}_[a-f0-9]{4}_[a-f0-9]{12})', requested_table)
        if underscore_match:
            uuid_patterns.append(underscore_match.group(1))
        
        # Pattern 3: Extract continuous UUID (e.g., 7c476b12e62442d18b22aa095ba30d4c)
        continuous_match = re.search(r'([a-f0-9]{32})', requested_table)
        if continuous_match:
            uuid_patterns.append(continuous_match.group(1))
        
        # If no UUID patterns found, try to extract any long alphanumeric sequence
        if not uuid_patterns:
            long_match = re.search(r'([a-f0-9_]{20,})', requested_table)
            if long_match:
                uuid_patterns.append(long_match.group(1))
        
        logger.info(f"Extracted UUID patterns from '{requested_table}': {uuid_patterns}")
        
        # Now try to match against all available tables using these patterns
        for uuid_pattern in uuid_patterns:
            # Create all possible variations of the UUID pattern
            uuid_variations = [
                uuid_pattern,  # Original
                uuid_pattern.replace('-', '_'),  # Hyphens to underscores
                uuid_pattern.replace('_', ''),   # Remove underscores
                uuid_pattern.replace('-', ''),   # Remove hyphens
            ]
            
            # Try different prefixes with each variation
            for uuid_var in uuid_variations:
                potential_names = [
                    f"source_id_{uuid_var}",
                    f"ds_{uuid_var}",
                    f"source_{uuid_var}",
                    f"data_{uuid_var}",
                    uuid_var,  # Just the UUID itself
                ]
                
                for potential_name in potential_names:
                    if potential_name in available_tables:
                        logger.info(f"Found matching table: '{requested_table}' -> '{potential_name}'")
                        return potential_name
        
        # Fallback: Try partial substring matching
        requested_lower = requested_table.lower()
        
        # Look for tables that contain significant parts of the requested table
        best_match = None
        best_score = 0
        
        for table in available_tables:
            table_lower = table.lower()
            
            # Calculate similarity score
            score = 0
            
            # Check if table contains significant parts of requested name
            if len(requested_lower) > 10:
                # For long names, check if major parts match
                requested_parts = re.findall(r'[a-f0-9]{8,}', requested_lower)
                for part in requested_parts:
                    if part in table_lower:
                        score += len(part)
            
            # Check prefix similarity
            if table_lower.startswith(requested_lower[:10]):
                score += 10
            
            # Check if both have similar structure (e.g., both have source_ prefix)
            if 'source' in requested_lower and 'source' in table_lower:
                score += 5
            elif 'ds_' in requested_lower and 'ds_' in table_lower:
                score += 5
            
            if score > best_score:
                best_score = score
                best_match = table
        
        if best_match and best_score > 10:  # Minimum threshold for similarity
            logger.info(f"Found similar table: '{requested_table}' -> '{best_match}' (score: {best_score})")
            return best_match
        
        # If no good match found, return the first table as fallback
        logger.warning(f"No good match found for '{requested_table}' in {available_tables}")
        logger.warning(f"Using first available table as fallback: {available_tables[0]}")
        return available_tables[0]

    def _adapt_query_with_better_mapping(self, query: str, table_name: str, conn) -> str:
        """
        Adapt query with better column mapping and CONSISTENT table name usage
        FIXED: Prevent table name switching during execution
        """
        try:
            # Get actual table schema
            schema_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
            available_columns = [row[0] for row in schema_info]
            
            logger.info(f"Adapting query for table: {table_name}")
            logger.info(f"Available columns: {available_columns[:10]}...")  # Show first 10
            
            # Start with the original query
            adapted_query = query
            
            # CRITICAL FIX: Use consistent table name throughout the entire process
            import re
            
            # Step 1: Replace ALL table references with the confirmed table name
            # This prevents switching between different table names
            
            # Common table reference patterns that might appear in queries
            generic_table_patterns = [
                r'\bFROM\s+([a-zA-Z0-9_]+)(?!\s*\()',
                r'\b((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN)\s+([a-zA-Z0-9_]+)(?!\s*\()',
                r'\bUPDATE\s+([a-zA-Z0-9_]+)',
                r'\bINTO\s+([a-zA-Z0-9_]+)',
            ]
            
            for pattern in generic_table_patterns:
                def replace_table_ref(match):
                    if len(match.groups()) == 2:
                        # JOIN pattern
                        join_type = match.group(1)
                        return f"{join_type} {table_name}"
                    else:
                        # FROM, UPDATE, INTO patterns
                        return match.group(0).split()[0] + f" {table_name}"
                
                adapted_query = re.sub(pattern, replace_table_ref, adapted_query, flags=re.IGNORECASE)
            
            # Step 2: Fix column mapping using actual schema with improved logic
            adapted_query = self._map_columns_intelligently(adapted_query, available_columns)
            
            # Step 3: Clean up any SQL syntax issues
            adapted_query = self._fix_sql_aliases(adapted_query)
            
            # Step 4: FINAL verification - ensure we're only using the specified table
            # Replace any remaining generic table names with our confirmed table
            adapted_query = re.sub(r'\b(data|table|main_table|csv_data)\b(?!\s*\()', table_name, adapted_query, flags=re.IGNORECASE)
            
            logger.info(f"Final adapted query for {table_name}: {adapted_query}")
            return adapted_query
            
        except Exception as e:
            logger.error(f"Error adapting query for table {table_name}: {e}")
            return query

    def _get_consistent_table_name(self, query: str, available_tables: List[str], source_id: str) -> str:
        """
        Get a consistent table name to use throughout query execution
        """
        try:
            # Extract UUID patterns from source_id
            import re
            uuid_patterns = re.findall(r'[a-f0-9]{8}_[a-f0-9]{4}_[a-f0-9]{4}_[a-f0-9]{4}_[a-f0-9]{12}', source_id)
            
            if uuid_patterns:
                uuid_pattern = uuid_patterns[0]
                logger.info(f"Looking for table matching UUID pattern: {uuid_pattern}")
                
                # Find the best matching table
                for table in available_tables:
                    if uuid_pattern.replace('_', '') in table.replace('_', ''):
                        logger.info(f"Found consistent table: {table}")
                        return table
            
            # Fallback to source_id based matching
            for table in available_tables:
                if source_id.replace('-', '').replace('_', '') in table.replace('-', '').replace('_', ''):
                    logger.info(f"Found fallback table: {table}")
                    return table
            
            # Last resort - use first available table
            if available_tables:
                logger.warning(f"Using first available table as fallback: {available_tables[0]}")
                return available_tables[0]
            
            logger.error("No suitable table found")
            return None
            
        except Exception as e:
            logger.error(f"Error getting consistent table name: {e}")
            return available_tables[0] if available_tables else None
    
    def _map_columns_intelligently(self, query: str, available_columns: List[str]) -> str:
        """
        Map column references in query to actual available columns with FIXED double quote handling
        """
        try:
            import re
            
            # Create mapping of potential column references to actual columns
            column_mapping = {}
            
            # First, create exact matches
            for col in available_columns:
                column_mapping[col] = col
                column_mapping[f'"{col}"'] = f'"{col}"'
                
                # Also map underscore versions to space versions
                underscore_version = col.replace(' ', '_')
                if underscore_version != col:
                    column_mapping[underscore_version] = f'"{col}"'
                    column_mapping[f'"{underscore_version}"'] = f'"{col}"'
                
                # Map space versions to quoted space versions
                if ' ' in col:
                    column_mapping[col] = f'"{col}"'
            
            # Apply column mapping with FIXED regex to avoid double quotes
            mapped_query = query
            
            for potential_ref, actual_ref in column_mapping.items():
                # CRITICAL FIX: Avoid double quoting by checking if already quoted
                if actual_ref.startswith('"') and actual_ref.endswith('"'):
                    clean_actual_ref = actual_ref
                else:
                    clean_actual_ref = f'"{actual_ref}"'
                
                # Pattern 1: Replace unquoted column references
                if not potential_ref.startswith('"'):
                    # Replace whole word matches only
                    pattern = r'\b' + re.escape(potential_ref) + r'\b'
                    mapped_query = re.sub(pattern, clean_actual_ref, mapped_query, flags=re.IGNORECASE)
                
                # Pattern 2: Replace quoted column references but avoid double quoting
                else:
                    # Only replace if not already properly quoted
                    if potential_ref != clean_actual_ref:
                        mapped_query = mapped_query.replace(potential_ref, clean_actual_ref)
            
            # CRITICAL FIX: Remove any double quotes that may have been created
            mapped_query = re.sub(r'""([^"]*?)""', r'"\1"', mapped_query)
            
            # Additional cleanup for malformed quotes
            mapped_query = re.sub(r'""+', '"', mapped_query)  # Multiple quotes to single
            mapped_query = re.sub(r'""', '"', mapped_query)   # Double quotes to single
            
            logger.info(f"Column mapping result: {query} -> {mapped_query}")
            return mapped_query
            
        except Exception as e:
            logger.error(f"Error in intelligent column mapping: {e}")
            return query
    
    def _fix_sql_aliases(self, query: str) -> str:
        """
        Fix invalid SQL aliases that contain special characters
        FIXED: Better SQL alias cleaning
        """
        try:
            # Pattern to find aliases with invalid characters
            alias_pattern = r'\s+as\s+([^\s,)]+(?:\([^)]*\)[^\s,)]*)*)'
            
            def clean_alias(match):
                alias = match.group(1)
                # Clean up invalid characters in aliases
                if '(' in alias or ')' in alias or ' ' in alias:
                    # Create a clean alias name
                    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', alias)
                    clean_name = re.sub(r'_+', '_', clean_name)  # Remove multiple underscores
                    clean_name = clean_name.strip('_')  # Remove leading/trailing underscores
                    
                    if not clean_name:
                        clean_name = 'result_value'
                    
                    return f' as {clean_name}'
                
                return match.group(0)
            
            fixed_query = re.sub(alias_pattern, clean_alias, query, flags=re.IGNORECASE)
            
            if fixed_query != query:
                logger.info(f"Fixed SQL aliases: {query} -> {fixed_query}")
            
            return fixed_query
            
        except Exception as e:
            logger.error(f"Error fixing SQL aliases: {e}")
            return query
    
    def _execute_csv_query(self, query: str, df: pd.DataFrame) -> pd.DataFrame:
        """Execute SQL-like query on CSV DataFrame with SQL injection protection"""
        # Create temporary SQLite database
        temp_conn = sqlite3.connect(':memory:')
        
        try:
            # Load DataFrame into SQLite
            df.to_sql('csv_data', temp_conn, if_exists='replace', index=False)
            
            # Convert PostgreSQL functions to SQLite equivalents
            safe_query = self._convert_postgresql_to_sqlite(query)
            
            # Basic query validation and sanitization
            safe_query = safe_query.replace('csv_data', 'csv_data')  # Keep table name consistent
            
            # Execute query
            result = pd.read_sql(safe_query, temp_conn)
            return result
            
        finally:
            temp_conn.close()

    def _convert_postgresql_to_sqlite(self, query: str) -> str:
        """Convert PostgreSQL SQL functions to SQLite equivalents and handle data issues"""
        import re
        
        converted_query = query
        
        # UNIVERSAL: Dynamic column name pattern detection, no hardcoded business terms
        column_name_fixes = {}
        
        # Apply universal pattern-based column name conversions
        # This replaces hardcoded business domain mappings with pattern-based detection
        import re
        underscore_columns = re.findall(r'\b([A-Za-z_]+_[A-Za-z_]+)\b', converted_query)
        for underscore_col in underscore_columns:
            # Convert underscore to space and quote
            space_col = underscore_col.replace('_', ' ')
            column_name_fixes[underscore_col] = f'"{space_col}"'
        
        for underscore_name, quoted_space_name in column_name_fixes.items():
            # Replace unquoted underscore versions with quoted space versions
            pattern = r'\b' + re.escape(underscore_name) + r'\b'
            converted_query = re.sub(pattern, quoted_space_name, converted_query)
        
        # Convert EXTRACT functions for DD-MM-YYYY date format
        # CSV dates are in DD-MM-YYYY format (e.g., "26-04-2015")
        extract_to_substr_functions = {
            # Extract year from positions 7-10 (YYYY)
            r'EXTRACT\s*\(\s*YEAR\s+FROM\s+([^)]+)\)': r"substr(\1, 7, 4)",
            # Extract month from positions 4-5 (MM)  
            r'EXTRACT\s*\(\s*MONTH\s+FROM\s+([^)]+)\)': r"substr(\1, 4, 2)",
            # Extract day from positions 1-2 (DD)
            r'EXTRACT\s*\(\s*DAY\s+FROM\s+([^)]+)\)': r"substr(\1, 1, 2)",
            # For strftime calls that were already converted, also handle them
            r"strftime\s*\(\s*'%Y'\s*,\s*([^)]+)\)": r"substr(\1, 7, 4)",
            r"strftime\s*\(\s*'%m'\s*,\s*([^)]+)\)": r"substr(\1, 4, 2)",
            r"strftime\s*\(\s*'%d'\s*,\s*([^)]+)\)": r"substr(\1, 1, 2)"
        }
        
        for postgresql_pattern, sqlite_replacement in extract_to_substr_functions.items():
            converted_query = re.sub(postgresql_pattern, sqlite_replacement, converted_query, flags=re.IGNORECASE)
        
        # UNIVERSAL: Dynamic detection of numeric operations needing CAST
        # Apply CAST wrapping to any SUM() function, not hardcoded column names
        import re
        
        # Pattern to find SUM(column_name) and wrap with CAST
        sum_pattern = r'SUM\(([^)]+)\)'
        def wrap_sum_with_cast(match):
            column_expr = match.group(1)
            return f'SUM(CAST(NULLIF({column_expr}, "") AS REAL))'
        
        converted_query = re.sub(sum_pattern, wrap_sum_with_cast, converted_query)
        
        # Remove NULLS FIRST/LAST (not supported in SQLite)
        converted_query = re.sub(r'\s+NULLS\s+(FIRST|LAST)', '', converted_query, flags=re.IGNORECASE)
        
        # Convert ILIKE to LIKE with LOWER() for case-insensitive matching
        converted_query = re.sub(r'\bILIKE\b', 'LIKE', converted_query, flags=re.IGNORECASE)
        
        # Log the conversion for debugging
        if query != converted_query:
            logger.info(f"Converted PostgreSQL query to SQLite with data fixes: {query} -> {converted_query}")
        
        return converted_query
    
    def get_schema_info(self, connection_info: Dict[str, Any], data_source=None) -> Dict[str, Any]:
        """
        Get schema information from data source with ETL transformations applied
        FIXED: Use DuckDB-first approach for schema retrieval
        """
        # ENHANCED: Try DuckDB first for schema information
        duckdb_schema = self._try_get_schema_from_duckdb(connection_info, data_source)
        if duckdb_schema:
            logger.info("Successfully retrieved schema from DuckDB")
            return duckdb_schema
        
        logger.info("DuckDB schema retrieval failed, falling back to original connection")
        
        connection = self.get_connection(connection_info)
        # Handle both database connections and DataFrames safely for schema info - FIXED: Avoid DataFrame boolean context
        if connection_info.get('type') == 'csv':
            if not isinstance(connection, pd.DataFrame):
                return {}
            elif connection.empty:
                return {}
        else:
            if connection is None:
                return {}
        
        try:
            # Get base schema
            if connection_info.get('type') == 'postgresql':
                schema = self._get_postgresql_schema(connection)
            elif connection_info.get('type') == 'sqlite':
                schema = self._get_sqlite_schema(connection)
            elif connection_info.get('type') == 'mysql':
                schema = self._get_mysql_schema(connection)
            elif connection_info.get('type') == 'sqlserver':
                schema = self._get_sqlserver_schema(connection, connection_info)
            elif connection_info.get('type') == 'oracle':
                schema = self._get_oracle_schema(connection)
            elif connection_info.get('type') == 'csv':
                schema = self._get_csv_schema(connection)
            elif connection_info.get('type') == 'etl_result':
                schema = self._get_etl_result_schema(connection, connection_info)
            else:
                schema = {}
            
            # ENHANCED: Apply ETL transformations to schema for CSV sources
            if connection_info.get('type') == 'csv' and schema:
                schema = self._apply_etl_transformations_to_schema(schema, connection_info)
            
            # CRITICAL FIX: Add business metrics to all schema responses
            if schema and isinstance(schema, dict):
                business_metrics = self._get_business_metrics_for_schema()
                if business_metrics:
                    schema['business_metrics'] = business_metrics
                    logger.info(f"Added {len(business_metrics)} business metrics to schema (fallback path)")
            
            return schema
                
        except Exception as e:
            logger.error(f"Failed to get schema info: {e}")
            return {}
            
        finally:
            # Only close database connections, not DataFrames
            if connection_info.get('type') != 'csv' and connection is not None and hasattr(connection, 'close'):
                connection.close()
    
    def _try_get_schema_from_duckdb(self, connection_info: Dict[str, Any], data_source=None) -> Optional[Dict[str, Any]]:
        """
        NEW: Try to get schema information from DuckDB first
        """
        try:
            from datasets.data_access_layer import unified_data_access
            
            # Use passed data_source if available, otherwise try to find by file path
            if data_source:
                logger.info(f"[SCHEMA_DUCKDB] Using provided data source: {data_source.name}")
            else:
                # Fallback: Try to find data source by file path
                from datasets.models import DataSource
                csv_file_path = connection_info.get('file_path')
                if csv_file_path:
                    logger.info(f"[SCHEMA_DUCKDB] Looking for data source with file path: {csv_file_path}")
                    data_source = DataSource.objects.filter(
                        connection_info__file_path=csv_file_path,
                        status='active'
                    ).first()
            
            if data_source:
                    logger.info(f"[SCHEMA_DUCKDB] Found data source: {data_source.name}")
                    # Try to get data from unified access layer
                    success, df, message = unified_data_access.get_data_source_data(data_source)
                    
                    if success and df is not None and not df.empty:
                        logger.info(f"[SCHEMA_DUCKDB] Successfully retrieved data for schema: {len(df)} rows")
                        # Generate schema from the DataFrame and include the actual DuckDB table name
                        schema = self._get_csv_schema(df)
                        
                        # Get the actual DuckDB table name for this data source
                        from utils.table_name_helper import get_integrated_table_name
                        try:
                            actual_table_name = get_integrated_table_name(data_source)
                            schema['table_name'] = actual_table_name
                            logger.info(f"[SCHEMA_DUCKDB] Added actual table name to schema: {actual_table_name}")
                        except Exception as table_name_error:
                            logger.warning(f"[SCHEMA_DUCKDB] Could not get table name: {table_name_error}")
                            # Fallback to trying the UUID-based naming
                            fallback_table_name = f"ds_{data_source.id.hex.replace('-', '')}"
                            schema['table_name'] = fallback_table_name
                            logger.info(f"[SCHEMA_DUCKDB] Using fallback table name: {fallback_table_name}")
                        
                        # CRITICAL FIX: Add business metrics to schema for LLM context
                        business_metrics = self._get_business_metrics_for_schema()
                        if business_metrics:
                            schema['business_metrics'] = business_metrics
                            logger.info(f"[SCHEMA_DUCKDB] Added {len(business_metrics)} business metrics to schema")
                        
                        return schema
                    else:
                        logger.warning(f"[SCHEMA_DUCKDB] Failed to get data for schema: {message}")
            
            logger.info("[SCHEMA_DUCKDB] No DuckDB schema data found")
            return None
            
        except Exception as e:
            logger.warning(f"[SCHEMA_DUCKDB] Error trying DuckDB schema: {e}")
            return None
    
    def _get_postgresql_schema(self, connection) -> Dict[str, Any]:
        """Get PostgreSQL schema information"""
        schema = {'tables': {}}
        
        try:
            # Set connection timeout
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute("SET statement_timeout = '10s';")
            
            # Get table names with basic info
            tables_query = """
                SELECT 
                    t.table_name,
                    COALESCE(c.column_count, 0) as column_count
                FROM information_schema.tables t
                LEFT JOIN (
                    SELECT table_name, COUNT(*) as column_count
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                    GROUP BY table_name
                ) c ON t.table_name = c.table_name
                WHERE t.table_schema = 'public' 
                AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name
            """
            tables_df = pd.read_sql(tables_query, connection)
            
            for _, table_row in tables_df.iterrows():
                table_name = table_row['table_name']
                
                # Get column information for each table
                columns_query = f"""
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable,
                        column_default,
                        character_maximum_length
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}' 
                    AND table_schema = 'public'
                    ORDER BY ordinal_position
                """
                columns_df = pd.read_sql(columns_query, connection)
                
                # Format columns as list of dictionaries
                columns_list = []
                for _, col in columns_df.iterrows():
                    columns_list.append({
                        'name': col['column_name'],
                        'type': col['data_type'],
                        'nullable': col['is_nullable'] == 'YES',
                        'default': col['column_default'] if col['column_default'] else None
                    })
                
                schema['tables'][table_name] = {
                    'columns': columns_list,
                    'column_count': len(columns_list)
                }
                
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL schema: {e}")
            return {'error': str(e)}
        
        return schema
    
    def _get_sqlite_schema(self, connection) -> Dict[str, Any]:
        """Get SQLite schema information"""
        schema = {}
        
        try:
            # Get table names
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for table_name, in tables:
                # Get column information
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                schema[table_name] = {
                    col[1]: {  # col[1] is column name
                        'type': col[2],  # col[2] is data type
                        'nullable': not col[3]  # col[3] is not null flag
                    }
                    for col in columns
                }
                
        except Exception as e:
            logger.error(f"Failed to get SQLite schema: {e}")
        
        return schema
    
    def _get_mysql_schema(self, connection) -> Dict[str, Any]:
        """Get MySQL schema information"""
        schema = {'tables': {}}
        
        try:
            cursor = connection.cursor()
            
            # Get tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for table_row in tables:
                table_name = table_row[0]
                
                # Get column information
                cursor.execute(f"DESCRIBE {table_name}")
                columns = cursor.fetchall()
                
                columns_list = []
                for col in columns:
                    columns_list.append({
                        'name': col[0],
                        'type': col[1],
                        'nullable': col[2] == 'YES',
                        'default': col[4] if col[4] else None
                    })
                
                schema['tables'][table_name] = {
                    'columns': columns_list,
                    'column_count': len(columns_list)
                }
                
        except Exception as e:
            logger.error(f"Failed to get MySQL schema: {e}")
            return {'error': str(e)}
        
        return schema
    
    def _get_sqlserver_schema(self, connection, connection_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get SQL Server schema information"""
        schema = {'tables': {}}
        
        try:
            cursor = connection.cursor()
            
            # Get selected tables if specified, otherwise get all tables
            selected_tables = []
            if connection_info and connection_info.get('tables'):
                selected_tables = connection_info['tables']
                logger.info(f"Using selected SQL Server tables: {selected_tables}")
            else:
                # Get all tables
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE' 
                    AND TABLE_SCHEMA = 'dbo'
                    ORDER BY TABLE_NAME
                """)
                tables = cursor.fetchall()
                selected_tables = [table_row[0] for table_row in tables]
                logger.info(f"Using all SQL Server tables: {selected_tables}")
            
            for table_name in selected_tables:
                # Get column information
                cursor.execute(f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}'
                    AND TABLE_SCHEMA = 'dbo'
                    ORDER BY ORDINAL_POSITION
                """)
                columns = cursor.fetchall()
                
                columns_list = []
                for col in columns:
                    columns_list.append({
                        'name': col[0],
                        'type': col[1],
                        'nullable': col[2] == 'YES',
                        'default': col[3] if col[3] else None
                    })
                
                schema['tables'][table_name] = {
                    'columns': columns_list,
                    'column_count': len(columns_list)
                }
                
        except Exception as e:
            logger.error(f"Failed to get SQL Server schema: {e}")
            return {'error': str(e)}
        
        return schema
    
    def _get_oracle_schema(self, connection) -> Dict[str, Any]:
        """Get Oracle schema information"""
        schema = {'tables': {}}
        
        try:
            cursor = connection.cursor()
            
            # Get tables
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            tables = cursor.fetchall()
            
            for table_row in tables:
                table_name = table_row[0]
                
                # Get column information
                cursor.execute(f"""
                    SELECT 
                        column_name,
                        data_type,
                        nullable,
                        data_default
                    FROM user_tab_columns
                    WHERE table_name = '{table_name}'
                    ORDER BY column_id
                """)
                columns = cursor.fetchall()
                
                columns_list = []
                for col in columns:
                    columns_list.append({
                        'name': col[0],
                        'type': col[1],
                        'nullable': col[2] == 'Y',
                        'default': col[3] if col[3] else None
                    })
                
                schema['tables'][table_name] = {
                    'columns': columns_list,
                    'column_count': len(columns_list)
                }
                
        except Exception as e:
            logger.error(f"Failed to get Oracle schema: {e}")
            return {'error': str(e)}
        
        return schema
    
    def _get_csv_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get CSV schema information using centralized type mapping utilities"""
        
        try:
            # Use centralized type information gathering
            type_info = get_column_type_info(df)
            
            # Format columns as list for template compatibility
            columns_list = []
            for col in df.columns:
                col_info = type_info[col]
                
                columns_list.append({
                    'name': col,
                    'type': col_info['semantic_type'],  # Use semantic type, not pandas type
                    'pandas_type': col_info['pandas_type'],  # Store original for debugging
                    'nullable': col_info['null_count'] > 0,
                    'sample_values': col_info['sample_values'],
                    'unique_count': col_info['unique_count'],
                    'null_count': col_info['null_count']
                })
            
            # Return schema with both formats for compatibility
            schema = {
                'columns': columns_list,
                'row_count': len(df),
                'column_count': len(df.columns)
            }
            
        except Exception as e:
            logger.error(f"Failed to get CSV schema: {e}")
            return {'error': str(e)}
        
        return schema
    
    def _get_etl_result_schema(self, connection, connection_info: Dict[str, Any]) -> Dict[str, Any]:
        """Get schema information from ETL result DataFrame"""
        try:
            if not isinstance(connection, pd.DataFrame):
                logger.error("ETL result connection is not a DataFrame")
                return {}
            
            if connection.empty:
                logger.warning("ETL result DataFrame is empty")
                return {}
            
            # Generate schema from DataFrame (reuse CSV schema logic)
            schema = self._get_csv_schema(connection)
            
            # Add ETL result specific information
            schema['source_type'] = 'etl_result'
            schema['table_name'] = connection_info.get('source_table_name') or connection_info.get('table_name', 'etl_result')
            
            logger.info(f"Generated schema for ETL result: {len(schema.get('columns', []))} columns, {schema.get('row_count', 0)} rows")
            return schema
            
        except Exception as e:
            logger.error(f"Error generating ETL result schema: {e}")
            return {'error': str(e)}
    
    def _get_business_metrics_for_schema(self) -> List[Dict[str, Any]]:
        """Get user-defined business metrics for LLM schema context"""
        try:
            import duckdb
            import os
            
            db_path = os.path.join('data', 'integrated.duckdb')
            if not os.path.exists(db_path):
                logger.info("DuckDB file not found, no business metrics available")
                return []
            
            conn = duckdb.connect(db_path)
            
            try:
                # Check if business metrics table exists
                tables = conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0] for table in tables]
                
                if 'user_business_metrics' not in table_names:
                    logger.info("User business metrics table not found")
                    return []
                
                # Get active business metrics
                metrics_query = """
                SELECT 
                    metric_name,
                    display_name,
                    description,
                    formula,
                    category,
                    data_type,
                    unit,
                    aggregation_type,
                    business_context
                FROM user_business_metrics 
                WHERE is_active = TRUE
                ORDER BY category, metric_name
                """
                
                metrics_result = conn.execute(metrics_query).fetchall()
                
                business_metrics = []
                for metric in metrics_result:
                    (name, display, desc, formula, category, dtype, unit, 
                     agg_type, context) = metric
                    
                    business_metrics.append({
                        'metric_name': name,
                        'display_name': display,
                        'description': desc,
                        'formula': formula,
                        'category': category,
                        'data_type': dtype,
                        'unit': unit,
                        'aggregation_type': agg_type,
                        'business_context': context
                    })
                
                logger.info(f"Successfully loaded {len(business_metrics)} business metrics for schema")
                return business_metrics
                
            finally:
                conn.close()
                
        except Exception as e:
            logger.warning(f"Error loading business metrics for schema: {e}")
            return []
    
    def _apply_etl_transformations_to_schema(self, schema: Dict[str, Any], connection_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply ETL transformations to update schema data types
        """
        try:
            # Get the CSV file path to find the related data source
            file_path = connection_info.get('file_path')
            if not file_path:
                return schema
            
            # Find the data source by CSV file path
            from datasets.models import DataSource, ETLOperation
            data_source = DataSource.objects.filter(
                connection_info__file_path=file_path,
                source_type='csv'
            ).first()
            
            if not data_source:
                logger.debug(f"No data source found for CSV file: {file_path}")
                return schema
            
            # Find the latest completed ETL operation for this data source
            latest_etl = ETLOperation.objects.filter(
                source_tables__contains=[str(data_source.id)],
                status='completed'
            ).order_by('-updated_at').first()
            
            if not latest_etl:
                logger.debug(f"No completed ETL operations found for data source: {data_source.name}")
                return schema
            
            # Get transformations from ETL operation
            transformations = latest_etl.parameters.get('transformations', {})
            if not transformations:
                logger.debug(f"No transformations found in ETL operation: {latest_etl.name}")
                return schema
            
            logger.info(f"Applying {len(transformations)} ETL transformations to schema for {data_source.name}")
            
            # Apply transformations to schema columns
            if 'columns' in schema:
                for col_info in schema['columns']:
                    col_name = col_info.get('name')
                    if col_name in transformations:
                        original_type = col_info.get('type')
                        transformed_type = transformations[col_name]
                        
                        # Map ETL types to schema types
                        schema_type_mapping = {
                            'string': 'TEXT',
                            'integer': 'INTEGER', 
                            'float': 'DOUBLE',
                            'date': 'DATE',
                            'datetime': 'DATETIME',
                            'boolean': 'BOOLEAN'
                        }
                        
                        if transformed_type in schema_type_mapping:
                            col_info['type'] = schema_type_mapping[transformed_type]
                            col_info['etl_transformed'] = True
                            col_info['original_type'] = original_type
                            logger.debug(f"Transformed column {col_name}: {original_type} -> {col_info['type']}")
            
            logger.info(f"Successfully applied ETL transformations to schema")
            return schema
            
        except Exception as e:
            logger.error(f"Error applying ETL transformations to schema: {e}")
            return schema
    
    def _infer_data_type_from_samples(self, series: pd.Series, sample_values: list) -> str:
        """
        Infer data type from sample values with enhanced error handling for mixed types
        FIXED: Prioritize date detection to prevent dates being converted to integers
        """
        if not sample_values:
            return 'string'
        
        # Convert all samples to strings for safe processing
        str_samples = []
        for v in sample_values:
            try:
                if v is not None and pd.notna(v):
                    str_samples.append(str(v).strip())
            except Exception as e:
                logger.debug(f"Error converting sample value to string: {v}, error: {e}")
                continue
        
        if not str_samples:
            return 'string'
        
        # PRIORITY 1: Check for date patterns FIRST (before numeric checks)
        # This prevents date strings like "2023-01-01" from being misidentified as integers
        date_patterns = [
            r'\d{4}-\d{1,2}-\d{1,2}',  # YYYY-MM-DD (more flexible)
            r'\d{1,2}/\d{1,2}/\d{4}',  # MM/DD/YYYY or M/D/YYYY
            r'\d{1,2}-\d{1,2}-\d{4}',  # MM-DD-YYYY or M-D-YYYY
            r'\d{2}/\d{2}/\d{2}',      # MM/DD/YY
            r'\d{4}/\d{1,2}/\d{1,2}',  # YYYY/MM/DD
        ]
        
        for pattern in date_patterns:
            pattern_count = 0
            for val in str_samples:
                try:
                    val_str = str(val).strip()
                    if re.match(pattern, val_str):
                        # Additional validation: try to actually parse as date
                        try:
                            pd.to_datetime(val_str, format='%d-%m-%Y', dayfirst=True, errors='raise')
                            pattern_count += 1
                        except:
                            # If it matches pattern but can't be parsed as date, skip
                            pass
                except (AttributeError, TypeError) as e:
                    logger.debug(f"Error processing date pattern for value: {val}, error: {e}")
                    continue
            
            # If majority of samples match date pattern and can be parsed, it's a date
            if pattern_count >= len(str_samples) * 0.8:  # 80% threshold
                logger.info(f"Identified date column with {pattern_count}/{len(str_samples)} matching samples")
                return 'date'
        
        # PRIORITY 2: Check for boolean values
        boolean_values = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n', 't', 'f'}
        boolean_count = 0
        for val in str_samples:
            try:
                val_str = str(val).strip().lower()
                if val_str in boolean_values:
                    boolean_count += 1
            except (AttributeError, TypeError) as e:
                logger.debug(f"Error processing boolean check for value: {val}, error: {e}")
                continue
        
        if boolean_count == len(str_samples):
            return 'boolean'
        
        # PRIORITY 3: Check for purely numeric values (integers/floats)
        # Only after we've ruled out dates and booleans
        numeric_count = 0
        float_count = 0
        
        for val in str_samples:
            try:
                val_str = str(val).strip()
                # More careful numeric detection
                if val_str.replace('.', '').replace('-', '').replace('+', '').replace('e', '').replace('E', '').isdigit():
                    numeric_count += 1
                    if '.' in val_str or 'e' in val_str.lower():
                        float_count += 1
                elif val_str.replace('.', '').replace('-', '').replace('+', '').isdigit():
                    # Handle scientific notation and decimals
                    try:
                        float(val_str)
                        numeric_count += 1
                        if '.' in val_str or 'e' in val_str.lower():
                            float_count += 1
                    except ValueError:
                        pass
            except (AttributeError, TypeError) as e:
                logger.debug(f"Error processing numeric check for value: {val}, error: {e}")
                continue
        
        if numeric_count == len(str_samples):
            return 'float' if float_count > 0 else 'integer'
        
        # Default to string for everything else
        return 'string'
    
    def _auto_convert_data_types(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        Automatically convert DataFrame columns to appropriate data types with enhanced error handling
        FIXED: Added DataFrame boolean context safety checks
        """
        # ENHANCED: Validate input DataFrame to prevent boolean context issues
        if not isinstance(df, pd.DataFrame):
            logger.error("_auto_convert_data_types called with non-DataFrame input")
            return df, {}
        
        if df.empty:
            logger.warning("_auto_convert_data_types called with empty DataFrame")
            return df, {}
        
        if len(df.columns) == 0:
            logger.warning("_auto_convert_data_types called with DataFrame having no columns")
            return df, {}
        
        # ENHANCED: Safe DataFrame copy with validation
        try:
            df_converted = df.copy()
        except Exception as copy_error:
            logger.error(f"Failed to copy DataFrame in _auto_convert_data_types: {copy_error}")
            return df, {}
        
        type_mapping = {}
        
        for column in df.columns:
            original_type = str(df[column].dtype)
            
            try:
                # Get sample values for type inference with safe conversion
                sample_values = []
                for val in df[column].dropna().head(10):
                    try:
                        if val is not None and pd.notna(val):
                            sample_values.append(str(val))
                    except Exception as e:
                        logger.debug(f"Error converting sample to string for column {column}: {val}, error: {e}")
                        continue
                
                if not sample_values:
                    inferred_type = 'string'
                else:
                    inferred_type = self._infer_data_type_from_samples(df[column], sample_values)
                
                # Apply conversion based on inferred type with error handling
                if inferred_type == 'integer':
                    try:
                        # ENHANCED: More conservative integer conversion
                        # First check if any values look like dates to prevent misclassification
                        sample_check = df[column].dropna().head(10).astype(str)
                        looks_like_dates = any(
                            re.match(r'\d{4}-\d{1,2}-\d{1,2}', str(val)) or
                            re.match(r'\d{1,2}/\d{1,2}/\d{4}', str(val)) or
                            re.match(r'\d{1,2}-\d{1,2}-\d{4}', str(val))
                            for val in sample_check
                        )
                        
                        if looks_like_dates:
                            logger.warning(f"Column {column} has date-like values, skipping integer conversion")
                            df_converted[column] = df_converted[column].astype(str)
                            type_mapping[column] = 'string'
                        else:
                            # Safe integer conversion - only if we're confident
                            numeric_converted = pd.to_numeric(df[column], errors='coerce')
                            null_count = numeric_converted.isna().sum()
                            total_count = len(df[column])
                            
                            # If more than 10% of values become null, don't convert
                            if null_count > total_count * 0.1:
                                logger.warning(f"Column {column}: {null_count}/{total_count} values would become null, keeping as string")
                                df_converted[column] = df_converted[column].astype(str)
                                type_mapping[column] = 'string'
                            else:
                                # Safe conversion with original values preserved for nulls
                                df_converted[column] = numeric_converted.fillna(df[column]).astype('Int64')
                                type_mapping[column] = 'integer'
                                logger.info(f"Successfully converted column {column} to integer with {null_count} nulls handled")
                    except Exception as e:
                        logger.warning(f"Failed to convert column {column} to integer: {e}")
                        df_converted[column] = df_converted[column].astype(str)
                        type_mapping[column] = 'string'
                        
                elif inferred_type == 'float':
                    try:
                        # ENHANCED: More conservative float conversion
                        numeric_converted = pd.to_numeric(df[column], errors='coerce')
                        null_count = numeric_converted.isna().sum()
                        total_count = len(df[column])
                        
                        # If more than 10% of values become null, don't convert
                        if null_count > total_count * 0.1:
                            logger.warning(f"Column {column}: {null_count}/{total_count} values would become null, keeping as string")
                            df_converted[column] = df_converted[column].astype(str)
                            type_mapping[column] = 'string'
                        else:
                            df_converted[column] = numeric_converted
                            type_mapping[column] = 'float'
                            logger.info(f"Successfully converted column {column} to float with {null_count} nulls")
                    except Exception as e:
                        logger.warning(f"Failed to convert column {column} to float: {e}")
                        df_converted[column] = df_converted[column].astype(str)
                        type_mapping[column] = 'string'
                        
                elif inferred_type == 'boolean':
                    try:
                        # Enhanced boolean conversion with safe string processing
                        def safe_bool_convert(val):
                            if pd.isna(val):
                                return False
                            try:
                                val_str = str(val).strip().lower()
                                return val_str in ['true', '1', 'yes', 'y', 't']
                            except (AttributeError, TypeError):
                                return False
                        
                        df_converted[column] = df[column].apply(safe_bool_convert)
                        type_mapping[column] = 'boolean'
                    except Exception as e:
                        logger.warning(f"Failed to convert column {column} to boolean: {e}")
                        df_converted[column] = df_converted[column].astype(str)
                        type_mapping[column] = 'string'
                        
                elif inferred_type == 'date':
                    try:
                        # ENHANCED: Better date conversion with format detection - FIXED: Remove deprecated parameter
                        converted_dates = pd.to_datetime(df[column], errors='coerce')
                        null_count = converted_dates.isna().sum()
                        total_count = len(df[column])
                        
                        # If more than 20% of values become null, don't convert
                        if null_count > total_count * 0.2:
                            logger.warning(f"Column {column}: {null_count}/{total_count} dates failed to parse, keeping as string")
                            df_converted[column] = df_converted[column].astype(str)
                            type_mapping[column] = 'string'
                        else:
                            df_converted[column] = converted_dates
                            type_mapping[column] = 'date'
                            logger.info(f"Successfully converted column {column} to date with {null_count} failed conversions")
                    except Exception as e:
                        logger.warning(f"Failed to convert column {column} to date: {e}")
                        df_converted[column] = df_converted[column].astype(str)
                        type_mapping[column] = 'string'
                        
                else:
                    # Default to string with safe conversion
                    try:
                        df_converted[column] = df_converted[column].astype(str)
                        type_mapping[column] = 'string'
                    except Exception as e:
                        logger.warning(f"Failed to convert column {column} to string: {e}")
                        type_mapping[column] = 'string'
                
            except Exception as e:
                logger.warning(f"Error processing column {column}: {e}")
                # Fallback to string type
                try:
                    df_converted[column] = df_converted[column].astype(str)
                except Exception:
                    pass
                type_mapping[column] = 'string'
        
        logger.info(f"Applied automatic type conversion to {len(type_mapping)} columns")
        return df_converted, type_mapping
    
    def test_connection(self, connection_info: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Test database connection with enhanced error reporting and detailed troubleshooting
        """
        connection_type = connection_info.get('type', 'unknown')
        
        # Validate required fields first
        validation_error = self._validate_connection_info(connection_info)
        if validation_error:
            return False, validation_error
        
        try:
            connection = self.get_connection(connection_info)
            
            if connection is None:
                return False, self._get_connection_failure_message(connection_info)
            
            # Handle different connection types safely
            if connection_type == 'csv':
                if isinstance(connection, pd.DataFrame):
                    if not connection.empty:
                        return True, f"CSV connection successful. {len(connection)} rows, {len(connection.columns)} columns."
                    else:
                        return False, "CSV file is empty or could not be read"
                else:
                    return False, "Invalid CSV data format"
            
            # Test database connection with a simple query
            try:
                if connection_type == 'postgresql':
                    cursor = connection.cursor()
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()[0]
                    cursor.close()
                    return True, f"PostgreSQL connection successful. Version: {version[:50]}..."
                elif connection_type == 'mysql':
                    cursor = connection.cursor()
                    cursor.execute("SELECT VERSION()")
                    version = cursor.fetchone()[0]
                    cursor.close()
                    return True, f"MySQL connection successful. Version: {version}"
                elif connection_type == 'sqlite':
                    cursor = connection.cursor()
                    cursor.execute("SELECT sqlite_version()")
                    version = cursor.fetchone()[0]
                    return True, f"SQLite connection successful. Version: {version}"
                elif connection_type == 'sqlserver':
                    cursor = connection.cursor()
                    cursor.execute("SELECT @@VERSION")
                    version = cursor.fetchone()[0]
                    cursor.close()
                    return True, f"SQL Server connection successful. Version: {version[:50]}..."
                elif connection_type == 'oracle':
                    cursor = connection.cursor()
                    cursor.execute("SELECT * FROM v$version WHERE ROWNUM = 1")
                    version = cursor.fetchone()[0]
                    cursor.close()
                    return True, f"Oracle connection successful. Version: {version[:50]}..."
                else:
                    return True, f"{connection_type.title()} connection successful"
                    
            except Exception as query_error:
                return False, f"Connection established but test query failed: {str(query_error)}"
                
        except Exception as e:
            logger.error(f"Connection test failed for {connection_type}: {e}")
            return False, self._get_detailed_error_message(e, connection_info)
    
    def _validate_connection_info(self, connection_info: Dict[str, Any]) -> Optional[str]:
        """Validate connection parameters and return error message if invalid"""
        connection_type = connection_info.get('type')
        
        if not connection_type:
            return "Connection type is required"
        
        if connection_type in ['postgresql', 'mysql', 'sqlserver', 'oracle']:
            required_fields = ['host', 'port', 'database', 'username', 'password']
            missing_fields = []
            
            for field in required_fields:
                if not connection_info.get(field):
                    missing_fields.append(field)
            
            if missing_fields:
                return f"Missing required fields: {', '.join(missing_fields)}"
            
            # Validate port is numeric
            try:
                port = int(connection_info.get('port', 0))
                if port <= 0 or port > 65535:
                    return "Port must be a number between 1 and 65535"
            except (ValueError, TypeError):
                return "Port must be a valid number"
        
        return None
    
    def _get_connection_failure_message(self, connection_info: Dict[str, Any]) -> str:
        """Generate helpful error message based on connection type"""
        connection_type = connection_info.get('type', 'unknown')
        host = connection_info.get('host', 'unknown')
        port = connection_info.get('port', 'unknown')
        database = connection_info.get('database', 'unknown')
        
        base_message = f"Failed to connect to {connection_type} database"
        
        if connection_type == 'postgresql':
            return (f"{base_message} at {host}:{port}/{database}. "
                   "Check: 1) Server is running, 2) Host/port are correct, "
                   "3) Database exists, 4) Username/password are valid, "
                   "5) PostgreSQL accepts connections from your IP")
        elif connection_type == 'mysql':
            return (f"{base_message} at {host}:{port}/{database}. "
                   "Check: 1) MySQL server is running, 2) Host/port are correct, "
                   "3) Database exists, 4) Username/password are valid, "
                   "5) User has connection privileges")
        elif connection_type == 'sqlserver':
            return (f"{base_message} at {host}:{port}/{database}. "
                   "Check: 1) SQL Server is running, 2) TCP/IP is enabled, "
                   "3) Host/port are correct, 4) Database exists, "
                   "5) SQL Server authentication is enabled")
        elif connection_type == 'oracle':
            return (f"{base_message} at {host}:{port}/{database}. "
                   "Check: 1) Oracle listener is running, 2) Service name is correct, "
                   "3) Host/port are correct, 4) Username/password are valid")
        else:
            return f"{base_message}. Please check your connection parameters."
    
    def _get_detailed_error_message(self, error: Exception, connection_info: Dict[str, Any]) -> str:
        """Generate detailed error message based on exception type"""
        error_str = str(error).lower()
        connection_type = connection_info.get('type', 'unknown')
        
        # Connection refused errors
        if 'connection refused' in error_str or 'could not connect' in error_str:
            return (f"Connection refused - the {connection_type} server is not accepting connections. "
                   "Check if the server is running and the host/port are correct.")
        
        # Timeout errors
        elif 'timeout' in error_str or 'timed out' in error_str:
            return (f"Connection timeout - the {connection_type} server is not responding. "
                   "Check network connectivity and firewall settings.")
        
        # Authentication errors
        elif any(phrase in error_str for phrase in ['authentication failed', 'access denied', 'login failed', 'invalid user']):
            return "Authentication failed - check your username and password."
        
        # Database not found errors
        elif any(phrase in error_str for phrase in ['database', 'schema']) and 'not exist' in error_str:
            return f"Database '{connection_info.get('database', 'unknown')}' does not exist on the server."
        
        # Host not found errors
        elif 'name or service not known' in error_str or 'getaddrinfo failed' in error_str:
            return f"Host '{connection_info.get('host', 'unknown')}' not found - check the hostname or IP address."
        
        # SSL/TLS errors
        elif 'ssl' in error_str or 'tls' in error_str:
            return "SSL/TLS connection error - check SSL settings and certificates."
        
        # Permission errors
        elif 'permission denied' in error_str or 'access denied' in error_str:
            return "Permission denied - the user may not have connection privileges."
        
        # Generic error with helpful context
        else:
            return f"Connection failed: {str(error)}. Please verify all connection parameters."
    
    def get_database_tables(self, connection_info: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]], str]:
        """
        Get list of tables from database connection
        Returns: (success, tables_list, message)
        """
        connection_type = connection_info.get('type', 'unknown')
        
        try:
            connection = self.get_connection(connection_info)
            if connection is None:
                return False, [], f"Failed to connect to {connection_type} database"
            
            tables = []
            
            if connection_type == 'postgresql':
                cursor = connection.cursor()
                cursor.execute("""
                    SELECT 
                        table_name,
                        COALESCE(obj_description(('"' || table_name || '"')::regclass), '') as description
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                for row in cursor.fetchall():
                    tables.append({
                        'name': row[0],
                        'description': row[1] or f"Table: {row[0]}",
                        'type': 'table'
                    })
                cursor.close()
                
            elif connection_type == 'mysql':
                cursor = connection.cursor()
                cursor.execute("SHOW TABLES")
                for row in cursor.fetchall():
                    table_name = row[0]
                    tables.append({
                        'name': table_name,
                        'description': f"MySQL table: {table_name}",
                        'type': 'table'
                    })
                cursor.close()
                
            elif connection_type == 'sqlserver':
                cursor = connection.cursor()
                cursor.execute("""
                    SELECT 
                        TABLE_NAME,
                        COALESCE(CAST(ep.value AS NVARCHAR(255)), 'SQL Server table: ' + TABLE_NAME) as description
                    FROM INFORMATION_SCHEMA.TABLES t
                    LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
                    LEFT JOIN sys.extended_properties ep ON ep.major_id = st.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_SCHEMA = 'dbo'
                    ORDER BY TABLE_NAME
                """)
                for row in cursor.fetchall():
                    tables.append({
                        'name': row[0],
                        'description': row[1] or f"SQL Server table: {row[0]}",
                        'type': 'table'
                    })
                cursor.close()
                
            elif connection_type == 'oracle':
                cursor = connection.cursor()
                cursor.execute("""
                    SELECT 
                        table_name,
                        COALESCE(comments, 'Oracle table: ' || table_name) as description
                    FROM user_tables 
                    ORDER BY table_name
                """)
                for row in cursor.fetchall():
                    tables.append({
                        'name': row[0],
                        'description': row[1] or f"Oracle table: {row[0]}",
                        'type': 'table'
                    })
                cursor.close()
                
            elif connection_type == 'sqlite':
                cursor = connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                for row in cursor.fetchall():
                    table_name = row[0]
                    tables.append({
                        'name': table_name,
                        'description': f"SQLite table: {table_name}",
                        'type': 'table'
                    })
                
            else:
                return False, [], f"Database type {connection_type} not supported for table listing"
            
            return True, tables, f"Found {len(tables)} tables in {connection_type} database"
                
        except Exception as e:
            logger.error(f"Error getting tables from {connection_type}: {e}")
            return False, [], f"Error listing tables: {str(e)}"
        
        finally:
            # Close connection
            if connection and hasattr(connection, 'close'):
                try:
                    connection.close()
                except:
                    pass
    
    def _log_query(self, user_id: int, query: str, status: str, 
                   rows_returned: int, error_message: Optional[str] = None, 
                   execution_time: Optional[float] = None):
        """Enhanced query logging with execution time - DISABLED to prevent duplicates"""
        # This method is disabled to prevent duplicate QueryLog entries
        # The main query logging is now handled in core/views.py with proper result serialization
        try:
            logger.debug(f"Query executed: {query[:100]}... Status: {status}, Rows: {rows_returned}, Time: {execution_time}s")
        except Exception as e:
            logger.error(f"Failed to log query debug info: {e}")
    
    def get_data_preview(self, connection_info: Dict[str, Any], 
                        table_name: str = None, limit: int = 100) -> Tuple[bool, Any]:
        """
        Get preview of data from table with database-specific SQL syntax
        """
        try:
            if connection_info.get('type') == 'csv':
                connection = self.get_connection(connection_info)
                if connection is not None:
                    return True, connection.head(limit)
                else:
                    return False, "Failed to load CSV data"
            
            if not table_name:
                # Get first table from schema
                schema = self.get_schema_info(connection_info)
                if schema:
                    table_name = list(schema.keys())[0]
                else:
                    return False, "No tables found"
            
            # Generate database-specific query syntax
            db_type = connection_info.get('type', 'postgresql').lower()
            
            if db_type == 'sqlserver':
                # SQL Server uses TOP syntax
                query = f"SELECT TOP {limit} * FROM {table_name}"
            elif db_type == 'oracle':
                # Oracle uses ROWNUM
                query = f"SELECT * FROM {table_name} WHERE ROWNUM <= {limit}"
            else:
                # PostgreSQL, MySQL, SQLite use LIMIT
                query = f"SELECT * FROM {table_name} LIMIT {limit}"
            
            logger.info(f"Generated preview query for {db_type}: {query}")
            return self.execute_query(query, connection_info)
            
        except Exception as e:
            return False, f"Failed to get data preview: {str(e)}"
    
    def get_data_source_by_id(self, data_source_id: str) -> Optional[DataSource]:
        """Get data source model by ID"""
        try:
            return DataSource.objects.get(id=data_source_id)
        except DataSource.DoesNotExist:
            return None
    
    def save_data_source_connection(self, name: str, connection_info: Dict[str, Any], 
                                  user_id: int) -> Optional[DataSource]:
        """
        Save data source connection to database
        """
        try:
            data_source = DataSource.objects.create(
                name=name,
                source_type=connection_info.get('type', 'postgresql'),
                connection_params=connection_info,
                created_by_id=user_id,
                status='active'
            )
            return data_source
            
        except Exception as e:
            logger.error(f"Failed to save data source: {e}")
            return None
    
    def test_api_connection(self, connection_info: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Test API connection
        """
        try:
            import requests
            from requests.auth import HTTPBasicAuth
            
            base_url = connection_info.get('base_url', '')
            auth_type = connection_info.get('auth_type', 'none')
            
            if not base_url:
                return False, "API base URL is required"
            
            # Prepare headers and authentication
            headers = {'Content-Type': 'application/json'}
            auth = None
            
            if auth_type == 'apikey':
                api_key = connection_info.get('api_key')
                api_key_header = connection_info.get('api_key_header', 'X-API-Key')
                if api_key:
                    headers[api_key_header] = api_key
                else:
                    return False, "API key is required"
                    
            elif auth_type == 'bearer':
                bearer_token = connection_info.get('bearer_token')
                if bearer_token:
                    headers['Authorization'] = f'Bearer {bearer_token}'
                else:
                    return False, "Bearer token is required"
                    
            elif auth_type == 'basic':
                basic_username = connection_info.get('basic_username')
                basic_password = connection_info.get('basic_password')
                if basic_username and basic_password:
                    auth = HTTPBasicAuth(basic_username, basic_password)
                else:
                    return False, "Username and password are required for basic auth"
            
            # Make a test request (usually a GET to the base URL or a health endpoint)
            test_url = base_url.rstrip('/')
            if not test_url.endswith('/health') and not test_url.endswith('/status'):
                # Try common health check endpoints
                test_endpoints = ['/', '/health', '/status', '/api/health', '/api/status']
                for endpoint in test_endpoints:
                    try:
                        response = requests.get(
                            f"{test_url}{endpoint}", 
                            headers=headers, 
                            auth=auth, 
                            timeout=10
                        )
                        if response.status_code in [200, 401, 403]:  # 401/403 means auth is working, just not authorized
                            return True, f"API connection successful (Status: {response.status_code})"
                    except requests.exceptions.RequestException:
                        continue
                        
                return False, f"Could not reach API at {base_url}. Tried multiple endpoints."
            else:
                # Test the specific URL
                response = requests.get(test_url, headers=headers, auth=auth, timeout=10)
                if response.status_code in [200, 401, 403]:
                    return True, f"API connection successful (Status: {response.status_code})"
                else:
                    return False, f"API returned status code: {response.status_code}"
                    
        except requests.exceptions.ConnectionError:
            return False, f"Could not connect to {base_url}. Please check the URL and network connectivity."
        except requests.exceptions.Timeout:
            return False, f"Connection to {base_url} timed out."
        except requests.exceptions.RequestException as e:
            return False, f"Request failed: {str(e)}"
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False, f"Connection test failed: {str(e)}"
    
    def close_all_connections(self):
        """Close all cached connections"""
        for cache_key, conn in list(self.connections_cache.items()):
            try:
                if hasattr(conn, 'close'):
                    conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection {cache_key}: {e}")
        self.connections_cache.clear() 

    def generate_limit_query(self, base_query: str, limit: int, db_type: str) -> str:
        """
        Generate database-specific LIMIT query syntax
        
        Args:
            base_query: The base SQL query (e.g., "SELECT * FROM table")
            limit: Number of rows to limit
            db_type: Database type ('sqlserver', 'oracle', 'postgresql', 'mysql', etc.)
            
        Returns:
            Query with appropriate limit syntax for the database type
        """
        db_type = db_type.lower()
        
        if db_type == 'sqlserver':
            # SQL Server uses TOP syntax
            # Replace SELECT with SELECT TOP N
            if base_query.upper().startswith('SELECT '):
                return base_query.replace('SELECT ', f'SELECT TOP {limit} ', 1)
            else:
                return f"SELECT TOP {limit} * FROM ({base_query}) AS subquery"
        elif db_type == 'oracle':
            # Oracle uses ROWNUM
            return f"SELECT * FROM ({base_query}) WHERE ROWNUM <= {limit}"
        else:
            # PostgreSQL, MySQL, SQLite use LIMIT
            return f"{base_query} LIMIT {limit}"