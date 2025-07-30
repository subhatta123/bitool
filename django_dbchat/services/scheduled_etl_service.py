"""
Scheduled ETL Service for automatic data refresh operations.
FIXED: Added proper transaction management and resource cleanup to prevent deadlocks.
"""
import logging
import time
import traceback
import pytz
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from django.utils import timezone
from django.db import transaction, connection
from django.conf import settings
from celery import shared_task
from celery.exceptions import Retry

from datasets.models import DataSource, ScheduledETLJob, ETLJobRunLog
from datasets.data_access_layer import unified_data_access
from services.data_service import DataService
from services.integration_service import DataIntegrationService
from services.universal_data_loader import universal_data_loader

logger = logging.getLogger(__name__)


class ScheduledETLService:
    """Service for managing scheduled ETL operations with proper resource management."""
    
    def __init__(self):
        self.data_service = DataService()
        self.integration_service = DataIntegrationService()
        self._duckdb_conn = None
        
    def __enter__(self):
        """Context manager entry - ensure clean resources."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        self._cleanup_resources()
        
    def _cleanup_resources(self):
        """Clean up any open connections and resources."""
        try:
            if self._duckdb_conn:
                self._duckdb_conn.close()
                self._duckdb_conn = None
            
            # Close Django database connections to prevent locks
            connection.close()
            
            # FIXED: Also close unified data access connections to prevent DuckDB locks
            try:
                if hasattr(unified_data_access, 'duckdb_connection') and unified_data_access.duckdb_connection:
                    unified_data_access.duckdb_connection.close()
                    unified_data_access.duckdb_connection = None
            except Exception as duck_error:
                logger.warning(f"Error closing DuckDB connection: {duck_error}")
            
        except Exception as e:
            logger.warning(f"Error during resource cleanup: {e}")
    
    def _resolve_csv_path_comprehensively(self, data_source) -> Optional[str]:
        """
        Comprehensively resolve CSV file path by checking multiple locations.
        Returns the actual file path if found, None otherwise.
        """
        connection_info = data_source.connection_info or {}
        file_path = connection_info.get('file_path', '')
        original_filename = connection_info.get('original_filename', '')
        
        if not file_path and not original_filename:
            return None
        
        from django.conf import settings
        import os
        
        # Generate potential paths to check
        potential_paths = []
        
        if file_path:
            # Try the stored path in various base directories
            potential_paths.extend([
                file_path,  # Absolute path
                os.path.join(settings.BASE_DIR, file_path),
                os.path.join(settings.MEDIA_ROOT, file_path) if hasattr(settings, 'MEDIA_ROOT') else None,
                os.path.join(settings.BASE_DIR, 'media', file_path),
                os.path.join(settings.BASE_DIR, 'django_dbchat', file_path),
                os.path.join(settings.BASE_DIR, 'data', file_path),
                os.path.join(settings.BASE_DIR, 'csv_files', file_path),
            ])
        
        if original_filename:
            # Search for the original filename in common directories
            search_dirs = [
                settings.BASE_DIR,
                os.path.join(settings.BASE_DIR, 'media'),
                os.path.join(settings.BASE_DIR, 'django_dbchat'),
                os.path.join(settings.BASE_DIR, 'data'),
                os.path.join(settings.BASE_DIR, 'csv_files'),
                getattr(settings, 'MEDIA_ROOT', ''),
            ]
            
            for search_dir in search_dirs:
                if search_dir and os.path.exists(search_dir):
                    for root, dirs, files in os.walk(search_dir):
                        if original_filename in files:
                            potential_paths.append(os.path.join(root, original_filename))
        
        # Filter out None values and check each path
        potential_paths = [p for p in potential_paths if p]
        
        logger.info(f"Resolving CSV path for {data_source.name}")
        logger.info(f"  Original file path: {file_path}")
        logger.info(f"  Original filename: {original_filename}")
        logger.info(f"  Checking {len(potential_paths)} potential paths...")
        
        for i, path in enumerate(potential_paths, 1):
            if os.path.exists(path):
                logger.info(f"  ✅ Found at path {i}: {path}")
                return path
            else:
                logger.debug(f"  ❌ Not found at path {i}: {path}")
        
        logger.warning(f"CSV file not found at any location for {data_source.name}")
        return None
        
    def execute_scheduled_job(self, job_id: str, triggered_by: str = 'schedule') -> Tuple[bool, str, Dict[str, Any]]:
        """
        Execute a scheduled ETL job with proper transaction management.
        FIXED: Added transaction management and resource cleanup.
        
        Args:
            job_id: ID of the scheduled job to execute
            triggered_by: What triggered this execution
            
        Returns:
            Tuple of (success, message, results)
        """
        run_log = None
        job = None
        
        try:
            # Use atomic transaction for initial setup
            with transaction.atomic():
                # Get the scheduled job
                try:
                    job = ScheduledETLJob.objects.select_for_update().get(id=job_id)
                except ScheduledETLJob.DoesNotExist:
                    return False, f"Scheduled job {job_id} not found", {}
                
                # Check if job can run (with bypass for manual triggers)
                if triggered_by not in ['manual', 'manual_api', 'test_web_path', 'manual_admin'] and not job.can_run_now():
                    return False, f"Job {job.name} is not scheduled to run now", {}
                
                # Log manual trigger bypass
                if triggered_by in ['manual', 'manual_api', 'test_web_path', 'manual_admin']:
                    logger.info(f"Manual trigger bypass: executing {job.name} immediately")
                
                # Create run log
                run_log = ETLJobRunLog.objects.create(
                    scheduled_job=job,
                    status='started',
                    triggered_by=triggered_by,
                    started_at=timezone.now()
                )
                
                logger.info(f"Starting scheduled ETL job: {job.name} (ID: {job_id})")
            
            # Execute the job logic outside the atomic transaction to prevent long-running locks
            success, results = self._execute_job_logic_with_resource_management(job, run_log)
            
            # Use separate atomic transaction for final updates
            with transaction.atomic():
                # Refresh objects from database to avoid stale data
                job.refresh_from_db()
                run_log.refresh_from_db()
                
                if success:
                    job.mark_success()
                    run_log.mark_completed('success')
                    message = f"Job {job.name} completed successfully"
                    logger.info(message)
                    
                    # Send success notification if configured
                    if job.notify_on_success:
                        self._send_notification_async(job, run_log, 'success')
                        
                else:
                    error_message = results.get('error', 'Unknown error')
                    job.mark_failure(error_message)
                    run_log.mark_completed('failed', error_message)
                    message = f"Job {job.name} failed: {error_message}"
                    logger.error(message)
                    
                    # Send failure notification if configured
                    if job.notify_on_failure:
                        self._send_notification_async(job, run_log, 'failed')
            
            return success, message, results
            
        except Exception as e:
            error_msg = f"Error executing scheduled job {job_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # Try to update run log and job status in separate transaction
            try:
                with transaction.atomic():
                    if run_log:
                        run_log.refresh_from_db()
                        run_log.mark_completed('failed', error_msg)
                    if job:
                        job.refresh_from_db()
                        job.mark_failure(error_msg)
            except Exception as update_error:
                logger.error(f"Failed to update job status after error: {update_error}")
                
            return False, error_msg, {'error': error_msg}
        finally:
            # Always cleanup resources
            self._cleanup_resources()
    
    def _execute_job_logic_with_resource_management(self, job: ScheduledETLJob, run_log: ETLJobRunLog) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute the core ETL logic with proper resource management.
        FIXED: Added connection pooling and resource cleanup.
        """
        
        results = {
            'data_sources_processed': [],
            'data_sources_failed': [],
            'data_sources_skipped': [],
            'total_records_processed': 0,
            'total_records_added': 0,
            'total_records_updated': 0,
            'execution_details': []
        }
        
        try:
            # Get data sources for this job in a separate query to avoid long locks
            data_sources = job.data_sources.filter(status='active')
            
            if not data_sources.exists():
                return False, {'error': 'No active data sources found for this job'}
            
            logger.info(f"Processing {data_sources.count()} data sources for job {job.name}")
            
            overall_success = True
            
            # Process each data source with proper resource management
            for data_source in data_sources:
                try:
                    logger.info(f"Processing data source: {data_source.name} (ID: {data_source.id})")
                    
                    # Update run log status in separate transaction
                    with transaction.atomic():
                        run_log.refresh_from_db()
                        run_log.status = 'running'
                        run_log.save()
                    
                    # Execute ETL for this data source with proper error handling
                    source_success, source_results = self._process_data_source_safely(data_source, job)
                    
                    if source_success:
                        results['data_sources_processed'].append(str(data_source.id))
                        results['total_records_processed'] += source_results.get('records_processed', 0)
                        results['total_records_added'] += source_results.get('records_added', 0)
                        results['total_records_updated'] += source_results.get('records_updated', 0)
                        
                        logger.info(f"Successfully processed data source {data_source.name}")
                    else:
                        results['data_sources_failed'].append(str(data_source.id))
                        overall_success = False
                        logger.error(f"Failed to process data source {data_source.name}: {source_results.get('error')}")
                    
                    results['execution_details'].append({
                        'data_source_id': str(data_source.id),
                        'data_source_name': data_source.name,
                        'success': source_success,
                        'details': source_results
                    })
                    
                except Exception as e:
                    error_msg = f"Error processing data source {data_source.name}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    
                    results['data_sources_failed'].append(str(data_source.id))
                    results['execution_details'].append({
                        'data_source_id': str(data_source.id),
                        'data_source_name': data_source.name,
                        'success': False,
                        'error': error_msg
                    })
                    overall_success = False
            
            # Update run log with results in separate transaction
            with transaction.atomic():
                run_log.refresh_from_db()
                run_log.data_sources_processed = results['data_sources_processed']
                run_log.data_sources_failed = results['data_sources_failed']
                run_log.data_sources_skipped = results['data_sources_skipped']
                run_log.total_records_processed = results['total_records_processed']
                run_log.total_records_added = results['total_records_added']
                run_log.total_records_updated = results['total_records_updated']
                run_log.save()
            
            if not overall_success:
                results['error'] = f"Failed to process {len(results['data_sources_failed'])} out of {data_sources.count()} data sources"
            
            return overall_success, results
            
        except Exception as e:
            error_msg = f"Critical error in job execution logic: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results['error'] = error_msg
            return False, results
    
    def _process_data_source_safely(self, data_source: DataSource, job: ScheduledETLJob) -> Tuple[bool, Dict[str, Any]]:
        """
        Process a single data source for ETL with proper resource management.
        FIXED: Added connection pooling, transaction management, and resource cleanup.
        """
        
        results = {
            'records_processed': 0,
            'records_added': 0,
            'records_updated': 0,
            'start_time': timezone.now().isoformat()
        }
        
        duckdb_conn = None
        
        try:
            # Determine ETL mode (full vs incremental)
            etl_mode = job.etl_config.get('mode', 'full')  # 'full' or 'incremental'
            
            logger.info(f"Processing data source {data_source.name} in {etl_mode} mode")
            
            if data_source.source_type == 'csv':
                return self._process_csv_data_source_safely(data_source, etl_mode, results)
            elif data_source.source_type in ['postgresql', 'mysql', 'oracle', 'sqlserver']:
                return self._process_database_data_source_safely(data_source, etl_mode, results)
            elif data_source.source_type == 'api':
                return self._process_api_data_source_safely(data_source, etl_mode, results)
            else:
                error_msg = f"Unsupported data source type: {data_source.source_type}"
                results['error'] = error_msg
                return False, results
                
        except Exception as e:
            error_msg = f"Error processing data source {data_source.name}: {str(e)}"
            results['error'] = error_msg
            logger.error(error_msg, exc_info=True)
            return False, results
        finally:
            # Cleanup any local resources
            if duckdb_conn:
                try:
                    duckdb_conn.close()
                except:
                    pass
    
    def _process_csv_data_source_safely(self, data_source: DataSource, etl_mode: str, results: Dict) -> Tuple[bool, Dict]:
        """
        Process CSV data source with proper transaction management.
        ENHANCED: Fetch fresh data from the original CSV file path.
        """
        duckdb_conn = None
        
        try:
            logger.info(f"Fetching fresh data from CSV source: {data_source.name}")
            
            # ENHANCED: Get the original CSV file path from data source connection info
            csv_file_path = data_source.connection_info.get('file_path')
            
            if not csv_file_path:
                # Try to get from other possible locations
                csv_file_path = data_source.connection_info.get('path')
                if not csv_file_path and hasattr(data_source, 'file_path'):
                    csv_file_path = data_source.file_path
            
            if not csv_file_path:
                results['error'] = f"CSV file path not found for data source {data_source.name}"
                return False, results
            
            logger.info(f"Reading fresh CSV data from: {csv_file_path}")
            
            # ENHANCED: Read fresh data directly from the CSV file
            try:
                import pandas as pd
                import os
                
                # ENHANCED: Use comprehensive path resolution to find CSV files
                found_path = self._resolve_csv_path_comprehensively(data_source)
                
                if not found_path:
                    error_msg = f"CSV file not found at path: {csv_file_path}"
                    logger.error(error_msg)
                    logger.info(f"Tried to resolve path for data source: {data_source.name}")
                    results['error'] = error_msg
                    return False, results
                else:
                    # Update the path if we found it in a different location
                    if found_path != csv_file_path:
                        logger.info(f"Found CSV at different path: {found_path} (was looking for {csv_file_path})")
                        csv_file_path = found_path
                        
                        # Update the data source with the correct path for future use
                        try:
                            with transaction.atomic():
                                data_source.refresh_from_db()
                                connection_info = data_source.connection_info.copy()
                                connection_info['file_path'] = os.path.relpath(found_path, settings.BASE_DIR)
                                connection_info['auto_resolved_path'] = True
                                connection_info['resolved_at'] = timezone.now().isoformat()
                                data_source.connection_info = connection_info
                                data_source.save(update_fields=['connection_info'])
                                logger.info(f"Updated data source with correct path: {connection_info['file_path']}")
                        except Exception as path_update_error:
                            logger.warning(f"Could not update data source path: {path_update_error}")
                    
                    logger.info(f"Using CSV file path: {csv_file_path}")
                
                # Read fresh data from CSV with comprehensive encoding handling
                encodings_to_try = ['utf-8', 'utf-16', 'latin-1', 'cp1252', 'iso-8859-1', 'windows-1252']
                separators_to_try = [',', ';', '\t', '|']
                
                df = None
                successful_encoding = None
                successful_separator = None
                
                for encoding in encodings_to_try:
                    for separator in separators_to_try:
                        try:
                            logger.info(f"Attempting to read CSV with encoding: {encoding}, separator: '{separator}'")
                            df = pd.read_csv(csv_file_path, encoding=encoding, sep=separator)
                            successful_encoding = encoding
                            successful_separator = separator
                            logger.info(f"Successfully read {len(df)} rows from CSV file (encoding: {encoding}, separator: '{separator}')")
                            break
                        except UnicodeDecodeError as e:
                            logger.warning(f"Failed to read CSV with {encoding} encoding: {e}")
                            continue
                        except Exception as e:
                            logger.warning(f"Failed to read CSV with {encoding} encoding, {separator} separator: {e}")
                            continue
                    if df is not None:
                        break
                
                if df is None:
                    raise Exception("Failed to read CSV file with any encoding or separator combination")
                
                if df.empty:
                    results['error'] = f"CSV file is empty: {csv_file_path}"
                    return False, results
                    
            except Exception as csv_error:
                results['error'] = f"Failed to read CSV file {csv_file_path}: {str(csv_error)}"
                return False, results
            
            # Get or create table name
            table_name = data_source.table_name or f"source_{str(data_source.id).replace('-', '_')}"
            
            # CRITICAL FIX: Use dedicated DuckDB connection to prevent locks
            import duckdb
            import os
            
            db_path = 'data/integrated.duckdb'
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Use a dedicated connection for this operation
            duckdb_conn = duckdb.connect(db_path)
            
            if not duckdb_conn:
                results['error'] = "DuckDB connection not available"
                return False, results
            
            logger.info(f"Processing fresh CSV data for table: {table_name}")
            
            # ENHANCED: Apply data type optimization and cleaning
            df = self._optimize_dataframe_types(df)
            
            # FIXED: Use atomic operations with proper error handling
            try:
                # Begin transaction-like operation
                with duckdb_conn.begin() if hasattr(duckdb_conn, 'begin') else duckdb_conn:
                    
                    # Handle incremental vs full refresh
                    if etl_mode == 'incremental':
                        # For incremental, we would compare timestamps or unique keys
                        # For now, implement as full refresh with timestamp tracking
                        logger.info(f"Processing incremental refresh for CSV: {data_source.name}")
                        
                        # Check if table exists and get last update info
                        try:
                            existing_count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                            logger.info(f"Existing table has {existing_count} rows, adding {len(df)} new rows")
                        except:
                            # Table doesn't exist, treat as full refresh
                            logger.info("Table doesn't exist, treating incremental as full refresh")
                            etl_mode = 'full'
                    
                    if etl_mode == 'full':
                        # Drop existing table and recreate (full refresh)
                        duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        logger.info(f"Dropped existing table: {table_name}")
                    
                    # Create table from DataFrame in one operation
                    duckdb_conn.register(f"{table_name}_temp", df)
                    
                    if etl_mode == 'incremental':
                        # For incremental, append to existing table
                        duckdb_conn.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_temp")
                    else:
                        # For full refresh, create new table
                        duckdb_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}_temp")
                    
                    duckdb_conn.unregister(f"{table_name}_temp")
                    
                    logger.info(f"Created table {table_name} with {len(df)} records")
                    
            except Exception as db_error:
                logger.error(f"Database operation failed: {db_error}")
                raise
                
            results['records_processed'] = len(df)
            results['records_added'] = len(df) if etl_mode == 'full' else len(df)
            results['records_updated'] = 0 if etl_mode == 'full' else len(df)
            results['end_time'] = timezone.now().isoformat()
            results['source_file'] = csv_file_path
            results['file_size'] = os.path.getsize(csv_file_path) if os.path.exists(csv_file_path) else 0
            
            # Update data source status and schema in separate transaction
            with transaction.atomic():
                data_source.refresh_from_db()
                data_source.last_synced = timezone.now()
                
                # ENHANCED: Update schema information with fresh CSV structure
                try:
                    # Generate fresh schema information from the DataFrame
                    schema_info = {
                        'columns': [],
                        'row_count': len(df),
                        'file_info': {
                            'path': csv_file_path,
                            'size': results['file_size'],
                            'last_modified': timezone.now().isoformat()
                        }
                    }
                    
                    for col in df.columns:
                        # Convert sample values to simple Python types to avoid serialization issues
                        sample_values = []
                        try:
                            samples = df[col].dropna().head(3)
                            for val in samples:
                                if pd.isna(val):
                                    continue
                                # Convert to simple types
                                if isinstance(val, (int, float, str, bool)):
                                    sample_values.append(val)
                                else:
                                    sample_values.append(str(val))
                        except Exception:
                            sample_values = []
                        
                        col_info = {
                            'name': str(col),
                            'type': str(df[col].dtype),
                            'nullable': bool(df[col].isnull().any()),
                            'unique_values': int(df[col].nunique()) if df[col].nunique() < 1000 else 1000,
                            'sample_values': sample_values
                        }
                        schema_info['columns'].append(col_info)
                    
                    # Update schema in the same transaction
                    data_source.schema_info = schema_info
                    
                    logger.info(f"✅ Schema updated for {data_source.name}: {len(schema_info['columns'])} columns detected")
                    
                    # Log column names for debugging
                    column_names = [col['name'] for col in schema_info['columns']]
                    logger.info(f"   Columns: {column_names}")
                    
                except Exception as schema_error:
                    logger.warning(f"Failed to update schema for {data_source.name}: {schema_error}")
                
                workflow_status = data_source.workflow_status or {}
                workflow_status['etl_completed'] = True
                workflow_status['last_etl_run'] = timezone.now().isoformat()
                workflow_status['fresh_data_loaded'] = True
                workflow_status['last_file_size'] = results['file_size']
                workflow_status['schema_updated'] = True
                data_source.workflow_status = workflow_status
                data_source.save()
            
            logger.info(f"Successfully refreshed CSV data source {data_source.name}: {len(df)} records from {csv_file_path}")
            return True, results
            
        except Exception as e:
            error_msg = f"Error processing CSV data source: {str(e)}"
            results['error'] = error_msg
            logger.error(error_msg, exc_info=True)
            return False, results
        finally:
            # Always cleanup DuckDB connection
            if duckdb_conn:
                try:
                    duckdb_conn.close()
                except:
                    pass
    
    def _optimize_dataframe_types(self, df):
        """Optimize DataFrame data types for better performance and storage."""
        try:
            import pandas as pd
            import numpy as np
            
            # Convert object columns to more efficient types where possible
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Try to convert to numeric
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass
                    
                    # Try to convert to datetime
                    if df[col].dtype == 'object':
                        try:
                            df[col] = pd.to_datetime(df[col], errors='ignore')
                        except:
                            pass
            
            # Optimize integer types
            for col in df.select_dtypes(include=['int64']).columns:
                col_min = df[col].min()
                col_max = df[col].max()
                
                if col_min >= -128 and col_max <= 127:
                    df[col] = df[col].astype(np.int8)
                elif col_min >= -32768 and col_max <= 32767:
                    df[col] = df[col].astype(np.int16)
                elif col_min >= -2147483648 and col_max <= 2147483647:
                    df[col] = df[col].astype(np.int32)
            
            # Optimize float types
            for col in df.select_dtypes(include=['float64']).columns:
                df[col] = pd.to_numeric(df[col], downcast='float')
            
            return df
            
        except Exception as e:
            logger.warning(f"DataFrame optimization failed: {e}")
            return df  # Return original if optimization fails
    
    def _process_database_data_source_safely(self, data_source: DataSource, etl_mode: str, results: Dict) -> Tuple[bool, Dict]:
        """
        Process database data source with proper transaction management.
        ENHANCED: Fetch fresh data from the actual database connection.
        """
        duckdb_conn = None
        source_conn = None
        
        try:
            logger.info(f"Fetching fresh data from database source: {data_source.name}")
            
            # ENHANCED: Establish fresh connection to source database
            connection_info = data_source.connection_info
            source_type = data_source.source_type.lower()
            
            # Extract connection parameters
            host = connection_info.get('host', 'localhost')
            port = connection_info.get('port')
            database = connection_info.get('database')
            username = connection_info.get('username')
            password = connection_info.get('password')
            
            if not all([host, database, username, password]):
                results['error'] = f"Missing connection parameters for database source {data_source.name}"
                return False, results
            
            logger.info(f"Connecting to {source_type} database: {host}:{port}/{database}")
            
            # ENHANCED: Create fresh database connection based on source type
            try:
                if source_type == 'postgresql':
                    import psycopg2
                    import psycopg2.extras
                    port = port or 5432
                    source_conn = psycopg2.connect(
                        host=host,
                        port=port,
                        database=database,
                        user=username,
                        password=password,
                        connect_timeout=30
                    )
                    
                elif source_type == 'mysql':
                    import mysql.connector
                    port = port or 3306
                    source_conn = mysql.connector.connect(
                        host=host,
                        port=port,
                        database=database,
                        user=username,
                        password=password,
                        connection_timeout=30
                    )
                    
                elif source_type == 'sqlserver':
                    import pyodbc
                    port = port or 1433
                    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password};TIMEOUT=30"
                    source_conn = pyodbc.connect(connection_string)
                    
                elif source_type == 'oracle':
                    import oracledb
                    port = port or 1521
                    dsn = f"{host}:{port}/{database}"
                    source_conn = oracledb.connect(
                        user=username,
                        password=password,
                        dsn=dsn
                    )
                    
                else:
                    results['error'] = f"Unsupported database type: {source_type}"
                    return False, results
                    
            except Exception as conn_error:
                results['error'] = f"Failed to connect to {source_type} database: {str(conn_error)}"
                return False, results
            
            logger.info(f"Successfully connected to {source_type} database")
            
            # ENHANCED: Execute fresh data query
            try:
                import pandas as pd
                
                # Get tables to process - either specified or discover all
                tables_to_process = connection_info.get('tables', [])
                
                if not tables_to_process:
                    # Discover tables from the database
                    if source_type == 'postgresql':
                        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                    elif source_type == 'mysql':
                        query = "SHOW TABLES"
                    elif source_type == 'sqlserver':
                        query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
                    elif source_type == 'oracle':
                        query = "SELECT table_name FROM user_tables"
                    
                    tables_df = pd.read_sql(query, source_conn)
                    tables_to_process = tables_df.iloc[:, 0].tolist()[:5]  # Limit to first 5 tables
                    logger.info(f"Discovered {len(tables_to_process)} tables: {tables_to_process}")
                
                total_records = 0
                
                # Process each table
                for table_name in tables_to_process:
                    try:
                        logger.info(f"Fetching fresh data from table: {table_name}")
                        
                        # Build data query with optional incremental support
                        data_query = f"SELECT * FROM {table_name}"
                        
                        # ENHANCED: Add incremental filtering if configured
                        if etl_mode == 'incremental':
                            incremental_column = connection_info.get('incremental_column')
                            if incremental_column:
                                # Get last processed timestamp/value
                                last_value = connection_info.get('last_incremental_value')
                                if last_value:
                                    if incremental_column.lower() in ['created_at', 'updated_at', 'timestamp']:
                                        data_query += f" WHERE {incremental_column} > '{last_value}'"
                                    else:
                                        data_query += f" WHERE {incremental_column} > {last_value}"
                                    logger.info(f"Incremental query: {data_query}")
                        
                        # ENHANCED: Add row limit for large tables
                        max_rows = connection_info.get('max_rows', 100000)
                        if source_type in ['postgresql', 'mysql']:
                            data_query += f" LIMIT {max_rows}"
                        elif source_type == 'sqlserver':
                            data_query = f"SELECT TOP {max_rows} * FROM ({data_query.replace('SELECT *', 'SELECT *')}) AS limited_query"
                        elif source_type == 'oracle':
                            data_query += f" AND ROWNUM <= {max_rows}"
                        
                        # Execute query and get fresh data
                        df = pd.read_sql(data_query, source_conn)
                        
                        if df.empty:
                            logger.info(f"No new data found in table: {table_name}")
                            continue
                        
                        logger.info(f"Retrieved {len(df)} fresh records from {table_name}")
                        
                        # ENHANCED: Apply data type optimization
                        df = self._optimize_dataframe_types(df)
                        
                        # Store in DuckDB
                        target_table_name = f"{data_source.name}_{table_name}".replace('-', '_').replace(' ', '_')
                        
                        # Get DuckDB connection
                        import duckdb
                        import os
                        
                        db_path = 'data/integrated.duckdb'
                        os.makedirs(os.path.dirname(db_path), exist_ok=True)
                        duckdb_conn = duckdb.connect(db_path)
                        
                        # Store data in DuckDB
                        with duckdb_conn.begin() if hasattr(duckdb_conn, 'begin') else duckdb_conn:
                            if etl_mode == 'full':
                                # Drop and recreate table
                                duckdb_conn.execute(f"DROP TABLE IF EXISTS {target_table_name}")
                                logger.info(f"Dropped existing table: {target_table_name}")
                            
                            # Register DataFrame and create/insert data
                            duckdb_conn.register(f"{target_table_name}_temp", df)
                            
                            if etl_mode == 'incremental':
                                try:
                                    # Check if table exists
                                    duckdb_conn.execute(f"SELECT 1 FROM {target_table_name} LIMIT 1")
                                    # Table exists, insert new data
                                    duckdb_conn.execute(f"INSERT INTO {target_table_name} SELECT * FROM {target_table_name}_temp")
                                    logger.info(f"Appended {len(df)} records to existing table {target_table_name}")
                                except:
                                    # Table doesn't exist, create it
                                    duckdb_conn.execute(f"CREATE TABLE {target_table_name} AS SELECT * FROM {target_table_name}_temp")
                                    logger.info(f"Created new table {target_table_name} with {len(df)} records")
                            else:
                                # Full refresh - create new table
                                duckdb_conn.execute(f"CREATE TABLE {target_table_name} AS SELECT * FROM {target_table_name}_temp")
                                logger.info(f"Created table {target_table_name} with {len(df)} records")
                            
                            duckdb_conn.unregister(f"{target_table_name}_temp")
                        
                        total_records += len(df)
                        
                        # Update incremental tracking if configured
                        if etl_mode == 'incremental' and connection_info.get('incremental_column'):
                            try:
                                max_value = df[connection_info['incremental_column']].max()
                                connection_info['last_incremental_value'] = str(max_value)
                                logger.info(f"Updated incremental value to: {max_value}")
                            except Exception as inc_error:
                                logger.warning(f"Failed to update incremental value: {inc_error}")
                        
                    except Exception as table_error:
                        logger.error(f"Error processing table {table_name}: {table_error}")
                        continue  # Continue with other tables
                
                if total_records == 0:
                    results['error'] = "No data retrieved from any tables"
                    return False, results
                
            except Exception as query_error:
                results['error'] = f"Failed to execute data query: {str(query_error)}"
                return False, results
            
            results['records_processed'] = total_records
            results['records_added'] = total_records if etl_mode == 'full' else total_records
            results['records_updated'] = 0 if etl_mode == 'full' else total_records
            results['end_time'] = timezone.now().isoformat()
            results['tables_processed'] = len(tables_to_process)
            results['source_host'] = host
            results['source_database'] = database
            
            # Update data source status
            with transaction.atomic():
                data_source.refresh_from_db()
                data_source.last_synced = timezone.now()
                # Update connection info with incremental tracking
                data_source.connection_info = connection_info
                workflow_status = data_source.workflow_status or {}
                workflow_status['etl_completed'] = True
                workflow_status['last_etl_run'] = timezone.now().isoformat()
                workflow_status['fresh_data_loaded'] = True
                workflow_status['tables_processed'] = len(tables_to_process)
                data_source.workflow_status = workflow_status
                data_source.save()
            
            logger.info(f"Successfully refreshed database source {data_source.name}: {total_records} records from {len(tables_to_process)} tables")
            return True, results
            
        except Exception as e:
            error_msg = f"Error processing database data source: {str(e)}"
            results['error'] = error_msg
            logger.error(error_msg, exc_info=True)
            return False, results
        finally:
            # Always cleanup connections
            if source_conn:
                try:
                    source_conn.close()
                    logger.info(f"Closed {source_type} database connection")
                except:
                    pass
            if duckdb_conn:
                try:
                    duckdb_conn.close()
                except:
                    pass
    
    def _process_api_data_source_safely(self, data_source: DataSource, etl_mode: str, results: Dict) -> Tuple[bool, Dict]:
        """
        Process API data source with proper resource management.
        FIXED: Added proper error handling and resource cleanup.
        """
        try:
            # This is a placeholder for API data source processing
            # Implementation would depend on the specific API structure
            
            api_config = data_source.connection_info
            api_url = api_config.get('url')
            
            if not api_url:
                results['error'] = "API URL not configured"
                return False, results
            
            # For now, return success but with 0 records
            # In a real implementation, you would:
            # 1. Make API requests with proper timeout and retry logic
            # 2. Parse the response
            # 3. Convert to DataFrame
            # 4. Store in DuckDB with proper transaction management
            
            results['records_processed'] = 0
            results['records_added'] = 0
            results['end_time'] = timezone.now().isoformat()
            
            logger.info(f"API data source processing not fully implemented: {data_source.name}")
            return True, results
            
        except Exception as e:
            error_msg = f"Error processing API data source: {str(e)}"
            results['error'] = error_msg
            logger.error(error_msg, exc_info=True)
            return False, results
    
    def _send_notification(self, job: ScheduledETLJob, run_log: ETLJobRunLog, status: str):
        """Send notification about job execution."""
        try:
            if not job.notification_emails:
                return
            
            from services.email_service import EmailService
            email_service = EmailService()
            
            subject = f"ETL Job {status.title()}: {job.name}"
            
            if status == 'success':
                body = f"""
                <h2>ETL Job Completed Successfully</h2>
                <p><strong>Job:</strong> {job.name}</p>
                <p><strong>Started:</strong> {run_log.started_at}</p>
                <p><strong>Completed:</strong> {run_log.completed_at}</p>
                <p><strong>Duration:</strong> {run_log.duration_formatted()}</p>
                <p><strong>Records Processed:</strong> {run_log.total_records_processed:,}</p>
                <p><strong>Records Added:</strong> {run_log.total_records_added:,}</p>
                <p><strong>Records Updated:</strong> {run_log.total_records_updated:,}</p>
                """
            else:
                body = f"""
                <h2>ETL Job Failed</h2>
                <p><strong>Job:</strong> {job.name}</p>
                <p><strong>Started:</strong> {run_log.started_at}</p>
                <p><strong>Error:</strong> {run_log.error_message}</p>
                <p><strong>Duration:</strong> {run_log.duration_formatted()}</p>
                """
            
            # Send to all configured email addresses
            for email in job.notification_emails:
                try:
                    email_service.send_dashboard_email(email, subject, body)
                except Exception as e:
                    logger.error(f"Failed to send notification to {email}: {e}")
            
        except Exception as e:
            logger.error(f"Error sending notification for job {job.name}: {e}")
    
    def _send_notification_async(self, job: ScheduledETLJob, run_log: ETLJobRunLog, status: str):
        """
        Send notification about job execution asynchronously.
        FIXED: Check if Redis is available, otherwise send synchronously.
        """
        try:
            from django.conf import settings
            
            # Check if we should use async (Redis) or sync (development mode)
            use_redis = getattr(settings, 'USE_REDIS', False)
            always_eager = getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False)
            
            if use_redis and not always_eager:
                # Use Celery async notification
                from celery import current_app
                current_app.send_task(
                    'services.scheduled_etl_service.send_etl_notification',
                    args=[job.id, run_log.id, status],
                    countdown=5  # Send after 5 seconds to ensure transaction is committed
                )
                logger.info(f"Queued async notification for job {job.name}: {status}")
            else:
                # Send synchronously (development mode without Redis)
                self._send_notification(job, run_log, status)
                logger.info(f"Sent sync notification for job {job.name}: {status}")
            
        except Exception as e:
            logger.error(f"Error with notification for job {job.name}: {e}")
            # Always try synchronous fallback
            try:
                self._send_notification(job, run_log, status)
                logger.info(f"Fallback sync notification sent for job {job.name}: {status}")
            except Exception as sync_error:
                logger.error(f"Fallback notification also failed: {sync_error}")

    # Keep the original _process_data_source method for backward compatibility
    def _process_data_source(self, data_source: DataSource, job: ScheduledETLJob) -> Tuple[bool, Dict[str, Any]]:
        """Process a single data source for ETL."""
        
        results = {
            'records_processed': 0,
            'records_added': 0,
            'records_updated': 0,
            'start_time': timezone.now().isoformat()
        }
        
        try:
            # Determine ETL mode (full vs incremental)
            etl_mode = job.etl_config.get('mode', 'full')  # 'full' or 'incremental'
            
            if data_source.source_type == 'csv':
                return self._process_csv_data_source(data_source, etl_mode, results)
            elif data_source.source_type in ['postgresql', 'mysql', 'oracle', 'sqlserver']:
                return self._process_database_data_source(data_source, etl_mode, results)
            elif data_source.source_type == 'api':
                return self._process_api_data_source(data_source, etl_mode, results)
            else:
                error_msg = f"Unsupported data source type: {data_source.source_type}"
                results['error'] = error_msg
                return False, results
                
        except Exception as e:
            error_msg = f"Error processing data source {data_source.name}: {str(e)}"
            results['error'] = error_msg
            logger.error(error_msg, exc_info=True)
            return False, results
    
    def _process_csv_data_source(self, data_source: DataSource, etl_mode: str, results: Dict) -> Tuple[bool, Dict]:
        """Process CSV data source."""
        try:
            # Use universal data loader to reload CSV data
            success, df, message = universal_data_loader.load_data_for_transformation(data_source)
            
            if not success or df is None:
                results['error'] = f"Failed to load CSV data: {message}"
                return False, results
            
            # Get or create table name
            table_name = data_source.table_name or f"source_{str(data_source.id).replace('-', '_')}"
            
            # Ensure DuckDB connection
            unified_data_access._ensure_duckdb_connection()
            conn = unified_data_access.duckdb_connection
            
            if not conn:
                results['error'] = "DuckDB connection not available"
                return False, results
            
            # Handle incremental vs full refresh
            if etl_mode == 'incremental':
                # For incremental, we would compare timestamps or unique keys
                # For now, implement as full refresh
                logger.info(f"Incremental mode requested but implementing as full refresh for CSV: {data_source.name}")
            
            # Drop existing table and recreate (full refresh)
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped existing table: {table_name}")
            except Exception as e:
                logger.warning(f"Could not drop table {table_name}: {e}")
            
            # Create table from DataFrame
            conn.register(table_name, df)
            
            # Make the table persistent
            conn.execute(f"CREATE TABLE {table_name}_temp AS SELECT * FROM {table_name}")
            conn.unregister(table_name)
            conn.execute(f"ALTER TABLE {table_name}_temp RENAME TO {table_name}")
            
            results['records_processed'] = len(df)
            results['records_added'] = len(df)
            results['end_time'] = timezone.now().isoformat()
            
            # Update data source status
            data_source.last_synced = timezone.now()
            workflow_status = data_source.workflow_status or {}
            workflow_status['etl_completed'] = True
            workflow_status['last_etl_run'] = timezone.now().isoformat()
            data_source.workflow_status = workflow_status
            data_source.save()
            
            logger.info(f"Successfully refreshed CSV data source {data_source.name}: {len(df)} records")
            return True, results
            
        except Exception as e:
            error_msg = f"Error processing CSV data source: {str(e)}"
            results['error'] = error_msg
            return False, results
    
    def _process_database_data_source(self, data_source: DataSource, etl_mode: str, results: Dict) -> Tuple[bool, Dict]:
        """Process database data source."""
        try:
            # Test connection first
            success, message = self.data_service.test_connection(data_source.connection_info)
            if not success:
                results['error'] = f"Database connection failed: {message}"
                return False, results
            
            # Get schema information
            schema_info = self.data_service.get_schema_info(data_source.connection_info, data_source)
            if not schema_info:
                results['error'] = "Failed to retrieve schema information"
                return False, results
            
            # For database sources, we need to determine what tables/queries to sync
            tables_to_sync = data_source.connection_info.get('tables', [])
            
            if not tables_to_sync:
                # If no specific tables, sync all available tables (be cautious)
                if isinstance(schema_info, dict) and 'tables' in schema_info:
                    tables_to_sync = [table['name'] for table in schema_info['tables'][:5]]  # Limit to 5 tables
                else:
                    results['error'] = "No tables specified for synchronization"
                    return False, results
            
            total_records = 0
            
            # Sync each table
            for table_name in tables_to_sync:
                try:
                    # Execute query to get data
                    if etl_mode == 'incremental':
                        # For incremental, add WHERE clause based on timestamp
                        # This is a simplified example - in production, you'd track last sync timestamps
                        query = f"SELECT * FROM {table_name} WHERE updated_at > CURRENT_DATE - INTERVAL '1 day'"
                    else:
                        query = f"SELECT * FROM {table_name}"
                    
                    query_success, df = self.data_service.execute_query(
                        query, 
                        data_source.connection_info, 
                        user_id=1  # System user
                    )
                    
                    if not query_success:
                        logger.error(f"Failed to query table {table_name}: {df}")
                        continue
                    
                    if df is not None and not df.empty:
                        # Store in DuckDB
                        unified_data_access._ensure_duckdb_connection()
                        conn = unified_data_access.duckdb_connection
                        
                        if conn:
                            target_table = f"{data_source.table_name}_{table_name}"
                            
                            # Drop and recreate for full refresh
                            if etl_mode == 'full':
                                try:
                                    conn.execute(f"DROP TABLE IF EXISTS {target_table}")
                                except:
                                    pass
                            
                            # Register and persist the data
                            conn.register(f"{target_table}_temp", df)
                            conn.execute(f"CREATE TABLE {target_table} AS SELECT * FROM {target_table}_temp")
                            conn.unregister(f"{target_table}_temp")
                            
                            total_records += len(df)
                            logger.info(f"Synced table {table_name}: {len(df)} records")
                    
                except Exception as table_error:
                    logger.error(f"Error syncing table {table_name}: {table_error}")
                    continue
            
            results['records_processed'] = total_records
            results['records_added'] = total_records if etl_mode == 'full' else 0
            results['records_updated'] = 0 if etl_mode == 'full' else total_records
            results['end_time'] = timezone.now().isoformat()
            
            # Update data source status
            data_source.last_synced = timezone.now()
            workflow_status = data_source.workflow_status or {}
            workflow_status['etl_completed'] = True
            workflow_status['last_etl_run'] = timezone.now().isoformat()
            data_source.workflow_status = workflow_status
            data_source.save()
            
            logger.info(f"Successfully refreshed database data source {data_source.name}: {total_records} total records")
            return True, results
            
        except Exception as e:
            error_msg = f"Error processing database data source: {str(e)}"
            results['error'] = error_msg
            return False, results
    
    def _process_api_data_source(self, data_source: DataSource, etl_mode: str, results: Dict) -> Tuple[bool, Dict]:
        """Process API data source."""
        try:
            # This is a placeholder for API data source processing
            # Implementation would depend on the specific API structure
            
            api_config = data_source.connection_info
            api_url = api_config.get('url')
            
            if not api_url:
                results['error'] = "API URL not configured"
                return False, results
            
            # For now, return success but with 0 records
            # In a real implementation, you would:
            # 1. Make API requests
            # 2. Parse the response
            # 3. Convert to DataFrame
            # 4. Store in DuckDB
            
            results['records_processed'] = 0
            results['records_added'] = 0
            results['end_time'] = timezone.now().isoformat()
            
            logger.info(f"API data source processing not fully implemented: {data_source.name}")
            return True, results
            
        except Exception as e:
            error_msg = f"Error processing API data source: {str(e)}"
            results['error'] = error_msg
            return False, results
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status information for a scheduled job."""
        try:
            job = ScheduledETLJob.objects.get(id=job_id)
            
            # Get recent run logs
            recent_runs = job.run_logs.order_by('-started_at')[:10]
            
            # Calculate success rate
            if recent_runs:
                successful_runs = sum(1 for run in recent_runs if run.status == 'success')
                success_rate = (successful_runs / len(recent_runs)) * 100
            else:
                success_rate = 0
            
            return {
                'job_id': str(job.id),
                'name': job.name,
                'status': job.status,
                'is_active': job.is_active,
                'schedule_type': job.schedule_type,
                'timezone': job.timezone,
                'last_run': job.last_run.isoformat() if job.last_run else None,
                'next_run': job.next_run.isoformat() if job.next_run else None,
                'last_run_status': job.last_run_status,
                'consecutive_failures': job.consecutive_failures,
                'success_rate': success_rate,
                'data_sources_count': job.data_sources.count(),
                'recent_runs': [
                    {
                        'id': str(run.id),
                        'status': run.status,
                        'started_at': run.started_at.isoformat(),
                        'completed_at': run.completed_at.isoformat() if run.completed_at else None,
                        'duration': run.duration_formatted(),
                        'records_processed': run.total_records_processed
                    }
                    for run in recent_runs
                ]
            }
            
        except ScheduledETLJob.DoesNotExist:
            return {'error': 'Job not found'}
        except Exception as e:
            logger.error(f"Error getting job status for {job_id}: {e}")
            return {'error': str(e)}


# Celery Tasks

@shared_task(bind=True, max_retries=2)  # REDUCED: Limit retries to prevent infinite loops
def execute_scheduled_etl_job(self, job_id: str, triggered_by: str = 'schedule'):
    """
    Celery task to execute a scheduled ETL job.
    FIXED: Added proper resource management and limited retries to prevent system freezes.
    
    Args:
        job_id: ID of the scheduled job to execute
        triggered_by: What triggered this execution
    """
    import socket
    
    try:
        logger.info(f"Starting ETL job execution: {job_id} (attempt {self.request.retries + 1})")
        
        # CRITICAL FIX: Use context manager for proper resource cleanup
        with ScheduledETLService() as etl_service:
            # Execute the job with proper error handling
            success, message, results = etl_service.execute_scheduled_job(job_id, triggered_by)
        
        # Update run log with worker information in separate transaction
        try:
            with transaction.atomic():
                run_logs = ETLJobRunLog.objects.filter(
                    scheduled_job_id=job_id,
                ).order_by('-started_at')[:1]  # Get most recent
                
                if run_logs.exists():
                    run_log = run_logs.first()
                    run_log.celery_task_id = self.request.id
                    run_log.worker_hostname = socket.gethostname()
                    run_log.save()
        except Exception as log_error:
            logger.warning(f"Could not update run log with worker info: {log_error}")
        
        result = {
            'success': success,
            'message': message,
            'job_id': job_id,
            'triggered_by': triggered_by,
            'results': results,
            'worker_hostname': socket.gethostname(),
            'task_id': self.request.id,
            'retry_count': self.request.retries
        }
        
        if success:
            logger.info(f"ETL job {job_id} completed successfully: {message}")
        else:
            logger.error(f"ETL job {job_id} failed: {message}")
        
        return result
        
    except Exception as exc:
        error_msg = f"Celery task failed for ETL job {job_id}: {str(exc)}"
        logger.error(error_msg, exc_info=True)
        
        # CRITICAL FIX: More careful retry logic to prevent infinite loops
        if self.request.retries < self.max_retries:
            # Check if this is a recoverable error
            if _is_recoverable_error(str(exc)):
                # Exponential backoff: 60s, 120s (only 2 retries total)
                countdown = 60 * (2 ** self.request.retries)
                logger.info(f"Retrying ETL job {job_id} in {countdown} seconds (attempt {self.request.retries + 1})")
                
                # Clean up any resources before retry
                try:
                    connection.close()
                except:
                    pass
                
                raise self.retry(countdown=countdown, exc=exc)
            else:
                logger.error(f"Non-recoverable error for ETL job {job_id}, not retrying: {error_msg}")
        
        # Mark the job as failed after all retries or non-recoverable error
        try:
            with transaction.atomic():
                job = ScheduledETLJob.objects.get(id=job_id)
                job.mark_failure(error_msg)
        except Exception as mark_error:
            logger.error(f"Failed to mark job as failed: {mark_error}")
        
        return {
            'success': False,
            'message': error_msg,
            'job_id': job_id,
            'triggered_by': triggered_by,
            'error': str(exc),
            'max_retries_exceeded': self.request.retries >= self.max_retries,
            'retry_count': self.request.retries
        }
    finally:
        # CRITICAL: Always cleanup Django database connections to prevent locks
        try:
            connection.close()
        except:
            pass

def _is_recoverable_error(error_msg: str) -> bool:
    """
    Determine if an error is recoverable and worth retrying.
    ADDED: Prevent infinite retries for non-recoverable errors.
    """
    recoverable_patterns = [
        'connection',
        'timeout',
        'temporary',
        'lock',
        'busy'
    ]
    
    non_recoverable_patterns = [
        'permission denied',
        'authentication failed',
        'table not found',
        'column not found',
        'syntax error',
        'configuration error'
    ]
    
    error_lower = error_msg.lower()
    
    # Check for non-recoverable errors first
    for pattern in non_recoverable_patterns:
        if pattern in error_lower:
            return False
    
    # Check for recoverable errors
    for pattern in recoverable_patterns:
        if pattern in error_lower:
            return True
    
    # Default to non-recoverable for unknown errors
    return False


@shared_task
def send_etl_notification(job_id: str, run_log_id: str, status: str):
    """
    Async task to send ETL job notifications.
    ADDED: Separate task for sending notifications to prevent blocking main ETL execution.
    """
    try:
        job = ScheduledETLJob.objects.get(id=job_id)
        run_log = ETLJobRunLog.objects.get(id=run_log_id)
        
        # Use the service to send notification
        etl_service = ScheduledETLService()
        etl_service._send_notification(job, run_log, status)
        
        logger.info(f"Notification sent for ETL job {job.name}: {status}")
        
    except Exception as e:
        logger.error(f"Failed to send ETL notification: {e}")


@shared_task
def schedule_pending_etl_jobs():
    """
    Celery task to check for pending ETL jobs and schedule them.
    FIXED: Added better error handling and resource management.
    This task runs every 5 minutes to check for jobs that need to be executed.
    """
    try:
        logger.info("Checking for pending ETL jobs...")
        
        # Get jobs that are ready to run with proper locking
        now = timezone.now()
        
        with transaction.atomic():
            pending_jobs = ScheduledETLJob.objects.select_for_update(skip_locked=True).filter(
                is_active=True,
                status='active',
                next_run__lte=now
            ).order_by('next_run')[:10]  # Limit to 10 jobs to prevent overload
        
        scheduled_count = 0
        
        for job in pending_jobs:
            try:
                # Double-check if job can run (race condition protection)
                if job.can_run_now():
                    # Schedule the job execution
                    task = execute_scheduled_etl_job.delay(str(job.id), 'schedule')
                    scheduled_count += 1
                    logger.info(f"Scheduled ETL job: {job.name} (ID: {job.id}) - Task: {task.id}")
                    
                    # Update job's next run time immediately to prevent duplicate scheduling
                    with transaction.atomic():
                        job.refresh_from_db()
                        job.update_next_run()
                    
            except Exception as job_error:
                logger.error(f"Error scheduling job {job.name}: {job_error}")
                continue
        
        logger.info(f"Scheduled {scheduled_count} ETL jobs for execution")
        
        return {
            'success': True,
            'scheduled_count': scheduled_count,
            'checked_at': now.isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error in schedule_pending_etl_jobs: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'success': False,
            'error': error_msg,
            'checked_at': timezone.now().isoformat()
        }
    finally:
        # Always cleanup database connections
        try:
            connection.close()
        except:
            pass


@shared_task
def cleanup_old_etl_logs():
    """
    Celery task to clean up old ETL job run logs.
    Keeps logs for the last 30 days by default.
    """
    try:
        from datetime import timedelta
        
        # Delete logs older than 30 days
        cutoff_date = timezone.now() - timedelta(days=30)
        
        deleted_count, _ = ETLJobRunLog.objects.filter(
            started_at__lt=cutoff_date
        ).delete()
        
        logger.info(f"Cleaned up {deleted_count} old ETL job run logs")
        
        return {
            'success': True,
            'deleted_count': deleted_count,
            'cutoff_date': cutoff_date.isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error cleaning up ETL logs: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'success': False,
            'error': error_msg
        }


@shared_task
def update_etl_job_schedules():
    """
    Celery task to update next_run times for all active ETL jobs.
    This ensures jobs stay on schedule even if there are system restarts.
    """
    try:
        updated_count = 0
        
        # Update all active jobs that don't have a next_run time
        jobs_to_update = ScheduledETLJob.objects.filter(
            is_active=True,
            status='active',
            next_run__isnull=True
        )
        
        for job in jobs_to_update:
            try:
                job.update_next_run()
                updated_count += 1
            except Exception as job_error:
                logger.error(f"Error updating schedule for job {job.name}: {job_error}")
                continue
        
        logger.info(f"Updated schedules for {updated_count} ETL jobs")
        
        return {
            'success': True,
            'updated_count': updated_count,
            'updated_at': timezone.now().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Error updating ETL job schedules: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'success': False,
            'error': error_msg
        } 