"""
Core application views
"""
import json
import uuid
from typing import Dict, Any, Optional
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.contrib import messages
from django.conf import settings
from django.utils import timezone
from django.db import models
import pandas as pd
import numpy as np
import logging
import time
from django.core.cache import cache

try:
    from services.llm_service import LLMService
except ImportError:
    LLMService = None

from services.data_service import DataService
from services.semantic_service import SemanticService
from core.models import QueryLog
from core.utils import create_plotly_figure, format_data_for_display, handle_query_error, make_json_serializable, safe_session_data, cleanup_corrupted_sessions
from datasets.models import DataSource
from dashboards.models import Dashboard
from licensing.decorators import viewer_or_creator_required, creator_required, admin_required, api_license_required

logger = logging.getLogger(__name__)

@csrf_exempt
def health_check(request):
    """Simple health check endpoint that doesn't require database access."""
    return JsonResponse({
        'status': 'ok',
        'message': 'Django ConvaBI is running!',
        'service': 'convabi-web'
    })


@login_required
def home(request):
    """Home page with workflow steps and statistics"""
    try:
        # Get statistics for the current user (owned + shared)
        from django.db import models
        data_sources_count = DataSource.objects.filter(
            models.Q(created_by=request.user) | models.Q(shared_with_users=request.user)
        ).distinct().count()
        
        # Only count queries if there are data sources, otherwise reset to 0
        if data_sources_count > 0:
            queries_count = QueryLog.objects.filter(user=request.user).count()
        else:
            queries_count = 0
            
        dashboards_count = Dashboard.objects.filter(owner=request.user).count()
        shared_dashboards_count = Dashboard.objects.filter(
            shared_with_users=request.user
        ).count()
        
        # Get ETL operations count
        try:
            from datasets.models import ETLOperation
            etl_operations_count = ETLOperation.objects.filter(created_by=request.user).count()
        except ImportError:
            etl_operations_count = 0
        
        # Get semantic layer count (filter by user's data sources)
        try:
            from datasets.models import SemanticTable
            semantic_tables_count = SemanticTable.objects.filter(
                data_source__created_by=request.user
            ).count()
        except ImportError:
            semantic_tables_count = 0
        
        # Calculate progress percentage based on completed steps
        progress_steps = 0
        if data_sources_count > 0:
            progress_steps += 1
        if etl_operations_count > 0:
            progress_steps += 1
        if semantic_tables_count > 0:
            progress_steps += 1
        # Only count queries step if there are data sources
        if queries_count > 0 and data_sources_count > 0:
            progress_steps += 1
        if dashboards_count > 0:
            progress_steps += 1
        
        progress_percentage = (progress_steps / 5) * 100
        
        # Get recent data sources (owned + shared)
        recent_data_sources = DataSource.objects.filter(
            models.Q(created_by=request.user) | models.Q(shared_with_users=request.user)
        ).distinct().order_by('-created_at')[:8]
        
        # Get recent queries for activity feed
        recent_queries = QueryLog.objects.filter(
            user=request.user
        ).order_by('-created_at')[:5]
        
        # Check LLM status
        llm_status = {
            'status': 'inactive',
            'provider': 'Not Configured'
        }
        
        # Try to detect configured LLM
        if hasattr(settings, 'OPENAI_API_KEY') and settings.OPENAI_API_KEY:
            llm_status = {
                'status': 'active',
                'provider': 'OpenAI'
            }
        elif hasattr(settings, 'OLLAMA_URL') and settings.OLLAMA_URL:
            llm_status = {
                'status': 'active', 
                'provider': 'Ollama'
            }
        
        # Prepare stats for template
        stats = {
            'data_sources': data_sources_count,
            'total_queries': queries_count,
            'dashboards': dashboards_count,
            'etl_operations': etl_operations_count,
            'semantic_tables': semantic_tables_count
        }
        
        context = {
            'stats': stats,
            'progress_percentage': int(progress_percentage),
            'recent_data_sources': recent_data_sources,
            'data_sources_count': data_sources_count,
            'queries_count': queries_count,
            'dashboards_count': dashboards_count,
            'shared_dashboards_count': shared_dashboards_count,
            'recent_queries': recent_queries,
            'llm_status': llm_status,
        }
        
        return render(request, 'core/home.html', context)
        
    except Exception as e:
        logger.error(f"Error loading home page: {e}")
        messages.error(request, "Failed to load home page")
        return render(request, 'core/home.html', {})


@method_decorator(login_required, name='dispatch')
@method_decorator(viewer_or_creator_required, name='dispatch')
class QueryView(View):
    """Main query interface for natural language to SQL conversion"""
    
    def get(self, request):
        """Display the query interface"""
        try:
            # Get available data sources for current user (owned + shared)
            from django.db import models
            data_sources = DataSource.objects.filter(
                models.Q(created_by=request.user) | models.Q(shared_with_users=request.user),
                status='active'
            ).distinct().order_by('-created_at')
            
            # Clean up old query result sessions to prevent memory leaks
            self._cleanup_old_sessions(request)

            # Get selected data source from GET param or session
            selected_data_source_id = request.GET.get('data_source_id') or request.session.get('last_selected_data_source_id')
            
            context = {
                'data_sources': data_sources,
                'selected_data_source_id': selected_data_source_id,
            }
            
            return render(request, 'core/query.html', context)
            
        except Exception as e:
            logger.error(f"Error loading query interface: {e}")
            messages.error(request, "Failed to load query interface")
            return redirect('dashboards:list')
    
    def _cleanup_old_sessions(self, request):
        """Clean up old query result sessions to prevent memory leaks"""
        try:
            # First clean up any corrupted sessions
            cleanup_corrupted_sessions(request)
            
            # Get all session keys
            session_keys = list(request.session.keys())
            
            # Clean up query result sessions older than 1 hour
            current_time = time.time()
            for key in session_keys:
                if key.startswith('query_result_') or key.startswith('query_context_'):
                    # Simple cleanup based on session key age (could be enhanced)
                    if len(session_keys) > 50:  # If too many sessions
                        try:
                            del request.session[key]
                        except:
                            pass
            
        except Exception as e:
            logger.warning(f"Failed to cleanup old sessions: {e}")
    
    def post(self, request):
        """Process natural language query with enhanced error handling and caching"""
        try:
            data = json.loads(request.body)
            natural_query = data.get('query', '').strip()
            data_source_id = data.get('data_source_id')
            clarification_context = data.get('clarification_context')
            use_cache = data.get('use_cache', True)
            
            # Store the selected data source in the session for persistence
            if data_source_id:
                request.session['last_selected_data_source_id'] = data_source_id
            
            if not natural_query:
                # On error, re-render the page with the selected data source
                return render(request, 'core/query.html', {
                    'data_sources': DataSource.objects.filter(created_by=request.user, status='active').order_by('-created_at'),
                    'selected_data_source_id': data_source_id,
                    'error': 'Query is required',
                })
            
            # Rate limiting check (simple implementation)
            user_query_cache_key = f"user_queries_{request.user.id}"
            recent_queries = cache.get(user_query_cache_key, [])
            current_time = time.time()
            
            # Remove queries older than 1 minute
            recent_queries = [q for q in recent_queries if current_time - q < 60]
            
            if len(recent_queries) >= 10:  # Max 10 queries per minute
                return JsonResponse({
                    'error': 'Rate limit exceeded. Please wait before submitting another query.'
                }, status=429)
            
            # Add current query to rate limit tracking
            recent_queries.append(current_time)
            cache.set(user_query_cache_key, recent_queries, timeout=300)
            
            # Get data source with validation (owned or shared)
            if data_source_id:
                try:
                    from django.db import models
                    data_source = DataSource.objects.get(
                        models.Q(id=data_source_id) & 
                        (models.Q(created_by=request.user) | models.Q(shared_with_users=request.user)),
                        status='active'
                    )
                except DataSource.DoesNotExist:
                    return JsonResponse({'error': 'Invalid or inaccessible data source'}, status=400)
            else:
                # Use first available data source for user (owned or shared)
                from django.db import models
                data_source = DataSource.objects.filter(
                    models.Q(created_by=request.user) | models.Q(shared_with_users=request.user),
                    status='active'
                ).first()
                if not data_source:
                    return JsonResponse({'error': 'No data source available'}, status=400)
            
            # Check if data source workflow allows queries
            workflow_status = data_source.workflow_status or {}
            
            # ENHANCED: More flexible workflow checking
            # Allow queries if ETL is completed OR if it's a simple CSV that doesn't need ETL
            can_query = False
            error_details = []
            
            # Check ETL completion
            if workflow_status.get('etl_completed', False):
                can_query = True
                logger.info(f"Query allowed: ETL completed for data source {data_source.id}")
            elif data_source.source_type == 'csv' and data_source.table_name:
                # For CSV files that have been loaded, allow queries even without explicit ETL
                can_query = True
                logger.info(f"Query allowed: CSV data source {data_source.id} has table_name")
            elif data_source.source_type in ['postgresql', 'mysql', 'oracle', 'sqlserver']:
                # For direct database connections, allow queries if connection is active
                can_query = data_source.status == 'active'
                if can_query:
                    logger.info(f"Query allowed: Direct database connection is active for {data_source.id}")
                else:
                    error_details.append("Database connection is not active")
            else:
                error_details.append("ETL process not completed")
            
            # Additional checks for data availability
            if can_query:
                # Verify data source has schema or data available
                if not data_source.schema_info and data_source.source_type != 'csv':
                    can_query = False
                    error_details.append("No schema information available")
                elif data_source.source_type == 'csv' and not data_source.connection_info.get('file_path'):
                    can_query = False
                    error_details.append("CSV file path not found")
            
            if not can_query:
                error_message = "Data source is not ready for querying"
                if error_details:
                    error_message += f": {'; '.join(error_details)}"
                
                logger.warning(f"Query blocked for data source {data_source.id}: {error_message}")
                
                # Provide helpful guidance
                guidance = []
                if not workflow_status.get('etl_completed', False) and data_source.source_type == 'csv':
                    guidance.append("Complete the ETL process in the Data Integration page")
                elif data_source.status != 'active':
                    guidance.append("Check data source connection status")
                
                response_data = {
                    'error': error_message,
                    'workflow_status': workflow_status,
                    'guidance': guidance
                }
                
                # Use 400 instead of 403 for better user experience
                return JsonResponse(response_data, status=400)
            
            # Initialize services
            semantic_service = SemanticService()
            data_service = DataService()
            
            # --- CRITICAL: ENSURE SEMANTIC LAYER EXISTS BEFORE LLM PROCESSING ---
            try:
                from services.dynamic_llm_service import DynamicLLMService
                llm_service = DynamicLLMService()
                
                # Discover available tables for this data source
                environment = llm_service.discover_data_environment()
                
                if environment['best_table']:
                    target_table = environment['best_table']
                    
                    # Check if semantic layer exists for the target table
                    semantic_schema = semantic_service.get_semantic_schema_for_table(target_table)
                    
                    if not semantic_schema:
                        logger.info(f"No semantic layer found for {target_table}, generating before query...")
                        # Generate semantic layer before processing query
                        semantic_success = semantic_service.auto_generate_metadata_from_table(target_table)
                        
                        if semantic_success:
                            logger.info(f"SUCCESS: Semantic layer generated for table: {target_table}")
                        else:
                            logger.warning(f"WARNING: Failed to generate semantic layer for: {target_table}")
                    else:
                                                  logger.info(f"INFO: Semantic layer already exists for table: {target_table}")
                        
            except Exception as semantic_prep_error:
                logger.warning(f"Semantic layer preprocessing error: {semantic_prep_error}")
                # Continue with query processing even if semantic preprocessing fails
            
            # Get schema information with caching
            schema_cache_key = f"schema_{data_source.id}"
            schema_info = cache.get(schema_cache_key)
            
            if not schema_info:
                schema_info = data_service.get_schema_info(data_source.connection_info, data_source)
                if schema_info:
                    cache.set(schema_cache_key, schema_info, timeout=1800)  # Cache for 30 minutes
            
            if not schema_info:
                return JsonResponse({
                    'error': 'Failed to retrieve schema information'
                }, status=500)
            
            # Process query with semantic enhancement and caching
            success, sql_or_clarification, clarification_question = semantic_service.get_enhanced_sql_from_openai(
                natural_query, schema_info, data_source.source_type
            )
            
            # Check if clarification is needed FIRST before any other logic
            if clarification_question:
                logger.info(f"Clarification needed for query: {natural_query}")
                # Create a session for this query with cleanup
                query_session_id = str(uuid.uuid4())
                request.session[f'query_context_{query_session_id}'] = {
                    'original_query': natural_query,
                    'data_source_id': str(data_source.id),
                    'schema_info': schema_info,
                    'clarification_question': clarification_question,
                    'timestamp': time.time()  # For cleanup
                }
                
                # Extract column information for frontend quick responses
                column_info = []
                if isinstance(schema_info, dict) and 'columns' in schema_info:
                    column_info = [col.get('name', '') for col in schema_info['columns'] if isinstance(col, dict)]
                
                return JsonResponse({
                    'needs_clarification': True,
                    'clarification_question': clarification_question,
                    'session_id': query_session_id,
                    'query_context': {
                        'original_query': natural_query,
                        'available_columns': column_info,
                        'data_source_type': data_source.source_type
                    }
                })
            
            # Check if we got success=True but empty SQL (force template fallback)
            if success and (not sql_or_clarification or sql_or_clarification.strip() == ''):
                logger.info("Got success=True but empty SQL in main query flow, forcing template fallback")
                success = False
                sql_or_clarification = "Empty SQL returned from semantic service"
            
            if not success:
                # Try template-based generation as fallback before returning error
                logger.info(f"LLM failed, attempting template generation for: {natural_query}")
                template_sql = semantic_service._try_template_sql_generation(natural_query, schema_info)
                
                if template_sql:
                    logger.info(f"Template SQL generated successfully: {template_sql}")
                    sql_or_clarification = template_sql
                    success = True
                else:
                    # Check if this is a validation error (user input issue) vs actual server error
                    error_message = sql_or_clarification
                    if any(phrase in error_message.lower() for phrase in [
                        'empty sql', 'rephrasing', 'incomplete', 'does not appear to be sql'
                    ]):
                        # This is a validation/input issue, not a server error
                        status_code = 400
                    else:
                        # This might be an actual server/LLM configuration issue
                        status_code = 500
                    
                    return JsonResponse({
                        'error': 'Failed to process query',
                        'details': error_message,
                        'suggestion': 'Please try rephrasing your question or check if your data contains the information you\'re looking for.'
                    }, status=status_code)
            
            # Execute SQL query with enhanced error handling
            success, result = data_service.execute_query(
                sql_or_clarification, 
                data_source.connection_info, 
                request.user.id,
                use_cache=use_cache
            )
            
            if not success:
                error_info = handle_query_error(Exception(result))
                
                # Update data source workflow status on error
                if 'connection' in result.lower():
                    workflow_status['query_enabled'] = False
                    data_source.workflow_status = workflow_status
                    data_source.save()
                
                return JsonResponse({
                    'error': 'Query execution failed',
                    'error_info': error_info,
                    'sql_query': sql_or_clarification
                }, status=400)
            
            # Successful query - update workflow status
            workflow_status['query_enabled'] = True
            data_source.workflow_status = workflow_status
            data_source.save()
            
            # Create query result session with pagination support
            result_session_id = str(uuid.uuid4())
            
            # Safely convert result to serializable format
            try:
                if isinstance(result, pd.DataFrame):
                    # Use DataFrame.empty instead of boolean context for DataFrame
                    if not result.empty:
                        # Convert DataFrame to JSON-serializable format
                        result_dict = result.to_dict('records')
                        # Ensure all values are JSON serializable
                        result_data = []
                        for row in result_dict:
                            serializable_row = {}
                            for key, value in row.items():
                                # Enhanced type checking to avoid DataFrame ambiguous error
                                try:
                                    if value is None:
                                        serializable_row[key] = None
                                    elif isinstance(value, pd.DataFrame):
                                        # Handle DataFrame values by converting to string
                                        serializable_row[key] = f"DataFrame({value.shape[0]}x{value.shape[1]})"
                                    elif isinstance(value, pd.Series):
                                        # Handle Series values by converting to list
                                        serializable_row[key] = value.tolist()
                                    elif not isinstance(value, str) and hasattr(value, 'dtype') and hasattr(value, '__len__'):
                                        # Fixed: Reordered conditions to avoid DataFrame boolean context issue
                                        if isinstance(value, np.ndarray):
                                            serializable_row[key] = value.tolist() if value.size <= 100 else f"Array(shape={value.shape})"
                                        else:
                                            serializable_row[key] = str(value)
                                    elif isinstance(value, (float, np.floating)):
                                        # Safe NaN checking for float values
                                        import math
                                        if isinstance(value, float):
                                            # Use math.isnan for standard float types
                                            if math.isnan(value):
                                                serializable_row[key] = None
                                            else:
                                                serializable_row[key] = float(value)
                                        elif hasattr(value, 'dtype') and np.issubdtype(value.dtype, np.floating):
                                            # Use numpy.isnan for numpy float types
                                            if np.isnan(value):
                                                serializable_row[key] = None
                                            else:
                                                serializable_row[key] = float(value)
                                        else:
                                            # Fallback conversion
                                            try:
                                                serializable_row[key] = float(value) if value is not None else None
                                            except (ValueError, TypeError):
                                                serializable_row[key] = None
                                    elif isinstance(value, (np.integer, np.floating)):
                                        serializable_row[key] = float(value)
                                    else:
                                        # For any other type, convert safely to string
                                        serializable_row[key] = value
                                except Exception as value_error:
                                    logger.warning(f"Error processing value {key}={value}: {value_error}")
                                    # Fallback to string representation
                                    serializable_row[key] = str(value) if value is not None else None
                            result_data.append(serializable_row)
                        
                        result_metadata = {
                            'total_rows': len(result),
                            'columns': [str(col) for col in result.columns],
                            'data_types': {str(col): str(result[col].dtype) for col in result.columns},
                            'page_info': {
                                'total_rows': len(result),
                                'page_size': 1000,
                                'total_pages': (len(result) + 999) // 1000,  # Calculate pages
                                'current_page': 1
                            }
                        }
                    else:
                        result_data = []
                        result_metadata = {'total_rows': 0, 'columns': [], 'data_types': {}}
                elif isinstance(result, pd.DataFrame):
                    # Handle DataFrame that wasn't caught by the main if statement
                    logger.info("DataFrame caught in elif clause - processing safely")
                    if not result.empty:
                        result_data = result.to_dict('records')
                        result_metadata = {
                            'total_rows': len(result),
                            'columns': [str(col) for col in result.columns],
                            'data_types': {str(col): str(result[col].dtype) for col in result.columns}
                        }
                    else:
                        result_data = []
                        result_metadata = {'total_rows': 0, 'columns': [], 'data_types': {}}
                elif hasattr(result, 'to_dict'):
                    result_data = result.to_dict()
                    result_metadata = {'type': 'dict'}
                else:
                    result_data = str(result)
                    result_metadata = {'type': 'string'}
            except Exception as e:
                logger.error(f"Error converting result to dict: {e}")
                # ENHANCED: Check specifically for DataFrame ambiguous error with better recovery
                if 'truth value of a DataFrame is ambiguous' in str(e):
                    logger.error("DataFrame boolean ambiguity detected in result processing")
                    # FIXED: Enhanced DataFrame recovery logic
                    try:
                        # ENHANCED: More comprehensive DataFrame detection and handling
                        # Fixed: Use isinstance to avoid DataFrame boolean context ambiguity
                        if isinstance(result, pd.DataFrame):
                            # This is definitely a DataFrame - use shape instead of .empty to avoid ambiguity
                            try:
                                if result.shape[0] > 0:
                                    # DataFrame has data, convert safely
                                    result_data = result.to_dict('records')
                                    result_metadata = {
                                        'total_rows': result.shape[0],
                                        'columns': list(result.columns),
                                        'data_types': {col: str(result[col].dtype) for col in result.columns},
                                        'error_recovered': True,
                                        'ambiguous_handled': True
                                    }
                                else:
                                    # Empty DataFrame
                                    result_data = []
                                    result_metadata = {
                                        'total_rows': 0,
                                        'columns': list(result.columns),
                                        'data_types': {},
                                        'error_recovered': True,
                                        'ambiguous_handled': True
                                    }
                            except Exception as shape_error:
                                logger.error(f"Error using shape-based DataFrame handling: {shape_error}")
                                # Final fallback to string representation
                                result_data = f"DataFrame processing error: {str(shape_error)}"
                                result_metadata = {
                                    'type': 'string',
                                    'error': str(e),
                                    'shape_error': str(shape_error),
                                    'fallback': True
                                }
                        else:
                            result_data = str(result)
                            result_metadata = {'type': 'string', 'error': str(e), 'recovered': False}
                    except Exception as recovery_error:
                        logger.error(f"Failed to recover from DataFrame error: {recovery_error}")
                        result_data = "Error processing query results"
                        result_metadata = {'type': 'string', 'error': str(e), 'recovery_error': str(recovery_error)}
                else:
                    result_data = str(result)
                    result_metadata = {'type': 'string', 'error': str(e)}
            
            request.session[f'query_result_{result_session_id}'] = {
                'sql_query': sql_or_clarification,
                'natural_query': natural_query,
                'data_source_id': str(data_source.id),
                'result_data': safe_session_data(result_data),
                'result_metadata': result_metadata,
                'timestamp': time.time(),  # For cleanup
                'cached': use_cache
            }
            
            return JsonResponse({
                'success': True,
                'result_id': result_session_id,
                'redirect_url': f'/query/result/{result_session_id}/',
                'row_count': result_metadata.get('total_rows', 0)
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            # Clean up any partial session data that might contain DataFrames
            try:
                if hasattr(e, 'args') and 'truth value of a DataFrame is ambiguous' in str(e):
                    logger.error("DataFrame ambiguous error detected - cleaning up session data")
                    # Remove any potential problematic session keys
                    cleanup_count = cleanup_corrupted_sessions(request)
                    logger.info(f"Cleaned up {cleanup_count} corrupted sessions")
            except Exception as cleanup_error:
                logger.error(f"Error during cleanup: {cleanup_error}")
            return JsonResponse({'error': 'Internal server error'}, status=500)
    
    def _paginate_result(self, result, page_size: int = 1000) -> Dict[str, Any]:
        """Paginate query results for better performance"""
        try:
            if isinstance(result, pd.DataFrame):
                total_rows = len(result)
                if total_rows <= page_size:
                    # Safely convert DataFrame to records with enhanced error handling
                    try:
                        records_data = result.to_dict('records')
                        # Apply safe serialization to the records
                        safe_records = []
                        for record in records_data:
                            safe_record = {}
                            for key, value in record.items():
                                try:
                                    # Apply the same safe value processing as in main result handler
                                    if value is None:
                                        safe_record[key] = None
                                    elif isinstance(value, pd.DataFrame):
                                        safe_record[key] = f"DataFrame({value.shape[0]}x{value.shape[1]})"
                                    elif isinstance(value, pd.Series):
                                        safe_record[key] = value.tolist()
                                    elif not isinstance(value, str) and hasattr(value, 'dtype') and hasattr(value, '__len__'):
                                        # Fixed: Reordered conditions to avoid DataFrame boolean context issue
                                        if isinstance(value, np.ndarray):
                                            safe_record[key] = value.tolist() if value.size <= 100 else f"Array(shape={value.shape})"
                                        else:
                                            safe_record[key] = str(value)
                                    elif isinstance(value, (float, np.floating)):
                                        # Type-safe NaN check for floating point values
                                        try:
                                            if pd.isna(value):
                                                safe_record[key] = None
                                            else:
                                                safe_record[key] = value
                                        except (TypeError, ValueError):
                                            safe_record[key] = value
                                    elif isinstance(value, (np.integer, np.floating)):
                                        safe_record[key] = float(value)
                                    else:
                                        safe_record[key] = value
                                except Exception as value_error:
                                    logger.warning(f"Error processing pagination value {key}={value}: {value_error}")
                                    safe_record[key] = str(value) if value is not None else None
                            safe_records.append(safe_record)
                        
                        return {
                            'data': safe_records,
                            'page_info': {
                                'total_rows': total_rows,
                                'page_size': page_size,
                                'total_pages': 1,
                                'current_page': 1
                            }
                        }
                    except Exception as records_error:
                        logger.error(f"Error converting DataFrame to records in pagination: {records_error}")
                        # Fallback to string representation
                        return {
                            'data': f"DataFrame with {total_rows} rows (conversion error)",
                            'page_info': {
                                'total_rows': total_rows,
                                'page_size': page_size,
                                'total_pages': 1,
                                'current_page': 1,
                                'error': str(records_error)
                            }
                        }
                else:
                    # Return first page with safe conversion
                    try:
                        first_page = result.head(page_size)
                        first_page_records = first_page.to_dict('records')
                        # Apply safe serialization to the first page records
                        safe_first_page = []
                        for record in first_page_records:
                            safe_record = {}
                            for key, value in record.items():
                                try:
                                    # Apply the same safe value processing
                                    if value is None:
                                        safe_record[key] = None
                                    elif isinstance(value, pd.DataFrame):
                                        safe_record[key] = f"DataFrame({value.shape[0]}x{value.shape[1]})"
                                    elif isinstance(value, pd.Series):
                                        safe_record[key] = value.tolist()
                                    elif not isinstance(value, str) and hasattr(value, 'dtype') and hasattr(value, '__len__'):
                                        # Fixed: Reordered conditions to avoid DataFrame boolean context issue
                                        if isinstance(value, np.ndarray):
                                            safe_record[key] = value.tolist() if value.size <= 100 else f"Array(shape={value.shape})"
                                        else:
                                            safe_record[key] = str(value)
                                    elif isinstance(value, (float, np.floating)):
                                        # Type-safe NaN check for floating point values
                                        try:
                                            if pd.isna(value):
                                                safe_record[key] = None
                                            else:
                                                safe_record[key] = value
                                        except (TypeError, ValueError):
                                            safe_record[key] = value
                                    elif isinstance(value, (np.integer, np.floating)):
                                        safe_record[key] = float(value)
                                    else:
                                        safe_record[key] = value
                                except Exception as value_error:
                                    logger.warning(f"Error processing pagination value {key}={value}: {value_error}")
                                    safe_record[key] = str(value) if value is not None else None
                            safe_first_page.append(safe_record)
                        
                        return {
                            'data': safe_first_page,
                            'page_info': {
                                'total_rows': total_rows,
                                'page_size': page_size,
                                'total_pages': (total_rows + page_size - 1) // page_size,
                                'current_page': 1,
                                'has_more': total_rows > page_size
                            }
                        }
                    except Exception as page_error:
                        logger.error(f"Error processing first page in pagination: {page_error}")
                        # Fallback to string representation
                        return {
                            'data': f"DataFrame first page (conversion error): {total_rows} total rows",
                            'page_info': {
                                'total_rows': total_rows,
                                'page_size': page_size,
                                'total_pages': (total_rows + page_size - 1) // page_size,
                                'current_page': 1,
                                'has_more': total_rows > page_size,
                                'error': str(page_error)
                            }
                        }
            else:
                # Handle non-DataFrame results (avoid DataFrame boolean context)
                try:
                    # Fixed: Explicit check to avoid DataFrame ambiguous truth value error
                    if isinstance(result, pd.DataFrame):
                        # This should have been caught above, but handle it safely
                        total_items = len(result)
                    elif hasattr(result, '__len__') and not isinstance(result, str):
                        total_items = len(result)
                    else:
                        total_items = 1
                        result = [result]  # Wrap single item in list
                    
                    return {
                        'data': result,
                        'page_info': {'total_rows': total_items, 'page_size': page_size, 'total_pages': 1, 'current_page': 1}
                    }
                except Exception:
                    # Fallback for any other type
                    return {
                        'data': str(result),
                        'page_info': {'total_rows': 1, 'page_size': page_size, 'total_pages': 1, 'current_page': 1}
                    }
                
        except Exception as e:
            logger.error(f"Error paginating result: {e}")
            return {
                'data': str(result),
                'page_info': {'error': str(e)}
            }


@method_decorator(login_required, name='dispatch')
@method_decorator(viewer_or_creator_required, name='dispatch')
class QueryResultView(View):
    """Display query results with visualizations"""
    
    def get(self, request, result_id):
        """Display query results"""
        try:
            # Get result from session
            session_key = f'query_result_{result_id}'
            logger.info(f"Looking for session key: {session_key}")
            
            if session_key not in request.session:
                logger.warning(f"Session key {session_key} not found. Available keys: {list(request.session.keys())}")
                messages.error(request, "Query result not found or expired")
                return redirect('core:query')
            
            try:
                result_data = request.session[session_key]
                logger.info(f"Successfully retrieved session data with keys: {list(result_data.keys()) if isinstance(result_data, dict) else type(result_data)}")
            except (UnicodeDecodeError, ValueError) as session_error:
                # Handle corrupted session data (e.g., binary data stored as text)
                logger.error(f"Corrupted session data for {session_key}: {session_error}")
                # Clean up the corrupted session data
                try:
                    del request.session[session_key]
                except:
                    pass
                messages.error(request, "Query result data is corrupted. Please try your query again.")
                return redirect('core:query')
            except Exception as unexpected_error:
                logger.error(f"Unexpected error retrieving session data: {unexpected_error}", exc_info=True)
                messages.error(request, f"Unexpected error: {str(unexpected_error)}")
                return redirect('core:query')
            
            # Initialize formatted_data with default empty structure
            formatted_data = {
                'columns': [],
                'rows': [],
                'total_rows': 0,
                'truncated': False
            }
            
            # Handle result data based on its format
            if isinstance(result_data['result_data'], list) and result_data['result_data']:
                # Simple list of records - format directly without pandas
                records = result_data['result_data']
                if records and isinstance(records[0], dict):
                    # Extract columns from first record
                    columns = list(records[0].keys())
                    # Extract rows as lists
                    rows = [[record[col] for col in columns] for record in records]
                    
                    formatted_data = {
                        'columns': columns,
                        'rows': rows,
                        'total_rows': len(records),
                        'truncated': False
                    }
                    logger.info(f"Formatted data with {len(columns)} columns and {len(rows)} rows")
                else:
                    # Empty or invalid data - keep default
                    logger.info("Empty or invalid result data")
            else:
                # Non-list data or empty - try to handle other formats
                logger.info(f"Result data is not a list or is empty: {type(result_data.get('result_data'))}")
                
                # Try to handle other data formats (like pandas DataFrame, etc.)
                try:
                    import pandas as pd
                    raw_data = result_data['result_data']
                    
                    if isinstance(raw_data, pd.DataFrame):
                        if not raw_data.empty:
                            columns = raw_data.columns.tolist()
                            rows = raw_data.values.tolist()
                            formatted_data = {
                                'columns': columns,
                                'rows': rows,
                                'total_rows': len(rows),
                                'truncated': False
                            }
                            logger.info(f"Formatted DataFrame with {len(columns)} columns and {len(rows)} rows")
                    elif isinstance(raw_data, dict):
                        # Single record
                        columns = list(raw_data.keys())
                        rows = [list(raw_data.values())]
                        formatted_data = {
                            'columns': columns,
                            'rows': rows,
                            'total_rows': 1,
                            'truncated': False
                        }
                        logger.info("Formatted single record as table")
                    elif isinstance(raw_data, str):
                        # String result - create simple display
                        formatted_data = {
                            'columns': ['Result'],
                            'rows': [[raw_data]],
                            'total_rows': 1,
                            'truncated': False
                        }
                        logger.info("Formatted string result")
                except Exception as format_error:
                    logger.warning(f"Error formatting non-list data: {format_error}")
                    # Keep default empty structure
            
            # Skip backend chart creation to avoid binary data issues
            # Charts will be generated on the frontend using Plotly.js
            chart_data_json = None
            logger.info("Skipping backend chart creation - using frontend generation")
            
            # Test context data for binary issues before rendering
            context = {
                'result_id': result_id,
                'sql_query': result_data['sql_query'],
                'natural_query': result_data['natural_query'],
                'data': formatted_data,
                'chart_data': chart_data_json,
                'has_data': formatted_data['total_rows'] > 0
            }
            
            # Test if context is JSON serializable
            try:
                json.dumps(context)
                logger.info("Context is JSON serializable - proceeding to render")
            except Exception as context_error:
                logger.error(f"Context contains non-serializable data: {context_error}")
                # Try to render with minimal context
                context = {
                    'result_id': result_id,
                    'sql_query': 'Test query',
                    'natural_query': 'Test data',
                    'data': {'columns': ['Category', 'Value'], 'rows': [['Test', 100]], 'total_rows': 1, 'truncated': False},
                    'chart_data': None,
                    'has_data': True
                }
            
            try:
                logger.info("About to render template...")
                # Use simple template to avoid binary data issues
                response = render(request, 'core/query_result_simple.html', context)
                logger.info("Template rendered successfully!")
                return response
            except Exception as render_error:
                logger.error(f"Template rendering failed: {render_error}", exc_info=True)
                messages.error(request, f"Failed to render results: {str(render_error)}")
                return redirect('core:query')
            
        except Exception as e:
            logger.error(f"Error displaying query result: {e}")
            messages.error(request, "Failed to display query results")
            return redirect('core:query')
    
    def post(self, request, result_id):
        """Handle chart type changes or dashboard additions"""
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            # Get result from session
            session_key = f'query_result_{result_id}'
            if session_key not in request.session:
                return JsonResponse({'error': 'Query result not found'}, status=404)
            
            try:
                result_data = request.session[session_key]
            except (UnicodeDecodeError, ValueError) as session_error:
                # Handle corrupted session data (e.g., binary data stored as text)
                logger.error(f"Corrupted session data for {session_key}: {session_error}")
                # Clean up the corrupted session data
                try:
                    del request.session[session_key]
                except:
                    pass
                return JsonResponse({'error': 'Query result data is corrupted. Please try your query again.'}, status=400)
            
            df = pd.DataFrame(result_data['result_data'])
            
            if action == 'change_chart':
                chart_type = data.get('chart_type', 'bar')
                x_column = data.get('x_column')
                y_column = data.get('y_column')
                
                chart_data = create_plotly_figure(
                    df, 
                    chart_type=chart_type,
                    x_column=x_column,
                    y_column=y_column,
                    title=f"Results for: {result_data['natural_query']}"
                )
                
                return JsonResponse({
                    'chart_data': make_json_serializable(chart_data)
                })
                
            elif action == 'add_to_dashboard':
                # Implement dashboard integration
                dashboard_name = data.get('dashboard_name', '').strip()
                dashboard_id = data.get('dashboard_id')
                chart_title = data.get('chart_title', result_data.get('natural_query', 'Chart'))
                chart_description = data.get('chart_description', '')
                chart_type = data.get('chart_type', 'bar')
                
                # Fix: Map frontend chart types to Django model choices
                chart_type_mapping = {
                    'bar': 'bar',
                    'Bar Chart': 'bar',
                    'line': 'line', 
                    'Line Chart': 'line',
                    'pie': 'pie',
                    'Pie Chart': 'pie',
                    'scatter': 'scatter',
                    'Scatter Plot': 'scatter',
                    'histogram': 'histogram',
                    'Histogram': 'histogram',
                    'heatmap': 'heatmap',
                    'treemap': 'treemap',
                    'gauge': 'gauge'
                }
                
                # Normalize chart type to match Django model choices
                normalized_chart_type = chart_type_mapping.get(chart_type, 'bar')
                
                try:
                    from dashboards.models import Dashboard, DashboardItem, DashboardShare
                    from dashboards.views import get_dashboard_permissions
                    
                    # Get or create dashboard
                    if dashboard_id and dashboard_id != 'new':
                        # Use existing dashboard - check permissions
                        try:
                            dashboard = Dashboard.objects.get(id=dashboard_id)
                            
                            # Check if user has edit permissions
                            has_access, can_edit, permission_level = get_dashboard_permissions(dashboard, request.user)
                            
                            if not has_access:
                                return JsonResponse({'error': 'Dashboard not found or access denied'}, status=404)
                            
                            if not can_edit:
                                return JsonResponse({'error': 'You do not have edit permissions for this dashboard'}, status=403)
                                
                        except Dashboard.DoesNotExist:
                            return JsonResponse({'error': 'Dashboard not found'}, status=404)
                    else:
                        # Create new dashboard
                        if not dashboard_name:
                            dashboard_name = f"Dashboard {timezone.now().strftime('%Y-%m-%d %H:%M')}"
                        
                        dashboard = Dashboard.objects.create(
                            name=dashboard_name,
                            description=f"Dashboard for analyzing {chart_title.lower()}",
                            owner=request.user
                        )
                    
                    # Determine chart configuration based on data
                    chart_config = {
                        'chart_type': normalized_chart_type,
                        'title': chart_title,
                        'description': chart_description
                    }
                    
                    # Extract chart parameters from data
                    if isinstance(result_data['result_data'], list) and result_data['result_data']:
                        first_record = result_data['result_data'][0]
                        columns = list(first_record.keys())
                        
                        if len(columns) >= 2:
                            chart_config['x_column'] = columns[0]
                            chart_config['y_column'] = columns[1]
                        elif len(columns) == 1:
                            # Single value - create KPI
                            chart_config['chart_type'] = 'gauge'
                            chart_config['value_column'] = columns[0]
                            normalized_chart_type = 'gauge'
                    
                    # Find next available position
                    existing_items = DashboardItem.objects.filter(dashboard=dashboard)
                    max_y = existing_items.aggregate(models.Max('position_y'))['position_y__max'] or 0
                    
                    # Create dashboard item with proper validation
                    dashboard_item = DashboardItem.objects.create(
                        dashboard=dashboard,
                        title=chart_title,
                        item_type='chart',
                        chart_type=normalized_chart_type,  # Use normalized chart type
                        query=result_data.get('sql_query', ''),
                        chart_config=chart_config,
                        data_source=result_data.get('data_source_id', ''),
                        position_x=0,
                        position_y=max_y + 1,
                        width=6,
                        height=4
                    )
                    
                    # Log successful creation for debugging
                    logger.info(f"Dashboard item created successfully: {dashboard_item.id} in dashboard {dashboard.id}")
                    
                    return JsonResponse({
                        'success': True,
                        'message': f'Chart added to dashboard "{dashboard.name}"',
                        'dashboard_id': str(dashboard.id),
                        'dashboard_name': dashboard.name,
                        'item_id': str(dashboard_item.id)
                    })
                    
                except Exception as dashboard_error:
                    logger.error(f"Error adding to dashboard: {dashboard_error}")
                    return JsonResponse({'error': f'Failed to add to dashboard: {str(dashboard_error)}'}, status=500)
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error handling query result action: {e}")
            return JsonResponse({'error': 'Internal server error'}, status=500)


@method_decorator(login_required, name='dispatch')
@method_decorator(viewer_or_creator_required, name='dispatch')
class ClarificationView(View):
    """Handle LLM clarification flow"""
    
    def post(self, request, session_id):
        """Process clarification response"""
        try:
            data = json.loads(request.body)
            clarification_response = data.get('response', '').strip()
            
            if not clarification_response:
                return JsonResponse({'error': 'Clarification response is required'}, status=400)
            
            # Get query context from session
            context_key = f'query_context_{session_id}'
            if context_key not in request.session:
                return JsonResponse({'error': 'Session expired'}, status=404)
            
            query_context = request.session[context_key]
            
            # Get data source
            data_source = DataSource.objects.get(id=query_context['data_source_id'])
            
            # Initialize services
            semantic_service = SemanticService()
            data_service = DataService()
            
            # Create enhanced query with clarification
            enhanced_query = f"""
Based on the user's clarification, please generate a specific SQL query:

ORIGINAL REQUEST: {query_context['original_query']}
USER CLARIFICATION: {clarification_response}

INSTRUCTIONS:
- Generate a complete SQL SELECT statement
- Use the clarification to resolve any ambiguities
- Focus on creating executable SQL, not asking more questions
- If the user wants "top 3 customers by total sales amount in South region", generate SQL to show exactly that

Please respond with ONLY the SQL query, no explanations or questions.
"""
            
            # Process enhanced query with debugging
            logger.info(f"Processing clarification for session {session_id}")
            logger.info(f"Enhanced query: {enhanced_query}")
            logger.info(f"Schema info available: {bool(query_context.get('schema_info'))}")
            
            success, sql_query, clarification_needed = semantic_service.get_enhanced_sql_from_openai(
                enhanced_query, query_context['schema_info'], data_source.source_type
            )
            
            logger.info(f"Clarification processing result - Success: {success}, SQL: {sql_query[:100] if sql_query else 'None'}, Clarification: {clarification_needed}")
            
            # Check if we got empty SQL even with success=True (common with clarification responses)
            if success and (not sql_query or sql_query.strip() == ''):
                logger.info("Got success=True but empty SQL, triggering template fallback")
                success = False  # Force fallback logic
                sql_query = "Empty SQL returned from semantic service"
            
            # At this point, if success is True, we should have valid SQL (either from LLM or template)
            if not success:
                # Check if this is another clarification request
                if sql_query and "CLARIFICATION_NEEDED:" in sql_query:
                    clarification_text = sql_query.replace("CLARIFICATION_NEEDED:", "").strip()
                    return JsonResponse({
                        'needs_clarification': True,
                        'clarification_question': clarification_text,
                        'session_id': session_id
                    })
                else:
                    # Try direct template-based generation as last resort
                    logger.info(f"Attempting template-based SQL generation for: {query_context['original_query']} + {clarification_response}")
                    
                    combined_query = f"{query_context['original_query']} {clarification_response}"
                    template_sql = semantic_service._try_template_sql_generation(combined_query, query_context['schema_info'])
                    
                    if template_sql:
                        logger.info(f"Template-based SQL generated: {template_sql}")
                        sql_query = template_sql
                        success = True
                    else:
                        # Try template generation with just the original query
                        logger.info("Trying template generation with original query only")
                        fallback_sql = semantic_service._try_template_sql_generation(query_context['original_query'], query_context['schema_info'])
                        
                        if fallback_sql:
                            logger.info(f"Fallback template SQL generated: {fallback_sql}")
                            sql_query = fallback_sql
                            success = True
                        else:
                            return JsonResponse({
                                'error': 'Unable to generate SQL query after trying all methods.',
                                'details': f"Original: {query_context['original_query']}, Clarification: {clarification_response}"
                            }, status=400)
            
            # Final check - if we still don't have success, return error
            if not success:
                return JsonResponse({
                    'error': 'Unable to generate SQL query after trying all methods.',
                    'details': f"Original: {query_context['original_query']}, Clarification: {clarification_response}"
                }, status=400)
            
            # Proceed with SQL execution if we have valid SQL
            logger.info(f"Proceeding with SQL execution: {sql_query}")
            
            # Execute SQL query
            success, result = data_service.execute_query(
                sql_query, 
                data_source.connection_info, 
                request.user.id
            )
            
            if not success:
                error_info = handle_query_error(Exception(result))
                return JsonResponse({
                    'error': 'Query execution failed',
                    'error_info': error_info,
                    'sql_query': sql_query
                }, status=400)
            
            # Create query result session
            result_session_id = str(uuid.uuid4())
            
            # Safely convert result to serializable format
            try:
                if isinstance(result, pd.DataFrame):
                    if not result.empty:
                        # Convert DataFrame to JSON-serializable format
                        result_dict = result.to_dict('records')
                        # Ensure all values are JSON serializable
                        result_data = []
                        for row in result_dict:
                            serializable_row = {}
                            for key, value in row.items():
                                # Enhanced type checking to avoid DataFrame ambiguous error
                                try:
                                    if value is None:
                                        serializable_row[key] = None
                                    elif isinstance(value, pd.DataFrame):
                                        # Handle DataFrame values by converting to string
                                        serializable_row[key] = f"DataFrame({value.shape[0]}x{value.shape[1]})"
                                    elif isinstance(value, pd.Series):
                                        # Handle Series values by converting to list
                                        serializable_row[key] = value.tolist()
                                    elif not isinstance(value, str) and hasattr(value, 'dtype') and hasattr(value, '__len__'):
                                        # Fixed: Reordered conditions to avoid DataFrame boolean context issue
                                        if isinstance(value, np.ndarray):
                                            serializable_row[key] = value.tolist() if value.size <= 100 else f"Array(shape={value.shape})"
                                        else:
                                            serializable_row[key] = str(value)
                                    elif isinstance(value, (float, np.floating)):
                                        # Safe NaN checking for float values
                                        import math
                                        if isinstance(value, float):
                                            # Use math.isnan for standard float types
                                            if math.isnan(value):
                                                serializable_row[key] = None
                                            else:
                                                serializable_row[key] = float(value)
                                        elif hasattr(value, 'dtype') and np.issubdtype(value.dtype, np.floating):
                                            # Use numpy.isnan for numpy float types
                                            if np.isnan(value):
                                                serializable_row[key] = None
                                            else:
                                                serializable_row[key] = float(value)
                                        else:
                                            # Fallback conversion
                                            try:
                                                serializable_row[key] = float(value) if value is not None else None
                                            except (ValueError, TypeError):
                                                serializable_row[key] = None
                                    elif isinstance(value, (np.integer, np.floating)):
                                        serializable_row[key] = float(value)
                                    else:
                                        # For any other type, convert safely to string
                                        serializable_row[key] = value
                                except Exception as value_error:
                                    logger.warning(f"Error processing value {key}={value}: {value_error}")
                                    # Fallback to string representation
                                    serializable_row[key] = str(value) if value is not None else None
                            result_data.append(serializable_row)
                    else:
                        result_data = []
                elif hasattr(result, 'to_dict'):
                    result_data = result.to_dict()
                else:
                    result_data = str(result)
            except Exception as e:
                logger.error(f"Error converting clarification result to dict: {e}")
                result_data = str(result)
            
            # ENHANCED: Create complete result metadata for chart generation and dashboard integration
            try:
                if isinstance(result, pd.DataFrame):
                    if not result.empty:
                        result_metadata = {
                            'total_rows': len(result),
                            'columns': [str(col) for col in result.columns],
                            'data_types': {str(col): str(result[col].dtype) for col in result.columns},
                            'page_info': {
                                'total_rows': len(result),
                                'page_size': 1000,
                                'total_pages': (len(result) + 999) // 1000,
                                'current_page': 1
                            }
                        }
                    else:
                        result_metadata = {'total_rows': 0, 'columns': [], 'data_types': {}}
                elif hasattr(result, 'to_dict'):
                    result_metadata = {'type': 'dict'}
                else:
                    result_metadata = {'type': 'string'}
            except Exception as metadata_error:
                logger.error(f"Error creating result metadata: {metadata_error}")
                result_metadata = {'type': 'string', 'error': str(metadata_error)}
            
            # Create enhanced natural query that includes clarification context
            enhanced_natural_query = f"{query_context['original_query']} (clarified: {clarification_response})"
            
            request.session[f'query_result_{result_session_id}'] = {
                'sql_query': sql_query,
                'natural_query': enhanced_natural_query,
                'data_source_id': query_context['data_source_id'],
                'result_data': safe_session_data(result_data),
                'result_metadata': result_metadata,  # ADDED: Complete metadata for charts
                'clarification_used': clarification_response,
                'timestamp': time.time(),  # ADDED: For cleanup
                'cached': False  # ADDED: Clarification queries are not cached
            }
            
            # Clean up context session
            del request.session[context_key]
            
            return JsonResponse({
                'success': True,
                'result_id': result_session_id,
                'redirect_url': f'/query/result/{result_session_id}/'
            })
            
        except Exception as e:
            logger.error(f"Error processing clarification: {e}")
            return JsonResponse({'error': 'Internal server error'}, status=500)


@method_decorator(login_required, name='dispatch')
class DataSourceView(View):
    """Manage data source connections"""
    
    def get(self, request):
        """List available data sources"""
        try:
            data_sources = DataSource.objects.filter(status='active')
            data_sources_list = []
            
            for ds in data_sources:
                data_sources_list.append({
                    'id': str(ds.id),
                    'name': ds.name,
                    'type': ds.source_type,
                    'status': 'active' if ds.status == 'active' else 'inactive',
                    'created_at': ds.created_at.isoformat()
                })
            
            return JsonResponse({
                'data_sources': data_sources_list
            })
            
        except Exception as e:
            logger.error(f"Error fetching data sources: {e}")
            return JsonResponse({'error': 'Failed to fetch data sources'}, status=500)
    
    def post(self, request):
        """Test data source connection"""
        try:
            data = json.loads(request.body)
            data_source_id = data.get('data_source_id')
            
            if not data_source_id:
                return JsonResponse({'error': 'Data source ID is required'}, status=400)
            
            # Get data source
            data_source = DataSource.objects.get(id=data_source_id, status='active')
            
            # Test connection
            data_service = DataService()
            success, message = data_service.test_connection(data_source.connection_info)
            
            return JsonResponse({
                'success': success,
                'message': message
            })
            
        except DataSource.DoesNotExist:
            return JsonResponse({'error': 'Data source not found'}, status=404)
        except Exception as e:
            logger.error(f"Error testing data source connection: {e}")
            return JsonResponse({'error': 'Failed to test connection'}, status=500)


@method_decorator(login_required, name='dispatch')
class QueryHistoryView(View):
    """Query history page view"""
    
    def get(self, request):
        """Display query history page"""
        try:
            # Get user's query history
            queries = QueryLog.objects.filter(user=request.user).order_by('-created_at')[:50]
            
            context = {
                'queries': queries,
                'title': 'Query History'
            }
            
            return render(request, 'core/query_history.html', context)
            
        except Exception as e:
            logger.error(f"Error loading query history page: {e}")
            messages.error(request, "Failed to load query history")
            return redirect('core:query')


@method_decorator(login_required, name='dispatch') 
class DataSourceInfoView(View):
    """Data source information API view"""
    
    def get(self, request, source_id):
        """Get detailed information about a data source"""
        try:
            # Get data source
            data_source = get_object_or_404(DataSource, id=source_id, status='active')
            
            # Initialize data service to get schema and sample data
            data_service = DataService()
            
            # Get schema information
            try:
                schema_info = data_service.get_schema_info(data_source.connection_info)
            except Exception as e:
                logger.warning(f"Failed to get schema for {source_id}: {e}")
                schema_info = {}
            
            # Prepare response data
            info = {
                'id': str(data_source.id),
                'name': data_source.name,
                'source_type': data_source.source_type,
                'status': data_source.status,
                'created_at': data_source.created_at.isoformat(),
                'updated_at': data_source.updated_at.isoformat(),
                'schema_info': schema_info,
                'sample_data': data_source.sample_data or {},
                'connection_status': 'connected' if data_source.status == 'active' else 'disconnected'
            }
            
            return JsonResponse({
                'success': True,
                'data_source': info
            })
            
        except DataSource.DoesNotExist:
            return JsonResponse({
                'success': False,
                'error': 'Data source not found'
            }, status=404)
        except Exception as e:
            logger.error(f"Error getting data source info for {source_id}: {e}")
            return JsonResponse({
                'success': False,
                'error': 'Failed to get data source information'
            }, status=500)


@login_required
@creator_required
def query_history(request):
    """Get user's query history"""
    try:
        queries = QueryLog.objects.filter(user=request.user).order_by('-created_at')[:20]
        
        history = []
        for query in queries:
            history.append({
                'id': query.pk,
                'query': query.natural_query[:100] + '...' if len(query.natural_query) > 100 else query.natural_query,
                'status': query.status,
                'created_at': query.created_at.isoformat(),
                'execution_time': query.execution_time
            })
        
        return JsonResponse({'history': history})
        
    except Exception as e:
        logger.error(f"Error fetching query history: {e}")
        return JsonResponse({'error': 'Failed to fetch query history'}, status=500)


@login_required
@admin_required
def llm_config(request):
    """LLM Configuration view for admins"""
    
    context = {
        'title': 'LLM Configuration',
        'message': 'LLM Configuration interface coming soon...'
    }
    
    return render(request, 'core/llm_config.html', context)


@login_required
@admin_required
def test_openai_connection(request):
    """Test OpenAI API connection"""
    
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        api_key = data.get('api_key')
        model = data.get('model', 'gpt-3.5-turbo')
        
        if not api_key:
            return JsonResponse({'error': 'API key is required'}, status=400)
        
        # Test OpenAI connection
        import openai
        openai.api_key = api_key
        
        try:
            # Make a simple test request
            client = openai.OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "Hello, this is a connection test."}],
                max_tokens=10
            )
            
            return JsonResponse({
                'success': True,
                'message': f'✅ Connected to OpenAI {model} successfully!',
                'model_info': {
                    'model': model,
                    'response_time': 'Fast',
                    'status': 'Active'
                }
            })
            
        except openai.AuthenticationError:
            return JsonResponse({
                'success': False,
                'message': '❌ Authentication failed. Please check your API key.'
            })
        except openai.RateLimitError:
            return JsonResponse({
                'success': False,
                'message': '❌ Rate limit exceeded. Please try again later.'
            })
        except openai.APIError as e:
            return JsonResponse({
                'success': False,
                'message': f'❌ OpenAI API error: {str(e)}'
            })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'❌ Connection failed: {str(e)}'
            })
            
    except ImportError:
        return JsonResponse({
            'success': False,
            'message': '❌ OpenAI library not installed. Run: pip install openai'
        })
    except Exception as e:
        logger.error(f"Error testing OpenAI connection: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@admin_required
def test_ollama_connection(request):
    """Test Ollama server connection"""
    
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        server_url = data.get('server_url', 'http://localhost:11434')
        model = data.get('model', 'sqlcoder:15b')
        timeout = int(data.get('timeout', 60))
        
        import requests
        
        # Test 1: Check if Ollama server is running
        try:
            health_response = requests.get(f"{server_url}/api/version", timeout=10)
            if health_response.status_code != 200:
                return JsonResponse({
                    'success': False,
                    'message': f'❌ Ollama server not responding at {server_url}'
                })
        except requests.exceptions.ConnectionError:
            return JsonResponse({
                'success': False,
                'message': f'❌ Cannot connect to Ollama server at {server_url}. Make sure Ollama is running.'
            })
        except requests.exceptions.Timeout:
            return JsonResponse({
                'success': False,
                'message': '❌ Connection timeout. Ollama server is not responding.'
            })
        
        # Test 2: Check if model is available
        try:
            models_response = requests.get(f"{server_url}/api/tags", timeout=10)
            if models_response.status_code == 200:
                available_models = models_response.json().get('models', [])
                model_names = [m.get('name', '') for m in available_models]
                
                if model not in model_names:
                    return JsonResponse({
                        'success': False,
                        'message': f'❌ Model "{model}" not found. Available models: {", ".join(model_names[:5])}',
                        'available_models': model_names
                    })
        except Exception as e:
            logger.warning(f"Could not check available models: {e}")
        
        # Test 3: Make a simple generation request
        try:
            test_payload = {
                "model": model,
                "prompt": "SELECT",
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "top_p": 0.9
                }
            }
            
            generation_response = requests.post(
                f"{server_url}/api/generate",
                json=test_payload,
                timeout=timeout
            )
            
            if generation_response.status_code == 200:
                result = generation_response.json()
                return JsonResponse({
                    'success': True,
                    'message': f'✅ Connected to Ollama successfully!',
                    'model_info': {
                        'server_url': server_url,
                        'model': model,
                        'status': 'Active',
                        'response_time': 'Good'
                    }
                })
            else:
                return JsonResponse({
                    'success': False,
                    'message': f'❌ Model generation failed. Status: {generation_response.status_code}'
                })
                
        except requests.exceptions.Timeout:
            return JsonResponse({
                'success': False,
                'message': f'❌ Model generation timeout after {timeout} seconds. Try increasing timeout or use a smaller model.'
            })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'❌ Generation test failed: {str(e)}'
            })
            
    except Exception as e:
        logger.error(f"Error testing Ollama connection: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@admin_required
def get_ollama_models(request):
    """Get available Ollama models"""
    
    try:
        data = json.loads(request.body) if request.body else {}
        server_url = data.get('server_url', 'http://localhost:11434')
        
        import requests
        
        response = requests.get(f"{server_url}/api/tags", timeout=10)
        if response.status_code == 200:
            models_data = response.json().get('models', [])
            models = []
            
            for model in models_data:
                models.append({
                    'name': model.get('name', ''),
                    'size': model.get('size', 0),
                    'modified_at': model.get('modified_at', '')
                })
            
            return JsonResponse({
                'success': True,
                'models': models
            })
        else:
            return JsonResponse({
                'success': False,
                'message': 'Failed to fetch models from Ollama server'
            })
            
    except requests.exceptions.ConnectionError:
        return JsonResponse({
            'success': False,
            'message': 'Cannot connect to Ollama server'
        })
    except Exception as e:
        logger.error(f"Error fetching Ollama models: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@login_required
@admin_required
def save_ollama_config(request):
    """Save Ollama configuration to database"""
    
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        from core.models import LLMConfig
        
        data = json.loads(request.body)
        server_url = data.get('server_url', 'http://localhost:11434')
        model = data.get('model', 'sqlcoder:15b')
        timeout = data.get('timeout', 60)
        temperature = data.get('temperature', 0.1)
        top_p = data.get('top_p', 0.9)
        
        # Validate required fields
        if not server_url or not model:
            return JsonResponse({'error': 'Server URL and model are required'}, status=400)
        
        # Create or update LLM configuration
        llm_config, created = LLMConfig.objects.get_or_create(
            provider='local',
            defaults={
                'base_url': server_url,
                'model_name': model,
                'temperature': float(temperature),
                'max_tokens': 2000,
                'system_prompt': 'You are an expert SQL query generator. Convert natural language questions into accurate SQL queries.',
                'additional_settings': {
                    'timeout': int(timeout),
                    'top_p': float(top_p),
                    'provider_type': 'ollama'
                },
                'updated_by': request.user,
                'is_active': True
            }
        )
        
        if not created:
            # Update existing configuration
            llm_config.base_url = server_url
            llm_config.model_name = model
            llm_config.temperature = float(temperature)
            llm_config.additional_settings.update({
                'timeout': int(timeout),
                'top_p': float(top_p),
                'provider_type': 'ollama'
            })
            llm_config.updated_by = request.user
            llm_config.save()
        
        logger.info(f"Ollama configuration saved by {request.user.username}: {server_url}, {model}")
        
        return JsonResponse({
            'success': True,
            'message': f'✅ Ollama configuration saved successfully!',
            'config_id': str(llm_config.pk)
        })
        
    except Exception as e:
        logger.error(f"Error saving Ollama configuration: {e}")
        return JsonResponse({'error': f'Failed to save configuration: {str(e)}'}, status=500)


@csrf_exempt
@login_required
@admin_required
def save_openai_config(request):
    """Save OpenAI configuration to database"""
    
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        from core.models import LLMConfig
        
        data = json.loads(request.body)
        api_key = data.get('api_key', '')
        model = data.get('model', 'gpt-3.5-turbo')
        temperature = data.get('temperature', 0.1)
        max_tokens = data.get('max_tokens', 2000)
        timeout = data.get('timeout', 30)
        
        # Validate required fields
        if not api_key:
            return JsonResponse({'error': 'API key is required'}, status=400)
        
        # Create or update LLM configuration
        llm_config, created = LLMConfig.objects.get_or_create(
            provider='openai',
            defaults={
                'api_key': api_key,
                'model_name': model,
                'temperature': float(temperature),
                'max_tokens': int(max_tokens),
                'system_prompt': 'You are an expert SQL query generator. Convert natural language questions into accurate SQL queries based on the provided database schema.',
                'additional_settings': {
                    'timeout': int(timeout)
                },
                'updated_by': request.user,
                'is_active': True
            }
        )
        
        if not created:
            # Update existing configuration
            llm_config.api_key = api_key
            llm_config.model_name = model
            llm_config.temperature = float(temperature)
            llm_config.max_tokens = int(max_tokens)
            llm_config.additional_settings.update({
                'timeout': int(timeout)
            })
            llm_config.updated_by = request.user
            llm_config.save()
        
        logger.info(f"OpenAI configuration saved by {request.user.username}: {model}")
        
        return JsonResponse({
            'success': True,
            'message': f'✅ OpenAI configuration saved successfully!',
            'config_id': str(llm_config.pk)
        })
        
    except Exception as e:
        logger.error(f"Error saving OpenAI configuration: {e}")
        return JsonResponse({'error': f'Failed to save configuration: {str(e)}'}, status=500)


@login_required
def test_chart_generation(request):
    """Test chart generation without LLM dependency"""
    try:
        # Create test result session with pure Python data (avoid pandas)
        result_session_id = str(uuid.uuid4())
        
        # Create simple Python data structures (guaranteed session-safe)
        raw_data = [
            {'Category': 'Sales', 'Value': 100},
            {'Category': 'Marketing', 'Value': 80},
            {'Category': 'Support', 'Value': 60},
            {'Category': 'Engineering', 'Value': 120}
        ]
        
        logger.info(f"Created simple test data with {len(raw_data)} records")
        
        # Store session data with pure Python types only - in the format expected by template
        session_data = {
            'sql_query': 'SELECT Category, Value FROM test_data',
            'natural_query': 'Show me test data',
            'data_source_id': 'test',
            'result_data': raw_data  # Keep original for compatibility
        }
        
        # Test that session data is JSON serializable
        try:
            json.dumps(session_data)
            logger.info("Session data is JSON serializable")
        except Exception as json_error:
            logger.error(f"Session data is not JSON serializable: {json_error}")
            raise
        
        request.session[f'query_result_{result_session_id}'] = session_data
        
        logger.info(f"Test chart session created: {result_session_id}")
        logger.info(f"Session data keys: {list(session_data.keys())}")
        
        # Redirect to the full template instead of simple HTML to test the actual functionality
        return redirect(f'/query/result/{result_session_id}/')
        
    except Exception as e:
        logger.error(f"Test chart generation failed: {e}", exc_info=True)
        return HttpResponse(f"<h1>Error</h1><p>{str(e)}</p>")


@login_required
@admin_required
def email_config(request):
    """Email Configuration page for superadmin users"""
    
    try:
        from core.models import EmailConfig
        
        # Get current email configuration
        email_config = EmailConfig.get_active_config()
        
        # Email provider presets for easier configuration
        email_presets = {
            'gmail': {
                'name': 'Gmail',
                'smtp_host': 'smtp.gmail.com',
                'smtp_port': 587,
                'encryption': 'tls',
                'help_text': 'Use your Gmail address and an App Password (not your regular password)'
            },
            'outlook': {
                'name': 'Outlook/Hotmail',
                'smtp_host': 'smtp-mail.outlook.com',
                'smtp_port': 587,
                'encryption': 'tls',
                'help_text': 'Use your Outlook.com or Hotmail.com email address'
            },
            'yahoo': {
                'name': 'Yahoo Mail',
                'smtp_host': 'smtp.mail.yahoo.com',
                'smtp_port': 587,
                'encryption': 'tls',
                'help_text': 'Use your Yahoo email address and App Password'
            },
            'custom': {
                'name': 'Custom SMTP Server',
                'smtp_host': '',
                'smtp_port': 587,
                'encryption': 'tls',
                'help_text': 'Enter your custom SMTP server details'
            }
        }
        
        context = {
            'title': 'Email Configuration',
            'email_config': email_config,
            'email_presets': email_presets,
            'encryption_choices': EmailConfig.ENCRYPTION_CHOICES,
        }
        
        return render(request, 'core/email_config.html', context)
        
    except Exception as e:
        logger.error(f"Error loading email configuration page: {e}")
        messages.error(request, "Failed to load email configuration")
        return redirect('core:home')


@csrf_exempt
@login_required
@admin_required
def save_email_config(request):
    """Save email configuration to database"""
    
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        from core.models import EmailConfig
        
        data = json.loads(request.body)
        
        # Extract and validate required fields
        smtp_host = data.get('smtp_host', '').strip()
        smtp_port = data.get('smtp_port', 587)
        smtp_username = data.get('smtp_username', '').strip()
        smtp_password = data.get('smtp_password', '').strip()
        sender_email = data.get('sender_email', '').strip()
        sender_name = data.get('sender_name', 'ConvaBI System').strip()
        encryption = data.get('encryption', 'tls')
        timeout = data.get('timeout', 30)
        
        # Validate required fields
        if not all([smtp_host, smtp_username, smtp_password, sender_email]):
            return JsonResponse({
                'error': 'SMTP host, username, password, and sender email are required'
            }, status=400)
        
        # Validate email format
        from django.core.validators import validate_email
        try:
            validate_email(sender_email)
        except Exception:
            return JsonResponse({'error': 'Invalid sender email format'}, status=400)
        
        # Validate port
        try:
            smtp_port = int(smtp_port)
            if not (1 <= smtp_port <= 65535):
                raise ValueError()
        except (ValueError, TypeError):
            return JsonResponse({'error': 'Invalid SMTP port'}, status=400)
        
        # Validate timeout
        try:
            timeout = int(timeout)
            if timeout < 1:
                raise ValueError()
        except (ValueError, TypeError):
            return JsonResponse({'error': 'Invalid timeout value'}, status=400)
        
        # Create or update email configuration
        email_config, created = EmailConfig.objects.get_or_create(
            defaults={
                'smtp_host': smtp_host,
                'smtp_port': smtp_port,
                'smtp_username': smtp_username,
                'smtp_password': smtp_password,
                'sender_email': sender_email,
                'sender_name': sender_name,
                'encryption': encryption,
                'timeout': timeout,
                'updated_by': request.user,
                'is_active': True
            }
        )
        
        if not created:
            # Update existing configuration
            email_config.smtp_host = smtp_host
            email_config.smtp_port = smtp_port
            email_config.smtp_username = smtp_username
            email_config.smtp_password = smtp_password
            email_config.sender_email = sender_email
            email_config.sender_name = sender_name
            email_config.encryption = encryption
            email_config.timeout = timeout
            email_config.updated_by = request.user
            email_config.save()
        
        logger.info(f"Email configuration saved by {request.user.username}: {smtp_host}:{smtp_port}")
        
        return JsonResponse({
            'success': True,
            'message': f'✅ Email configuration saved successfully!',
            'config_id': str(email_config.pk)
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        logger.error(f"Error saving email configuration: {e}")
        return JsonResponse({'error': f'Failed to save configuration: {str(e)}'}, status=500)


@csrf_exempt
@login_required
@admin_required
def test_email_config(request):
    """Test email configuration"""
    
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)
    
    try:
        from core.models import EmailConfig
        from services.email_service import EmailService
        
        data = json.loads(request.body)
        test_email = data.get('test_email', '').strip()
        use_saved_config = data.get('use_saved_config', True)
        send_test_email = data.get('send_test_email', False)
        
        # Only require test_email if we're actually sending a test email
        if send_test_email and not test_email:
            return JsonResponse({'error': 'Test email address is required for sending test email'}, status=400)
        
        # For connection test, we don't need a real test email
        if not test_email:
            test_email = 'test@example.com'  # Dummy email for validation
        
        # Validate email format
        from django.core.validators import validate_email
        try:
            validate_email(test_email)
        except Exception:
            return JsonResponse({'error': 'Invalid test email format'}, status=400)
        
        if use_saved_config:
            # Test using saved configuration
            email_config = EmailConfig.get_active_config()
            if not email_config:
                return JsonResponse({
                    'error': 'No email configuration found. Please save configuration first.'
                }, status=400)
            
            # Test connection using the model's test method
            success, message = email_config.test_connection()
            
            if success:
                # If only testing connection (not sending email)
                if not send_test_email:
                    return JsonResponse({
                        'success': True,
                        'message': f'✅ SMTP connection test successful! Server: {email_config.smtp_host}:{email_config.smtp_port}'
                    })
                
                # If connection test passed, try sending a test email
                try:
                    email_service = EmailService()
                    subject = "ConvaBI Email Configuration Test"
                    body = f"""
                    <h2>Email Configuration Test</h2>
                    <p>This is a test email to verify your ConvaBI email configuration.</p>
                    <p><strong>Configuration Details:</strong></p>
                    <ul>
                        <li>SMTP Server: {email_config.smtp_host}:{email_config.smtp_port}</li>
                        <li>Encryption: {email_config.encryption.upper()}</li>
                        <li>Sender: {email_config.sender_name} &lt;{email_config.sender_email}&gt;</li>
                    </ul>
                    <p>If you received this email, your configuration is working correctly!</p>
                    <hr>
                    <p><small>Test conducted by: {request.user.username} at {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}</small></p>
                    """
                    
                    email_sent = email_service.send_dashboard_email(
                        recipient_email=test_email,
                        subject=subject,
                        body=body
                    )
                    
                    if email_sent:
                        return JsonResponse({
                            'success': True,
                            'message': f'✅ Email test successful! Test email sent to {test_email}'
                        })
                    else:
                        return JsonResponse({
                            'success': False,
                            'message': '❌ SMTP connection successful, but failed to send test email'
                        })
                        
                except Exception as email_error:
                    return JsonResponse({
                        'success': False,
                        'message': f'❌ SMTP connection successful, but email sending failed: {str(email_error)}'
                    })
            else:
                return JsonResponse({
                    'success': False,
                    'message': f'❌ Email configuration test failed: {message}'
                })
        else:
            # Test using form data without saving
            smtp_host = data.get('smtp_host', '').strip()
            smtp_port = data.get('smtp_port', 587)
            smtp_username = data.get('smtp_username', '').strip()
            smtp_password = data.get('smtp_password', '').strip()
            encryption = data.get('encryption', 'tls')
            timeout = data.get('timeout', 30)
            
            if not all([smtp_host, smtp_username, smtp_password]):
                return JsonResponse({
                    'error': 'SMTP host, username, and password are required for testing'
                }, status=400)
            
            try:
                import smtplib
                
                # Validate port and timeout
                smtp_port = int(smtp_port)
                timeout = int(timeout)
                
                # Test SMTP connection
                server = smtplib.SMTP(smtp_host, smtp_port, timeout=timeout)
                
                if encryption == 'tls':
                    server.starttls()
                elif encryption == 'ssl':
                    # For SSL, we need to use SMTP_SSL
                    server.quit()
                    server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=timeout)
                
                server.login(smtp_username, smtp_password)
                server.quit()
                
                return JsonResponse({
                    'success': True,
                    'message': f'✅ SMTP connection test successful! Server: {smtp_host}:{smtp_port}'
                })
                
            except Exception as test_error:
                return JsonResponse({
                    'success': False,
                    'message': f'❌ SMTP connection test failed: {str(test_error)}'
                })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        logger.error(f"Error testing email configuration: {e}")
        return JsonResponse({'error': f'Email test failed: {str(e)}'}, status=500)


@login_required
@admin_required
def get_email_status(request):
    """Get current email configuration status"""
    
    try:
        from core.models import EmailConfig
        
        email_config = EmailConfig.get_active_config()
        
        if email_config:
            status_data = {
                'configured': True,
                'verified': email_config.is_verified,
                'smtp_host': email_config.smtp_host,
                'smtp_port': email_config.smtp_port,
                'sender_email': email_config.sender_email,
                'encryption': email_config.encryption,
                'last_tested': email_config.last_tested.isoformat() if email_config.last_tested else None,
                'test_status': email_config.test_status,
                'updated_at': email_config.updated_at.isoformat(),
                'updated_by': email_config.updated_by.username if email_config.updated_by else None
            }
        else:
            status_data = {
                'configured': False,
                'verified': False,
                'message': 'No email configuration found'
            }
        
        return JsonResponse({
            'success': True,
            'status': status_data
        })
        
    except Exception as e:
        logger.error(f"Error getting email status: {e}")
        return JsonResponse({'error': str(e)}, status=500) 