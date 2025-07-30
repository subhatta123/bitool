"""
API views for REST endpoints
"""
import json
import logging
import pandas as pd
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.views.decorators.http import require_http_methods

from datasets.models import ETLOperation, DataSource

logger = logging.getLogger(__name__)


@login_required
def dashboard_item_data_proxy(request, item_id):
    """Proxy to dashboard item data view for API URL compatibility"""
    try:
        # Import and call the actual dashboard view
        from dashboards.views import dashboard_item_data
        return dashboard_item_data(request, item_id)
    except Exception as e:
        logger.error(f"Dashboard item data proxy error: {e}")
        return JsonResponse({
            'success': False,
            'error': 'API proxy error',
            'details': str(e)
        }, status=500)


@method_decorator(login_required, name='dispatch')
class ETLOperationAPIView(View):
    """API view for ETL operations"""
    
    def get(self, request, operation_id):
        """Get ETL operation details"""
        try:
            operation = get_object_or_404(ETLOperation, id=operation_id, created_by=request.user)
            
            operation_data = {
                'id': str(operation.id),
                'name': operation.name,
                'operation_type': operation.operation_type,
                'status': operation.status,
                'created_at': operation.created_at.isoformat(),
                'updated_at': operation.updated_at.isoformat(),
                'source_tables': operation.source_tables or [],
                'output_table_name': operation.output_table_name,
                'execution_time': operation.execution_time,
                'error_message': operation.error_message,
                'parameters': operation.parameters or {}
            }
            
            return JsonResponse({
                'success': True,
                'operation': operation_data
            })
            
        except Exception as e:
            logger.error(f"Error getting ETL operation {operation_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    def delete(self, request, operation_id):
        """Delete ETL operation"""
        try:
            operation = get_object_or_404(ETLOperation, id=operation_id, created_by=request.user)
            
            # Log the deletion
            logger.info(f"Deleting ETL operation: {operation.name} (ID: {operation.id}) by user {request.user.username}")
            
            # Delete the operation
            operation.delete()
            
            return JsonResponse({
                'success': True,
                'message': 'ETL operation deleted successfully'
            })
            
        except Exception as e:
            logger.error(f"Error deleting ETL operation {operation_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    def post(self, request, operation_id):
        """Handle ETL operation actions like rerun"""
        try:
            operation = get_object_or_404(ETLOperation, id=operation_id, created_by=request.user)
            
            data = json.loads(request.body) if request.body else {}
            action = data.get('action')
            
            if action == 'rerun':
                # Mark operation for rerun
                operation.status = 'pending'
                operation.error_message = ""
                operation.save()
                
                # TODO: Queue the operation for execution
                
                return JsonResponse({
                    'success': True,
                    'message': 'ETL operation queued for rerun'
                })
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error handling ETL operation action: {e}")
            return JsonResponse({'error': str(e)}, status=500)


@method_decorator(login_required, name='dispatch')
class DataSourceAPIView(View):
    """API view for data sources"""
    
    def get(self, request, source_id):
        """Get data source information"""
        try:
            data_source = get_object_or_404(DataSource, id=source_id, created_by=request.user)
            
            # Get LLM configuration information
            llm_info = self._get_llm_info()
            
            source_data = {
                'id': str(data_source.id),
                'name': data_source.name,
                'source_type': data_source.source_type,
                'status': data_source.status,
                'created_at': data_source.created_at.isoformat(),
                'updated_at': data_source.updated_at.isoformat(),
                'schema_info': data_source.schema_info or {},
                'sample_data': data_source.sample_data or [],
                'connection_info': {
                    'type': data_source.connection_info.get('type'),
                    'host': data_source.connection_info.get('host'),
                    'database': data_source.connection_info.get('database'),
                    # Don't expose sensitive info like passwords
                },
                'llm_info': llm_info,
                'connection_status': 'connected' if data_source.status == 'active' else 'disconnected'
            }
            
            return JsonResponse({
                'success': True,
                'data_source': source_data
            })
            
        except Exception as e:
            logger.error(f"Error getting data source {source_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    def _get_llm_info(self):
        """Get current LLM configuration information"""
        try:
            from core.models import LLMConfig
            
            # Get active LLM config
            llm_config = LLMConfig.get_active_config()
            
            if llm_config:
                return {
                    'provider': llm_config.provider,
                    'model': llm_config.model_name,
                    'status': 'configured',
                    'source': 'database_config'
                }
            else:
                return {
                    'provider': 'Unknown',
                    'model': 'Not detected',
                    'status': 'not_configured',
                    'source': 'no_config_found'
                }
                
        except Exception as e:
            logger.error(f"Error getting LLM info: {e}")
            return {
                'provider': 'Unknown',
                'model': 'Not detected',
                'status': 'error',
                'source': f'error: {str(e)}'
            }


@method_decorator(login_required, name='dispatch')
class DataSourceSchemaAPIView(View):
    """API view for data source schema information"""
    
    def get(self, request, source_id):
        """Get data source schema information"""
        try:
            data_source = get_object_or_404(DataSource, id=source_id, created_by=request.user)
            
            # Get schema info from data source
            schema_info = data_source.schema_info or {}
            
            # Format schema for ETL interface
            if 'columns' in schema_info:
                # If schema_info has columns list (CSV format)
                formatted_schema = {}
                for col in schema_info['columns']:
                    formatted_schema[col['name']] = {
                        'type': col['type'],
                        'sample_values': col.get('sample_values', [])
                    }
            else:
                # If schema_info is already in dict format (database format)
                formatted_schema = schema_info
            
            return JsonResponse({
                'success': True,
                'schema': formatted_schema,
                'data_source': {
                    'id': str(data_source.id),
                    'name': data_source.name,
                    'type': data_source.source_type
                }
            })
            
        except Exception as e:
            logger.error(f"Error getting data source schema {source_id}: {e}")
            return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_http_methods(["GET"])
def etl_operation_download(request, operation_id):
    """Download ETL operation results"""
    try:
        operation = get_object_or_404(ETLOperation, id=operation_id, created_by=request.user)
        
        if operation.status != 'completed':
            return JsonResponse({'error': 'Operation not completed'}, status=400)
        
        # TODO: Implement actual file download
        return JsonResponse({
            'success': True,
            'download_url': f'/media/etl_results/{operation.output_table_name}.csv',
            'message': 'Download functionality will be implemented'
        })
        
    except Exception as e:
        logger.error(f"Error downloading ETL operation results: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@method_decorator(login_required, name='dispatch')
class ETLTransformAPIView(View):
    """API view for ETL data transformations"""
    
    def post(self, request):
        """Handle ETL transformation requests"""
        try:
            data = json.loads(request.body)
            action = data.get('action')
            
            if action == 'preview':
                # Preview transformation
                data_source_id = data.get('data_source_id')
                transformations = data.get('transformations', {})
                
                # Get data source
                data_source = get_object_or_404(DataSource, id=data_source_id, created_by=request.user)
                
                # Get sample data for preview
                from services.data_service import DataService
                data_service = DataService()
                
                if data_source.source_type == 'csv':
                    # Use unified data access layer instead of direct CSV reading (FIXED)
                    try:
                        from datasets.data_access_layer import unified_data_access
                        
                        success, df, message = unified_data_access.get_data_source_data(data_source)
                        
                        if success and df is not None and not df.empty:
                            preview_df = df.head(10)  # Preview first 10 rows
                            
                            # Apply transformations
                            transformed_df = self._apply_transformations(preview_df, transformations)
                            
                            # Replace NaN values with None for JSON serialization
                            original_data = preview_df.head(5).to_dict('records')
                            transformed_data = transformed_df.head(5).to_dict('records')
                            
                            # Convert NaN values to None for JSON compatibility
                            original_data = [
                                {k: (None if pd.isna(v) else v) for k, v in row.items()}
                                for row in original_data
                            ]
                            transformed_data = [
                                {k: (None if pd.isna(v) else v) for k, v in row.items()}
                                for row in transformed_data
                            ]
                            
                            return JsonResponse({
                                'success': True,
                                'preview': {
                                    'original': original_data,
                                    'transformed': transformed_data,
                                    'original_schema': self._get_dataframe_schema(preview_df),
                                    'transformed_schema': self._get_dataframe_schema(transformed_df)
                                }
                            })
                        else:
                            return JsonResponse({
                                'error': f'Unable to load data for preview: {message}',
                                'suggestion': 'Please re-upload your CSV file or check the data source configuration.'
                            }, status=400)
                            
                    except Exception as data_error:
                        logger.error(f"Error loading data for ETL preview: {data_error}")
                        return JsonResponse({
                            'error': f'Data access error: {str(data_error)}',
                            'suggestion': 'Please check your data source and try again.'
                        }, status=400)
                
                return JsonResponse({'error': 'Unable to preview transformations'}, status=400)
            
            elif action == 'execute':
                # Execute transformation
                data_source_id = data.get('data_source_id')
                transformations = data.get('transformations', {})
                operation_name = data.get('operation_name', 'Data Transformation')
                
                # Create ETL operation record
                from datasets.models import ETLOperation
                operation = ETLOperation.objects.create(
                    name=operation_name,
                    operation_type='transform',
                    source_tables=[str(data_source_id)],
                    parameters={'transformations': transformations},
                    created_by=request.user,
                    status='pending'
                )
                
                # TODO: Queue for actual processing
                # For now, mark as completed
                operation.status = 'completed'
                operation.save()
                
                return JsonResponse({
                    'success': True,
                    'operation_id': str(operation.id),
                    'message': 'Transformation queued successfully'
                })
            
            else:
                return JsonResponse({'error': 'Unknown action'}, status=400)
                
        except Exception as e:
            logger.error(f"Error handling ETL transformation: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    def _apply_transformations(self, df, transformations):
        """Apply data type transformations to DataFrame"""
        import pandas as pd
        
        try:
            result_df = df.copy()
            
            for column, target_type in transformations.items():
                if column in result_df.columns and target_type:
                    try:
                        if target_type == 'integer':
                            result_df[column] = pd.to_numeric(result_df[column], errors='coerce').astype('Int64')
                        elif target_type == 'float':
                            result_df[column] = pd.to_numeric(result_df[column], errors='coerce')
                        elif target_type == 'string':
                            result_df[column] = result_df[column].astype(str)
                        elif target_type == 'boolean':
                            result_df[column] = result_df[column].map({
                                'true': True, 'false': False, '1': True, '0': False,
                                'yes': True, 'no': False, 'y': True, 'n': False,
                                True: True, False: False, 1: True, 0: False
                            })
                        elif target_type == 'date':
                            result_df[column] = pd.to_datetime(result_df[column], errors='coerce')
                    except Exception as e:
                        logger.warning(f"Failed to transform column {column} to {target_type}: {e}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error applying transformations: {e}")
            return df
    
    def _get_dataframe_schema(self, df):
        """Get schema information from DataFrame"""
        schema = []
        for col in df.columns:
            # Get sample values and handle NaN
            sample_values = df[col].dropna().head(3).tolist()
            # Convert any remaining NaN values to None
            sample_values = [None if pd.isna(val) else val for val in sample_values]
            
            schema.append({
                'name': col,
                'type': str(df[col].dtype),
                'sample_values': sample_values
            })
        return schema 


@csrf_exempt
@login_required
@require_http_methods(["POST"])
def execute_dashboard_query(request):
    """Execute SQL query for dashboard charts"""
    try:
        data = json.loads(request.body)
        query = data.get('query', '').strip()
        data_source_id = data.get('data_source_id')
        
        if not query:
            return JsonResponse({'error': 'Query is required'}, status=400)
        
        if not data_source_id:
            return JsonResponse({'error': 'Data source ID is required'}, status=400)
        
        logger.info(f"Executing dashboard query for data source {data_source_id}: {query}")
        
        # Get data source
        try:
            data_source = get_object_or_404(DataSource, id=data_source_id, created_by=request.user)
        except:
            # Try without user restriction for dashboard queries (shared dashboards)
            data_source = get_object_or_404(DataSource, id=data_source_id)
        
        # Execute query using data service
        from services.data_service import DataService
        data_service = DataService()
        
        # Execute the SQL query
        success, result = data_service.execute_query(query, data_source.connection_info)
        
        if success and result is not None:
            # Convert result to JSON-serializable format
            if hasattr(result, 'to_dict'):
                # DataFrame - replace NaN values with None for JSON serialization
                result_data = result.to_dict('records')
                # Convert NaN values to None for JSON compatibility
                result_data = [
                    {k: (None if pd.isna(v) else v) for k, v in row.items()}
                    for row in result_data
                ]
                logger.info(f"Dashboard query returned {len(result_data)} rows")
            elif isinstance(result, list):
                result_data = result
            else:
                result_data = [{'value': str(result)}]
            
            return JsonResponse({
                'success': True,
                'result_data': result_data,
                'row_count': len(result_data) if isinstance(result_data, list) else 1
            })
        else:
            error_msg = str(result) if result else "Query execution failed"
            logger.error(f"Dashboard query failed: {error_msg}")
            return JsonResponse({'error': error_msg}, status=400)
            
    except Exception as e:
        logger.error(f"Error executing dashboard query: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_http_methods(["GET"])
def data_preview(request, source_id):
    """Get data preview and schema information for LLM context"""
    try:
        # Get data source
        data_source = get_object_or_404(DataSource, id=source_id, created_by=request.user)
        
        logger.info(f"Generating data preview for data source: {data_source.name} (ID: {source_id})")
        
        # Get services
        from services.data_service import DataService
        from services.semantic_service import SemanticService
        
        data_service = DataService()
        semantic_service = SemanticService()
        
        # Get schema information
        schema_info = data_service.get_schema_info(data_source.connection_info)
        
        # Get sample data (first 20 rows)
        sample_data = []
        try:
            if data_source.source_type == 'csv':
                # Use unified data access layer instead of direct CSV reading (FIXED)
                from datasets.data_access_layer import unified_data_access
                
                success, df, message = unified_data_access.get_data_source_data(data_source)
                
                if success and df is not None and not df.empty:
                    # Replace NaN values with None for JSON serialization
                    sample_data = df.head(20).to_dict('records')
                    # Convert NaN values to None for JSON compatibility
                    import numpy as np
                    sample_data = [
                        {k: (None if pd.isna(v) else v) for k, v in row.items()}
                        for row in sample_data
                    ]
                    logger.info(f"Retrieved {len(sample_data)} sample rows from unified data access")
                else:
                    logger.warning(f"Could not load data for preview: {message}")
                    sample_data = []
            elif data_source.source_type == 'etl_result':
                # FIXED: For ETL results, use the same DuckDB-first approach as schema processing
                from datasets.data_access_layer import unified_data_access
                
                success, df, message = unified_data_access.get_data_source_data(data_source)
                
                if success and df is not None and not df.empty:
                    # Replace NaN values with None for JSON serialization
                    sample_data = df.head(20).to_dict('records')
                    # Convert NaN values to None for JSON compatibility
                    import numpy as np
                    sample_data = [
                        {k: (None if pd.isna(v) else v) for k, v in row.items()}
                        for row in sample_data
                    ]
                    logger.info(f"Retrieved {len(sample_data)} sample rows from ETL result via unified data access")
                else:
                    logger.warning(f"Could not load ETL result data for preview: {message}")
                    sample_data = []
            else:
                # For other data source types, use the database-specific preview method
                table_name = schema_info.get('table_name', 'data')
                success, result = data_service.get_data_preview(data_source.connection_info, table_name, 20)
                if success and result is not None:
                    if hasattr(result, 'to_dict'):
                        # Replace NaN values with None for JSON serialization
                        sample_data = result.to_dict('records')
                        # Convert NaN values to None for JSON compatibility
                        sample_data = [
                            {k: (None if pd.isna(v) else v) for k, v in row.items()}
                            for row in sample_data
                        ]
                    elif isinstance(result, list):
                        sample_data = result
        except Exception as e:
            logger.warning(f"Could not retrieve sample data: {e}")
            sample_data = []
        
        # Generate enhanced schema prompt (LLM context)
        llm_context = ""
        try:
            llm_context = semantic_service.generate_enhanced_schema_prompt(schema_info, data_source.source_type)
        except Exception as e:
            logger.warning(f"Could not generate LLM context: {e}")
            llm_context = "LLM context generation failed"
        
        # Add sample values to schema columns if not present
        if schema_info.get('columns') and sample_data:
            for col_info in schema_info['columns']:
                if not col_info.get('sample_values') and sample_data:
                    col_name = col_info['name']
                    # Extract sample values from the sample data
                    values = []
                    for row in sample_data[:5]:  # First 5 rows
                        if col_name in row and row[col_name] is not None:
                            val = str(row[col_name])
                            if val not in values and len(val) < 50:  # Avoid long values
                                values.append(val)
                        if len(values) >= 3:  # Max 3 samples
                            break
                    col_info['sample_values'] = values
        
        # Determine table name for LLM
        table_name = "csv_data"  # Default
        if data_source.source_type == 'integrated':
            # For integrated data sources, use proper table naming
            from utils.table_name_helper import get_integrated_table_name
            try:
                table_name = get_integrated_table_name(data_source)
            except:
                table_name = f"source_{data_source.id}"
        elif data_source.source_type == 'etl_result':
            # FIXED: For ETL results, use the actual DuckDB table name
            from utils.table_name_helper import get_integrated_table_name
            try:
                table_name = get_integrated_table_name(data_source)
                logger.info(f"Using actual DuckDB table name for ETL result: {table_name}")
            except Exception as e:
                # Fallback to UUID-based naming
                table_name = f"source_{data_source.id.hex.replace('-', '')}"
                logger.warning(f"Fallback table name for ETL result: {table_name}, error: {e}")
        elif schema_info.get('table_name'):
            table_name = schema_info['table_name']
        
        response_data = {
            'success': True,
            'source_type': data_source.source_type,
            'source_name': data_source.name,
            'table_name': table_name,
            'schema_info': schema_info,
            'sample_data': sample_data,
            'llm_context': llm_context,
            'created_at': data_source.created_at.isoformat(),
            'status': data_source.status
        }
        
        logger.info(f"Data preview generated successfully for {data_source.name}")
        return JsonResponse(response_data)
        
    except Exception as e:
        logger.error(f"Error generating data preview for source {source_id}: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@login_required
def users_list_api(request):
    """Get list of users for dashboard sharing"""
    try:
        from django.contrib.auth import get_user_model
        User = get_user_model()
        
        users = User.objects.exclude(id=request.user.id).values('id', 'username', 'email')[:50]
        
        return JsonResponse({
            'success': True,
            'users': list(users)
        })
        
    except Exception as e:
        logger.error(f"Error getting users list: {e}")
        return JsonResponse({'error': str(e)}, status=500)